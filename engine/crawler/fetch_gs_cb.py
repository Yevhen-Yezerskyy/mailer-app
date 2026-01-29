# FILE: engine/crawler/fetch_gs_cb.py  (обновлено — 2026-01-29)
# PATH: engine/crawler/fetch_gs_cb.py
# PURPOSE:
# - Очередь строим через queue_builder.get_crawler(task_id) (cb_id, rate, collected) + round-robin по task.
# - Перед использованием перепроверяем cb_crawler.collected пачкой; обновлённые флаги сохраняем через queue_builder.put_crawler.
# - plz/branch_slug НЕ кешируем (cb_id ~ миллионы): при ребилде очереди делаем ОДИН SELECT по всем cb_id из out и вклеиваем мету в очередь.
# - В кеше очереди: cbq:list (pickle(list[tuple(cb_id, plz, branch_slug, task_id)])) и cbq:cb2task:<cb_id> (pickle(int task_id)).

from __future__ import annotations

import os
import pickle
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from scrapy.crawler import CrawlerProcess

from engine.common.cache.client import CLIENT
from engine.common.db import fetch_all, fetch_one
from engine.core_validate import queue_builder
from engine.crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider

# -------------------------
# Settings
# -------------------------
QUEUE_BUILD_LIMIT = 500
PER_TASK_PICK_LIMIT = 500

RATE_CONTACTS_PRIORITY_OFFSET = 50

CBQ_LIST_KEY = "cbq:list"
CB2TASK_PREFIX = "cbq:cb2task:"

QUEUE_LOCK_KEY = "cbq:lock"
LOCK_TTL_SEC = 60.0

QUEUE_TTL_SEC = 60 * 60  # 1 час (для очереди и cb2task)
LOCK_RETRY_SLEEP_SEC = 0.10


# -------------------------
# Models
# -------------------------
@dataclass(frozen=True)
class QueueItem:
    cb_crawler_id: int
    plz: str
    branch_slug: str
    task_id: int


# -------------------------
# Cache helpers (queue)
# -------------------------
def _cache_get_queue() -> List[QueueItem]:
    payload = CLIENT.get(CBQ_LIST_KEY, ttl_sec=QUEUE_TTL_SEC)
    if not payload:
        return []
    try:
        raw = pickle.loads(payload)
    except Exception:
        return []

    if not isinstance(raw, list):
        return []

    out: List[QueueItem] = []
    for it in raw:
        if not (isinstance(it, (tuple, list)) and len(it) == 4):
            continue
        cb_id, plz, branch_slug, task_id = it
        if not isinstance(cb_id, int) or not isinstance(task_id, int):
            continue
        out.append(QueueItem(int(cb_id), str(plz), str(branch_slug), int(task_id)))
    return out


def _cache_set_queue(items: Sequence[QueueItem]) -> None:
    raw = [(it.cb_crawler_id, it.plz, it.branch_slug, it.task_id) for it in items]
    CLIENT.set(CBQ_LIST_KEY, pickle.dumps(raw, protocol=pickle.HIGHEST_PROTOCOL), ttl_sec=QUEUE_TTL_SEC)


def _cache_set_cb2task(cb_crawler_id: int, task_id: int) -> None:
    CLIENT.set(
        f"{CB2TASK_PREFIX}{int(cb_crawler_id)}",
        pickle.dumps(int(task_id), protocol=pickle.HIGHEST_PROTOCOL),
        ttl_sec=QUEUE_TTL_SEC,
    )


# -------------------------
# Lock helpers
# -------------------------
def _lock_acquire(owner: str) -> str:
    while True:
        resp = CLIENT.lock_try(QUEUE_LOCK_KEY, ttl_sec=LOCK_TTL_SEC, owner=owner)
        if resp and resp.get("acquired") is True and isinstance(resp.get("token"), str):
            return resp["token"]
        time.sleep(LOCK_RETRY_SLEEP_SEC)


def _lock_release(token: str) -> None:
    CLIENT.lock_release(QUEUE_LOCK_KEY, token=token)


# -------------------------
# Spider runner
# -------------------------
def _run_spider(cb_crawler_id: int, plz: str, branch_slug: str) -> None:
    process = CrawlerProcess(
        settings={
            "LOG_LEVEL": "ERROR",
            "TELNETCONSOLE_ENABLED": False,
            "DOWNLOAD_DELAY": 2.0,
            "RANDOMIZE_DOWNLOAD_DELAY": True,
            "CONCURRENT_REQUESTS": 1,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
            "AUTOTHROTTLE_ENABLED": True,
            "AUTOTHROTTLE_START_DELAY": 2.0,
            "AUTOTHROTTLE_MAX_DELAY": 30.0,
            "AUTOTHROTTLE_TARGET_CONCURRENCY": 0.5,
            "COOKIES_ENABLED": True,
        }
    )
    process.crawl(
        GelbeSeitenCBSpider,
        plz=plz,
        branch_slug=branch_slug,
        cb_crawler_id=cb_crawler_id,
    )
    process.start()


# -------------------------
# Public: reset
# -------------------------
def cbq_reset_cache() -> None:
    _cache_set_queue([])
    print("[cbq] RESET: cbq:list cleared")


# -------------------------
# Target tasks
# -------------------------
def _get_active_task_ids() -> List[int]:
    rows = fetch_all(
        """
        SELECT id
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND collected = false
        ORDER BY id ASC
        """
    )
    return [int(r[0]) for r in rows] if rows else []


def _task_is_underdone(task_id: int) -> bool:
    row = fetch_one(
        """
        SELECT 1
        FROM rate_contacts
        WHERE task_id = %s
        OFFSET %s
        LIMIT 1
        """,
        (int(task_id), RATE_CONTACTS_PRIORITY_OFFSET),
    )
    return not bool(row)


def _get_target_task_ids(active_task_ids: List[int]) -> Tuple[str, List[int]]:
    underdone: List[int] = []
    for tid in active_task_ids:
        if _task_is_underdone(int(tid)):
            underdone.append(int(tid))
    if underdone:
        return f"A_UNDERDONE_LT_{RATE_CONTACTS_PRIORITY_OFFSET+1}", underdone
    return f"B_ALL_GE_{RATE_CONTACTS_PRIORITY_OFFSET+1}", active_task_ids


# -------------------------
# Collected recheck + persist crawler
# -------------------------
def _cb_collected_map(cb_ids: List[int]) -> Dict[int, bool]:
    if not cb_ids:
        return {}
    rows = fetch_all(
        """
        SELECT id, collected
        FROM cb_crawler
        WHERE id = ANY(%s::bigint[])
        """,
        (list(map(int, cb_ids)),),
    )
    out: Dict[int, bool] = {}
    for cb_id, collected in rows:
        out[int(cb_id)] = bool(collected)
    return out


def _refresh_crawler_and_pick(*, task_id: int, limit: int) -> List[int]:
    crawler = list(queue_builder.get_crawler(int(task_id)))
    if not crawler:
        return []

    cb_ids = [int(cb_id) for (cb_id, _rate, _col) in crawler]
    cmap = _cb_collected_map(cb_ids)

    updated: List[Tuple[int, int, bool]] = []
    picked: List[int] = []

    for cb_id, rate, old_col in crawler:
        now_col = bool(cmap.get(int(cb_id), bool(old_col)))
        updated.append((int(cb_id), int(rate), bool(now_col)))
        if (not now_col) and (len(picked) < int(limit)):
            picked.append(int(cb_id))

    queue_builder.put_crawler(int(task_id), updated)
    return picked


# -------------------------
# RR build (no shuffle)
# -------------------------
def _round_robin_one_by_one(picked: Dict[int, List[int]], limit: int) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []  # (cb_id, task_id)
    if limit <= 0 or not picked:
        return out

    task_ids = list(picked.keys())

    while len(out) < limit:
        progressed = False
        for tid in task_ids:
            lst = picked.get(tid)
            if not lst:
                continue
            cb_id = lst.pop(0)
            out.append((int(cb_id), int(tid)))
            progressed = True
            if len(out) >= limit:
                break
        if not progressed:
            break

    return out


def _load_cbmeta_map(cb_ids: List[int]) -> Dict[int, Tuple[str, str]]:
    if not cb_ids:
        return {}
    rows = fetch_all(
        """
        SELECT id, plz, branch_slug
        FROM cb_crawler
        WHERE id = ANY(%s::bigint[])
        """,
        (list(map(int, cb_ids)),),
    )
    out: Dict[int, Tuple[str, str]] = {}
    for cb_id, plz, branch_slug in rows:
        out[int(cb_id)] = (str(plz), str(branch_slug))
    return out


def _rebuild_queue() -> List[QueueItem]:
    active = _get_active_task_ids()
    print(f"[cbq] rebuild: active_tasks={len(active)}")

    if not active:
        print("[cbq] rebuild: no active tasks -> out=0")
        return []

    mode, targets = _get_target_task_ids(active)
    print(f"[cbq] rebuild: mode={mode} target_tasks={len(targets)} build_limit={QUEUE_BUILD_LIMIT}")

    picked: Dict[int, List[int]] = {}
    touched = 0

    for tid in targets:
        touched += 1
        cb_ids = _refresh_crawler_and_pick(task_id=int(tid), limit=int(PER_TASK_PICK_LIMIT))
        if cb_ids:
            picked[int(tid)] = cb_ids

    print(f"[cbq] rebuild: tasks_touched={touched} tasks_with_items={len(picked)}")

    rr = _round_robin_one_by_one(picked, limit=QUEUE_BUILD_LIMIT)
    cb_ids_all = [cb_id for (cb_id, _tid) in rr]
    meta_map = _load_cbmeta_map(cb_ids_all)

    out: List[QueueItem] = []
    missing_meta = 0
    for cb_id, tid in rr:
        meta = meta_map.get(int(cb_id))
        if not meta:
            missing_meta += 1
            continue
        plz, branch_slug = meta
        out.append(QueueItem(int(cb_id), str(plz), str(branch_slug), int(tid)))

    print(f"[cbq] rebuild done: rr={len(rr)} out={len(out)} meta_miss={missing_meta}")
    return out


# -------------------------
# Public: worker
# -------------------------
def worker_run_once() -> None:
    owner = f"crawl_cb:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    token = _lock_acquire(owner=owner)

    item: Optional[QueueItem] = None
    try:
        q = _cache_get_queue()
        if not q:
            q = _rebuild_queue()
            _cache_set_queue(q)

        if q:
            item = q[0]
            _cache_set_queue(q[1:])
            _cache_set_cb2task(int(item.cb_crawler_id), int(item.task_id))
    finally:
        _lock_release(token)

    if not item:
        print("[cbq] queue empty; nothing to do")
        return

    print(
        f"[cbq] pop cb_crawler_id={item.cb_crawler_id} task_id={item.task_id} "
        f"plz='{item.plz}' branch='{item.branch_slug}'"
    )
    _run_spider(cb_crawler_id=int(item.cb_crawler_id), plz=str(item.plz), branch_slug=str(item.branch_slug))
