# FILE: engine/crawler/fetch_gs_cb.py  (новое — 2026-01-03)
# PATH: engine/crawler/fetch_gs_cb.py
# Смысл:
# - Очередь строим просто: для каждого task_id ищем ПЕРВУЮ score-группу с uncollected и берём из неё cb_id LIMIT=QUEUE_BUILD_LIMIT.
# - Потом делаем round-robin по 1 элементу, без shuffle, чтобы все task-и попали.
# - В кеше только: cbq:list (pickle(list[tuple(cb_id, plz, branch_slug, task_id)])) и cbq:cb2task:<cb_id> (pickle(int task_id)).
# - worker_run_once: под lock rebuild при пустой очереди, pop 1 элемент, запускает паука без lock.
# - cbq_reset_cache: раз в 10 минут очищает cbq:list.

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
from engine.crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider
from engine.core_validate.val_expand_processor import _build_score_groups

# -------------------------
# Settings
# -------------------------
QUEUE_BUILD_LIMIT = 500
RATE_CONTACTS_PRIORITY_OFFSET = 50

PAIRS_SQL_CHUNK = 2000

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
# Cache helpers
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
# Group fetch
# -------------------------
def _pairs_chunks(pairs: List[Tuple[int, int]]) -> List[List[Tuple[int, int]]]:
    return [pairs[i : i + PAIRS_SQL_CHUNK] for i in range(0, len(pairs), PAIRS_SQL_CHUNK)]


def _fetch_group_uncollected_limited(*, task_id: int, pairs: List[Tuple[int, int]], limit: int) -> List[QueueItem]:
    if limit <= 0 or not pairs:
        return []

    out: List[QueueItem] = []
    left = int(limit)

    for chunk in _pairs_chunks(pairs):
        if left <= 0:
            break

        values_sql = ", ".join(["(%s,%s)"] * len(chunk))
        params: List[int] = []
        for city_id, branch_id in chunk:
            params.extend((int(city_id), int(branch_id)))

        rows = fetch_all(
            f"""
            WITH pairs(city_id, branch_id) AS (VALUES {values_sql})
            SELECT c.id, c.plz, c.branch_slug
            FROM cb_crawler c
            JOIN pairs p
              ON p.city_id = c.city_id
             AND p.branch_id = c.branch_id
            WHERE c.collected = false
            ORDER BY c.id ASC
            LIMIT %s
            """,
            tuple(params + [left]),
        )

        for cb_id, plz, branch_slug in rows:
            cb_id_i = int(cb_id)
            it = QueueItem(cb_id_i, str(plz), str(branch_slug), int(task_id))
            _cache_set_cb2task(cb_id_i, int(task_id))
            out.append(it)
            left -= 1
            if left <= 0:
                break

    return out


def _pick_for_task_first_uncollected_group(*, task_id: int) -> List[QueueItem]:
    groups = _build_score_groups(int(task_id))
    if not groups:
        print(f"[cbq] task={task_id} groups=0 -> skip")
        return []

    for group_i, (score, pairs) in enumerate(groups):
        items = _fetch_group_uncollected_limited(task_id=int(task_id), pairs=pairs, limit=QUEUE_BUILD_LIMIT)
        print(f"[cbq] task={task_id} group_i={group_i} score={score} pairs={len(pairs)} picked={len(items)}")
        if items:
            return items

    print(f"[cbq] task={task_id} no_uncollected_in_any_group")
    return []


# -------------------------
# RR build (no shuffle)
# -------------------------
def _round_robin_one_by_one(picked: Dict[int, List[QueueItem]], limit: int) -> List[QueueItem]:
    out: List[QueueItem] = []
    if limit <= 0 or not picked:
        return out

    task_ids = list(picked.keys())

    while len(out) < limit:
        progressed = False
        for tid in task_ids:
            lst = picked.get(tid)
            if not lst:
                continue
            out.append(lst.pop(0))
            progressed = True
            if len(out) >= limit:
                break
        if not progressed:
            break

    return out


def _rebuild_queue() -> List[QueueItem]:
    active = _get_active_task_ids()
    print(f"[cbq] rebuild: active_tasks={len(active)}")

    if not active:
        print("[cbq] rebuild: no active tasks -> out=0")
        return []

    mode, targets = _get_target_task_ids(active)
    print(f"[cbq] rebuild: mode={mode} target_tasks={len(targets)} build_limit={QUEUE_BUILD_LIMIT}")

    picked: Dict[int, List[QueueItem]] = {}
    touched = 0

    for tid in targets:
        touched += 1
        items = _pick_for_task_first_uncollected_group(task_id=int(tid))
        if items:
            picked[int(tid)] = items

    print(f"[cbq] rebuild: tasks_touched={touched} tasks_with_items={len(picked)}")

    out = _round_robin_one_by_one(picked, limit=QUEUE_BUILD_LIMIT)
    print(f"[cbq] rebuild done: out={len(out)}")
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
    finally:
        _lock_release(token)

    if not item:
        print("[cbq] queue empty; nothing to do")
        return

    print(
        f"[cbq] pop cb_crawler_id={item.cb_crawler_id} task_id={item.task_id} "
        f"plz='{item.plz}' branch='{item.branch_slug}'"
    )
    _run_spider(cb_crawler_id=item.cb_crawler_id, plz=item.plz, branch_slug=item.branch_slug)
