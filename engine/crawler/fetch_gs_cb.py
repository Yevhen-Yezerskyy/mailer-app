# FILE: engine/crawler/fetch_gs_cb.py  (обновлено — 2025-12-29)
# PATH: engine/crawler/fetch_gs_cb.py
# Смысл:
# - Очередь cbq:list перестроена под ту же рейтинговую логику, что и val_expand_processor:
#   * окно (TOP_LIMIT) и score-группы берём импортом из engine/core_validate/val_expand_processor.py
#   * из каждой задачи используем ПЕРВУЮ (лучшую) score-группу; если из неё ничего не набрали — задача “не вышла”
# - Random остаётся, но снижен до 10% и распределён по очереди (не хвостом).
# - Фильтрация collected делается ТОЛЬКО на этапе построения очереди.
# - Внешний обработчик: раз в 2 часа выставляет aap_audience_audiencetask.collected=true, если по задаче больше нечего собирать.
# - Сброс очереди: cbq_reset_cache (как и раньше).

from __future__ import annotations

import os
import pickle
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Sequence, Tuple

from scrapy.crawler import CrawlerProcess

from engine.common.cache.client import CLIENT
from engine.common.db import fetch_all, fetch_one
from engine.crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider

# источник истины для окна и групп
from engine.core_validate.val_expand_processor import TOP_LIMIT, _build_score_groups

# -------------------------
# cache queue config
# -------------------------
QUEUE_LOCK_KEY = "cbq:lock"
QUEUE_DATA_KEY = "cbq:list"

LOCK_TTL_SEC = 60.0
QUEUE_TTL_SEC = 60 * 60  # 1 hour (best-effort; reset'ом чистим чаще)

REBUILD_SIZE = 500
RANDOM_SHARE = 0.10  # 10%
RANDOM_TARGET = int(REBUILD_SIZE * RANDOM_SHARE)

BATCH_PAIRS = 200  # чтобы не раздувать VALUES

LOCK_RETRY_SLEEP_SEC = 0.10
LOCK_RENEW_EVERY_SEC = 7.0


def cbq_reset_cache() -> None:
    print("[cbq] RESET: cbq:list = []")
    payload = pickle.dumps([], protocol=pickle.HIGHEST_PROTOCOL)
    CLIENT.set(QUEUE_DATA_KEY, payload, ttl_sec=QUEUE_TTL_SEC)


@dataclass(frozen=True)
class QueueItem:
    cb_crawler_id: int
    plz: str
    branch_slug: str


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _cache_get_queue() -> List[QueueItem]:
    payload = CLIENT.get(QUEUE_DATA_KEY, ttl_sec=QUEUE_TTL_SEC)
    if not payload:
        return []
    try:
        obj = pickle.loads(payload)
        if not isinstance(obj, list):
            return []
        out: List[QueueItem] = []
        for it in obj:
            if (
                isinstance(it, (tuple, list))
                and len(it) == 3
                and isinstance(it[0], int)
                and isinstance(it[1], str)
                and isinstance(it[2], str)
            ):
                out.append(QueueItem(int(it[0]), str(it[1]), str(it[2])))
        return out
    except Exception:
        return []


def _cache_set_queue(items: Sequence[QueueItem]) -> None:
    raw = [(it.cb_crawler_id, it.plz, it.branch_slug) for it in items]
    payload = pickle.dumps(raw, protocol=pickle.HIGHEST_PROTOCOL)
    CLIENT.set(QUEUE_DATA_KEY, payload, ttl_sec=QUEUE_TTL_SEC)


def _lock_acquire(owner: str) -> Tuple[str, float]:
    while True:
        resp = CLIENT.lock_try(QUEUE_LOCK_KEY, ttl_sec=LOCK_TTL_SEC, owner=owner)
        if resp and resp.get("acquired") is True and isinstance(resp.get("token"), str):
            return resp["token"], time.monotonic()
        time.sleep(LOCK_RETRY_SLEEP_SEC)


def _lock_renew_if_needed(token: str, last_renew_monotonic: float) -> float:
    now_m = time.monotonic()
    if (now_m - last_renew_monotonic) < LOCK_RENEW_EVERY_SEC:
        return last_renew_monotonic
    ok = CLIENT.lock_renew(QUEUE_LOCK_KEY, ttl_sec=LOCK_TTL_SEC, token=token)
    return now_m if ok else last_renew_monotonic


def _lock_release(token: str) -> None:
    CLIENT.lock_release(QUEUE_LOCK_KEY, token=token)


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
            "ITEM_PIPELINES": {
                "engine.crawler.pipelines.pipeline_gs_cb.GSCBPipeline": 300,
            },
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
# worker-режим: очередь в кеше + lock
# -------------------------
def worker_run_once() -> None:
    owner = f"cb_processor:{os.getpid()}:{uuid.uuid4().hex[:10]}"
    token, last_renew = _lock_acquire(owner=owner)
    item: Optional[QueueItem] = None
    try:
        q = _cache_get_queue()

        if not q:
            q, last_renew = _rebuild_queue_500(token=token, last_renew=last_renew)
            _cache_set_queue(q)

        if q:
            item = q[0]
            rest = q[1:]
            _cache_set_queue(rest)

    finally:
        _lock_release(token=token)

    if not item:
        print("[cbq] queue empty; nothing to do")
        return

    print(f"[cbq] pop cb_crawler_id={item.cb_crawler_id} plz={item.plz} branch={item.branch_slug}")
    _run_spider(cb_crawler_id=item.cb_crawler_id, plz=item.plz, branch_slug=item.branch_slug)


def _rebuild_queue_500(token: str, last_renew: float) -> Tuple[List[QueueItem], float]:
    """
    Build new queue of size REBUILD_SIZE:
    - приоритетные: задачи run_processing=true AND collected=false, по updated_at desc
      * из каждой задачи используем ПЕРВУЮ score-группу (лучшую)
      * если из неё 0 — задача не дала элементов
    - random 10% распределяем по очереди, не хвостом
    - если приоритетных не хватило — остаток добиваем random
    """
    last_renew = _lock_renew_if_needed(token, last_renew)

    tasks = fetch_all(
        """
        SELECT id, updated_at
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND collected = false
        ORDER BY updated_at DESC, id DESC
        """
    )

    print(f"[cbq] rebuild size={REBUILD_SIZE} random=~{RANDOM_TARGET} window=TOP_LIMIT={TOP_LIMIT}")
    print(f"[cbq] tasks candidates={len(tasks)}")

    priority: List[QueueItem] = []
    random_items: List[QueueItem] = []

    # 1) набираем приоритетные (пока не соберём REBUILD_SIZE - RANDOM_TARGET)
    need_priority = max(0, REBUILD_SIZE - RANDOM_TARGET)

    for task_id, _updated_at in tasks:
        if len(priority) >= need_priority:
            break

        last_renew = _lock_renew_if_needed(token, last_renew)
        picked = _pick_from_task_best_group(task_id=int(task_id), need=need_priority - len(priority))
        if picked:
            print(f"[cbq] task_id={int(task_id)} +{len(picked)} (best score-group)")
            priority.extend(picked)

    # 2) random 10% (или сколько получится), но если приоритетных меньше — random добивает остаток тоже
    need_total = REBUILD_SIZE
    need_random = max(RANDOM_TARGET, need_total - len(priority))
    if need_random > 0:
        last_renew = _lock_renew_if_needed(token, last_renew)
        rnd = _pick_random_cb(need=need_random)
        random_items.extend(rnd)

    # 3) смешиваем random по очереди (в случайные позиции)
    out = _merge_random(priority, random_items, target_size=REBUILD_SIZE)

    print(f"[cbq] rebuild done: priority={len(priority)} random={len(random_items)} out={len(out)}")
    return out, last_renew


def _pick_from_task_best_group(*, task_id: int, need: int) -> List[QueueItem]:
    if need <= 0:
        return []

    groups = _build_score_groups(task_id)
    if not groups:
        return []

    score, pairs = groups[0]  # ТОЛЬКО лучшая score-группа
    if not pairs:
        return []

    out: List[QueueItem] = []
    i = 0

    while i < len(pairs) and len(out) < need:
        batch = pairs[i : i + BATCH_PAIRS]
        i += BATCH_PAIRS

        values_sql = ", ".join(["(%s,%s)"] * len(batch))
        params: List[int] = []
        for city_id, branch_id in batch:
            params.append(int(city_id))
            params.append(int(branch_id))

        rows = fetch_all(
            f"""
            WITH pairs(city_id, branch_id) AS (VALUES {values_sql})
            SELECT c.id, c.plz, c.branch_slug
            FROM cb_crawler c
            JOIN pairs p ON p.city_id = c.city_id AND p.branch_id = c.branch_id
            WHERE c.collected = false
            ORDER BY c.id ASC
            LIMIT %s
            """,
            tuple(params) + (need - len(out),),
        )

        for cb_id, plz, branch_slug in rows:
            out.append(QueueItem(int(cb_id), str(plz), str(branch_slug)))
            if len(out) >= need:
                break

    if out:
        print(f"[cbq] task_id={task_id} best_score={score} picked={len(out)}")
    return out


def _pick_random_cb(*, need: int) -> List[QueueItem]:
    if need <= 0:
        return []

    row = fetch_one("SELECT max(id) FROM cb_crawler")
    max_id = int(row[0]) if row and row[0] is not None else 0
    if max_id <= 0:
        return []

    start = random.randint(1, max_id)

    rows = fetch_all(
        """
        SELECT id, plz, branch_slug
        FROM cb_crawler
        WHERE collected = false
          AND id >= %s
        ORDER BY id ASC
        LIMIT %s
        """,
        (start, need * 3),
    )

    out: List[QueueItem] = []
    for cb_id, plz, branch_slug in rows:
        out.append(QueueItem(int(cb_id), str(plz), str(branch_slug)))
        if len(out) >= need:
            return out

    if len(out) < need:
        rows2 = fetch_all(
            """
            SELECT id, plz, branch_slug
            FROM cb_crawler
            WHERE collected = false
              AND id < %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (start, (need - len(out)) * 3),
        )
        for cb_id, plz, branch_slug in rows2:
            out.append(QueueItem(int(cb_id), str(plz), str(branch_slug)))
            if len(out) >= need:
                break

    return out[:need]


def _merge_random(priority: List[QueueItem], rnd: List[QueueItem], target_size: int) -> List[QueueItem]:
    # хотим распределить rnd по очереди, а не хвостом
    pr = priority[:]
    rr = rnd[:]
    total = pr + rr
    if len(total) <= target_size:
        # если коротко — просто перемешиваем равномерно и возвращаем как есть
        random.shuffle(total)
        return total

    # берём нужное количество rnd (если rnd больше)
    need = max(0, target_size - len(pr))
    rr = rr[:need]

    base = pr[: max(0, target_size - len(rr))]
    if not rr:
        return base[:target_size]

    slots = target_size
    positions = sorted(random.sample(range(slots), k=len(rr)))

    out: List[QueueItem] = []
    bi = 0
    ri = 0
    for pos in range(slots):
        if ri < len(rr) and positions[ri] == pos:
            out.append(rr[ri])
            ri += 1
        else:
            if bi < len(base):
                out.append(base[bi])
                bi += 1
            else:
                # если вдруг base короче — добиваем остатком rr
                if ri < len(rr):
                    out.append(rr[ri])
                    ri += 1
                else:
                    break

    return out[:target_size]


# -------------------------
# внешний обработчик: выставление aap_audience_audiencetask.collected=true
# -------------------------
def cb_mark_tasks_collected() -> None:
    """
    Раз в 2 часа:
    - ищем задачи run_processing=true AND collected=false
    - если по задаче больше нет ни одного cb_crawler.collected=false (по лучшей score-группе окна) — ставим collected=true
    """
    tasks = fetch_all(
        """
        SELECT id
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND collected = false
        ORDER BY id ASC
        """
    )
    if not tasks:
        return

    print(f"[cbq] mark_collected: tasks={len(tasks)}")

    for (task_id_raw,) in tasks:
        task_id = int(task_id_raw)

        groups = _build_score_groups(task_id)
        if not groups:
            fetch_one(
                """
                UPDATE aap_audience_audiencetask
                SET collected = true
                WHERE id = %s
                RETURNING id
                """,
                (task_id,),
            )
            print(f"[cbq] task_id={task_id} -> collected=true (no groups)")
            continue

        _score, pairs = groups[0]
        if not pairs:
            fetch_one(
                """
                UPDATE aap_audience_audiencetask
                SET collected = true
                WHERE id = %s
                RETURNING id
                """,
                (task_id,),
            )
            print(f"[cbq] task_id={task_id} -> collected=true (empty best group)")
            continue

        has_any = False
        i = 0
        while i < len(pairs) and not has_any:
            batch = pairs[i : i + BATCH_PAIRS]
            i += BATCH_PAIRS

            values_sql = ", ".join(["(%s,%s)"] * len(batch))
            params: List[int] = []
            for city_id, branch_id in batch:
                params.append(int(city_id))
                params.append(int(branch_id))

            row = fetch_one(
                f"""
                WITH pairs(city_id, branch_id) AS (VALUES {values_sql})
                SELECT 1
                FROM cb_crawler c
                JOIN pairs p ON p.city_id = c.city_id AND p.branch_id = c.branch_id
                WHERE c.collected = false
                LIMIT 1
                """,
                tuple(params),
            )
            if row:
                has_any = True

        if not has_any:
            fetch_one(
                """
                UPDATE aap_audience_audiencetask
                SET collected = true
                WHERE id = %s
                RETURNING id
                """,
                (task_id,),
            )
            print(f"[cbq] task_id={task_id} -> collected=true (no uncollected in best group)")


if __name__ == "__main__":
    # debug: запуск одного uncollected
    row = fetch_one(
        """
        SELECT id, plz, branch_slug
        FROM cb_crawler
        WHERE collected = false
        ORDER BY id
        LIMIT 1
        """
    )
    if not row:
        print("DEBUG: no uncollected cb_crawler rows")
    else:
        cb_crawler_id, plz, branch_slug = row
        print(f"DEBUG: picked cb_crawler_id={cb_crawler_id} plz={plz} branch={branch_slug}")
        _run_spider(cb_crawler_id=int(cb_crawler_id), plz=str(plz), branch_slug=str(branch_slug))
