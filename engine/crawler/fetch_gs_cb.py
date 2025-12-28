# FILE: engine/crawler/fetch_gs_cb.py  (обновлено — 2025-12-28)
# PATH: engine/crawler/fetch_gs_cb.py
# Смысл:
# - Исправлен критический баг окна: больше нет bulk-select + LIMIT по cb_crawler (ломало 300×300 и “дырки”).
# - Окно = реальное декартово произведение top-N cities × top-N branches, обход по score.
# - Для каждой пары (city_id, branch_id) — точечный SELECT LIMIT 1 (как договаривались).
# - Доп. фиксы:
#   * owner lock’а стал реально уникальным (иначе коллизии при параллели в одну секунду).
#   * WINDOW_LIMIT обратно 300 (окно 300×300).
#   * убраны потенциальные типовые глюки с ANY(empty_array) — делаем 2 варианта SELECT.

from __future__ import annotations

import os
import pickle
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Set, Tuple

from scrapy.crawler import CrawlerProcess

from engine.common.cache.client import CLIENT
from engine.common.db import fetch_all, fetch_one
from engine.crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider

# -------------------------
# cache queue config
# -------------------------
QUEUE_LOCK_KEY = "cbq:lock"
QUEUE_DATA_KEY = "cbq:list"

LOCK_TTL_SEC = 60.0
QUEUE_TTL_SEC = 60 * 60  # 1 hour (best-effort; внешним процессом можно нулить хоть каждые 10 минут)

REBUILD_SIZE = 500
RANDOM_SHARE = 0.30
RANDOM_SIZE = int(REBUILD_SIZE * RANDOM_SHARE)
WINDOW_SIZE = REBUILD_SIZE - RANDOM_SIZE

# окно 300×300
WINDOW_LIMIT = 400

FRESH_HOURS = 2
FRESH_QUOTA = 10
STALE_QUOTA = 1

LOCK_RETRY_SLEEP_SEC = 0.10
LOCK_RENEW_EVERY_SEC = 7.0


def cbq_reset_cache() -> None:
    print("CACHE RESET!!!!!!!!!!!!!!!!!")
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
    """
    Возвращает (token, last_renew_monotonic).
    Ретраит до успеха.
    """
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
# дебаг-режим: как было
# -------------------------
def main():
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
        return

    cb_crawler_id, plz, branch_slug = row

    print(f"DEBUG: picked cb_crawler_id={cb_crawler_id} plz={plz} branch={branch_slug}")

    _run_spider(cb_crawler_id=cb_crawler_id, plz=plz, branch_slug=branch_slug)


# -------------------------
# worker-режим: очередь в кеше + lock
# -------------------------
def worker_run_once() -> None:
    """
    Один проход процессора:
    - под lock берём item из кеш-очереди (если пусто — rebuild на 500)
    - запускаем паука
    """
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
        print("DEBUG: queue empty after rebuild; nothing to do")
        return

    print(
        f"DEBUG: queue pop cb_crawler_id={item.cb_crawler_id} plz={item.plz} branch={item.branch_slug}"
    )
    _run_spider(cb_crawler_id=item.cb_crawler_id, plz=item.plz, branch_slug=item.branch_slug)


def _rebuild_queue_500(token: str, last_renew: float) -> Tuple[List[QueueItem], float]:
    """
    Build new queue of size REBUILD_SIZE (500):
    - WINDOW_SIZE (350) from task windows
    - RANDOM_SIZE (150) random from cb_crawler collected=false
    """
    out: List[QueueItem] = []
    used: Set[int] = set()

    # --- tasks (run_processing=true AND collected=false), freshest first ---
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

    fresh_cutoff = _now_utc() - timedelta(hours=FRESH_HOURS)

    for task_id, updated_at in tasks:
        if len(out) >= WINDOW_SIZE:
            break

        is_fresh = bool(updated_at and updated_at >= fresh_cutoff)
        quota = FRESH_QUOTA if is_fresh else STALE_QUOTA

        need = min(quota, WINDOW_SIZE - len(out))
        if need <= 0:
            break

        last_renew = _lock_renew_if_needed(token, last_renew)
        picked, last_renew = _pick_from_task_window(
            task_id=int(task_id),
            need=need,
            used=used,
            token=token,
            last_renew=last_renew,
        )
        for it in picked:
            used.add(it.cb_crawler_id)
            out.append(it)

    # --- random tail (30%) ---
    last_renew = _lock_renew_if_needed(token, last_renew)
    rnd, last_renew = _pick_random_cb(
        need=REBUILD_SIZE - len(out),
        used=used,
        token=token,
        last_renew=last_renew,
    )
    for it in rnd:
        used.add(it.cb_crawler_id)
        out.append(it)

    # if still short (например задач нет) — добиваем рандомом
    if len(out) < REBUILD_SIZE:
        last_renew = _lock_renew_if_needed(token, last_renew)
        more, last_renew = _pick_random_cb(
            need=REBUILD_SIZE - len(out),
            used=used,
            token=token,
            last_renew=last_renew,
        )
        for it in more:
            used.add(it.cb_crawler_id)
            out.append(it)

    return out[:REBUILD_SIZE], last_renew


def _build_pairs_by_score(
    city_rate: Dict[int, int],
    branch_rate: Dict[int, int],
) -> List[Tuple[int, int, int]]:
    """
    Пары (score, city_id, branch_id) отсортированы по score ASC, стабильно по id.
    WINDOW_LIMIT=300 => 90k пар, ок для 1 ребилда / 10 минут.
    """
    pairs: List[Tuple[int, int, int]] = []
    for city_id, cr in city_rate.items():
        for branch_id, br in branch_rate.items():
            pairs.append((int(cr) * int(br), int(city_id), int(branch_id)))
    pairs.sort(key=lambda x: (x[0], x[1], x[2]))
    return pairs


def _pick_from_task_window(
    *,
    task_id: int,
    need: int,
    used: Set[int],
    token: str,
    last_renew: float,
) -> Tuple[List[QueueItem], float]:
    """
    Окно = top-WINDOW_LIMIT cities × top-WINDOW_LIMIT branches.
    ВАЖНО: больше нет bulk-select + LIMIT по cb_crawler.
    Идём по парам в порядке score и точечно берём 1 cb_crawler на пару.
    """
    # cities
    last_renew = _lock_renew_if_needed(token, last_renew)
    cities = fetch_all(
        """
        SELECT value_id, rate
        FROM crawl_tasks
        WHERE task_id = %s AND type = 'city'
        ORDER BY rate ASC, value_id ASC
        LIMIT %s
        """,
        (task_id, WINDOW_LIMIT),
    )
    if not cities:
        return [], last_renew

    # branches
    last_renew = _lock_renew_if_needed(token, last_renew)
    branches = fetch_all(
        """
        SELECT value_id, rate
        FROM crawl_tasks
        WHERE task_id = %s AND type = 'branch'
        ORDER BY rate ASC, value_id ASC
        LIMIT %s
        """,
        (task_id, WINDOW_LIMIT),
    )
    if not branches:
        return [], last_renew

    city_rate: Dict[int, int] = {int(cid): int(rate) for cid, rate in cities}
    branch_rate: Dict[int, int] = {int(bid): int(rate) for bid, rate in branches}

    pairs = _build_pairs_by_score(city_rate, branch_rate)

    picked: List[QueueItem] = []
    used_list: List[int] = list(used)  # <= 500, ок

    renew_every = 250  # не дрочим renew на каждый селект
    i = 0

    for _, city_id, branch_id in pairs:
        if len(picked) >= need:
            break

        i += 1
        if (i % renew_every) == 0:
            last_renew = _lock_renew_if_needed(token, last_renew)

        if used_list:
            row = fetch_one(
                """
                SELECT id, plz, branch_slug
                FROM cb_crawler
                WHERE collected = false
                  AND city_id = %s
                  AND branch_id = %s
                  AND NOT (id = ANY(%s))
                ORDER BY id ASC
                LIMIT 1
                """,
                (city_id, branch_id, used_list),
            )
        else:
            row = fetch_one(
                """
                SELECT id, plz, branch_slug
                FROM cb_crawler
                WHERE collected = false
                  AND city_id = %s
                  AND branch_id = %s
                ORDER BY id ASC
                LIMIT 1
                """,
                (city_id, branch_id),
            )

        if not row:
            continue

        cb_id, plz, branch_slug = row
        cb_id_i = int(cb_id)
        if cb_id_i in used:
            # гонка/параллель/рандом — не страшно
            continue

        it = QueueItem(cb_id_i, str(plz), str(branch_slug))
        picked.append(it)
        used.add(cb_id_i)
        used_list.append(cb_id_i)

    # ВАЖНО: “таск выработан” только если ПРОШЛИ ВЕСЬ ОКНО-ПЕРЕБОР и реально ничего нет
    if not picked:
        last_renew = _lock_renew_if_needed(token, last_renew)
        fetch_one(
            """
            UPDATE aap_audience_audiencetask
            SET collected = true
            WHERE id = %s
            RETURNING id
            """,
            (task_id,),
        )

    return picked, last_renew


def _pick_random_cb(
    *,
    need: int,
    used: Set[int],
    token: str,
    last_renew: float,
) -> Tuple[List[QueueItem], float]:
    if need <= 0:
        return [], last_renew

    last_renew = _lock_renew_if_needed(token, last_renew)
    row = fetch_one("SELECT max(id) FROM cb_crawler")
    max_id = int(row[0]) if row and row[0] is not None else 0
    if max_id <= 0:
        return [], last_renew

    start = random.randint(1, max_id)

    last_renew = _lock_renew_if_needed(token, last_renew)
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
        cb_id_i = int(cb_id)
        if cb_id_i in used:
            continue
        out.append(QueueItem(cb_id_i, str(plz), str(branch_slug)))
        if len(out) >= need:
            return out, last_renew

    # wrap-around if not enough
    if len(out) < need:
        last_renew = _lock_renew_if_needed(token, last_renew)
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
            cb_id_i = int(cb_id)
            if cb_id_i in used:
                continue
            out.append(QueueItem(cb_id_i, str(plz), str(branch_slug)))
            if len(out) >= need:
                break

    return out[:need], last_renew


if __name__ == "__main__":
    main()
