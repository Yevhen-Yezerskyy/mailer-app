# FILE: engine/core_validate/val_expand_processor.py  (обновлено — 2026-01-03)
# Смысл:
# - Единый выбор task_id (LIGHT/FULL) через pick_task_id() + кеш эталонных counts и кеш ok/skip по task_id в рамках epoch.
# - LIGHT не зовёт _build_score_groups(), если есть cursor + pairs_cache; _build_score_groups() вызывается только когда надо перейти.
# - LIGHT может за тик перескочить до MAX_GROUP_ADVANCES_PER_TICK полностью-collected пустых групп.
# - Закрыты логические дыры (5) и (6) строго и детерминированно:
#   (5) pairs: делаем столько окон, сколько нужно: pairs[0:W], pairs[W:2W], ... (без head/mid/tail).
#   (6) cb_ids: внутри каждого окна pairs делаем столько окон cb_ids, сколько нужно: OFFSET k*CB_IDS_LIMIT, LIMIT CB_IDS_LIMIT.
# - WINDOW_LIMIT=100_000 и _build_score_groups(task_id) сохранены нетронутыми.

from __future__ import annotations

import heapq
import math
import pickle
import time
from typing import Dict, List, Optional, Tuple

from engine.common.cache.client import CLIENT as CACHE
from engine.common.db import execute, fetch_all, fetch_one
from engine.common.worker import Worker

# -------------------------
# Settings
# -------------------------
BATCH_PAIRS = 200

# bounded вставка контактов за одну итерацию (на группу за тик)
CONTACTS_INSERT_LIMIT = 200

# окна по cb_crawler.id
CB_IDS_LIMIT = 500

WINDOW_LIMIT = 100_000  # <-- НЕ ТРОГАТЬ, кравлер на этом живёт
TOP_LIMIT = WINDOW_LIMIT # <-- какой-то джипити решил, что надо переименовать переменную, хоть сам и написал, что не трогать. И вот приходится дописывать ))

MAX_RATE_CONTACTS_PER_TASK = 50_000

# LIGHT: максимум сколько полностью-collected групп можно перескочить за один тик
MAX_GROUP_ADVANCES_PER_TICK = 50

# окно pairs (сколько пар кладём в VALUES одним запросом)
PAIRS_WINDOW_SIZE = 4 * BATCH_PAIRS  # 800

CACHE_TTL_SEC = 24 * 60 * 60
CACHE_EPOCH_KEY = "val_expand:epoch"


def _p(msg: str) -> None:
    print(f"[val_expand] {msg}")


def _cache_get_obj(key: str) -> Optional[object]:
    payload = CACHE.get(key, ttl_sec=CACHE_TTL_SEC)
    if payload is None:
        return None
    try:
        return pickle.loads(payload)
    except Exception:
        return None


def _cache_set_obj(key: str, obj: object) -> bool:
    try:
        payload = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        return False
    return CACHE.set(key, payload, ttl_sec=CACHE_TTL_SEC)


def _get_epoch() -> int:
    v = _cache_get_obj(CACHE_EPOCH_KEY)
    if isinstance(v, int) and v > 0:
        return v
    epoch = int(time.time())
    _cache_set_obj(CACHE_EPOCH_KEY, epoch)
    return epoch


def _set_epoch(epoch: int) -> None:
    _cache_set_obj(CACHE_EPOCH_KEY, epoch)


def _key(epoch: int, suffix: str) -> str:
    return f"val_expand:{epoch}:{suffix}"


def _cursor_key(epoch: int, task_id: int) -> str:
    return _key(epoch, f"cursor:{task_id}")


def _get_cursor_score(epoch: int, task_id: int) -> Optional[int]:
    v = _cache_get_obj(_cursor_key(epoch, task_id))
    return int(v) if isinstance(v, int) else None


def _set_cursor_score(epoch: int, task_id: int, score: int) -> None:
    _cache_set_obj(_cursor_key(epoch, task_id), int(score))


def _pairs_key(epoch: int, task_id: int, score: int) -> str:
    return _key(epoch, f"pairs:{task_id}:{score}")


def _get_pairs_cached(epoch: int, task_id: int, score: int) -> Optional[List[Tuple[int, int]]]:
    v = _cache_get_obj(_pairs_key(epoch, task_id, score))
    if isinstance(v, list) and v and isinstance(v[0], tuple) and len(v[0]) == 2:
        try:
            return [(int(a), int(b)) for (a, b) in v]
        except Exception:
            return None
    return None


def _set_pairs_cached(epoch: int, task_id: int, score: int, pairs: List[Tuple[int, int]]) -> None:
    _cache_set_obj(_pairs_key(epoch, task_id, score), pairs)


def _get_ref_counts(epoch: int) -> Tuple[int, int]:
    k = _key(epoch, "ref_counts")
    v = _cache_get_obj(k)
    if isinstance(v, tuple) and len(v) == 2:
        a, b = v
        if isinstance(a, int) and isinstance(b, int):
            return a, b

    row1 = fetch_one("SELECT count(*) FROM cities_sys")
    row2 = fetch_one("SELECT count(*) FROM gb_branches")
    cities_ref = int(row1[0]) if row1 else 0
    branches_ref = int(row2[0]) if row2 else 0
    _cache_set_obj(k, (cities_ref, branches_ref))
    return cities_ref, branches_ref


def _task_ok_key(epoch: int, task_id: int) -> str:
    return _key(epoch, f"task_ok:{task_id}")


def _is_task_counts_ok(epoch: int, task_id: int) -> bool:
    cached = _cache_get_obj(_task_ok_key(epoch, task_id))
    if isinstance(cached, bool):
        return cached

    cities_ref, branches_ref = _get_ref_counts(epoch)

    row_c = fetch_one("SELECT count(*) FROM crawl_tasks WHERE task_id = %s AND type = 'city'", (task_id,))
    row_b = fetch_one("SELECT count(*) FROM crawl_tasks WHERE task_id = %s AND type = 'branch'", (task_id,))
    cnt_city_task = int(row_c[0]) if row_c else 0
    cnt_branch_task = int(row_b[0]) if row_b else 0

    ok = (cnt_city_task == cities_ref) and (cnt_branch_task == branches_ref)
    _cache_set_obj(_task_ok_key(epoch, task_id), bool(ok))
    return ok


def pick_task_id(epoch: int) -> Optional[int]:
    row = fetch_one(
        """
        SELECT id
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND collected = false
        ORDER BY random()
        LIMIT 1
        """
    )
    if not row:
        return None

    task_id = int(row[0])
    if not _is_task_counts_ok(epoch, task_id):
        cities_ref, branches_ref = _get_ref_counts(epoch)
        row_c = fetch_one("SELECT count(*) FROM crawl_tasks WHERE task_id = %s AND type = 'city'", (task_id,))
        row_b = fetch_one("SELECT count(*) FROM crawl_tasks WHERE task_id = %s AND type = 'branch'", (task_id,))
        cnt_city_task = int(row_c[0]) if row_c else 0
        cnt_branch_task = int(row_b[0]) if row_b else 0
        _p(
            f"TASK task_id={task_id} invalid counts: "
            f"cities {cnt_city_task}/{cities_ref}, branches {cnt_branch_task}/{branches_ref} -> skip"
        )
        return None

    return task_id


# -------------------------
# НЕ ТРОГАТЬ: источник последовательности
# -------------------------
def _build_score_groups(task_id: int) -> List[Tuple[int, List[Tuple[int, int]]]]:
    cities = fetch_all(
        """
        SELECT value_id, rate
        FROM crawl_tasks
        WHERE task_id = %s AND type = 'city'
        ORDER BY rate ASC, value_id ASC
        """,
        (task_id,),
    )
    branches = fetch_all(
        """
        SELECT value_id, rate
        FROM crawl_tasks
        WHERE task_id = %s AND type = 'branch'
        ORDER BY rate ASC, value_id ASC
        """,
        (task_id,),
    )

    if not cities or not branches:
        return []

    C: List[Tuple[int, int]] = [(int(rate), int(cid)) for (cid, rate) in cities]
    B: List[Tuple[int, int]] = [(int(rate), int(bid)) for (bid, rate) in branches]

    outer_is_branch = len(B) <= len(C)
    outer = B if outer_is_branch else C
    inner = C if outer_is_branch else B

    max_pairs = len(outer) * len(inner)
    k_limit = WINDOW_LIMIT if WINDOW_LIMIT < max_pairs else max_pairs

    h: List[Tuple[int, int, int, int, int]] = []
    for i, (orate, oid) in enumerate(outer):
        irate0, iid0 = inner[0]

        if outer_is_branch:
            city_id = int(iid0)
            branch_id = int(oid)
            score = int(orate) * int(irate0)
        else:
            city_id = int(oid)
            branch_id = int(iid0)
            score = int(orate) * int(irate0)

        heapq.heappush(h, (score, city_id, branch_id, i, 0))

    groups: Dict[int, List[Tuple[int, int]]] = {}
    for _ in range(k_limit):
        if not h:
            break

        score, city_id, branch_id, i, j = heapq.heappop(h)
        groups.setdefault(int(score), []).append((int(city_id), int(branch_id)))

        j2 = j + 1
        if j2 < len(inner):
            orate, oid = outer[i]
            irate2, iid2 = inner[j2]

            if outer_is_branch:
                city_id2 = int(iid2)
                branch_id2 = int(oid)
                score2 = int(orate) * int(irate2)
            else:
                city_id2 = int(oid)
                branch_id2 = int(iid2)
                score2 = int(orate) * int(irate2)

            heapq.heappush(h, (score2, city_id2, branch_id2, i, j2))

    out: List[Tuple[int, List[Tuple[int, int]]]] = []
    for score in sorted(groups.keys()):
        pairs = groups[score]
        pairs.sort(key=lambda x: (x[0], x[1]))
        out.append((int(score), pairs))
    return out


def _first_score(groups: List[Tuple[int, List[Tuple[int, int]]]]) -> Optional[int]:
    return int(groups[0][0]) if groups else None


def _next_score(groups: List[Tuple[int, List[Tuple[int, int]]]], score: int) -> Optional[int]:
    found = False
    for s, _pairs in groups:
        if found:
            return int(s)
        if int(s) == int(score):
            found = True
    return None


def _pairs_for_score(groups: List[Tuple[int, List[Tuple[int, int]]]], score: int) -> Optional[List[Tuple[int, int]]]:
    for s, ps in groups:
        if int(s) == int(score):
            return ps
    return None


def _has_uncollected_in_group(*, pairs: List[Tuple[int, int]]) -> bool:
    i = 0
    while i < len(pairs):
        batch = pairs[i : i + BATCH_PAIRS]
        i += BATCH_PAIRS

        values_sql = ", ".join(["(%s,%s)"] * len(batch))
        params: List[int] = []
        for city_id, branch_id in batch:
            params.append(int(city_id))
            params.append(int(branch_id))

        row_unc = fetch_one(
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
        if row_unc:
            return True

    return False


def _iter_pairs_windows(pairs: List[Tuple[int, int]]):
    n = len(pairs)
    if n <= 0:
        return
    # строгое покрытие всего списка окон по PAIRS_WINDOW_SIZE
    for off in range(0, n, PAIRS_WINDOW_SIZE):
        yield pairs[off : off + PAIRS_WINDOW_SIZE]


def _insert_missing_for_pairs_window_with_cb_offset(
    *,
    task_id: int,
    score: int,
    pairs_window: List[Tuple[int, int]],
    cb_offset: int,
    cb_limit: int,
    contacts_limit: int,
) -> int:
    if not pairs_window or contacts_limit <= 0:
        return 0

    values_sql = ", ".join(["(%s,%s)"] * len(pairs_window))
    params: List[int] = []
    for city_id, branch_id in pairs_window:
        params.append(int(city_id))
        params.append(int(branch_id))

    row_ins = fetch_one(
        f"""
        WITH pairs(city_id, branch_id) AS (VALUES {values_sql}),
        cb_ids AS (
            SELECT c.id
            FROM cb_crawler c
            JOIN pairs p ON p.city_id = c.city_id AND p.branch_id = c.branch_id
            WHERE c.collected = true
            ORDER BY c.id ASC
            OFFSET %s
            LIMIT %s
        ),
        cb_arr AS (
            SELECT COALESCE(array_agg(id), '{{}}'::bigint[])::bigint[] AS ids
            FROM cb_ids
        ),
        cand AS (
            SELECT r.id AS contact_id
            FROM raw_contacts_aggr r, cb_arr a
            WHERE a.ids <> '{{}}'::bigint[]
              AND r.cb_crawler_ids && a.ids
              AND NOT EXISTS (
                  SELECT 1
                  FROM rate_contacts rc
                  WHERE rc.task_id = %s AND rc.contact_id = r.id
              )
            ORDER BY r.id ASC
            LIMIT %s
        ),
        ins AS (
            INSERT INTO rate_contacts (task_id, contact_id, rate_cb)
            SELECT %s, c.contact_id, %s
            FROM cand c
            ON CONFLICT (task_id, contact_id) DO NOTHING
            RETURNING 1
        )
        SELECT count(*) FROM ins
        """,
        tuple(
            params
            + [
                int(cb_offset),
                int(cb_limit),
                int(task_id),
                int(contacts_limit),
                int(task_id),
                int(score),
            ]
        ),
    )
    return int(row_ins[0]) if row_ins else 0


def _count_cb_collected_for_pairs_window(*, pairs_window: List[Tuple[int, int]]) -> int:
    if not pairs_window:
        return 0
    values_sql = ", ".join(["(%s,%s)"] * len(pairs_window))
    params: List[int] = []
    for city_id, branch_id in pairs_window:
        params.append(int(city_id))
        params.append(int(branch_id))

    row = fetch_one(
        f"""
        WITH pairs(city_id, branch_id) AS (VALUES {values_sql})
        SELECT count(*)
        FROM cb_crawler c
        JOIN pairs p ON p.city_id = c.city_id AND p.branch_id = c.branch_id
        WHERE c.collected = true
        """,
        tuple(params),
    )
    return int(row[0]) if row else 0


def _insert_missing_for_group(*, task_id: int, score: int, pairs: List[Tuple[int, int]]) -> int:
    """
    Строгое покрытие (без эвристики):
    - делаем столько окон pairs, сколько нужно (0..len step PAIRS_WINDOW_SIZE)
    - внутри каждого окна pairs делаем столько окон cb_ids, сколько нужно (0..count step CB_IDS_LIMIT)
    В рамках одного тика. Останавливаемся только если исчерпали CONTACTS_INSERT_LIMIT.
    """
    total_inserted = 0
    remaining = CONTACTS_INSERT_LIMIT

    for pw in _iter_pairs_windows(pairs):
        if remaining <= 0:
            break

        total_cb = _count_cb_collected_for_pairs_window(pairs_window=pw)
        if total_cb <= 0:
            continue

        # сколько cb-окон нужно
        cb_passes = int(math.ceil(total_cb / float(CB_IDS_LIMIT)))
        for k in range(cb_passes):
            if remaining <= 0:
                break
            cb_offset = k * CB_IDS_LIMIT

            ins = _insert_missing_for_pairs_window_with_cb_offset(
                task_id=task_id,
                score=score,
                pairs_window=pw,
                cb_offset=cb_offset,
                cb_limit=CB_IDS_LIMIT,
                contacts_limit=remaining,
            )
            total_inserted += ins
            remaining -= ins

            # если из этого cb-окна не вставили ничего — это ок, идём дальше (покрываем весь объём)

    return total_inserted


# -------------------------
# LIGHT
# -------------------------
def light_run_once() -> None:
    epoch = _get_epoch()
    task_id = pick_task_id(epoch)
    if not task_id:
        return

    cursor_score = _get_cursor_score(epoch, task_id)

    # быстрый путь: cursor + pairs_cache => вообще без _build_score_groups()
    if cursor_score is not None:
        pairs = _get_pairs_cached(epoch, task_id, int(cursor_score))
        if pairs is not None:
            has_uncollected = _has_uncollected_in_group(pairs=pairs)
            inserted = _insert_missing_for_group(task_id=task_id, score=int(cursor_score), pairs=pairs)

            _p(
                f"LIGHT task_id={task_id} score={cursor_score} "
                f"pairs={len(pairs)} has_uncollected={has_uncollected} inserted={inserted}"
            )

            if has_uncollected or inserted > 0:
                return

            # fully-collected и inserted=0 -> прыгаем дальше (до 50 групп за тик)
            groups = _build_score_groups(task_id)
            if not groups:
                return

            advances = 0
            while advances < MAX_GROUP_ADVANCES_PER_TICK:
                nxt = _next_score(groups, int(cursor_score))
                if nxt is None:
                    return

                cursor_score = int(nxt)
                _set_cursor_score(epoch, task_id, int(cursor_score))

                pairs = _get_pairs_cached(epoch, task_id, int(cursor_score))
                if pairs is None:
                    ps = _pairs_for_score(groups, int(cursor_score))
                    if ps is None:
                        return
                    pairs = ps
                    _set_pairs_cached(epoch, task_id, int(cursor_score), pairs)

                has_uncollected = _has_uncollected_in_group(pairs=pairs)
                inserted = _insert_missing_for_group(task_id=task_id, score=int(cursor_score), pairs=pairs)

                _p(
                    f"LIGHT task_id={task_id} ADV={advances+1}/{MAX_GROUP_ADVANCES_PER_TICK} "
                    f"score={cursor_score} pairs={len(pairs)} has_uncollected={has_uncollected} inserted={inserted}"
                )

                if has_uncollected or inserted > 0:
                    return

                advances += 1

            return

    # нет курсора / нет pairs_cache: нужен groups
    groups = _build_score_groups(task_id)
    if not groups:
        _p(f"LIGHT task_id={task_id} no groups -> stop")
        return

    if cursor_score is None:
        first = _first_score(groups)
        if first is None:
            return
        cursor_score = int(first)
        _set_cursor_score(epoch, task_id, int(cursor_score))

    advances = 0
    while advances <= MAX_GROUP_ADVANCES_PER_TICK:
        pairs = _get_pairs_cached(epoch, task_id, int(cursor_score))
        if pairs is None:
            ps = _pairs_for_score(groups, int(cursor_score))
            if ps is None:
                return
            pairs = ps
            _set_pairs_cached(epoch, task_id, int(cursor_score), pairs)

        has_uncollected = _has_uncollected_in_group(pairs=pairs)
        inserted = _insert_missing_for_group(task_id=task_id, score=int(cursor_score), pairs=pairs)

        _p(
            f"LIGHT task_id={task_id} score={cursor_score} "
            f"pairs={len(pairs)} has_uncollected={has_uncollected} inserted={inserted}"
        )

        if has_uncollected or inserted > 0:
            return

        nxt = _next_score(groups, int(cursor_score))
        if nxt is None:
            return
        cursor_score = int(nxt)
        _set_cursor_score(epoch, task_id, int(cursor_score))
        advances += 1


# -------------------------
# FULL (15 минут, 1 случайный task)
# -------------------------
def full_reconcile_once() -> None:
    epoch = _get_epoch()
    task_id = pick_task_id(epoch)
    if not task_id:
        return

    groups = _build_score_groups(task_id)
    if not groups:
        _p(f"FULL task_id={task_id} no groups -> stop")
        return

    _p(f"FULL task_id={task_id} start groups={len(groups)}")
    for score, pairs in groups:
        has_uncollected = _has_uncollected_in_group(pairs=pairs)
        if has_uncollected:
            _p(f"FULL task_id={task_id} STOP on score={score} (has uncollected) -> do nothing")
            return

        inserted = _insert_missing_for_group(task_id=task_id, score=int(score), pairs=pairs)
        if inserted > 0:
            _p(f"FULL task_id={task_id} score={score} inserted={inserted}")

    _p(f"FULL task_id={task_id} reached end of groups")


# -------------------------
# mark_collected_once (DONE, без добора)
# -------------------------
def mark_collected_once() -> None:
    rows = fetch_all(
        """
        SELECT id
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND collected = false
        ORDER BY id ASC
        """
    )
    if not rows:
        return

    for (task_id_raw,) in rows:
        task_id = int(task_id_raw)

        row_cnt = fetch_one("SELECT count(*) FROM rate_contacts WHERE task_id = %s", (task_id,))
        cnt = int(row_cnt[0]) if row_cnt else 0
        if cnt >= MAX_RATE_CONTACTS_PER_TASK:
            execute(
                """
                UPDATE aap_audience_audiencetask
                SET collected = true
                WHERE id = %s
                """,
                (task_id,),
            )
            _p(f"MARK task_id={task_id} rate_contacts={cnt} -> collected=true (limit)")
            continue

        groups = _build_score_groups(task_id)
        if not groups:
            execute(
                """
                UPDATE aap_audience_audiencetask
                SET collected = true
                WHERE id = %s
                """,
                (task_id,),
            )
            _p(f"MARK task_id={task_id} no groups -> collected=true")
            continue

        any_uncollected = False
        for _score, pairs in groups:
            if _has_uncollected_in_group(pairs=pairs):
                any_uncollected = True
                break

        if not any_uncollected:
            execute(
                """
                UPDATE aap_audience_audiencetask
                SET collected = true
                WHERE id = %s
                """,
                (task_id,),
            )
            _p(f"MARK task_id={task_id} all pairs collected=true -> collected=true (done)")


# -------------------------
# LIGHT cache reset (раз в час)
# -------------------------
def reset_cache_once() -> None:
    epoch = int(time.time())
    _set_epoch(epoch)
    _p(f"cache reset: epoch={epoch} (LIGHT cursor+pairs will restart)")


def main() -> None:
    w = Worker(
        name="val_expand_processor",
        tick_sec=1,
        max_parallel=1,
    )

    w.register(
        name="light_expand_rate_contacts",
        fn=light_run_once,
        every_sec=2,
        timeout_sec=120,
        singleton=True,
        heavy=False,
        priority=5,
    )

    w.register(
        name="full_reconcile_rate_contacts",
        fn=full_reconcile_once,
        every_sec=60,  # 1 минута
        timeout_sec=900,
        singleton=True,
        heavy=True,
        priority=3,
    )

    w.register(
        name="mark_tasks_collected",
        fn=mark_collected_once,
        every_sec=7200,  # 2 часа
        timeout_sec=900,
        singleton=True,
        heavy=True,
        priority=2,
    )

    w.register(
        name="light_cache_reset",
        fn=reset_cache_once,
        every_sec=3600,  # 1 час
        timeout_sec=60,
        singleton=True,
        heavy=False,
        priority=1,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
