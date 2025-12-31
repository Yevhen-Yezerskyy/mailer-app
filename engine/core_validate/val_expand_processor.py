# FILE: engine/core_validate/val_expand_processor.py

from __future__ import annotations

import pickle
import time
import heapq
from typing import Dict, List, Optional, Tuple

from engine.common.cache.client import CLIENT as CACHE
from engine.common.db import execute, fetch_all, fetch_one
from engine.common.worker import Worker

TOP_LIMIT = 300
BATCH_PAIRS = 200

WINDOW_LIMIT = 100_000

MAX_RATE_CONTACTS_PER_TASK = 50_000  # <-- меняй тут при необходимости

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

    city_rate: Dict[int, int] = {int(cid): int(rate) for (cid, rate) in cities}
    branch_rate: Dict[int, int] = {int(bid): int(rate) for (bid, rate) in branches}

    # Топ WINDOW_LIMIT пар с минимальным score, без полной сортировки 1M элементов
    top = heapq.nsmallest(
        WINDOW_LIMIT,
        (
            (cr * br, city_id, branch_id)
            for city_id, cr in city_rate.items()
            for branch_id, br in branch_rate.items()
        ),
    )

    groups: Dict[int, List[Tuple[int, int]]] = {}
    for score, city_id, branch_id in top:
        groups.setdefault(int(score), []).append((int(city_id), int(branch_id)))

    out: List[Tuple[int, List[Tuple[int, int]]]] = []
    for score in sorted(groups.keys()):
        pairs = groups[score]
        pairs.sort(key=lambda x: (x[0], x[1]))
        out.append((score, pairs))
    return out

def _start_index_by_score(groups: List[Tuple[int, List[Tuple[int, int]]]], score: Optional[int]) -> int:
    if score is None:
        return 0
    for i, (s, _pairs) in enumerate(groups):
        if s == score:
            return i
    return 0


def run_once() -> None:
    epoch = _get_epoch()
    _p(f"tick epoch={epoch}")

    # 0) выбираем одну задачу (ТОЛЬКО collected=false)
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
        return
    task_id = int(row[0])

    # 1) валидация counts
    cities_ref, branches_ref = _get_ref_counts(epoch)
    row_c = fetch_one(
        "SELECT count(*) FROM crawl_tasks WHERE task_id = %s AND type = 'city'",
        (task_id,),
    )
    row_b = fetch_one(
        "SELECT count(*) FROM crawl_tasks WHERE task_id = %s AND type = 'branch'",
        (task_id,),
    )
    cnt_city_task = int(row_c[0]) if row_c else 0
    cnt_branch_task = int(row_b[0]) if row_b else 0

    if cnt_city_task != cities_ref or cnt_branch_task != branches_ref:
        _p(
            f"task_id={task_id} invalid counts: "
            f"cities {cnt_city_task}/{cities_ref}, branches {cnt_branch_task}/{branches_ref} -> skip"
        )
        return

    groups = _build_score_groups(task_id)
    if not groups:
        _p(f"task_id={task_id} no groups -> stop")
        return

    cursor_score = _get_cursor_score(epoch, task_id)
    start_i = _start_index_by_score(groups, cursor_score)
    if cursor_score is None:
        _p(f"task_id={task_id} start=FIRST_GROUP groups={len(groups)}")
    else:
        _p(f"task_id={task_id} start=CURSOR score={cursor_score} idx={start_i}/{len(groups)}")

    # 2) идём по группам начиная с курсора
    for score, pairs in groups[start_i:]:
        _p(f"task_id={task_id} score={score} pairs_count={len(pairs)}")

        collected_ids_all: List[int] = []
        has_not_collected_any = False
        total_rows_any = 0

        i = 0
        while i < len(pairs):
            batch = pairs[i : i + BATCH_PAIRS]
            i += BATCH_PAIRS

            values_sql = ", ".join(["(%s,%s)"] * len(batch))
            params: List[int] = []
            for city_id, branch_id in batch:
                params.append(int(city_id))
                params.append(int(branch_id))

            row_cb = fetch_one(
                f"""
                WITH pairs(city_id, branch_id) AS (VALUES {values_sql})
                SELECT
                    count(*)::int AS total_rows,
                    array_agg(c.id) FILTER (WHERE c.collected = true)::bigint[] AS collected_ids,
                    bool_or(c.collected = false) AS has_not_collected
                FROM cb_crawler c
                JOIN pairs p ON p.city_id = c.city_id AND p.branch_id = c.branch_id
                """,
                tuple(params),
            )

            if not row_cb:
                continue

            total_rows = int(row_cb[0]) if row_cb[0] is not None else 0
            total_rows_any += total_rows

            collected_ids = row_cb[1]
            has_not_collected = bool(row_cb[2]) if row_cb[2] is not None else False

            if has_not_collected:
                has_not_collected_any = True

            if collected_ids:
                collected_ids_all.extend([int(x) for x in collected_ids])

        # 3) вставляем collected=true (всегда, даже если группа не fully)
        if collected_ids_all:
            row_ins = fetch_one(
                """
                WITH cb_arr AS (
                    SELECT %s::bigint[] AS ids
                ),
                ins AS (
                    INSERT INTO rate_contacts (task_id, contact_id, rate_cb)
                    SELECT %s, r.id, %s
                    FROM raw_contacts_aggr r, cb_arr a
                    WHERE r.cb_crawler_ids && a.ids
                    ON CONFLICT (task_id, contact_id) DO NOTHING
                    RETURNING 1
                )
                SELECT count(*) FROM ins
                """,
                (collected_ids_all, task_id, score),
            )
            inserted = int(row_ins[0]) if row_ins else 0
        else:
            inserted = 0

        fully_collected = (not has_not_collected_any) and (total_rows_any > 0)

        _p(
            f"task_id={task_id} score={score} "
            f"total_rows={total_rows_any} collected_ids={len(collected_ids_all)} "
            f"inserted={inserted} fully_collected={fully_collected}"
        )

        # 4) управление ТОЛЬКО по collected (cursor)
        if not fully_collected:
            _set_cursor_score(epoch, task_id, score)
            _p(f"task_id={task_id} STOP on score={score} (not fully collected) -> cursor saved")
            return

        continue

    _p(f"task_id={task_id} reached end of groups -> no stop point found")


def mark_collected_once() -> None:
    """
    Глобальный обзорщик (независим от expander):
    - collected=true если rate_contacts >= LIMIT
    - collected=true если по cursor-логике дошли до конца групп и не нашли stop-point
      (то есть "в последнем элементе не осталось никого, и все")
    """
    epoch = _get_epoch()

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
            _p(f"task_id={task_id} rate_contacts={cnt} -> collected=true (limit)")
            continue

        groups = _build_score_groups(task_id)
        if not groups:
            # нет групп -> считаем done
            execute(
                """
                UPDATE aap_audience_audiencetask
                SET collected = true
                WHERE id = %s
                """,
                (task_id,),
            )
            _p(f"task_id={task_id} no groups -> collected=true")
            continue

        cursor_score = _get_cursor_score(epoch, task_id)
        start_i = _start_index_by_score(groups, cursor_score)

        stop_found = False

        for score, pairs in groups[start_i:]:
            has_not_collected_any = False
            total_rows_any = 0

            i = 0
            while i < len(pairs):
                batch = pairs[i : i + BATCH_PAIRS]
                i += BATCH_PAIRS

                values_sql = ", ".join(["(%s,%s)"] * len(batch))
                params: List[int] = []
                for city_id, branch_id in batch:
                    params.append(int(city_id))
                    params.append(int(branch_id))

                row_cb = fetch_one(
                    f"""
                    WITH pairs(city_id, branch_id) AS (VALUES {values_sql})
                    SELECT
                        count(*)::int AS total_rows,
                        bool_or(c.collected = false) AS has_not_collected
                    FROM cb_crawler c
                    JOIN pairs p ON p.city_id = c.city_id AND p.branch_id = c.branch_id
                    """,
                    tuple(params),
                )
                if not row_cb:
                    continue

                total_rows = int(row_cb[0]) if row_cb[0] is not None else 0
                total_rows_any += total_rows

                has_not_collected = bool(row_cb[1]) if row_cb[1] is not None else False
                if has_not_collected:
                    has_not_collected_any = True

            fully_collected = (not has_not_collected_any) and (total_rows_any > 0)
            if not fully_collected:
                _set_cursor_score(epoch, task_id, score)
                stop_found = True
                _p(f"task_id={task_id} not fully collected on score={score} -> cursor saved (no collected)")
                break

        if not stop_found:
            execute(
                """
                UPDATE aap_audience_audiencetask
                SET collected = true
                WHERE id = %s
                """,
                (task_id,),
            )
            _p(f"task_id={task_id} end of groups -> collected=true (done)")


def reset_cache_once() -> None:
    epoch = int(time.time())
    _set_epoch(epoch)
    _p(f"cache reset: epoch={epoch} (next ticks will start from first group)")


def main() -> None:
    w = Worker(
        name="val_expand_processor",
        tick_sec=1,
        max_parallel=1,
    )

    w.register(
        name="expand_rate_contacts",
        fn=run_once,
        every_sec=2,
        timeout_sec=900,
        singleton=True,
        heavy=True,
        priority=5,
    )

    w.register(
        name="mark_tasks_collected",
        fn=mark_collected_once,
        every_sec=600,
        timeout_sec=600,
        singleton=True,
        heavy=True,
        priority=3,
    )

    w.register(
        name="expand_rate_contacts_reset",
        fn=reset_cache_once,
        every_sec=3600,
        timeout_sec=3600,
        singleton=True,
        heavy=True,
        priority=1,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
