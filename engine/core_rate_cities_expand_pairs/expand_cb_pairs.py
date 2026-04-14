# FILE: engine/core_rate_cities_expand_pairs/expand_cb_pairs.py
# DATE: 2026-03-26
# PURPOSE: Builds sorted PLZ-branch pair windows for ready audience tasks, upserts
# them into cb_crawl_pairs/task_cb_ratings, and tracks source snapshot hashes on the task.

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from engine.common.cache.client import CLIENT
from engine.common.db import get_connection
from engine.common.logs import log
from engine.common.utils import h64_text


INITIAL_EXPAND_LIMIT = 10_000
TOPUP_INTERVAL = 5_000
LOW_WATERMARK = 5_000
TMP_STAGE_PASSES = 3
TASK_LOCK_TTL_SEC = 300
LOG_FILE = "expand_cb_pairs.log"
LOG_FOLDER = "processing"

PairRate = Tuple[int, int, int]  # (plz_id, branch_id, rate)


def _load_task_state(task_id: int) -> Tuple[int, int, int, int]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(
                string_agg(
                    city_id::text || ':' || COALESCE(rate::text, ''),
                    '|'
                    ORDER BY city_id
                ),
                ''
            )
            FROM public.task_city_ratings
            WHERE task_id = %s
            """,
            (int(task_id),),
        )
        city_hash = int(h64_text(str((cur.fetchone() or [""])[0] or "")))

        cur.execute(
            """
            SELECT COALESCE(
                string_agg(
                    branch_id::text || ':' || COALESCE(rate::text, ''),
                    '|'
                    ORDER BY branch_id
                ),
                ''
            )
            FROM public.task_branch_ratings
            WHERE task_id = %s
            """,
            (int(task_id),),
        )
        branch_hash = int(h64_text(str((cur.fetchone() or [""])[0] or "")))

        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM (
                SELECT ps.id
                FROM public.task_city_ratings tcr
                JOIN public.__city__plz_map m
                  ON m.city_id = tcr.city_id
                JOIN public.plz_sys ps
                  ON ps.plz = m.plz
                WHERE tcr.task_id = %s
                  AND tcr.rate IS NOT NULL
                GROUP BY ps.id
            ) q
            """,
            (int(task_id),),
        )
        plz_cnt = int((cur.fetchone() or [0])[0] or 0)

        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.task_branch_ratings
            WHERE task_id = %s
              AND rate IS NOT NULL
            """,
            (int(task_id),),
        )
        branch_cnt = int((cur.fetchone() or [0])[0] or 0)
    return city_hash, branch_hash, plz_cnt, branch_cnt


def _load_full_snapshot(task_id: int) -> Tuple[int, int, List[PairRate], int, int]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(
                string_agg(
                    city_id::text || ':' || COALESCE(rate::text, ''),
                    '|'
                    ORDER BY city_id
                ),
                ''
            )
            FROM public.task_city_ratings
            WHERE task_id = %s
            """,
            (int(task_id),),
        )
        city_hash = int(h64_text(str((cur.fetchone() or [""])[0] or "")))

        cur.execute(
            """
            SELECT COALESCE(
                string_agg(
                    branch_id::text || ':' || COALESCE(rate::text, ''),
                    '|'
                    ORDER BY branch_id
                ),
                ''
            )
            FROM public.task_branch_ratings
            WHERE task_id = %s
            """,
            (int(task_id),),
        )
        branch_hash = int(h64_text(str((cur.fetchone() or [""])[0] or "")))

        cur.execute(
            """
            SELECT ps.id, MIN(tcr.rate)::int AS rate
            FROM public.task_city_ratings tcr
            JOIN public.__city__plz_map m
              ON m.city_id = tcr.city_id
            JOIN public.plz_sys ps
              ON ps.plz = m.plz
            WHERE tcr.task_id = %s
              AND tcr.rate IS NOT NULL
            GROUP BY ps.id
            ORDER BY MIN(tcr.rate) ASC, ps.id ASC
            """,
            (int(task_id),),
        )
        plz_rates = [(int(rate), int(plz_id)) for plz_id, rate in (cur.fetchall() or [])]

        cur.execute(
            """
            SELECT branch_id, rate
            FROM public.task_branch_ratings
            WHERE task_id = %s
              AND rate IS NOT NULL
            ORDER BY rate ASC, branch_id ASC
            """,
            (int(task_id),),
        )
        branch_rates = [(int(rate), int(branch_id)) for branch_id, rate in (cur.fetchall() or [])]

    out: List[PairRate] = []
    for city_rate, plz_id in plz_rates:
        for branch_rate, branch_id in branch_rates:
            out.append((int(plz_id), int(branch_id), int(city_rate) * int(branch_rate)))
    out.sort(key=lambda item: (int(item[2]), int(item[0]), int(item[1])))
    return city_hash, branch_hash, out, len(plz_rates), len(branch_rates)


def _stage_task_pairs(cur: Any, pairs: List[PairRate]) -> int:
    if not pairs:
        return 0

    cur.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS __cb_ratings_tmp__ (
            plz_id bigint NOT NULL,
            branch_id bigint NOT NULL,
            rate integer NOT NULL,
            PRIMARY KEY (plz_id, branch_id)
        ) ON COMMIT DELETE ROWS
        """
    )
    cur.execute("TRUNCATE TABLE __cb_ratings_tmp__")

    total = 0
    chunk_size = max(1, (len(pairs) + int(TMP_STAGE_PASSES) - 1) // int(TMP_STAGE_PASSES))
    for off in range(0, len(pairs), chunk_size):
        chunk = pairs[off: off + chunk_size]
        plz_arr = [int(plz_id) for plz_id, _branch_id, _rate in chunk]
        branch_arr = [int(branch_id) for _plz_id, branch_id, _rate in chunk]
        rate_arr = [int(rate) for _plz_id, _branch_id, rate in chunk]
        cur.execute(
            """
            INSERT INTO __cb_ratings_tmp__ (plz_id, branch_id, rate)
            SELECT u.plz_id, u.branch_id, u.rate
            FROM unnest(%s::bigint[], %s::bigint[], %s::integer[]) AS u(plz_id, branch_id, rate)
            ON CONFLICT (plz_id, branch_id) DO UPDATE
            SET rate = EXCLUDED.rate
            """,
            (plz_arr, branch_arr, rate_arr),
        )
        total += int(len(chunk))
    return total


def _upsert_task_pairs(cur: Any, task_id: int) -> int:
    cur.execute(
        """
        INSERT INTO public.cb_crawl_pairs (plz_id, branch_id)
        SELECT t.plz_id, t.branch_id
        FROM __cb_ratings_tmp__ t
        ON CONFLICT (plz_id, branch_id) DO NOTHING
        """
    )
    cur.execute(
        """
        WITH pair_map AS (
            SELECT t.plz_id, t.branch_id, t.rate, cb.id AS cb_id
            FROM __cb_ratings_tmp__ t
            JOIN public.cb_crawl_pairs cb
              ON cb.plz_id = t.plz_id
             AND cb.branch_id = t.branch_id
        )
        INSERT INTO public.task_cb_ratings (task_id, cb_id, rate)
        SELECT %s, pm.cb_id, pm.rate
        FROM pair_map pm
        ON CONFLICT (task_id, cb_id) DO UPDATE
        SET rate = EXCLUDED.rate,
            updated_at = now()
        WHERE public.task_cb_ratings.rate IS DISTINCT FROM EXCLUDED.rate
        """,
        (int(task_id),),
    )
    return int(cur.rowcount or 0)


def _delete_task_pairs_tail(cur: Any, task_id: int) -> int:
    cur.execute("SELECT 1 FROM __cb_ratings_tmp__ LIMIT 1")
    if cur.fetchone() is None:
        cur.execute("DELETE FROM public.task_cb_ratings WHERE task_id = %s", (int(task_id),))
        return int(cur.rowcount or 0)

    cur.execute(
        """
        WITH keep_cb AS (
            SELECT cb.id
            FROM __cb_ratings_tmp__ t
            JOIN public.cb_crawl_pairs cb
              ON cb.plz_id = t.plz_id
             AND cb.branch_id = t.branch_id
        )
        DELETE FROM public.task_cb_ratings tcr
        WHERE tcr.task_id = %s
          AND NOT EXISTS (
              SELECT 1
              FROM keep_cb kc
              WHERE kc.id = tcr.cb_id
          )
        """,
        (int(task_id),),
    )
    return int(cur.rowcount or 0)


def _save_task_hashes(cur: Any, task_id: int, city_hash: int, branch_hash: int) -> None:
    cur.execute(
        """
        UPDATE public.aap_audience_audiencetask
        SET rating_city_hash = %s,
            rating_branch_hash = %s,
            updated_at = now()
        WHERE id = %s
        """,
        (int(city_hash), int(branch_hash), int(task_id)),
    )


def _clear_task_pairs_tmp(cur: Any) -> None:
    cur.execute("TRUNCATE TABLE __cb_ratings_tmp__")


def _task_lock_key(task_id: int) -> str:
    return f"core_tasks:expand_cb_pairs:task:{int(task_id)}"


def _try_lock_task(task_id: int, owner: str) -> Optional[str]:
    resp = CLIENT.lock_try(_task_lock_key(task_id), ttl_sec=TASK_LOCK_TTL_SEC, owner=owner)
    if not resp or resp.get("acquired") is not True or not isinstance(resp.get("token"), str):
        return None
    return str(resp["token"])


def _release_task_lock(task_id: int, token: Optional[str]) -> None:
    if not token:
        return
    try:
        CLIENT.lock_release(_task_lock_key(task_id), token=str(token))
    except Exception:
        pass


def _pick_initial_task() -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    owner = f"{os.getpid()}:{int(time.time())}"
    started_at = time.perf_counter()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.id, t.ready, t.active, t.archived, t.rating_city_hash, t.rating_branch_hash
            FROM public.aap_audience_audiencetask t
            WHERE COALESCE(t.archived, false) = false
              AND COALESCE(t.ready, false) = false
              AND EXISTS (
                  SELECT 1
                  FROM public.task_city_ratings tcr
                  WHERE tcr.task_id = t.id
              )
              AND EXISTS (
                  SELECT 1
                  FROM public.task_branch_ratings tbr
                  WHERE tbr.task_id = t.id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.task_city_ratings tcr
                  WHERE tcr.task_id = t.id
                    AND tcr.rate IS NULL
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.task_branch_ratings tbr
                  WHERE tbr.task_id = t.id
                    AND tbr.rate IS NULL
              )
            ORDER BY random()
            LIMIT 1
            """
        )
        row = cur.fetchone()
    sql_pick_ms = int((time.perf_counter() - started_at) * 1000)
    if not row:
        return None, None, sql_pick_ms

    task_id = int(row[0])
    token = _try_lock_task(task_id, owner)
    if not token:
        return None, None, sql_pick_ms

    task = {
        "task_id": task_id,
        "ready": bool(row[1]),
        "active": bool(row[2]),
        "archived": bool(row[3]),
        "rating_city_hash": int(row[4]) if row[4] is not None else None,
        "rating_branch_hash": int(row[5]) if row[5] is not None else None,
    }
    if task["ready"] or task["archived"]:
        _release_task_lock(task_id, token)
        return None, None, sql_pick_ms

    return task, token, sql_pick_ms


def _pick_active_task() -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    owner = f"{os.getpid()}:{int(time.time())}"
    started_at = time.perf_counter()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.id, t.ready, t.active, t.archived, t.rating_city_hash, t.rating_branch_hash
            FROM public.aap_audience_audiencetask t
            WHERE COALESCE(t.archived, false) = false
              AND (
                  COALESCE(t.active, false) = true
                  OR (
                      COALESCE(t.ready, false) = false
                      AND EXISTS (
                          SELECT 1
                          FROM public.task_cb_ratings tcr
                          WHERE tcr.task_id = t.id
                      )
                  )
              )
            ORDER BY random()
            LIMIT 1
            """
        )
        row = cur.fetchone()
    sql_pick_ms = int((time.perf_counter() - started_at) * 1000)
    if not row:
        return None, None, sql_pick_ms

    task_id = int(row[0])
    token = _try_lock_task(task_id, owner)
    if not token:
        return None, None, sql_pick_ms

    task = {
        "task_id": task_id,
        "ready": bool(row[1]),
        "active": bool(row[2]),
        "archived": bool(row[3]),
        "rating_city_hash": int(row[4]) if row[4] is not None else None,
        "rating_branch_hash": int(row[5]) if row[5] is not None else None,
    }
    return task, token, sql_pick_ms


def _probe_task_rows(task_id: int, offset: int, *, only_positive: bool = False) -> list[tuple[int]]:
    sql = """
        SELECT tcr.id
        FROM public.task_cb_ratings tcr
        WHERE tcr.task_id = %s
          {rate_filter}
        ORDER BY tcr.rate ASC NULLS LAST, tcr.id ASC
        OFFSET %s
        LIMIT 2
    """.format(rate_filter="AND tcr.rate > 0" if only_positive else "")
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (int(task_id), max(0, int(offset))))
        return list(cur.fetchall() or [])


def _has_task_row_at_rank(task_id: int, rank_1_based: int, *, only_positive: bool = False) -> bool:
    if int(rank_1_based) <= 0:
        return False
    rows = _probe_task_rows(int(task_id), int(rank_1_based) - 1, only_positive=bool(only_positive))
    return bool(rows)


def _load_task_pair_count_without_count(task_id: int, *, only_positive: bool = False) -> int:
    if not _has_task_row_at_rank(int(task_id), 1, only_positive=bool(only_positive)):
        return 0

    step = int(max(1, TOPUP_INTERVAL))
    probe_size = int(step)
    lower_bound = 1

    while True:
        rows = _probe_task_rows(int(task_id), int(probe_size) - 1, only_positive=bool(only_positive))
        if len(rows) >= 2:
            lower_bound = int(probe_size) + 1
            probe_size = int(probe_size) * 2
            continue
        if len(rows) == 1:
            return int(probe_size)
        break

    lo = int(max(1, lower_bound))
    hi = int(probe_size) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _has_task_row_at_rank(int(task_id), int(mid), only_positive=bool(only_positive)):
            lo = int(mid)
        else:
            hi = int(mid) - 1
    return int(lo)


def _has_watermark_uncollected(task_id: int, *, prefix_limit: Optional[int] = None) -> bool:
    if prefix_limit is not None and int(prefix_limit) <= 0:
        return False

    if prefix_limit is None:
        sql = """
            SELECT 1
            FROM public.task_cb_ratings tcr
            JOIN public.cb_crawl_pairs cp
              ON cp.id = tcr.cb_id
            WHERE tcr.task_id = %s
              AND tcr.rate > 0
              AND cp.collected = false
            ORDER BY tcr.rate ASC NULLS LAST, tcr.id ASC
            OFFSET %s
            LIMIT 1
        """
        params: tuple[int, int] = (int(task_id), int(LOW_WATERMARK - 1))
    else:
        sql = """
            WITH prefix AS (
                SELECT tcr.id, tcr.cb_id, tcr.rate
                FROM public.task_cb_ratings tcr
                WHERE tcr.task_id = %s
                  AND tcr.rate > 0
                ORDER BY tcr.rate ASC NULLS LAST, tcr.id ASC
                LIMIT %s
            )
            SELECT 1
            FROM prefix p
            JOIN public.cb_crawl_pairs cp
              ON cp.id = p.cb_id
            WHERE cp.collected = false
            ORDER BY p.rate ASC NULLS LAST, p.id ASC
            OFFSET %s
            LIMIT 1
        """
        params = (int(task_id), int(prefix_limit), int(LOW_WATERMARK - 1))

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone() is not None


def _zero_task_rates(task_id: int, *, city_hash: int, branch_hash: int) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.task_cb_ratings tcr
            SET rate = 0,
                updated_at = now()
            WHERE tcr.task_id = %s
              AND tcr.rate IS DISTINCT FROM 0
            """,
            (int(task_id),),
        )
        updated_cnt = int(cur.rowcount or 0)
        _save_task_hashes(cur, int(task_id), int(city_hash), int(branch_hash))
        conn.commit()
    return int(updated_cnt)


def _has_watermark_uncollected_in_pairs(pairs: List[PairRate]) -> bool:
    if not pairs:
        return False
    with get_connection() as conn, conn.cursor() as cur:
        _stage_task_pairs(cur, pairs)
        cur.execute(
            """
            INSERT INTO public.cb_crawl_pairs (plz_id, branch_id)
            SELECT t.plz_id, t.branch_id
            FROM __cb_ratings_tmp__ t
            ON CONFLICT (plz_id, branch_id) DO NOTHING
            """
        )
        cur.execute(
            """
            SELECT 1
            FROM __cb_ratings_tmp__ t
            JOIN public.cb_crawl_pairs cp
              ON cp.plz_id = t.plz_id
             AND cp.branch_id = t.branch_id
            WHERE cp.collected = false
            ORDER BY t.rate ASC, t.plz_id ASC, t.branch_id ASC
            OFFSET %s
            LIMIT 1
            """,
            (int(LOW_WATERMARK - 1),),
        )
        found = cur.fetchone() is not None
        _clear_task_pairs_tmp(cur)
        conn.commit()
    return bool(found)


def _write_selected_pairs(
    task_id: int,
    selected_pairs: List[PairRate],
    *,
    city_hash: int,
    branch_hash: int,
    delete_tail: bool,
    save_hashes: bool,
) -> Dict[str, int]:
    sql_stage_ms = 0
    sql_upsert_ms = 0
    sql_delete_tail_ms = 0
    sql_save_hash_ms = 0
    sql_clear_tmp_ms = 0
    written = 0
    deleted = 0

    with get_connection() as conn, conn.cursor() as cur:
        started_at = time.perf_counter()
        _stage_task_pairs(cur, selected_pairs)
        sql_stage_ms = int((time.perf_counter() - started_at) * 1000)

        started_at = time.perf_counter()
        written = _upsert_task_pairs(cur, task_id)
        sql_upsert_ms = int((time.perf_counter() - started_at) * 1000)

        if delete_tail:
            started_at = time.perf_counter()
            deleted = _delete_task_pairs_tail(cur, task_id)
            sql_delete_tail_ms = int((time.perf_counter() - started_at) * 1000)

        if save_hashes:
            started_at = time.perf_counter()
            _save_task_hashes(cur, task_id, city_hash, branch_hash)
            sql_save_hash_ms = int((time.perf_counter() - started_at) * 1000)

        started_at = time.perf_counter()
        _clear_task_pairs_tmp(cur)
        sql_clear_tmp_ms = int((time.perf_counter() - started_at) * 1000)

    return {
        "written": int(written),
        "deleted": int(deleted),
        "sql_stage_ms": int(sql_stage_ms),
        "sql_upsert_ms": int(sql_upsert_ms),
        "sql_delete_tail_ms": int(sql_delete_tail_ms),
        "sql_save_hash_ms": int(sql_save_hash_ms),
        "sql_clear_tmp_ms": int(sql_clear_tmp_ms),
    }


def run_initial_once() -> Dict[str, Any]:
    task, lock_token, sql_pick_ms = _pick_initial_task()
    if not task:
        result = {"mode": "noop", "reason": "no_initial_task", "sql_pick_ms": int(sql_pick_ms)}
        log(LOG_FILE, folder=LOG_FOLDER, message=json.dumps({"event": "expand_cb_pairs_initial", **result}, ensure_ascii=False, default=str))
        return result

    task_id = int(task["task_id"])
    try:
        mode = "noop"
        reason = ""
        city_hash = 0
        branch_hash = 0
        plz_cnt = 0
        branch_cnt = 0
        full_pairs_cnt = 0
        selected_pairs: List[PairRate] = []
        written = 0
        deleted = 0
        sql_full_snapshot_ms = 0
        sql_check_has_pairs_ms = 0
        sql_snapshot_ms = 0
        sql_check_full_ms = 0
        sql_check_uncollected_ms = 0
        sql_existing_keys_ms = 0
        sql_stage_ms = 0
        sql_upsert_ms = 0
        sql_delete_tail_ms = 0
        sql_save_hash_ms = 0
        sql_clear_tmp_ms = 0
        has_watermark_uncollected = False
        stale = False
        need_full_refresh = False
        need_topup = False

        started_at = time.perf_counter()
        has_pairs = _has_task_row_at_rank(task_id, 1)
        sql_check_has_pairs_ms = int((time.perf_counter() - started_at) * 1000)

        if has_pairs:
            mode = "noop"
            reason = "already_has_pairs"
        else:
            started_at = time.perf_counter()
            city_hash, branch_hash, full_pairs, plz_cnt, branch_cnt = _load_full_snapshot(task_id)
            sql_full_snapshot_ms = int((time.perf_counter() - started_at) * 1000)
            full_pairs_cnt = len(full_pairs)

            if not full_pairs_cnt:
                mode = "noop"
                reason = "empty_expansion"
            else:
                mode = "initial_insert"
                selected_pairs = list(full_pairs[:int(INITIAL_EXPAND_LIMIT)])
                write_stats = _write_selected_pairs(
                    task_id,
                    selected_pairs,
                    city_hash=int(city_hash),
                    branch_hash=int(branch_hash),
                    delete_tail=True,
                    save_hashes=True,
                )
                written = int(write_stats["written"])
                deleted = int(write_stats["deleted"])
                sql_stage_ms = int(write_stats["sql_stage_ms"])
                sql_upsert_ms = int(write_stats["sql_upsert_ms"])
                sql_delete_tail_ms = int(write_stats["sql_delete_tail_ms"])
                sql_save_hash_ms = int(write_stats["sql_save_hash_ms"])
                sql_clear_tmp_ms = int(write_stats["sql_clear_tmp_ms"])

        result = {
            "mode": str(mode),
            "task_id": task_id,
            "city_hash": int(city_hash),
            "branch_hash": int(branch_hash),
            "has_pairs": bool(has_pairs),
            "has_watermark_uncollected": bool(has_watermark_uncollected),
            "stale": bool(stale),
            "need_full_refresh": bool(need_full_refresh),
            "need_topup": bool(need_topup),
            "plz_cnt": int(plz_cnt),
            "branch_cnt": int(branch_cnt),
            "full_pairs_cnt": int(full_pairs_cnt),
            "selected_pairs_cnt": len(selected_pairs),
            "written": int(written),
            "deleted": int(deleted),
            "low_watermark": int(LOW_WATERMARK),
            "expand_limit": int(INITIAL_EXPAND_LIMIT),
            "sql_pick_ms": int(sql_pick_ms),
            "sql_snapshot_ms": int(sql_snapshot_ms),
            "sql_full_snapshot_ms": int(sql_full_snapshot_ms),
            "sql_check_has_pairs_ms": int(sql_check_has_pairs_ms),
            "sql_check_full_ms": int(sql_check_full_ms),
            "sql_check_uncollected_ms": int(sql_check_uncollected_ms),
            "sql_existing_keys_ms": int(sql_existing_keys_ms),
            "sql_stage_ms": int(sql_stage_ms),
            "sql_upsert_ms": int(sql_upsert_ms),
            "sql_delete_tail_ms": int(sql_delete_tail_ms),
            "sql_save_hash_ms": int(sql_save_hash_ms),
            "sql_clear_tmp_ms": int(sql_clear_tmp_ms),
        }
        if reason:
            result["reason"] = str(reason)
        log(LOG_FILE, folder=LOG_FOLDER, message=json.dumps({"event": "expand_cb_pairs_initial", **result}, ensure_ascii=False, default=str))
        return result
    finally:
        _release_task_lock(task_id, lock_token)


def run_active_once() -> Dict[str, Any]:
    task, lock_token, sql_pick_ms = _pick_active_task()
    if not task:
        result = {"mode": "noop", "reason": "no_active_task", "sql_pick_ms": int(sql_pick_ms)}
        log(LOG_FILE, folder=LOG_FOLDER, message=json.dumps({"event": "expand_cb_pairs_active", **result}, ensure_ascii=False, default=str))
        return result

    task_id = int(task["task_id"])
    try:
        mode = "noop"
        reason = ""
        city_hash = 0
        branch_hash = 0
        plz_cnt = 0
        branch_cnt = 0
        full_pairs_cnt = 0
        selected_pairs: List[PairRate] = []
        written = 0
        has_watermark_uncollected = False
        stale = False
        need_full_refresh = False
        need_topup = False
        deleted = 0
        sql_snapshot_ms = 0
        sql_full_snapshot_ms = 0
        sql_check_has_pairs_ms = 0
        sql_check_full_ms = 0
        sql_check_uncollected_ms = 0
        sql_existing_keys_ms = 0
        sql_stage_ms = 0
        sql_upsert_ms = 0
        sql_delete_tail_ms = 0
        sql_save_hash_ms = 0
        sql_clear_tmp_ms = 0
        zeroed_rates = 0

        started_at = time.perf_counter()
        has_pairs = _has_task_row_at_rank(task_id, 1)
        sql_check_has_pairs_ms = int((time.perf_counter() - started_at) * 1000)

        if not has_pairs:
            started_at = time.perf_counter()
            city_hash, branch_hash, full_pairs, plz_cnt, branch_cnt = _load_full_snapshot(task_id)
            sql_full_snapshot_ms = int((time.perf_counter() - started_at) * 1000)
            full_pairs_cnt = len(full_pairs)
            if not full_pairs_cnt:
                mode = "noop"
                reason = "empty_expansion"
            else:
                target_cnt = min(int(INITIAL_EXPAND_LIMIT), int(full_pairs_cnt))
                selected_pairs = list(full_pairs[:target_cnt])
                write_stats = _write_selected_pairs(
                    task_id,
                    selected_pairs,
                    city_hash=int(city_hash),
                    branch_hash=int(branch_hash),
                    delete_tail=True,
                    save_hashes=True,
                )
                written = int(write_stats["written"])
                deleted = int(write_stats["deleted"])
                sql_stage_ms = int(write_stats["sql_stage_ms"])
                sql_upsert_ms = int(write_stats["sql_upsert_ms"])
                sql_delete_tail_ms = int(write_stats["sql_delete_tail_ms"])
                sql_save_hash_ms = int(write_stats["sql_save_hash_ms"])
                sql_clear_tmp_ms = int(write_stats["sql_clear_tmp_ms"])
                mode = "insert"
        else:
            started_at = time.perf_counter()
            city_hash, branch_hash, plz_cnt, branch_cnt = _load_task_state(task_id)
            sql_snapshot_ms = int((time.perf_counter() - started_at) * 1000)
            stale = (
                int(task["rating_city_hash"]) != int(city_hash) if task["rating_city_hash"] is not None else True
            ) or (
                int(task["rating_branch_hash"]) != int(branch_hash) if task["rating_branch_hash"] is not None else True
            )

            if stale:
                need_full_refresh = True

                started_at = time.perf_counter()
                current_pair_cnt = _load_task_pair_count_without_count(task_id, only_positive=False)
                sql_check_full_ms = int((time.perf_counter() - started_at) * 1000)

                started_at = time.perf_counter()
                city_hash, branch_hash, full_pairs, plz_cnt, branch_cnt = _load_full_snapshot(task_id)
                sql_full_snapshot_ms = int((time.perf_counter() - started_at) * 1000)
                full_pairs_cnt = len(full_pairs)

                if full_pairs_cnt <= 0:
                    zeroed_rates = _zero_task_rates(task_id, city_hash=int(city_hash), branch_hash=int(branch_hash))
                    mode = "update_zero_rates"
                    reason = "empty_expansion_zeroed"
                else:
                    base_target = min(int(current_pair_cnt), int(full_pairs_cnt))
                    base_pairs = list(full_pairs[:base_target])
                    started_at = time.perf_counter()
                    has_watermark_uncollected = _has_watermark_uncollected_in_pairs(base_pairs)
                    sql_check_uncollected_ms = int((time.perf_counter() - started_at) * 1000)

                    final_target = int(base_target)
                    if not has_watermark_uncollected:
                        grown_target = min(int(base_target) + int(TOPUP_INTERVAL), int(full_pairs_cnt))
                        if grown_target > final_target:
                            need_topup = True
                            final_target = int(grown_target)

                    selected_pairs = list(full_pairs[:final_target])
                    if selected_pairs:
                        final_stats = _write_selected_pairs(
                            task_id,
                            selected_pairs,
                            city_hash=int(city_hash),
                            branch_hash=int(branch_hash),
                            delete_tail=True,
                            save_hashes=True,
                        )
                        written = int(final_stats["written"])
                        deleted = int(final_stats["deleted"])
                        sql_stage_ms = int(final_stats["sql_stage_ms"])
                        sql_upsert_ms = int(final_stats["sql_upsert_ms"])
                        sql_delete_tail_ms = int(final_stats["sql_delete_tail_ms"])
                        sql_save_hash_ms = int(final_stats["sql_save_hash_ms"])
                        sql_clear_tmp_ms = int(final_stats["sql_clear_tmp_ms"])

                    mode = "update_refresh_topup" if need_topup else "update_refresh"
            else:
                started_at = time.perf_counter()
                has_watermark_uncollected = _has_watermark_uncollected(task_id)
                sql_check_uncollected_ms = int((time.perf_counter() - started_at) * 1000)

                if has_watermark_uncollected:
                    mode = "update_noop"
                else:
                    need_topup = True
                    started_at = time.perf_counter()
                    current_pair_cnt = _load_task_pair_count_without_count(task_id, only_positive=True)
                    sql_check_full_ms = int((time.perf_counter() - started_at) * 1000)

                    started_at = time.perf_counter()
                    city_hash, branch_hash, full_pairs, plz_cnt, branch_cnt = _load_full_snapshot(task_id)
                    sql_full_snapshot_ms = int((time.perf_counter() - started_at) * 1000)
                    full_pairs_cnt = len(full_pairs)

                    if full_pairs_cnt <= 0:
                        mode = "update_noop"
                        reason = "empty_expansion"
                    else:
                        target_cnt = min(int(current_pair_cnt) + int(TOPUP_INTERVAL), int(full_pairs_cnt))
                        if target_cnt <= int(current_pair_cnt):
                            mode = "update_noop"
                            reason = "snapshot_exhausted"
                        else:
                            selected_pairs = list(full_pairs[:target_cnt])
                            write_stats = _write_selected_pairs(
                                task_id,
                                selected_pairs,
                                city_hash=int(city_hash),
                                branch_hash=int(branch_hash),
                                delete_tail=True,
                                save_hashes=False,
                            )
                            written = int(write_stats["written"])
                            deleted = int(write_stats["deleted"])
                            sql_stage_ms = int(write_stats["sql_stage_ms"])
                            sql_upsert_ms = int(write_stats["sql_upsert_ms"])
                            sql_delete_tail_ms = int(write_stats["sql_delete_tail_ms"])
                            sql_clear_tmp_ms = int(write_stats["sql_clear_tmp_ms"])
                            mode = "update_topup"

        result = {
            "mode": str(mode),
            "task_id": task_id,
            "city_hash": int(city_hash),
            "branch_hash": int(branch_hash),
            "has_pairs": bool(has_pairs),
            "has_watermark_uncollected": bool(has_watermark_uncollected),
            "stale": bool(stale),
            "need_full_refresh": bool(need_full_refresh),
            "need_topup": bool(need_topup),
            "plz_cnt": int(plz_cnt),
            "branch_cnt": int(branch_cnt),
            "full_pairs_cnt": int(full_pairs_cnt),
            "selected_pairs_cnt": len(selected_pairs),
            "written": int(written),
            "deleted": int(deleted),
            "zeroed_rates": int(zeroed_rates),
            "low_watermark": int(LOW_WATERMARK),
            "expand_limit": int(INITIAL_EXPAND_LIMIT),
            "topup_interval": int(TOPUP_INTERVAL),
            "sql_pick_ms": int(sql_pick_ms),
            "sql_snapshot_ms": int(sql_snapshot_ms),
            "sql_full_snapshot_ms": int(sql_full_snapshot_ms),
            "sql_check_has_pairs_ms": int(sql_check_has_pairs_ms),
            "sql_check_full_ms": int(sql_check_full_ms),
            "sql_check_uncollected_ms": int(sql_check_uncollected_ms),
            "sql_existing_keys_ms": int(sql_existing_keys_ms),
            "sql_stage_ms": int(sql_stage_ms),
            "sql_upsert_ms": int(sql_upsert_ms),
            "sql_delete_tail_ms": int(sql_delete_tail_ms),
            "sql_save_hash_ms": int(sql_save_hash_ms),
            "sql_clear_tmp_ms": int(sql_clear_tmp_ms),
        }
        if reason:
            result["reason"] = str(reason)
        log(LOG_FILE, folder=LOG_FOLDER, message=json.dumps({"event": "expand_cb_pairs_active", **result}, ensure_ascii=False, default=str))
        return result
    finally:
        _release_task_lock(task_id, lock_token)


def run_once() -> Dict[str, Any]:
    return run_active_once()


def main() -> None:
    print(json.dumps(run_active_once(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
