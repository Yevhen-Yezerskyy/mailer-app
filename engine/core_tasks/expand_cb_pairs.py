# FILE: engine/core_tasks/expand_cb_pairs.py
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


EXPAND_LIMIT = 50_000
LOW_WATERMARK = 1_000
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


def _pick_task() -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    owner = f"{os.getpid()}:{int(time.time())}"
    started_at = time.perf_counter()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, ready, archived, collected, rating_city_hash, rating_branch_hash
            FROM public.aap_audience_audiencetask
            WHERE ready = true
              AND archived = false
              AND collected = false
            ORDER BY random()
            LIMIT 1
            """
        )
        row = cur.fetchone()
    sql_pick_ms = int((time.perf_counter() - started_at) * 1000)
    if not row:
        return None, None, sql_pick_ms

    task_id = int(row[0])
    lock_key = f"core_tasks:expand_cb_pairs:task:{int(task_id)}"
    resp = CLIENT.lock_try(lock_key, ttl_sec=TASK_LOCK_TTL_SEC, owner=owner)
    if not resp or resp.get("acquired") is not True or not isinstance(resp.get("token"), str):
        return None, None, sql_pick_ms

    token = str(resp["token"])
    task = {
        "task_id": int(row[0]),
        "ready": bool(row[1]),
        "archived": bool(row[2]),
        "collected": bool(row[3]),
        "rating_city_hash": int(row[4]) if row[4] is not None else None,
        "rating_branch_hash": int(row[5]) if row[5] is not None else None,
    }
    if not task["ready"] or task["archived"] or task["collected"]:
        try:
            CLIENT.lock_release(lock_key, token=token)
        except Exception:
            pass
        return None, None, sql_pick_ms

    return task, token, sql_pick_ms


def run_once() -> Dict[str, Any]:
    task, lock_token, sql_pick_ms = _pick_task()
    if not task:
        result = {"mode": "noop", "reason": "no_ready_task", "sql_pick_ms": int(sql_pick_ms)}
        log(LOG_FILE, folder=LOG_FOLDER, message=json.dumps({"event": "expand_cb_pairs", **result}, ensure_ascii=False, default=str))
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
        has_1000th_unprocessed = False
        stale = False
        need_full_refresh = False
        need_topup = False
        deleted = 0
        sql_snapshot_ms = 0
        sql_full_snapshot_ms = 0
        sql_check_has_pairs_ms = 0
        sql_check_full_ms = 0
        sql_check_unprocessed_ms = 0
        sql_existing_keys_ms = 0
        sql_stage_ms = 0
        sql_upsert_ms = 0
        sql_delete_tail_ms = 0
        sql_save_hash_ms = 0
        sql_clear_tmp_ms = 0

        started_at = time.perf_counter()
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM public.task_cb_ratings
                WHERE task_id = %s
                LIMIT 1
                """,
                (int(task_id),),
            )
            has_pairs = cur.fetchone() is not None
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
                mode = "insert"
                selected_pairs = list(full_pairs[:EXPAND_LIMIT])
        else:
            started_at = time.perf_counter()
            city_hash, branch_hash, plz_cnt, branch_cnt = _load_task_state(task_id)
            sql_snapshot_ms = int((time.perf_counter() - started_at) * 1000)
            full_pairs_cnt = int(plz_cnt) * int(branch_cnt)
            stale = (
                int(task["rating_city_hash"]) != int(city_hash) if task["rating_city_hash"] is not None else True
            ) or (
                int(task["rating_branch_hash"]) != int(branch_hash) if task["rating_branch_hash"] is not None else True
            )
            if stale:
                started_at = time.perf_counter()
                city_hash, branch_hash, full_pairs, plz_cnt, branch_cnt = _load_full_snapshot(task_id)
                sql_full_snapshot_ms = int((time.perf_counter() - started_at) * 1000)
                full_pairs_cnt = len(full_pairs)
                need_full_refresh = True
                if not full_pairs_cnt:
                    mode = "update_noop"
                    reason = "empty_expansion"
                else:
                    mode = "update_refresh"
                    selected_pairs = list(full_pairs[:EXPAND_LIMIT])
            else:
                if not full_pairs_cnt:
                    mode = "update_noop"
                    reason = "empty_expansion"
                elif full_pairs_cnt < EXPAND_LIMIT:
                    mode = "update_noop"
                else:
                    started_at = time.perf_counter()
                    with get_connection() as conn, conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT 1
                            FROM public.task_cb_ratings
                            WHERE task_id = %s
                            ORDER BY rate ASC, cb_id ASC
                            OFFSET %s
                            LIMIT 1
                            """,
                            (int(task_id), int(full_pairs_cnt - 1)),
                        )
                        has_last_pair = cur.fetchone() is not None
                    sql_check_full_ms = int((time.perf_counter() - started_at) * 1000)

                    if has_last_pair:
                        mode = "update_noop"
                    else:
                        started_at = time.perf_counter()
                        with get_connection() as conn, conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT 1
                                FROM public.task_cb_ratings
                                WHERE task_id = %s
                                  AND processed = false
                                ORDER BY rate ASC, cb_id ASC
                                OFFSET %s
                                LIMIT 1
                                """,
                                (int(task_id), int(LOW_WATERMARK - 1)),
                            )
                            has_1000th_unprocessed = cur.fetchone() is not None
                        sql_check_unprocessed_ms = int((time.perf_counter() - started_at) * 1000)

                        need_topup = bool(not has_1000th_unprocessed)
                        if not need_topup:
                            mode = "update_noop"
                        else:
                            started_at = time.perf_counter()
                            full_city_hash, full_branch_hash, full_pairs, plz_cnt, branch_cnt = _load_full_snapshot(task_id)
                            sql_full_snapshot_ms = int((time.perf_counter() - started_at) * 1000)
                            city_hash = int(full_city_hash)
                            branch_hash = int(full_branch_hash)
                            full_pairs_cnt = len(full_pairs)
                            if (
                                int(task["rating_city_hash"]) != int(city_hash) if task["rating_city_hash"] is not None else True
                            ) or (
                                int(task["rating_branch_hash"]) != int(branch_hash) if task["rating_branch_hash"] is not None else True
                            ):
                                stale = True
                                need_topup = False
                                need_full_refresh = True
                                if not full_pairs_cnt:
                                    mode = "update_noop"
                                    reason = "empty_expansion"
                                else:
                                    mode = "update_refresh"
                                    selected_pairs = list(full_pairs[:EXPAND_LIMIT])
                            else:
                                started_at = time.perf_counter()
                                with get_connection() as conn, conn.cursor() as cur:
                                    cur.execute(
                                        """
                                        SELECT cb.plz_id, cb.branch_id
                                        FROM public.task_cb_ratings tcr
                                        JOIN public.cb_crawl_pairs cb
                                          ON cb.id = tcr.cb_id
                                        WHERE tcr.task_id = %s
                                        """,
                                        (int(task_id),),
                                    )
                                    existing_keys = {
                                        (int(row[0]), int(row[1]))
                                        for row in (cur.fetchall() or [])
                                        if row
                                    }
                                sql_existing_keys_ms = int((time.perf_counter() - started_at) * 1000)

                                if len(existing_keys) >= full_pairs_cnt:
                                    mode = "update_noop"
                                else:
                                    mode = "update_topup"
                                    seen: Set[Tuple[int, int]] = set()
                                    for plz_id, branch_id, rate in full_pairs:
                                        key = (int(plz_id), int(branch_id))
                                        if key in existing_keys or key in seen:
                                            continue
                                        seen.add(key)
                                        selected_pairs.append((int(plz_id), int(branch_id), int(rate)))
                                        if len(selected_pairs) >= int(EXPAND_LIMIT):
                                            break
                                    if not selected_pairs:
                                        mode = "update_noop"

        if selected_pairs:
            if not has_pairs:
                with get_connection() as conn, conn.cursor() as cur:
                    started_at = time.perf_counter()
                    _stage_task_pairs(cur, selected_pairs)
                    sql_stage_ms = int((time.perf_counter() - started_at) * 1000)
                    started_at = time.perf_counter()
                    written = _upsert_task_pairs(cur, task_id)
                    sql_upsert_ms = int((time.perf_counter() - started_at) * 1000)
                    started_at = time.perf_counter()
                    _save_task_hashes(cur, task_id, city_hash, branch_hash)
                    sql_save_hash_ms = int((time.perf_counter() - started_at) * 1000)
                    started_at = time.perf_counter()
                    _clear_task_pairs_tmp(cur)
                    sql_clear_tmp_ms = int((time.perf_counter() - started_at) * 1000)
            elif need_full_refresh:
                with get_connection() as conn, conn.cursor() as cur:
                    started_at = time.perf_counter()
                    _stage_task_pairs(cur, selected_pairs)
                    sql_stage_ms = int((time.perf_counter() - started_at) * 1000)
                    started_at = time.perf_counter()
                    written = _upsert_task_pairs(cur, task_id)
                    sql_upsert_ms = int((time.perf_counter() - started_at) * 1000)
                    started_at = time.perf_counter()
                    deleted = _delete_task_pairs_tail(cur, task_id)
                    sql_delete_tail_ms = int((time.perf_counter() - started_at) * 1000)
                    started_at = time.perf_counter()
                    _save_task_hashes(cur, task_id, city_hash, branch_hash)
                    sql_save_hash_ms = int((time.perf_counter() - started_at) * 1000)
                    started_at = time.perf_counter()
                    _clear_task_pairs_tmp(cur)
                    sql_clear_tmp_ms = int((time.perf_counter() - started_at) * 1000)
            else:
                with get_connection() as conn, conn.cursor() as cur:
                    started_at = time.perf_counter()
                    _stage_task_pairs(cur, selected_pairs)
                    sql_stage_ms = int((time.perf_counter() - started_at) * 1000)
                    started_at = time.perf_counter()
                    written = _upsert_task_pairs(cur, task_id)
                    sql_upsert_ms = int((time.perf_counter() - started_at) * 1000)
                    started_at = time.perf_counter()
                    _clear_task_pairs_tmp(cur)
                    sql_clear_tmp_ms = int((time.perf_counter() - started_at) * 1000)

        result = {
            "mode": str(mode),
            "task_id": task_id,
            "city_hash": int(city_hash),
            "branch_hash": int(branch_hash),
            "has_pairs": bool(has_pairs),
            "has_1000th_unprocessed": bool(has_1000th_unprocessed),
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
            "expand_limit": int(EXPAND_LIMIT),
            "sql_pick_ms": int(sql_pick_ms),
            "sql_snapshot_ms": int(sql_snapshot_ms),
            "sql_full_snapshot_ms": int(sql_full_snapshot_ms),
            "sql_check_has_pairs_ms": int(sql_check_has_pairs_ms),
            "sql_check_full_ms": int(sql_check_full_ms),
            "sql_check_unprocessed_ms": int(sql_check_unprocessed_ms),
            "sql_existing_keys_ms": int(sql_existing_keys_ms),
            "sql_stage_ms": int(sql_stage_ms),
            "sql_upsert_ms": int(sql_upsert_ms),
            "sql_delete_tail_ms": int(sql_delete_tail_ms),
            "sql_save_hash_ms": int(sql_save_hash_ms),
            "sql_clear_tmp_ms": int(sql_clear_tmp_ms),
        }
        if reason:
            result["reason"] = str(reason)
        log(LOG_FILE, folder=LOG_FOLDER, message=json.dumps({"event": "expand_cb_pairs", **result}, ensure_ascii=False, default=str))
        return result
    finally:
        if lock_token:
            try:
                CLIENT.lock_release(f"core_tasks:expand_cb_pairs:task:{int(task_id)}", token=lock_token)
            except Exception:
                pass


def main() -> None:
    print(json.dumps(run_once(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
