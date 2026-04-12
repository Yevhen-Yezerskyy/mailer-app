# FILE: engine/core_status/status.py
# DATE: 2026-04-05
# PURPOSE: Temporary status helpers and audience task active recalculation.

from __future__ import annotations

from engine.common.db import get_connection
from engine.core_status.is_active import is_more_needed


def is_active(task: dict[str, object]) -> bool:
    if not bool(task.get("ready")):
        return False
    if bool(task.get("archived")):
        return False
    if not bool(task.get("user_active")):
        return False
    if not bool(is_more_needed(int(task["id"]))):
        return False
    return True


def run_ready_once() -> dict[str, int | str]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH task_scan AS (
                SELECT t.id::bigint AS task_id
                FROM public.aap_audience_audiencetask t
                WHERE COALESCE(t.archived, false) = false
            ),
            task_state AS (
                SELECT ts.task_id
                FROM task_scan ts
                WHERE EXISTS (
                    SELECT 1
                    FROM public.task_cb_ratings tcr
                    WHERE tcr.task_id = ts.task_id
                      AND tcr.rate > 0
                )
            ),
            upd AS (
                UPDATE public.aap_audience_audiencetask t
                SET ready = CASE
                                WHEN ts.task_id IS NOT NULL THEN true
                                ELSE false
                            END,
                    updated_at = now()
                FROM task_scan s
                LEFT JOIN task_state ts
                  ON ts.task_id = s.task_id
                WHERE t.id = s.task_id
                  AND t.ready IS DISTINCT FROM CASE
                                                   WHEN ts.task_id IS NOT NULL THEN true
                                                   ELSE false
                                               END
                RETURNING t.id
            )
            SELECT
                (SELECT COUNT(*)::int FROM task_scan) AS scanned_cnt,
                (SELECT COUNT(*)::int FROM task_state) AS matched_cnt,
                (SELECT COUNT(*)::int FROM upd) AS updated_cnt
            """
        )
        row = cur.fetchone() or [0, 0, 0]
        conn.commit()

    return {
        "mode": "ok",
        "scanned_cnt": int(row[0] or 0),
        "matched_cnt": int(row[1] or 0),
        "updated_cnt": int(row[2] or 0),
    }


def run_active_once() -> dict[str, int | str]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM public.aap_audience_audiencetask t
            ORDER BY t.id ASC
            """
        )
        rows = cur.fetchall() or []
        if not rows:
            return {
                "mode": "ok",
                "scanned_cnt": 0,
                "updated_cnt": 0,
                "active_true_cnt": 0,
                "active_false_cnt": 0,
            }

        columns = [str(desc[0]) for desc in (cur.description or [])]
        next_states: list[tuple[int, bool]] = []
        for row in rows:
            task = {column: row[idx] for idx, column in enumerate(columns)}
            next_states.append((int(task["id"]), bool(is_active(task))))

        task_ids = [task_id for task_id, _is_active in next_states]
        active_values = [is_active_value for _task_id, is_active_value in next_states]

        cur.execute(
            """
            WITH data(task_id, active_value) AS (
                SELECT * FROM unnest(%s::bigint[], %s::boolean[])
            ),
            upd AS (
                UPDATE public.aap_audience_audiencetask t
                SET active = data.active_value,
                    updated_at = now()
                FROM data
                WHERE t.id = data.task_id
                  AND t.active IS DISTINCT FROM data.active_value
                RETURNING t.id, t.active
            )
            SELECT COUNT(*)::int AS updated_cnt
            FROM upd
            """,
            [task_ids, active_values],
        )
        updated_cnt = int((cur.fetchone() or [0])[0] or 0)
        conn.commit()

    active_true_cnt = sum(1 for _task_id, is_active_value in next_states if is_active_value)
    active_false_cnt = len(next_states) - active_true_cnt
    return {
        "mode": "ok",
        "scanned_cnt": int(len(next_states)),
        "updated_cnt": int(updated_cnt),
        "active_true_cnt": int(active_true_cnt),
        "active_false_cnt": int(active_false_cnt),
    }
