# FILE: engine/core_tasks/ready_tasks.py
# DATE: 2026-03-25
# PURPOSE: Recomputes the ready flag for active audience tasks based on the presence
# and completeness of branch/city ratings.

from __future__ import annotations

import json
from typing import Any, Dict

from engine.common.db import get_connection
from engine.common.logs import log


LOG_FILE = "ready_tasks.log"
LOG_FOLDER = "processing"


def run_once() -> Dict[str, Any]:
    import time

    t0 = time.perf_counter()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH task_ready AS (
              SELECT
                t.id AS task_id,
                (
                  EXISTS (
                    SELECT 1
                    FROM public.task_branch_ratings tbr
                    WHERE tbr.task_id = t.id
                  )
                  AND EXISTS (
                    SELECT 1
                    FROM public.task_city_ratings tcr
                    WHERE tcr.task_id = t.id
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM public.task_branch_ratings tbr
                    WHERE tbr.task_id = t.id
                      AND tbr.rate IS NULL
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM public.task_city_ratings tcr
                    WHERE tcr.task_id = t.id
                      AND tcr.rate IS NULL
                  )
                ) AS ready_value
              FROM public.aap_audience_audiencetask t
              WHERE t.archived = false
                AND t.collected = false
            ),
            upd AS (
              UPDATE public.aap_audience_audiencetask t
              SET ready = tr.ready_value
              FROM task_ready tr
              WHERE t.id = tr.task_id
                AND t.ready IS DISTINCT FROM tr.ready_value
              RETURNING t.id, t.ready
            )
            SELECT
              (SELECT COUNT(*)::int FROM task_ready) AS scanned_cnt,
              (SELECT COUNT(*)::int FROM upd) AS updated_cnt,
              (SELECT COUNT(*)::int FROM upd WHERE ready = true) AS ready_true_cnt,
              (SELECT COUNT(*)::int FROM upd WHERE ready = false) AS ready_false_cnt
            """
        )
        row = cur.fetchone()
    sql_ms = int((time.perf_counter() - t0) * 1000)

    result = {
        "mode": "ok",
        "scanned_cnt": int(row[0] or 0),
        "updated_cnt": int(row[1] or 0),
        "ready_true_cnt": int(row[2] or 0),
        "ready_false_cnt": int(row[3] or 0),
        "sql_ms": int(sql_ms),
    }
    log(LOG_FILE, folder=LOG_FOLDER, message=json.dumps({"event": "ready_tasks", **result}, ensure_ascii=False))
    return result
