# FILE: engine/core_status/is_active.py
# DATE: 2026-04-06
# PURPOSE: Helper functions for audience task active evaluation.

from __future__ import annotations

from engine.common.cache.client import memo
from engine.common.db import fetch_one


def _is_more_needed(task_id: int) -> bool:
    row = fetch_one(
        """
        SELECT
            COUNT(*)::int AS cnt,
            COALESCE(w.access_type, '') AS access_type
        FROM public.sending_lists sl
        JOIN public.aap_audience_audiencetask t
          ON t.id = sl.task_id
        JOIN public.accounts_workspaces w
          ON w.id = t.workspace_id
        WHERE sl.task_id = %s
          AND sl.rate < t.rate_limit
        GROUP BY w.access_type
        """,
        [int(task_id)],
    )
    cnt = int((row or [0, ""])[0] or 0)
    access_type = str((row or [0, ""])[1] or "").strip().lower()
    limit = 20 if access_type == "test" else 500
    return cnt < limit


def is_more_needed(task_id: int, update: bool = False) -> bool:
    # NOTE: when we add task.rate_limit edits, refresh this cache there too via update=True.
    return bool(
        memo(
            int(task_id),
            _is_more_needed,
            version="core_status_is_more_needed_v1",
            update=bool(update),
        )
    )
