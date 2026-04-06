# FILE: engine/core_status/is_active.py
# DATE: 2026-04-06
# PURPOSE: Helper functions for audience task active evaluation.

from __future__ import annotations

from engine.common.cache.client import memo
from engine.common.db import fetch_one


def _is_more_needed(task_id: int) -> bool:
    row = fetch_one(
        """
        SELECT COUNT(*)::int AS cnt
        FROM public.sending_lists sl
        JOIN public.aap_audience_audiencetask t
          ON t.id = sl.task_id
        WHERE sl.task_id = %s
          AND sl.rate < t.rate_limit
        """,
        [int(task_id)],
    )
    cnt = int((row or [0])[0] or 0)
    return cnt < 500


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
