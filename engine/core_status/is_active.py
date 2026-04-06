# FILE: engine/core_status/is_active.py
# DATE: 2026-04-06
# PURPOSE: Helper functions for audience task active evaluation.

from __future__ import annotations

import json

from engine.common.cache.client import CLIENT
from engine.common.db import fetch_one


TEST_RATE_LIMIT = 60
TEST_GOOD_RATE_LIMIT = 20
FULL_RATE_LIMIT = 1500
FULL_GOOD_RATE_LIMIT = 500
FULL_RATE_SAMPLE_LIMIT = FULL_RATE_LIMIT // 10
CACHE_TTL_SEC = 7 * 24 * 60 * 60


def _full_state_cache_key(task_id: int) -> str:
    return f"core_status:is_more_needed:state:{int(task_id)}"


def _get_task_meta(task_id: int) -> tuple[str, int]:
    row = fetch_one(
        """
        SELECT
            COALESCE(LOWER(w.access_type), '') AS access_type,
            COALESCE(t.rate_limit, 0)::int AS rate_limit
        FROM public.aap_audience_audiencetask t
        JOIN public.accounts_workspaces w
          ON w.id = t.workspace_id
        WHERE t.id = %s
        """,
        [int(task_id)],
    )
    if not row:
        return ("", 0)
    return (str(row[0] or "").strip().lower(), int(row[1] or 0))


def _count_rated_good(task_id: int, rate_limit: int) -> int:
    row = fetch_one(
        """
        SELECT COUNT(*)::int
        FROM public.sending_lists sl
        WHERE sl.task_id = %s
          AND sl.rate IS NOT NULL
          AND sl.rate < %s
        """,
        [int(task_id), int(rate_limit)],
    )
    return int((row or [0])[0] or 0)


def _count_rated(task_id: int) -> int:
    row = fetch_one(
        """
        SELECT COUNT(*)::int
        FROM public.sending_lists sl
        WHERE sl.task_id = %s
          AND sl.rate IS NOT NULL
        """,
        [int(task_id)],
    )
    return int((row or [0])[0] or 0)


def _get_full_state(task_id: int) -> str:
    raw = CLIENT.get(_full_state_cache_key(int(task_id)), ttl_sec=CACHE_TTL_SEC)
    if raw is None:
        return ""
    try:
        return bytes(raw).decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def _write_full_stats(task_id: int, rate_limit: int) -> None:
    row = fetch_one(
        """
        WITH latest AS (
            SELECT sl.rate
            FROM public.sending_lists sl
            WHERE sl.task_id = %s
              AND sl.rate IS NOT NULL
            ORDER BY sl.updated_at DESC NULLS LAST, sl.aggr_contact_cb_id DESC
            LIMIT %s
        )
        SELECT
            COUNT(*) FILTER (WHERE rate < %s)::int AS good_cnt,
            COUNT(*) FILTER (WHERE rate >= %s)::int AS bad_cnt,
            COUNT(*)::int AS total_cnt
        FROM latest
        """,
        [int(task_id), int(FULL_RATE_SAMPLE_LIMIT), int(rate_limit), int(rate_limit)],
    )
    payload = {
        "good_cnt": int((row or [0, 0, 0])[0] or 0),
        "bad_cnt": int((row or [0, 0, 0])[1] or 0),
        "rate_limit": int(rate_limit),
        "total_cnt": int((row or [0, 0, 0])[2] or 0),
    }
    CLIENT.set(
        _full_state_cache_key(int(task_id)),
        json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ttl_sec=CACHE_TTL_SEC,
    )


def _is_more_needed(task_id: int) -> bool:
    access_type, rate_limit = _get_task_meta(int(task_id))

    if access_type == "test":
        if _count_rated_good(int(task_id), int(rate_limit)) >= TEST_GOOD_RATE_LIMIT:
            return False
        if _count_rated(int(task_id)) >= TEST_RATE_LIMIT:
            return False
        return True

    if access_type in {"full", "super", "custom"}:
        if _count_rated_good(int(task_id), int(rate_limit)) >= FULL_GOOD_RATE_LIMIT:
            return False

        if _count_rated(int(task_id)) >= FULL_RATE_LIMIT:
            state = _get_full_state(int(task_id))
            if state == "Continue":
                return True
            if state:
                return False
            _write_full_stats(int(task_id), int(rate_limit))
            return False

        return True

    if access_type == "stat_only":
        return False

    if access_type == "closed":
        return False

    return False


def is_more_needed(task_id: int, update: bool = False) -> bool:
    # NOTE: when we add task.rate_limit edits, refresh logic for these task cache keys there too.
    _ = bool(update)
    return bool(_is_more_needed(int(task_id)))
