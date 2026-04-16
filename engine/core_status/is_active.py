# FILE: engine/core_status/is_active.py
# DATE: 2026-04-06
# PURPOSE: Helper functions for audience task active evaluation.

from __future__ import annotations

import json
import random
from typing import Any

from engine.common.cache.client import CLIENT
from engine.common.db import fetch_all, fetch_one


LIMIT_TEST_CONTACTS_RATED = 60
LIMIT_TEST_CONTACTS_RATED_GOOD = 20

LIMIT_FULL_CONTACTS_RATED = 100
LIMIT_FULL_GOOD_BAD_RATED_RATIIO = 0.25
LIMIT_FULL_BAD_RATED_STEP = 3000

CACHE_TTL_SEC = 7 * 24 * 60 * 60
TEST_MORE_NEEDED_CACHE_TTL_MIN_SEC = 60 * 60
TEST_MORE_NEEDED_CACHE_TTL_MAX_SEC = 3 * 60 * 60
FULL_MORE_NEEDED_CACHE_TTL_MIN_SEC = 60 * 60
FULL_MORE_NEEDED_CACHE_TTL_MAX_SEC = 90 * 60


def _full_state_cache_key(task_id: int) -> str:
    return f"core_status:is_more_needed:state:{int(task_id)}"


def _full_more_needed_cache_key(task_id: int) -> str:
    return f"core_status:is_more_needed:full:{int(task_id)}"


def _test_more_needed_cache_key(task_id: int) -> str:
    return f"core_status:is_more_needed:test:{int(task_id)}"


def _test_more_needed_cache_ttl_sec() -> int:
    return int(random.randint(int(TEST_MORE_NEEDED_CACHE_TTL_MIN_SEC), int(TEST_MORE_NEEDED_CACHE_TTL_MAX_SEC)))


def _full_more_needed_cache_ttl_sec() -> int:
    return int(random.randint(int(FULL_MORE_NEEDED_CACHE_TTL_MIN_SEC), int(FULL_MORE_NEEDED_CACHE_TTL_MAX_SEC)))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


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


def _get_cached_bool(cache_key: str) -> bool | None:
    raw = CLIENT.get(str(cache_key), ttl_sec=CACHE_TTL_SEC)
    if raw is None:
        return None
    try:
        value = bytes(raw).decode("utf-8", errors="replace").strip().lower()
    except Exception:
        return None
    if value == "1":
        return True
    if value == "0":
        return False
    return None


def _get_test_more_needed(task_id: int) -> bool | None:
    return _get_cached_bool(_test_more_needed_cache_key(int(task_id)))


def _get_full_more_needed(task_id: int) -> bool | None:
    return _get_cached_bool(_full_more_needed_cache_key(int(task_id)))


def _set_cached_bool(cache_key: str, value: bool, ttl_sec: int) -> None:
    CLIENT.set(
        str(cache_key),
        (b"1" if bool(value) else b"0"),
        ttl_sec=int(ttl_sec),
    )


def _set_test_more_needed(task_id: int, value: bool) -> None:
    _set_cached_bool(_test_more_needed_cache_key(int(task_id)), bool(value), _test_more_needed_cache_ttl_sec())


def _set_full_more_needed(task_id: int, value: bool) -> None:
    _set_cached_bool(_full_more_needed_cache_key(int(task_id)), bool(value), _full_more_needed_cache_ttl_sec())


def _load_full_state_payload(task_id: int) -> dict[str, Any]:
    raw = CLIENT.get(_full_state_cache_key(int(task_id)), ttl_sec=CACHE_TTL_SEC)
    if raw is None:
        return {}
    try:
        text = bytes(raw).decode("utf-8", errors="replace").strip()
    except Exception:
        return {}
    if not text:
        return {}
    if text == "Continue":
        return {
            "mode": "continue_window",
            "step": int(LIMIT_FULL_BAD_RATED_STEP),
            "collected": 0,
            "remaining": int(LIMIT_FULL_BAD_RATED_STEP),
        }
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_full_state_payload(task_id: int, payload: dict[str, Any]) -> None:
    CLIENT.set(
        _full_state_cache_key(int(task_id)),
        json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ttl_sec=CACHE_TTL_SEC,
    )


def _clear_full_state(task_id: int) -> None:
    CLIENT.delete_many([_full_state_cache_key(int(task_id))])


def clear_is_more_needed_state_cache(task_id: int) -> None:
    _clear_full_state(int(task_id))


def clear_is_more_needed_full_cache(task_id: int) -> None:
    CLIENT.delete_many([_full_more_needed_cache_key(int(task_id))])


def invalidate_is_more_needed_cache(task_id: int) -> None:
    CLIENT.delete_many(
        [
            _test_more_needed_cache_key(int(task_id)),
            _full_more_needed_cache_key(int(task_id)),
        ]
    )


def start_full_continue_window(task_id: int) -> None:
    step_i = max(1, int(LIMIT_FULL_BAD_RATED_STEP))
    payload = {
        "mode": "continue_window",
        "step": int(step_i),
        "collected": 0,
        "remaining": int(step_i),
    }
    _save_full_state_payload(int(task_id), payload)
    invalidate_is_more_needed_cache(int(task_id))


def register_full_continue_progress(task_id: int, added_cnt: int) -> None:
    added_i = max(0, int(added_cnt))
    if added_i <= 0:
        return

    payload = _load_full_state_payload(int(task_id))
    if str(payload.get("mode") or "") != "continue_window":
        return

    step_i = max(1, _safe_int(payload.get("step"), int(LIMIT_FULL_BAD_RATED_STEP)))
    collected_i = max(0, _safe_int(payload.get("collected"), 0)) + int(added_i)
    if int(collected_i) >= int(step_i):
        _clear_full_state(int(task_id))
        return

    payload["step"] = int(step_i)
    payload["collected"] = int(collected_i)
    payload["remaining"] = int(step_i - collected_i)
    _save_full_state_payload(int(task_id), payload)


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
        [int(task_id), int(LIMIT_FULL_BAD_RATED_STEP), int(rate_limit), int(rate_limit)],
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


def _load_active_campaign_demands(task_id: int) -> list[tuple[int, int]]:
    rows = fetch_all(
        """
        SELECT
            COALESCE(c.sent_num, 0)::int AS sent_num,
            GREATEST(COALESCE(c.to_send_num, 0)::int - COALESCE(c.sent_num, 0)::int, 0)::int AS need_num
        FROM public.campaigns_campaigns c
        WHERE c.sending_list_id = %s
          AND COALESCE(c.archived, false) = false
        """,
        [int(task_id)],
    )
    out: list[tuple[int, int]] = []
    for row in rows or []:
        sent_i = max(0, _safe_int((row or [0, 0])[0], 0))
        need_i = max(0, _safe_int((row or [0, 0])[1], 0))
        out.append((int(sent_i), int(need_i)))
    return out


def _is_full_needed_by_campaigns(task_id: int, rate_limit: int) -> tuple[bool, int]:
    campaigns = _load_active_campaign_demands(int(task_id))
    if not campaigns:
        if _count_rated(int(task_id)) >= LIMIT_FULL_CONTACTS_RATED:
            return (False, int(LIMIT_FULL_CONTACTS_RATED))
        return (True, int(LIMIT_FULL_CONTACTS_RATED))

    good_total = _count_rated_good(int(task_id), int(rate_limit))
    all_campaigns_closed = True
    max_deficit = 0

    for sent_num, need_num in campaigns:
        need_i = max(0, int(need_num))
        if need_i <= 0:
            continue
        available_i = max(0, int(good_total) - int(sent_num))
        if int(available_i) > int(need_i):
            continue
        all_campaigns_closed = False
        deficit_i = max(0, int(need_i) - int(available_i))
        if int(deficit_i) > int(max_deficit):
            max_deficit = int(deficit_i)

    if all_campaigns_closed:
        return (False, 1)
    return (True, max(1, int(max_deficit)))


def _load_latest_quality_stats(task_id: int, rate_limit: int, sample_size: int) -> tuple[int, int, int]:
    sample_i = max(1, int(sample_size))
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
        [int(task_id), int(sample_i), int(rate_limit), int(rate_limit)],
    )
    return (
        _safe_int((row or [0, 0, 0])[0], 0),
        _safe_int((row or [0, 0, 0])[1], 0),
        _safe_int((row or [0, 0, 0])[2], 0),
    )


def _compute_full_more_needed(task_id: int, rate_limit: int) -> bool:
    needed, sample_size = _is_full_needed_by_campaigns(int(task_id), int(rate_limit))
    if not bool(needed):
        _clear_full_state(int(task_id))
        return False

    state_payload = _load_full_state_payload(int(task_id))
    if str(state_payload.get("mode") or "") == "continue_window":
        return True

    good_cnt, bad_cnt, total_cnt = _load_latest_quality_stats(int(task_id), int(rate_limit), int(sample_size))
    if int(total_cnt) < int(sample_size):
        if state_payload:
            _clear_full_state(int(task_id))
        return True

    ratio = (float(good_cnt) / float(total_cnt)) if int(total_cnt) > 0 else 0.0
    if ratio < float(LIMIT_FULL_GOOD_BAD_RATED_RATIIO):
        _write_full_stats(int(task_id), int(rate_limit))
        payload = _load_full_state_payload(int(task_id))
        payload["mode"] = "blocked_low_quality"
        payload["sample_size"] = int(sample_size)
        payload["good_cnt"] = int(good_cnt)
        payload["bad_cnt"] = int(bad_cnt)
        payload["total_cnt"] = int(total_cnt)
        payload["rate_limit"] = int(rate_limit)
        _save_full_state_payload(int(task_id), payload)
        return False

    if state_payload:
        _clear_full_state(int(task_id))
    return True


def is_more_needed(task_id: int, update: bool = False) -> bool:
    # NOTE: when we add task.rate_limit edits, refresh logic for these task cache keys there too.
    force_update = bool(update)
    task_id_i = int(task_id)
    access_type, rate_limit = _get_task_meta(int(task_id_i))

    if access_type == "test":
        if not bool(force_update):
            cached = _get_test_more_needed(int(task_id_i))
            if cached is not None:
                return bool(cached)

        needed = True
        if _count_rated_good(int(task_id_i), int(rate_limit)) >= LIMIT_TEST_CONTACTS_RATED_GOOD:
            needed = False
        elif _count_rated(int(task_id_i)) >= LIMIT_TEST_CONTACTS_RATED:
            needed = False

        _set_test_more_needed(int(task_id_i), bool(needed))
        return bool(needed)

    if access_type in {"full", "super", "custom"}:
        if not bool(force_update):
            cached = _get_full_more_needed(int(task_id_i))
            if cached is not None:
                return bool(cached)
        needed = bool(_compute_full_more_needed(int(task_id_i), int(rate_limit)))
        _set_full_more_needed(int(task_id_i), bool(needed))
        return bool(needed)

    if access_type == "stat_only":
        return False

    if access_type == "closed":
        return False

    return False
