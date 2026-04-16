# FILE: engine/core_status/status.py
# DATE: 2026-04-05
# PURPOSE: Temporary status helpers and audience task active recalculation.

from __future__ import annotations

import json
import math
import random
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, Optional, Tuple
from zoneinfo import ZoneInfo

from engine.common.cache.client import CLIENT
from engine.common.db import get_connection
from engine.common.db import fetch_all
from engine.common.db import fetch_one
from engine.common.email_template import _is_de_public_holiday
from engine.core_status.is_active import is_more_needed

_TZ_BERLIN = ZoneInfo("Europe/Berlin")
_CACHE_TTL_MIN_SEC = 8 * 60
_CACHE_TTL_MAX_SEC = 12 * 60


def _cache_ttl_sec() -> int:
    return int(random.randint(_CACHE_TTL_MIN_SEC, _CACHE_TTL_MAX_SEC))


def _ws_window_cache_key(workspace_id: str) -> str:
    return f"core_status:campaign:window:ws:{str(workspace_id)}"


def _global_window_cache_key() -> str:
    return "core_status:campaign:window:global"


def _mailbox_limits_cache_key(mailbox_id: int) -> str:
    return f"core_status:campaign:limits:mailbox:{int(mailbox_id)}"


def _cache_get_dict(key: str) -> Optional[dict[str, Any]]:
    raw = CLIENT.get(str(key), ttl_sec=1)
    if raw is None:
        return None
    try:
        parsed = json.loads(bytes(raw).decode("utf-8", errors="replace"))
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _cache_set_dict(key: str, payload: dict[str, Any]) -> None:
    try:
        CLIENT.set(
            str(key),
            json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            ttl_sec=_cache_ttl_sec(),
        )
    except Exception:
        pass


def _parse_hhmm_to_minutes(value: str) -> Optional[int]:
    try:
        raw = str(value or "").strip()
        if ":" not in raw:
            return None
        h, m = raw.split(":", 1)
        hh = int(h)
        mm = int(m)
        if hh < 0 or hh > 23 or mm < 0 or mm > 59:
            return None
        return int(hh * 60 + mm)
    except Exception:
        return None


def _iter_slots(slots_obj: Any) -> Iterable[Tuple[str, str]]:
    if not isinstance(slots_obj, list):
        return []
    out: list[Tuple[str, str]] = []
    for item in slots_obj:
        if isinstance(item, dict):
            a = str(item.get("from") or "").strip()
            b = str(item.get("to") or "").strip()
            if a and b:
                out.append((a, b))
            continue
        if isinstance(item, (list, tuple)) and len(item) == 2:
            a = str(item[0] or "").strip()
            b = str(item[1] or "").strip()
            if a and b:
                out.append((a, b))
    return out


def _window_is_nonempty(window_obj: object) -> bool:
    if not isinstance(window_obj, dict):
        return False
    for value in window_obj.values():
        if isinstance(value, list) and len(value) > 0:
            return True
    return False


def _day_key_for_date(day_value: date) -> str:
    if _is_de_public_holiday(day_value):
        return "hol"
    wd = day_value.weekday()
    return ("mon", "tue", "wed", "thu", "fri", "sat", "sun")[wd]


def _sum_window_minutes_for_date(window_obj: object, day_value: date) -> int:
    if not isinstance(window_obj, dict):
        return 0
    key = _day_key_for_date(day_value)
    total = 0
    for a_str, b_str in _iter_slots(window_obj.get(key, [])):
        a = _parse_hhmm_to_minutes(a_str)
        b = _parse_hhmm_to_minutes(b_str)
        if a is None or b is None:
            continue
        if b <= a:
            continue
        total += int(b - a)
    return int(total)


def _is_now_in_send_window(now_de: datetime, window_obj: object) -> bool:
    if not isinstance(window_obj, dict):
        return False
    key = _day_key_for_date(now_de.date())
    cur = int(now_de.hour * 60 + now_de.minute)
    for a_str, b_str in _iter_slots(window_obj.get(key, [])):
        a = _parse_hhmm_to_minutes(a_str)
        b = _parse_hhmm_to_minutes(b_str)
        if a is None or b is None:
            continue
        if b <= a:
            continue
        if a <= cur < b:
            return True
    return False


def _pick_window_day(now_de: datetime, window_obj: object, lookahead_days: int = 14) -> Tuple[Optional[date], int]:
    today_minutes = _sum_window_minutes_for_date(window_obj, now_de.date())
    if today_minutes > 0:
        return (now_de.date(), int(today_minutes))

    for offset in range(1, int(lookahead_days) + 1):
        day_value = now_de.date() + timedelta(days=int(offset))
        day_minutes = _sum_window_minutes_for_date(window_obj, day_value)
        if day_minutes > 0:
            return (day_value, int(day_minutes))
    return (None, 0)


def _remaining_window_minutes_today(now_de: datetime, window_obj: object) -> int:
    if not isinstance(window_obj, dict):
        return 0
    key = _day_key_for_date(now_de.date())
    cur = int(now_de.hour * 60 + now_de.minute)
    total = 0
    for a_str, b_str in _iter_slots(window_obj.get(key, [])):
        a = _parse_hhmm_to_minutes(a_str)
        b = _parse_hhmm_to_minutes(b_str)
        if a is None or b is None:
            continue
        if b <= a:
            continue
        if cur >= b:
            continue
        start = max(int(a), cur)
        if b > start:
            total += int(b - start)
    return int(total)


def _first_future_window_day(now_de: datetime, window_obj: object, lookahead_days: int = 14) -> Tuple[Optional[date], int]:
    if not isinstance(window_obj, dict):
        return (None, 0)
    for offset in range(1, int(lookahead_days) + 1):
        day_value = now_de.date() + timedelta(days=int(offset))
        day_minutes = _sum_window_minutes_for_date(window_obj, day_value)
        if day_minutes > 0:
            return (day_value, int(day_minutes))
    return (None, 0)


def _effective_window(campaign_window: object, workspace_window: object, global_window: object) -> dict[str, Any]:
    if _window_is_nonempty(campaign_window):
        return campaign_window if isinstance(campaign_window, dict) else {}
    if _window_is_nonempty(workspace_window):
        return workspace_window if isinstance(workspace_window, dict) else {}
    if _window_is_nonempty(global_window):
        return global_window if isinstance(global_window, dict) else {}
    return {}


def _load_global_window() -> dict[str, Any]:
    cache_key = _global_window_cache_key()
    cached = _cache_get_dict(cache_key)
    if cached is not None:
        return cached

    row = fetch_one(
        """
        SELECT global_global_window
        FROM public.aap_settings_global_sending_settings
        WHERE singleton_key = 1
        LIMIT 1
        """,
        [],
    )
    value = row[0] if row and isinstance(row[0], dict) else {}
    payload = value if isinstance(value, dict) else {}
    _cache_set_dict(cache_key, payload)
    return payload


def _load_workspace_windows(workspace_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
    ws_ids = sorted({str(x) for x in workspace_ids if str(x or "").strip()})
    out: dict[str, dict[str, Any]] = {}
    missing: list[str] = []

    for ws_id in ws_ids:
        cached = _cache_get_dict(_ws_window_cache_key(ws_id))
        if cached is None:
            missing.append(ws_id)
            continue
        out[ws_id] = cached

    if missing:
        rows = fetch_all(
            """
            SELECT workspace_id::text, value_json
            FROM public.aap_settings_sending_settings
            WHERE workspace_id::text = ANY(%s)
            """,
            [missing],
        )
        fetched_ids: set[str] = set()
        for ws_id, value_json in rows:
            ws_key = str(ws_id or "").strip()
            if not ws_key:
                continue
            payload = value_json if isinstance(value_json, dict) else {}
            out[ws_key] = payload
            _cache_set_dict(_ws_window_cache_key(ws_key), payload)
            fetched_ids.add(ws_key)

        for ws_id in missing:
            if ws_id in fetched_ids:
                continue
            out[ws_id] = {}
            _cache_set_dict(_ws_window_cache_key(ws_id), {})

    return out


def _load_mailbox_limits(mailbox_ids: Iterable[int]) -> dict[int, tuple[int, int]]:
    ids = sorted({int(x) for x in mailbox_ids if int(x) > 0})
    out: dict[int, tuple[int, int]] = {}
    missing: list[int] = []

    for mailbox_id in ids:
        cached = _cache_get_dict(_mailbox_limits_cache_key(int(mailbox_id)))
        if not isinstance(cached, dict):
            missing.append(int(mailbox_id))
            continue
        out[int(mailbox_id)] = (
            int(cached.get("limit_hour") or 0),
            int(cached.get("limit_day") or 0),
        )

    if missing:
        rows = fetch_all(
            """
            SELECT id, COALESCE(limit_hour, 0)::int AS limit_hour, COALESCE(limit_day, 0)::int AS limit_day
            FROM public.aap_settings_mailboxes
            WHERE id = ANY(%s)
            """,
            [missing],
        )
        fetched_ids: set[int] = set()
        for mailbox_id, limit_hour, limit_day in rows:
            mid = int(mailbox_id)
            payload = {"limit_hour": int(limit_hour or 0), "limit_day": int(limit_day or 0)}
            out[mid] = (payload["limit_hour"], payload["limit_day"])
            _cache_set_dict(_mailbox_limits_cache_key(mid), payload)
            fetched_ids.add(mid)

        for mailbox_id in missing:
            if int(mailbox_id) in fetched_ids:
                continue
            out[int(mailbox_id)] = (0, 0)
            _cache_set_dict(_mailbox_limits_cache_key(int(mailbox_id)), {"limit_hour": 0, "limit_day": 0})

    return out


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


def run_campaign_status_once() -> dict[str, int | str]:
    now_de = datetime.now(tz=ZoneInfo("UTC")).astimezone(_TZ_BERLIN)

    rows = fetch_all(
        """
        SELECT
            c.id::bigint AS campaign_id,
            c.workspace_id::text AS workspace_id,
            c.mailbox_id::bigint AS mailbox_id,
            c.user_active,
            c.archived,
            c.window
        FROM public.campaigns_campaigns c
        ORDER BY c.id ASC
        """,
        [],
    )
    if not rows:
        return {
            "mode": "ok",
            "scanned_cnt": 0,
            "updated_cnt": 0,
            "active_true_cnt": 0,
            "active_false_cnt": 0,
            "interval_nonnull_cnt": 0,
        }

    pool_rows = [row for row in rows if bool(row[3]) and (not bool(row[4]))]
    ws_windows = _load_workspace_windows(str(row[1] or "").strip() for row in pool_rows)
    mailbox_limits = _load_mailbox_limits(int(row[2]) for row in pool_rows if row[2] is not None)
    global_window = _load_global_window()

    campaign_rows: list[dict[str, Any]] = []

    mailbox_day_total_minutes: dict[tuple[int, date], int] = {}
    mailbox_day_campaign_count: dict[tuple[int, date], int] = {}

    active_true_cnt = 0

    for campaign_id, workspace_id, mailbox_id, user_active, archived, campaign_window in rows:
        cid = int(campaign_id)
        ws_id = str(workspace_id or "").strip()
        mid = int(mailbox_id)
        camp_window = campaign_window if isinstance(campaign_window, dict) else {}
        is_pool_eligible = bool(user_active) and (not bool(archived))
        ws_window = ws_windows.get(ws_id, {}) if is_pool_eligible else {}
        effective_window = _effective_window(camp_window, ws_window, global_window) if is_pool_eligible else {}

        is_active_now = bool(is_pool_eligible) and _is_now_in_send_window(now_de, effective_window)

        today_minutes = _sum_window_minutes_for_date(effective_window, now_de.date())
        interval_day, interval_day_minutes = _pick_window_day(now_de, effective_window)
        remaining_today_minutes = _remaining_window_minutes_today(now_de, effective_window)
        future_day, future_minutes = _first_future_window_day(now_de, effective_window)

        # Aggregate by mailbox+day across all pool campaigns that have a window on that day.
        # Horizon matches window day-picking helpers: today + next 14 days.
        if is_pool_eligible:
            for offset in range(0, 15):
                day_value = now_de.date() + timedelta(days=int(offset))
                minutes_value = _sum_window_minutes_for_date(effective_window, day_value)
                if int(minutes_value) <= 0:
                    continue
                key = (int(mid), day_value)
                mailbox_day_total_minutes[key] = int(mailbox_day_total_minutes.get(key, 0)) + int(minutes_value)
                mailbox_day_campaign_count[key] = int(mailbox_day_campaign_count.get(key, 0)) + 1

        if is_active_now:
            active_true_cnt += 1

        campaign_rows.append(
            {
                "campaign_id": int(cid),
                "mailbox_id": int(mid),
                "is_pool_eligible": bool(is_pool_eligible),
                "is_active_now": bool(is_active_now),
                "interval_day": interval_day,
                "interval_day_minutes": int(interval_day_minutes),
                "today_minutes": int(today_minutes),
                "remaining_today_minutes": int(remaining_today_minutes),
                "future_day": future_day,
                "future_minutes": int(future_minutes),
            }
        )

    campaign_ids: list[int] = []
    active_values: list[bool] = []
    interval_values: list[Optional[int]] = []
    to_send_values: list[Optional[int]] = []
    interval_nonnull_cnt = 0
    to_send_nonnull_cnt = 0

    def _shared_interval_for_day_ms(mailbox_id: int, day_value: Optional[date], lim_hour: int, lim_day: int) -> Optional[int]:
        if day_value is None or int(lim_hour) <= 0 or int(lim_day) <= 0:
            return None
        key = (int(mailbox_id), day_value)
        total_minutes_day = int(mailbox_day_total_minutes.get(key, 0))
        total_campaigns_day = int(mailbox_day_campaign_count.get(key, 0))
        if total_minutes_day <= 0 or total_campaigns_day <= 0:
            return None
        interval_day_ms = (float(total_minutes_day) * 60_000.0) / float(lim_day)
        interval_hour_ms = (3_600_000.0 * float(total_campaigns_day)) / float(lim_hour)
        return int(math.ceil(max(interval_day_ms, interval_hour_ms)))

    for row_obj in campaign_rows:
        cid = int(row_obj["campaign_id"])
        mid = int(row_obj["mailbox_id"])
        limit_hour, limit_day = mailbox_limits.get(mid, (0, 0))
        is_pool_eligible = bool(row_obj["is_pool_eligible"])

        interval_day = row_obj["interval_day"]
        interval_day_minutes = int(row_obj["interval_day_minutes"])

        interval_ms: Optional[int] = None
        if (
            is_pool_eligible
            and interval_day is not None
            and interval_day_minutes > 0
            and int(limit_hour) > 0
            and int(limit_day) > 0
        ):
            interval_ms = _shared_interval_for_day_ms(mid, interval_day, limit_hour, limit_day)

        to_send_num: Optional[int] = None
        if is_pool_eligible:
            send_today = 0
            send_next = 0
            has_part = False

            today_minutes = int(row_obj["today_minutes"])
            remaining_today_minutes = int(row_obj["remaining_today_minutes"])
            if today_minutes > 0 and remaining_today_minutes > 0:
                interval_today_ms = _shared_interval_for_day_ms(mid, now_de.date(), limit_hour, limit_day)
                if interval_today_ms is not None and int(interval_today_ms) > 0:
                    send_today = int((int(remaining_today_minutes) * 60_000) // int(interval_today_ms))
                    has_part = True

            future_day = row_obj["future_day"]
            future_minutes = int(row_obj["future_minutes"])
            if future_day is not None and future_minutes > 0:
                interval_next_ms = _shared_interval_for_day_ms(mid, future_day, limit_hour, limit_day)
                if interval_next_ms is not None and int(interval_next_ms) > 0:
                    send_next = int((int(future_minutes) * 60_000) // int(interval_next_ms))
                    has_part = True

            if has_part:
                to_send_num = int(send_today + send_next)

        campaign_ids.append(cid)
        active_values.append(bool(row_obj["is_active_now"]))
        interval_values.append(interval_ms)
        to_send_values.append(to_send_num)

        if interval_ms is not None:
            interval_nonnull_cnt += 1
        if to_send_num is not None:
            to_send_nonnull_cnt += 1

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH data(campaign_id, active_value, sending_interval_value, to_send_value) AS (
                SELECT *
                FROM unnest(%s::bigint[], %s::boolean[], %s::integer[], %s::integer[])
            ),
            upd AS (
                UPDATE public.campaigns_campaigns c
                SET active = data.active_value,
                    sending_interval = data.sending_interval_value,
                    to_send_num = data.to_send_value,
                    updated_at = now()
                FROM data
                WHERE c.id = data.campaign_id
                  AND (
                      c.active IS DISTINCT FROM data.active_value
                      OR c.sending_interval IS DISTINCT FROM data.sending_interval_value
                      OR c.to_send_num IS DISTINCT FROM data.to_send_value
                  )
                RETURNING c.id
            )
            SELECT COUNT(*)::int
            FROM upd
            """,
            [campaign_ids, active_values, interval_values, to_send_values],
        )
        updated_cnt = int((cur.fetchone() or [0])[0] or 0)
        conn.commit()

    active_false_cnt = int(len(campaign_ids) - active_true_cnt)
    return {
        "mode": "ok",
        "scanned_cnt": int(len(campaign_ids)),
        "updated_cnt": int(updated_cnt),
        "active_true_cnt": int(active_true_cnt),
        "active_false_cnt": int(active_false_cnt),
        "interval_nonnull_cnt": int(interval_nonnull_cnt),
        "to_send_nonnull_cnt": int(to_send_nonnull_cnt),
    }


def run_campaign_sent_recount_once() -> dict[str, int | str]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH sent AS (
                SELECT
                    sl.campaign_id::bigint AS campaign_id,
                    COUNT(sl.id)::int AS sent_cnt
                FROM public.sending_log sl
                WHERE COALESCE(sl.processed, false) = true
                GROUP BY sl.campaign_id
            ),
            calc AS (
                SELECT
                    c.id::bigint AS campaign_id,
                    COALESCE(s.sent_cnt, 0)::int AS sent_value
                FROM public.campaigns_campaigns c
                LEFT JOIN sent s
                  ON s.campaign_id = c.id
            ),
            upd AS (
                UPDATE public.campaigns_campaigns c
                SET sent_num = calc.sent_value,
                    updated_at = now()
                FROM calc
                WHERE c.id = calc.campaign_id
                  AND c.sent_num IS DISTINCT FROM calc.sent_value
                RETURNING c.id
            )
            SELECT
                (SELECT COUNT(*)::int FROM public.campaigns_campaigns) AS scanned_cnt,
                (SELECT COUNT(*)::int FROM upd) AS updated_cnt,
                (
                    SELECT COUNT(*)::int
                    FROM public.campaigns_campaigns c
                    WHERE COALESCE(c.sent_num, 0) > 0
                ) AS sent_nonzero_cnt
            """
        )
        row = cur.fetchone() or [0, 0, 0]
        conn.commit()

    return {
        "mode": "ok",
        "scanned_cnt": int(row[0] or 0),
        "updated_cnt": int(row[1] or 0),
        "sent_nonzero_cnt": int(row[2] or 0),
    }
