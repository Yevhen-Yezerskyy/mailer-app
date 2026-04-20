# FILE: web/panel/views.py
# DATE: 2026-03-08
# PURPOSE: panel main views: overview + stats + switch-user.

from __future__ import annotations

import json
import random
from datetime import timedelta
from zoneinfo import ZoneInfo

import phonenumbers
from django.conf import settings
from django.contrib import messages
from django.contrib.auth import login as auth_login
from django.db import connection
from django.http import JsonResponse
from django.shortcuts import redirect
from django.shortcuts import render
from django.urls import reverse
from django.views.decorators.http import require_POST
from django.utils import timezone
from django.utils.translation import gettext as _trans

from engine.common.cache.client import CLIENT
from engine.common.email_template import _is_de_public_holiday
from mailer_web.access import encode_id, decode_id
from mailer_web.format_contact import get_category_title, get_city_title
from mailer_web.models import ClientUser
from engine.common.utils import parse_json_object
from panel.aap_audience.models import AudienceTask
from panel.aap_campaigns.models import Campaign, Letter
from panel.aap_settings.models import GlobalSendingSettings, SendingSettings, default_global_global_window_json


_TZ_BERLIN = ZoneInfo("Europe/Berlin")


def _parse_hhmm_to_minutes(v: str):
    try:
        h, m = str(v or "").strip().split(":", 1)
        hh = int(h)
        mm = int(m)
        if hh < 0 or hh > 23 or mm < 0 or mm > 59:
            return None
        return hh * 60 + mm
    except Exception:
        return None


def _window_is_nonempty(win: object) -> bool:
    if not isinstance(win, dict):
        return False
    for v in win.values():
        if isinstance(v, list) and len(v) > 0:
            return True
    return False


def _iter_slots(slots_obj):
    if not isinstance(slots_obj, list):
        return []
    out = []
    for it in slots_obj:
        if isinstance(it, dict):
            a = str(it.get("from") or "").strip()
            b = str(it.get("to") or "").strip()
            if a and b:
                out.append((a, b))
            continue
        if isinstance(it, (list, tuple)) and len(it) == 2:
            a = str(it[0] or "").strip()
            b = str(it[1] or "").strip()
            if a and b:
                out.append((a, b))
    return out


def _is_now_in_send_window(now_de, camp_window: object, global_window: object) -> bool:
    win = camp_window if _window_is_nonempty(camp_window) else (global_window if isinstance(global_window, dict) else {})
    if not isinstance(win, dict):
        return False

    today = now_de.date()
    if _is_de_public_holiday(today):
        key = "hol"
    else:
        wd = now_de.weekday()
        key = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")[wd]

    cur = now_de.hour * 60 + now_de.minute
    for a_str, b_str in _iter_slots(win.get(key, [])):
        a = _parse_hhmm_to_minutes(a_str)
        b = _parse_hhmm_to_minutes(b_str)
        if a is None or b is None or b <= a:
            continue
        if a <= cur < b:
            return True
    return False


def _resolve_global_window(ws_id):
    global_default = default_global_global_window_json()
    gss, _created_gss = GlobalSendingSettings.objects.get_or_create(
        singleton_key=1,
        defaults={"global_global_window": global_default},
    )
    global_global_window = gss.global_global_window if isinstance(gss.global_global_window, dict) else {}
    if not _window_is_nonempty(global_global_window):
        global_global_window = global_default

    ss, _created = SendingSettings.objects.get_or_create(
        workspace_id=ws_id,
        defaults={"value_json": global_global_window},
    )
    global_window_json = ss.value_json if isinstance(ss.value_json, dict) else {}
    if not _window_is_nonempty(global_window_json):
        global_window_json = global_global_window
    return global_window_json


def _sent_counts_by_campaign_ids(campaign_ids):
    out = {}
    ids = [int(x) for x in (campaign_ids or []) if int(x) > 0]
    if not ids:
        return out
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT campaign_id, COUNT(id) AS sent_cnt
            FROM public.sending_log
            WHERE campaign_id = ANY(%s)
              AND status = 'SEND'
            GROUP BY campaign_id
            """,
            [ids],
        )
        for cid, sent_cnt in cur.fetchall() or []:
            out[int(cid)] = int(sent_cnt or 0)
    return out


def _views_counts_by_campaign_ids(campaign_ids):
    out = {}
    ids = [int(x) for x in (campaign_ids or []) if int(x) > 0]
    if not ids:
        return out
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT lg.campaign_id, COUNT(DISTINCT lg.aggr_contact_cb_id) AS views_cnt
            FROM public.mailbox_stats ms
            JOIN public.sending_log lg
              ON lg.id = ms.letter_id
            WHERE lg.campaign_id = ANY(%s)
            GROUP BY lg.campaign_id
            """,
            [ids],
        )
        for cid, views_cnt in cur.fetchall() or []:
            out[int(cid)] = int(views_cnt or 0)
    return out


def _overview_site_click_rows(ws_id, limit: int = 8):
    out: list[dict] = []
    if not ws_id:
        return out
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              MAX(ms.time) AS seen_at,
              c.id AS campaign_id,
              COALESCE(NULLIF(trim(c.title), ''), '#' || c.id::text) AS campaign_title,
              LOWER(COALESCE(t.type, '')) AS campaign_type,
              lg.aggr_contact_cb_id::bigint AS aggr_contact_id,
              COALESCE(NULLIF(trim(ac.company_name), ''), NULLIF(trim(ac.email), ''), '—') AS contact_name,
              COUNT(*)::int AS visits_cnt
            FROM public.mailbox_stats ms
            JOIN public.sending_log lg
              ON lg.id = ms.letter_id
            JOIN public.campaigns_campaigns c
              ON c.id = lg.campaign_id
             AND c.workspace_id = %s::uuid
            LEFT JOIN public.aap_audience_audiencetask t
              ON t.id = c.sending_list_id
            LEFT JOIN public.aggr_contacts_cb ac
              ON ac.id = lg.aggr_contact_cb_id
            GROUP BY
              c.id,
              c.title,
              t.type,
              lg.aggr_contact_cb_id,
              ac.company_name,
              ac.email
            ORDER BY seen_at DESC
            LIMIT %s
            """,
            [ws_id, int(max(1, limit))],
        )
        for seen_at, campaign_id, campaign_title, campaign_type, aggr_contact_id, contact_name, visits_cnt in cur.fetchall() or []:
            out.append(
                {
                    "row_key": (
                        f"site:{int(campaign_id)}:{int(aggr_contact_id or 0)}:"
                        f"{_fmt_dt_short(seen_at)}:{int(visits_cnt or 0)}"
                    ),
                    "time_text": _fmt_dt_short(seen_at),
                    "campaign_title": str(campaign_title or "").strip() or f"#{int(campaign_id)}",
                    "campaign_type": str(campaign_type or "").strip().lower(),
                    "contact_name": str(contact_name or "").strip() or "—",
                    "contact_modal_url": _contact_modal_url(aggr_contact_id),
                    "visits_count": int(visits_cnt or 0),
                    "visits_count_fmt": f"{int(visits_cnt or 0):,}".replace(",", " "),
                }
            )
    return out


def _format_total(value: int) -> str:
    return f"{int(value):,}".replace(",", " ")


def _build_stats_page_items(*, page: int, total_pages: int) -> list[dict]:
    if total_pages <= 1:
        return []
    out: list[dict] = []
    for number in range(1, total_pages + 1):
        is_edge = (number == 1) or (number == total_pages)
        is_near = abs(number - page) <= 3
        if is_edge or is_near:
            out.append(
                {
                    "kind": "page",
                    "number": number,
                    "is_current": number == page,
                }
            )
            continue
        if not out or out[-1].get("kind") != "gap":
            out.append({"kind": "gap"})
    return out


def _stats_site_click_rows_page(ws_id, *, limit: int = 100, page: int = 1):
    out: list[dict] = []
    if not ws_id:
        return {
            "rows": out,
            "total": 0,
            "page": 1,
            "pages": 1,
            "has_prev": False,
            "has_next": False,
            "prev_page": 1,
            "next_page": 1,
        }

    try:
        page_i = int(page or 1)
    except Exception:
        page_i = 1
    page_i = max(1, page_i)
    try:
        limit_i = int(limit or 100)
    except Exception:
        limit_i = 100
    limit_i = max(1, limit_i)

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM (
              SELECT
                c.id,
                lg.aggr_contact_cb_id,
                COALESCE(NULLIF(trim(ac.company_name), ''), NULLIF(trim(ac.email), ''), '—')
              FROM public.mailbox_stats ms
              JOIN public.sending_log lg
                ON lg.id = ms.letter_id
              JOIN public.campaigns_campaigns c
                ON c.id = lg.campaign_id
               AND c.workspace_id = %s::uuid
              LEFT JOIN public.aggr_contacts_cb ac
                ON ac.id = lg.aggr_contact_cb_id
              GROUP BY
                c.id,
                lg.aggr_contact_cb_id,
                COALESCE(NULLIF(trim(ac.company_name), ''), NULLIF(trim(ac.email), ''), '—')
            ) q
            """,
            [ws_id],
        )
        total = int((cur.fetchone() or [0])[0] or 0)

    pages = max(1, (total + limit_i - 1) // limit_i) if total > 0 else 1
    page_i = min(page_i, pages)
    offset_i = (page_i - 1) * limit_i

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              MAX(ms.time) AS seen_at,
              c.id AS campaign_id,
              COALESCE(NULLIF(trim(c.title), ''), '#' || c.id::text) AS campaign_title,
              LOWER(COALESCE(t.type, '')) AS campaign_type,
              lg.aggr_contact_cb_id::bigint AS aggr_contact_id,
              COALESCE(NULLIF(trim(ac.company_name), ''), NULLIF(trim(ac.email), ''), '—') AS contact_name,
              COUNT(*)::int AS visits_cnt
            FROM public.mailbox_stats ms
            JOIN public.sending_log lg
              ON lg.id = ms.letter_id
            JOIN public.campaigns_campaigns c
              ON c.id = lg.campaign_id
             AND c.workspace_id = %s::uuid
            LEFT JOIN public.aap_audience_audiencetask t
              ON t.id = c.sending_list_id
            LEFT JOIN public.aggr_contacts_cb ac
              ON ac.id = lg.aggr_contact_cb_id
            GROUP BY
              c.id,
              c.title,
              t.type,
              lg.aggr_contact_cb_id,
              ac.company_name,
              ac.email
            ORDER BY seen_at DESC
            LIMIT %s
            OFFSET %s
            """,
            [ws_id, int(limit_i), int(offset_i)],
        )
        for seen_at, campaign_id, campaign_title, campaign_type, aggr_contact_id, contact_name, visits_cnt in cur.fetchall() or []:
            out.append(
                {
                    "row_key": (
                        f"site:{int(campaign_id)}:{int(aggr_contact_id or 0)}:"
                        f"{_fmt_dt_short(seen_at)}:{int(visits_cnt or 0)}"
                    ),
                    "time_text": _fmt_dt_short(seen_at),
                    "campaign_title": str(campaign_title or "").strip() or f"#{int(campaign_id)}",
                    "campaign_type": str(campaign_type or "").strip().lower(),
                    "contact_name": str(contact_name or "").strip() or "—",
                    "contact_modal_url": _contact_modal_url(aggr_contact_id),
                    "visits_count": int(visits_cnt or 0),
                    "visits_count_fmt": f"{int(visits_cnt or 0):,}".replace(",", " "),
                }
            )

    return {
        "rows": out,
        "total": int(total),
        "total_display": _format_total(int(total)),
        "page": int(page_i),
        "pages": int(pages),
        "has_prev": bool(page_i > 1),
        "has_next": bool(page_i < pages),
        "prev_page": int(page_i - 1 if page_i > 1 else 1),
        "next_page": int(page_i + 1 if page_i < pages else pages),
        "page_items": _build_stats_page_items(page=int(page_i), total_pages=int(pages)),
    }


def _stats_site_click_rows_for_campaign(ws_id, campaign_id: int):
    out: list[dict] = []
    try:
        campaign_id_i = int(campaign_id or 0)
    except Exception:
        campaign_id_i = 0
    if not ws_id or campaign_id_i <= 0:
        return out

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              MAX(ms.time) AS seen_at,
              c.id AS campaign_id,
              COALESCE(NULLIF(trim(c.title), ''), '#' || c.id::text) AS campaign_title,
              LOWER(COALESCE(t.type, '')) AS campaign_type,
              lg.aggr_contact_cb_id::bigint AS aggr_contact_id,
              COALESCE(NULLIF(trim(ac.company_name), ''), NULLIF(trim(ac.email), ''), '—') AS contact_name,
              COUNT(*)::int AS visits_cnt
            FROM public.mailbox_stats ms
            JOIN public.sending_log lg
              ON lg.id = ms.letter_id
            JOIN public.campaigns_campaigns c
              ON c.id = lg.campaign_id
             AND c.workspace_id = %s::uuid
             AND c.id = %s
            LEFT JOIN public.aap_audience_audiencetask t
              ON t.id = c.sending_list_id
            LEFT JOIN public.aggr_contacts_cb ac
              ON ac.id = lg.aggr_contact_cb_id
            GROUP BY
              c.id,
              c.title,
              t.type,
              lg.aggr_contact_cb_id,
              ac.company_name,
              ac.email
            ORDER BY seen_at DESC
            """,
            [ws_id, int(campaign_id_i)],
        )
        for seen_at, campaign_id_v, campaign_title, campaign_type, aggr_contact_id, contact_name, visits_cnt in cur.fetchall() or []:
            out.append(
                {
                    "row_key": (
                        f"site:{int(campaign_id_v)}:{int(aggr_contact_id or 0)}:"
                        f"{_fmt_dt_short(seen_at)}:{int(visits_cnt or 0)}"
                    ),
                    "time_text": _fmt_dt_short(seen_at),
                    "campaign_title": str(campaign_title or "").strip() or f"#{int(campaign_id_v)}",
                    "campaign_type": str(campaign_type or "").strip().lower(),
                    "contact_name": str(contact_name or "").strip() or "—",
                    "contact_modal_url": _contact_modal_url(aggr_contact_id),
                    "visits_count": int(visits_cnt or 0),
                    "visits_count_fmt": f"{int(visits_cnt or 0):,}".replace(",", " "),
                }
            )
    return out


def _stats_sending_rows_page(ws_id, *, limit: int = 100, page: int = 1):
    out: list[dict] = []
    if not ws_id:
        return {
            "rows": out,
            "total": 0,
            "page": 1,
            "pages": 1,
            "has_prev": False,
            "has_next": False,
            "prev_page": 1,
            "next_page": 1,
            "total_display": "0",
            "page_items": [],
        }

    try:
        page_i = int(page or 1)
    except Exception:
        page_i = 1
    page_i = max(1, page_i)
    try:
        limit_i = int(limit or 100)
    except Exception:
        limit_i = 100
    limit_i = max(1, limit_i)

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.sending_log lg
            JOIN public.campaigns_campaigns c
              ON c.id = lg.campaign_id
             AND c.workspace_id = %s::uuid
            """,
            [ws_id],
        )
        total = int((cur.fetchone() or [0])[0] or 0)

    pages = max(1, (total + limit_i - 1) // limit_i) if total > 0 else 1
    page_i = min(page_i, pages)
    offset_i = (page_i - 1) * limit_i

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              lg.id::bigint AS log_id,
              COALESCE(lg.processed_at, lg.created_at) AS event_at,
              c.id AS campaign_id,
              COALESCE(NULLIF(trim(c.title), ''), '#' || c.id::text) AS campaign_title,
              LOWER(COALESCE(t.type, '')) AS campaign_type,
              lg.aggr_contact_cb_id::bigint AS aggr_contact_id,
              COALESCE(NULLIF(trim(ac.company_name), ''), NULLIF(trim(ac.email), ''), '—') AS contact_name,
              UPPER(COALESCE(lg.status, '')) AS send_status
            FROM public.sending_log lg
            JOIN public.campaigns_campaigns c
              ON c.id = lg.campaign_id
             AND c.workspace_id = %s::uuid
            LEFT JOIN public.aap_audience_audiencetask t
              ON t.id = c.sending_list_id
            LEFT JOIN public.aggr_contacts_cb ac
              ON ac.id = lg.aggr_contact_cb_id
            ORDER BY COALESCE(lg.processed_at, lg.created_at) DESC, lg.id DESC
            LIMIT %s
            OFFSET %s
            """,
            [ws_id, int(limit_i), int(offset_i)],
        )
        for log_id, event_at, campaign_id, campaign_title, campaign_type, aggr_contact_id, contact_name, send_status in cur.fetchall() or []:
            status_text = str(send_status or "").strip().upper()
            is_ok = bool(status_text == "SEND")
            out.append(
                {
                    "row_key": (
                        f"send:{int(log_id or 0)}:{int(campaign_id or 0)}:"
                        f"{int(aggr_contact_id or 0)}:{status_text or 'UNKNOWN'}"
                    ),
                    "time_text": _fmt_dt_short(event_at),
                    "campaign_title": str(campaign_title or "").strip() or f"#{int(campaign_id)}",
                    "campaign_type": str(campaign_type or "").strip().lower(),
                    "contact_name": str(contact_name or "").strip() or "—",
                    "contact_modal_url": _contact_modal_url(aggr_contact_id),
                    "send_status": status_text or "UNKNOWN",
                    "is_ok": is_ok,
                    "icon": ("check" if is_ok else "info"),
                }
            )

    return {
        "rows": out,
        "total": int(total),
        "total_display": _format_total(int(total)),
        "page": int(page_i),
        "pages": int(pages),
        "has_prev": bool(page_i > 1),
        "has_next": bool(page_i < pages),
        "prev_page": int(page_i - 1 if page_i > 1 else 1),
        "next_page": int(page_i + 1 if page_i < pages else pages),
        "page_items": _build_stats_page_items(page=int(page_i), total_pages=int(pages)),
    }


def _stats_sending_rows_for_campaign_page(ws_id, campaign_id: int, *, limit: int = 200, page: int = 1):
    out: list[dict] = []
    try:
        campaign_id_i = int(campaign_id or 0)
    except Exception:
        campaign_id_i = 0
    if not ws_id or campaign_id_i <= 0:
        return {
            "rows": out,
            "total": 0,
            "page": 1,
            "pages": 1,
            "has_prev": False,
            "has_next": False,
            "prev_page": 1,
            "next_page": 1,
            "total_display": "0",
            "page_items": [],
        }

    try:
        page_i = int(page or 1)
    except Exception:
        page_i = 1
    page_i = max(1, page_i)
    try:
        limit_i = int(limit or 200)
    except Exception:
        limit_i = 200
    limit_i = max(1, limit_i)

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.sending_log lg
            JOIN public.campaigns_campaigns c
              ON c.id = lg.campaign_id
             AND c.workspace_id = %s::uuid
             AND c.id = %s
            """,
            [ws_id, int(campaign_id_i)],
        )
        total = int((cur.fetchone() or [0])[0] or 0)

    pages = max(1, (total + limit_i - 1) // limit_i) if total > 0 else 1
    page_i = min(page_i, pages)
    offset_i = (page_i - 1) * limit_i

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              lg.id::bigint AS log_id,
              COALESCE(lg.processed_at, lg.created_at) AS event_at,
              c.id AS campaign_id,
              COALESCE(NULLIF(trim(c.title), ''), '#' || c.id::text) AS campaign_title,
              LOWER(COALESCE(t.type, '')) AS campaign_type,
              lg.aggr_contact_cb_id::bigint AS aggr_contact_id,
              COALESCE(NULLIF(trim(ac.company_name), ''), NULLIF(trim(ac.email), ''), '—') AS contact_name,
              UPPER(COALESCE(lg.status, '')) AS send_status
            FROM public.sending_log lg
            JOIN public.campaigns_campaigns c
              ON c.id = lg.campaign_id
             AND c.workspace_id = %s::uuid
             AND c.id = %s
            LEFT JOIN public.aap_audience_audiencetask t
              ON t.id = c.sending_list_id
            LEFT JOIN public.aggr_contacts_cb ac
              ON ac.id = lg.aggr_contact_cb_id
            ORDER BY COALESCE(lg.processed_at, lg.created_at) DESC, lg.id DESC
            LIMIT %s
            OFFSET %s
            """,
            [ws_id, int(campaign_id_i), int(limit_i), int(offset_i)],
        )
        for log_id, event_at, campaign_id_v, campaign_title, campaign_type, aggr_contact_id, contact_name, send_status in cur.fetchall() or []:
            status_text = str(send_status or "").strip().upper()
            is_ok = bool(status_text == "SEND")
            out.append(
                {
                    "row_key": (
                        f"send:{int(log_id or 0)}:{int(campaign_id_v or 0)}:"
                        f"{int(aggr_contact_id or 0)}:{status_text or 'UNKNOWN'}"
                    ),
                    "time_text": _fmt_dt_short(event_at),
                    "campaign_title": str(campaign_title or "").strip() or f"#{int(campaign_id_v)}",
                    "campaign_type": str(campaign_type or "").strip().lower(),
                    "contact_name": str(contact_name or "").strip() or "—",
                    "contact_modal_url": _contact_modal_url(aggr_contact_id),
                    "send_status": status_text or "UNKNOWN",
                    "is_ok": is_ok,
                    "icon": ("check" if is_ok else "info"),
                }
            )

    return {
        "rows": out,
        "total": int(total),
        "total_display": _format_total(int(total)),
        "page": int(page_i),
        "pages": int(pages),
        "has_prev": bool(page_i > 1),
        "has_next": bool(page_i < pages),
        "prev_page": int(page_i - 1 if page_i > 1 else 1),
        "next_page": int(page_i + 1 if page_i < pages else pages),
        "page_items": _build_stats_page_items(page=int(page_i), total_pages=int(pages)),
    }


def _overview_mailing_stats_by_task_ids(task_ids):
    out: dict[int, dict] = {}
    ids = [int(x) for x in (task_ids or []) if int(x) > 0]
    if not ids:
        return out
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              sl.task_id::bigint AS task_id,
              COUNT(*)::int AS total_count,
              COUNT(*) FILTER (WHERE sl.rate IS NOT NULL)::int AS rated_count,
              COUNT(*) FILTER (WHERE sl.rate IS NULL)::int AS unrated_count,
              COUNT(*) FILTER (WHERE sl.rate IS NOT NULL AND sl.rate <= t.rate_limit)::int AS good_count,
              COUNT(*) FILTER (WHERE sl.rate IS NOT NULL AND sl.rate > t.rate_limit)::int AS bad_count
            FROM public.sending_lists sl
            JOIN public.aap_audience_audiencetask t
              ON t.id = sl.task_id
            WHERE sl.task_id = ANY(%s)
              AND COALESCE(sl.removed, false) = false
            GROUP BY sl.task_id
            """,
            [ids],
        )
        for task_id, total_count, rated_count, unrated_count, good_count, bad_count in cur.fetchall() or []:
            out[int(task_id)] = {
                "total_count": int(total_count or 0),
                "rated_count": int(rated_count or 0),
                "unrated_count": int(unrated_count or 0),
                "good_count": int(good_count or 0),
                "bad_count": int(bad_count or 0),
            }
    return out


def _is_task_exhausted(task) -> bool:
    if not task:
        return False
    try:
        key = f"core_crawler:task_exhausted:{int(task.id)}"
        return bool(CLIENT.get(key, ttl_sec=24 * 60 * 60))
    except Exception:
        return False


def _overview_mailing_items(ws_id):
    if not ws_id:
        return []

    tasks = list(
        AudienceTask.objects.filter(workspace_id=ws_id, archived=False)
        .only("id", "title", "type", "ready", "active", "user_active", "archived", "rate_limit")
        .order_by("id")
    )
    if not tasks:
        return []

    task_ids: list[int] = []
    for task in tasks:
        task_id = int(task.id) if task and getattr(task, "id", None) is not None else 0
        if task_id > 0:
            task_ids.append(task_id)

    stats_by_task = _overview_mailing_stats_by_task_ids(task_ids)

    out: list[dict] = []
    for task in tasks:
        task_id = int(task.id)
        stats = stats_by_task.get(task_id, {})

        title = str(getattr(task, "title", "") or "").strip() or f"#{task_id}"

        task_type = str(getattr(task, "type", "") or "").strip().lower()
        task_ui_id = encode_id(int(task_id))
        pause_modal_url = reverse("audience:create_pause_info_modal") + f"?id={task_ui_id}" if task_ui_id else ""
        if task_ui_id and task_type == "buy":
            rating_url = reverse("audience:create_edit_buy_mailing_list_id", args=[task_ui_id])
        elif task_ui_id and task_type == "sell":
            rating_url = reverse("audience:create_edit_sell_mailing_list_id", args=[task_ui_id])
        else:
            rating_url = ""

        processing_enabled = bool(getattr(task, "user_active", False))
        processing_toggleable = bool(getattr(task, "ready", False) and not bool(getattr(task, "archived", False)))
        is_task_active = bool(getattr(task, "active", False) and not bool(getattr(task, "archived", False)))

        good_count = int(stats.get("good_count", 0) or 0)
        bad_count = int(stats.get("bad_count", 0) or 0)
        rated_count = int(stats.get("rated_count", 0) or 0)
        unrated_count = int(stats.get("unrated_count", 0) or 0)
        rate_limit = int(getattr(task, "rate_limit", 0) or 0)
        is_exhausted = _is_task_exhausted(task)
        is_completed = bool(is_exhausted and int(unrated_count) <= 0)
        running_now = bool(is_task_active and not is_completed)

        out.append(
            {
                "list_id": task_id,
                "list_ui_id": task_ui_id,
                "task_id": task_id,
                "task_ui_id": task_ui_id,
                "title": title,
                "type": task_type,
                "rating_url": rating_url,
                "processing_enabled": processing_enabled,
                "processing_toggleable": processing_toggleable,
                "is_running": running_now,
                "is_completed": is_completed,
                "pause_modal_url": pause_modal_url,
                "good_count": good_count,
                "good_count_fmt": f"{good_count:,}".replace(",", " "),
                "bad_count": bad_count,
                "bad_count_fmt": f"{bad_count:,}".replace(",", " "),
                "rated_count": rated_count,
                "rated_count_fmt": f"{rated_count:,}".replace(",", " "),
                "unrated_count": unrated_count,
                "unrated_count_fmt": f"{unrated_count:,}".replace(",", " "),
                "rate_limit": rate_limit,
                "rate_limit_display": str(rate_limit),
            }
        )
    return out


def _pct_one_decimal(numerator: object, denominator: object) -> str:
    try:
        num = float(numerator or 0)
        den = float(denominator or 0)
    except Exception:
        return "0.0"
    if den <= 0:
        return "0.0"
    return f"{(num * 100.0) / den:.1f}"


def _effective_window(camp_window: object, global_window: object) -> dict:
    if _window_is_nonempty(camp_window) and isinstance(camp_window, dict):
        return camp_window
    if isinstance(global_window, dict):
        return global_window
    return {}


def _window_slots_for_date(win: dict, day_obj) -> list[tuple[int, int]]:
    if _is_de_public_holiday(day_obj):
        key = "hol"
    else:
        key = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")[day_obj.weekday()]
    slots: list[tuple[int, int]] = []
    for a_str, b_str in _iter_slots(win.get(key, [])):
        a = _parse_hhmm_to_minutes(a_str)
        b = _parse_hhmm_to_minutes(b_str)
        if a is None or b is None or b <= a:
            continue
        slots.append((int(a), int(b)))
    slots.sort(key=lambda x: (x[0], x[1]))
    return slots


def _next_window_start_dt(now_de, camp_window: object, global_window: object):
    win = _effective_window(camp_window, global_window)
    if not isinstance(win, dict) or not _window_is_nonempty(win):
        return now_de

    base_date = now_de.date()
    cur_min = int(now_de.hour) * 60 + int(now_de.minute)
    for day_shift in range(0, 15):
        day_obj = base_date + timedelta(days=day_shift)
        slots = _window_slots_for_date(win, day_obj)
        if not slots:
            continue
        for start_min, end_min in slots:
            if day_shift == 0:
                if start_min <= cur_min < end_min:
                    return now_de
                if cur_min >= start_min:
                    continue
            return now_de.replace(
                year=day_obj.year,
                month=day_obj.month,
                day=day_obj.day,
                hour=int(start_min // 60),
                minute=int(start_min % 60),
                second=0,
                microsecond=0,
            )
    return now_de


def _fmt_dt_short(dt_obj) -> str:
    try:
        return dt_obj.astimezone(_TZ_BERLIN).strftime("%d.%m %H:%M")
    except Exception:
        return "--.-- --:--"


def _contact_modal_url(aggr_contact_id: object) -> str:
    try:
        cid = int(aggr_contact_id or 0)
    except Exception:
        cid = 0
    if cid <= 0:
        return ""
    return reverse("contact_modal") + f"?id={encode_id(int(cid))}"


def _planned_contacts_by_campaign_ids(campaign_ids):
    out: dict[int, list[dict]] = {}
    ids = [int(x) for x in (campaign_ids or []) if int(x) > 0]
    if not ids:
        return out
    with connection.cursor() as cur:
        cur.execute(
            """
            WITH selected_campaigns AS (
              SELECT
                c.id,
                c.sending_list_id,
                c.campaign_parent_id,
                COALESCE(c.send_after_parent_days, 0)::int AS send_after_parent_days
              FROM public.campaigns_campaigns c
              WHERE c.id = ANY(%s)
            ),
            parent_send AS (
              SELECT
                lg.campaign_id AS parent_campaign_id,
                lg.aggr_contact_cb_id,
                MAX(COALESCE(lg.processed_at, lg.created_at)) AS parent_sent_at
              FROM public.sending_log lg
              JOIN (
                SELECT DISTINCT sc.campaign_parent_id
                FROM selected_campaigns sc
                WHERE sc.campaign_parent_id IS NOT NULL
              ) p
                ON p.campaign_parent_id = lg.campaign_id
              WHERE lg.processed = true
                AND lg.status = 'SEND'
              GROUP BY lg.campaign_id, lg.aggr_contact_cb_id
            ),
            planned AS (
              SELECT
                sc.id AS campaign_id,
                sl.aggr_contact_cb_id AS aggr_contact_id,
                COALESCE(NULLIF(trim(ac.company_name), ''), NULLIF(trim(ac.email), ''), '—') AS company_name,
                ROW_NUMBER() OVER (
                  PARTITION BY sc.id
                  ORDER BY sl.rate ASC NULLS LAST, sl.rate_cb ASC NULLS LAST, sl.aggr_contact_cb_id ASC
                ) AS rn
              FROM selected_campaigns sc
              JOIN public.aap_audience_audiencetask t
                ON t.id = sc.sending_list_id
              JOIN public.sending_lists sl
                ON sl.task_id = sc.sending_list_id
              JOIN public.aggr_contacts_cb ac
                ON ac.id = sl.aggr_contact_cb_id
              LEFT JOIN parent_send ps
                ON ps.parent_campaign_id = sc.campaign_parent_id
               AND ps.aggr_contact_cb_id = sl.aggr_contact_cb_id
              LEFT JOIN public.sending_log lg
                ON lg.campaign_id = sc.id
               AND lg.aggr_contact_cb_id = sl.aggr_contact_cb_id
              WHERE COALESCE(sl.removed, false) = false
                AND sl.rate IS NOT NULL
                AND sl.rate <= t.rate_limit
                AND COALESCE(ac.blocked, false) = false
                AND COALESCE(ac.wrong_email, false) = false
                AND lg.id IS NULL
                AND (
                  sc.campaign_parent_id IS NULL
                  OR (
                    ps.parent_sent_at IS NOT NULL
                    AND ps.parent_sent_at <= now() - (sc.send_after_parent_days * interval '1 day')
                  )
                )
            )
            SELECT campaign_id, aggr_contact_id, company_name, rn
            FROM planned
            WHERE rn <= 8
            ORDER BY campaign_id ASC, rn ASC
            """,
            [ids],
        )
        for campaign_id, aggr_contact_id, company_name, rn in cur.fetchall() or []:
            cid = int(campaign_id)
            out.setdefault(cid, []).append(
                {
                    "aggr_contact_id": int(aggr_contact_id) if aggr_contact_id is not None else None,
                    "company_name": str(company_name or "").strip() or "—",
                    "rn": int(rn or 0),
                }
            )
    return out


def _recent_sending_rows_by_campaign_ids(campaign_ids):
    out: dict[int, list[dict]] = {}
    ids = [int(x) for x in (campaign_ids or []) if int(x) > 0]
    if not ids:
        return out
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT campaign_id, sent_at, company_name, status, aggr_contact_id
            FROM (
              SELECT
                lg.campaign_id,
                COALESCE(lg.processed_at, lg.created_at) AS sent_at,
                COALESCE(NULLIF(trim(ac.company_name), ''), NULLIF(trim(ac.email), ''), '—') AS company_name,
                COALESCE(lg.status, '') AS status,
                lg.aggr_contact_cb_id AS aggr_contact_id,
                ROW_NUMBER() OVER (
                  PARTITION BY lg.campaign_id
                  ORDER BY COALESCE(lg.processed_at, lg.created_at) DESC, lg.id DESC
                ) AS rn
              FROM public.sending_log lg
              LEFT JOIN public.aggr_contacts_cb ac
                ON ac.id = lg.aggr_contact_cb_id
              WHERE lg.campaign_id = ANY(%s)
            ) q
            WHERE rn <= 3
            ORDER BY campaign_id ASC, sent_at DESC
            """,
            [ids],
        )
        for campaign_id, sent_at, company_name, status, aggr_contact_id in cur.fetchall() or []:
            cid = int(campaign_id)
            out.setdefault(cid, []).append(
                {
                    "sent_at": sent_at,
                    "company_name": str(company_name or "").strip() or "—",
                    "status": str(status or "").strip().upper(),
                    "aggr_contact_id": int(aggr_contact_id) if aggr_contact_id is not None else None,
                }
            )
    return out


def _build_timeline_rows(
    *,
    now_de,
    camp_window: object,
    global_window: object,
    sending_interval_ms: object,
    is_running: bool,
    is_in_window: bool,
    planned_contacts: list[dict],
    recent_rows: list[dict],
):
    try:
        interval_ms_i = int(sending_interval_ms or 0)
    except Exception:
        interval_ms_i = 0
    interval_sec = max(1.0, (float(interval_ms_i) / 1000.0)) if interval_ms_i > 0 else 30.0

    next_start_dt = _next_window_start_dt(now_de, camp_window, global_window)
    base_plan_dt = now_de if bool(is_running and is_in_window) else next_start_dt

    def _planned_row(idx: int, tone: str = "blue", icon: str = "clock"):
        name = "—"
        aggr_contact_id = None
        if 0 <= int(idx) < len(planned_contacts):
            name = str((planned_contacts[idx] or {}).get("company_name") or "").strip() or "—"
            aggr_contact_id = (planned_contacts[idx] or {}).get("aggr_contact_id")
        dt_obj = base_plan_dt + timedelta(seconds=float(max(0, int(idx))) * interval_sec)
        cid = int(aggr_contact_id or 0) if aggr_contact_id is not None else 0
        return {
            "time_text": _fmt_dt_short(dt_obj),
            "company_name": name,
            "contact_modal_url": _contact_modal_url(aggr_contact_id),
            "row_key": f"plan:{int(idx)}:{cid}",
            "tone": str(tone),
            "icon": str(icon),
        }

    sent_rows: list[dict] = []
    for it in (recent_rows or [])[:3]:
        st = str((it or {}).get("status") or "").strip().upper()
        sent_at = (it or {}).get("sent_at") or now_de
        sent_rows.append(
            {
                "time_text": _fmt_dt_short(sent_at),
                "company_name": str((it or {}).get("company_name") or "").strip() or "—",
                "contact_modal_url": _contact_modal_url((it or {}).get("aggr_contact_id")),
                "row_key": (
                    f"sent:{str((it or {}).get('status') or '').strip().upper()}:"
                    f"{int((it or {}).get('aggr_contact_id') or 0)}:"
                    f"{_fmt_dt_short(sent_at)}"
                ),
                "tone": "green" if st == "SEND" else "gray",
                "icon": "check" if st == "SEND" else "info",
            }
        )

    yellow_row: dict
    if bool(is_running and is_in_window):
        active_name = "—"
        active_contact_id = None
        if planned_contacts:
            active_name = str((planned_contacts[0] or {}).get("company_name") or "").strip() or "—"
            active_contact_id = (planned_contacts[0] or {}).get("aggr_contact_id")
        yellow_row = {
            "time_text": _fmt_dt_short(now_de),
            "company_name": active_name,
            "contact_modal_url": _contact_modal_url(active_contact_id),
            "row_key": f"yellow:active:{int(active_contact_id or 0)}",
            "tone": "yellow",
            "icon": "spinner",
        }
    else:
        yellow_planned = _planned_row(0, tone="yellow", icon="clock")
        yellow_row = {
            "time_text": yellow_planned.get("time_text") or _fmt_dt_short(next_start_dt),
            "company_name": yellow_planned.get("company_name") or "—",
            "contact_modal_url": yellow_planned.get("contact_modal_url") or "",
            "row_key": str(yellow_planned.get("row_key") or "yellow:plan:0"),
            "tone": "yellow",
            "icon": "clock",
        }

    sent_count = max(0, min(3, len(sent_rows)))
    blue_needed = 4 - sent_count
    blue_rows: list[dict] = []
    # Keep nearest planned contact right above yellow; farther ones go higher.
    for i in range(blue_needed):
        planned_idx = 1 + (blue_needed - 1 - i)
        blue_rows.append(_planned_row(planned_idx, tone="blue", icon="clock"))

    rows = blue_rows + [yellow_row] + sent_rows[:sent_count]
    return rows[:5]


def _demo_contacts_pool(limit: int = 24) -> list[dict]:
    out: list[dict] = []
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              id,
              COALESCE(NULLIF(trim(company_name), ''), NULLIF(trim(email), ''), '—') AS company_name
            FROM public.aggr_contacts_cb
            WHERE COALESCE(blocked, false) = false
            ORDER BY random()
            LIMIT %s
            """,
            [int(limit)],
        )
        for cid, company_name in cur.fetchall() or []:
            out.append(
                {
                    "id": int(cid),
                    "company_name": str(company_name or "").strip() or "—",
                    "contact_modal_url": _contact_modal_url(cid),
                }
            )
    if out:
        return out
    return [
        {"id": 0, "company_name": _trans("Контакт 1"), "contact_modal_url": ""},
        {"id": 0, "company_name": _trans("Контакт 2"), "contact_modal_url": ""},
        {"id": 0, "company_name": _trans("Контакт 3"), "contact_modal_url": ""},
        {"id": 0, "company_name": _trans("Контакт 4"), "contact_modal_url": ""},
        {"id": 0, "company_name": _trans("Контакт 5"), "contact_modal_url": ""},
    ]


def _build_demo_overview(company_name: str, now_de) -> dict:
    contacts = _demo_contacts_pool(limit=24)
    contacts_len = max(1, len(contacts))
    sent_count = int(random.randint(1000, 1500))
    views_count = max(0, int(round(float(sent_count) * random.uniform(0.09, 0.12))))
    views_pct = _pct_one_decimal(views_count, sent_count)
    interval_sec = 45
    active_index = 0
    sent_history_indices = [((contacts_len - 1 - i) % contacts_len) for i in range(min(3, contacts_len))]

    def _contact(idx: int) -> dict:
        safe_idx = int(idx) % contacts_len
        item = contacts[safe_idx]
        return {
            "idx": safe_idx,
            "company_name": str(item.get("company_name") or "—"),
            "contact_modal_url": str(item.get("contact_modal_url") or ""),
        }

    timeline_rows: list[dict] = []
    sent_shown = min(3, len(sent_history_indices))
    blue_needed = 4 - sent_shown
    for offset in range(blue_needed, 0, -1):
        c = _contact(active_index + offset)
        dt_obj = now_de + timedelta(seconds=interval_sec * offset)
        timeline_rows.append(
            {
                "time_text": _fmt_dt_short(dt_obj),
                "company_name": c["company_name"],
                "contact_modal_url": c["contact_modal_url"],
                "row_key": f"demo:plan:{c['idx']}:{offset}",
                "tone": "blue",
                "icon": "clock",
            }
        )

    active_contact = _contact(active_index)
    timeline_rows.append(
        {
            "time_text": _fmt_dt_short(now_de),
            "company_name": active_contact["company_name"],
            "contact_modal_url": active_contact["contact_modal_url"],
            "row_key": f"demo:yellow:{active_contact['idx']}",
            "tone": "yellow",
            "icon": "spinner",
        }
    )

    for j in range(sent_shown):
        c = _contact(sent_history_indices[j])
        dt_obj = now_de - timedelta(seconds=interval_sec * (j + 1))
        timeline_rows.append(
            {
                "time_text": _fmt_dt_short(dt_obj),
                "company_name": c["company_name"],
                "contact_modal_url": c["contact_modal_url"],
                "row_key": f"demo:sent:{c['idx']}:{j}",
                "tone": "green",
                "icon": "check",
            }
        )

    ws_name = str(company_name or "").strip() or "—"
    return {
        "title": _trans("Успешная кампания по рассылке - %(name)s") % {"name": ws_name},
        "sent_count": sent_count,
        "sent_count_fmt": f"{sent_count:,}".replace(",", " "),
        "views_count": views_count,
        "views_count_fmt": f"{views_count:,}".replace(",", " "),
        "views_pct": views_pct,
        "timeline_rows": timeline_rows[:5],
        "contacts": contacts,
        "active_index": active_index,
        "sent_history_indices": sent_history_indices,
        "interval_sec": interval_sec,
    }


def dashboard(request):
    if not request.user.is_authenticated:
        return render(request, "public/login.html", status=401)

    ws_id = getattr(request, "workspace_id", None)
    if ws_id is None:
        return render(request, "panels/access_denied.html")

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "toggle_user_active":
            post_id = (request.POST.get("id") or "").strip()
            try:
                target_pk = int(decode_id(post_id))
            except Exception:
                target_pk = 0

            if target_pk > 0:
                target = Campaign.objects.filter(id=target_pk, workspace_id=ws_id, archived=False).only("id", "user_active").first()
                if target:
                    letter_obj = (
                        Letter.objects.filter(workspace_id=ws_id, campaign_id=int(target.id))
                        .only("ready_content")
                        .first()
                    )
                    has_ready_letter = bool(letter_obj and str(getattr(letter_obj, "ready_content", "") or "").strip())
                    if has_ready_letter:
                        target.user_active = not bool(target.user_active)
                        target.save(update_fields=["user_active", "updated_at"])
            return redirect(request.get_full_path())

        if action == "toggle_task_user_active":
            post_id = (request.POST.get("id") or "").strip()
            try:
                target_pk = int(decode_id(post_id))
            except Exception:
                target_pk = 0

            if target_pk > 0:
                task = (
                    AudienceTask.objects.filter(id=target_pk, workspace_id=ws_id, archived=False)
                    .only("id", "ready", "user_active")
                    .first()
                )
                if task and bool(task.ready):
                    task.user_active = not bool(task.user_active)
                    task.save(update_fields=["user_active", "updated_at"])
            return redirect(request.get_full_path())

    now_de = timezone.now().astimezone(_TZ_BERLIN)
    global_window_json = _resolve_global_window(ws_id)
    ready_letter_campaign_ids = {
        int(x.campaign_id)
        for x in Letter.objects.filter(workspace_id=ws_id).only("campaign_id", "ready_content")
        if str(getattr(x, "ready_content", "") or "").strip()
    }
    campaigns = [
        {
            "id": int(c.id),
            "ui_id": encode_id(int(c.id)),
            "title": (c.title or "").strip() or f"#{int(c.id)}",
            "type": str(getattr(getattr(c, "sending_list", None), "type", "") or "").strip().lower(),
            "user_active": bool(getattr(c, "user_active", False)),
            "window": getattr(c, "window", None),
            "sending_interval": getattr(c, "sending_interval", None),
            "has_ready_letter": int(c.id) in ready_letter_campaign_ids,
            "is_in_window": bool(
                bool(getattr(c, "user_active", False)) and (int(c.id) in ready_letter_campaign_ids)
                and _is_now_in_send_window(now_de, getattr(c, "window", None), global_window_json)
            ),
        }
        for c in Campaign.objects.filter(workspace_id=ws_id, archived=False)
        .select_related("sending_list")
        .only("id", "title", "user_active", "window", "sending_interval", "sending_list__type")
        .order_by("id")
    ]
    campaign_ids = [int(it["id"]) for it in campaigns]
    sent_by_campaign_id = _sent_counts_by_campaign_ids(campaign_ids)
    views_by_campaign_id = _views_counts_by_campaign_ids(campaign_ids)
    planned_by_campaign = _planned_contacts_by_campaign_ids(campaign_ids)
    recent_by_campaign = _recent_sending_rows_by_campaign_ids(campaign_ids)
    for it in campaigns:
        it["is_running"] = bool(it["user_active"] and it["has_ready_letter"])
        it["sent_count"] = int(sent_by_campaign_id.get(int(it["id"]), 0))
        it["sent_count_fmt"] = f"{int(it['sent_count']):,}".replace(",", " ")
        it["views_count"] = int(views_by_campaign_id.get(int(it["id"]), 0))
        it["views_count_fmt"] = f"{int(it['views_count']):,}".replace(",", " ")
        it["views_pct"] = _pct_one_decimal(it["views_count"], it["sent_count"])
        it["timeline_rows"] = _build_timeline_rows(
            now_de=now_de,
            camp_window=it.get("window"),
            global_window=global_window_json,
            sending_interval_ms=it.get("sending_interval"),
            is_running=bool(it["is_running"]),
            is_in_window=bool(it["is_in_window"]),
            planned_contacts=planned_by_campaign.get(int(it["id"]), []),
            recent_rows=recent_by_campaign.get(int(it["id"]), []),
        )
    workspace_company_name = ""
    if getattr(request.user, "workspace", None):
        workspace_company_name = str(getattr(request.user.workspace, "company_name", "") or "").strip()
    overview_demo = _build_demo_overview(workspace_company_name, now_de) if not campaigns else None
    overview_site_click_rows = _overview_site_click_rows(ws_id, limit=8)
    mailing_items = _overview_mailing_items(ws_id)
    mailing_list_rows = [mailing_items[i : i + 2] for i in range(0, len(mailing_items), 2)]
    campaign_rows = [campaigns[i : i + 2] for i in range(0, len(campaigns), 2)]
    return render(
        request,
        "panels/overview.html",
        {
            "campaign_rows": campaign_rows,
            "mailing_list_rows": mailing_list_rows,
            "overview_demo": overview_demo,
            "overview_site_click_rows": overview_site_click_rows,
        },
    )


def overview_live_stats(request):
    if not request.user.is_authenticated:
        return JsonResponse({"ok": False, "error": "unauthorized"}, status=401)

    ws_id = getattr(request, "workspace_id", None)
    if ws_id is None:
        return JsonResponse({"ok": False, "error": "access_denied"}, status=403)

    now_de = timezone.now().astimezone(_TZ_BERLIN)
    global_window_json = _resolve_global_window(ws_id)
    ready_letter_campaign_ids = {
        int(x.campaign_id)
        for x in Letter.objects.filter(workspace_id=ws_id).only("campaign_id", "ready_content")
        if str(getattr(x, "ready_content", "") or "").strip()
    }
    campaigns = list(
        Campaign.objects.filter(workspace_id=ws_id, archived=False)
        .only("id", "user_active", "window", "sending_interval")
        .order_by("id")
    )
    campaign_ids = [int(x.id) for x in campaigns if getattr(x, "id", None) is not None]
    sent_by_campaign_id = _sent_counts_by_campaign_ids(campaign_ids)
    views_by_campaign_id = _views_counts_by_campaign_ids(campaign_ids)
    planned_by_campaign = _planned_contacts_by_campaign_ids(campaign_ids)
    recent_by_campaign = _recent_sending_rows_by_campaign_ids(campaign_ids)
    items = []
    for camp in campaigns:
        cid = int(camp.id)
        ui = encode_id(int(cid))
        sent = int(sent_by_campaign_id.get(int(cid), 0))
        views = int(views_by_campaign_id.get(int(cid), 0))
        views_pct = _pct_one_decimal(views, sent)
        has_ready_letter = int(cid) in ready_letter_campaign_ids
        is_running = bool(bool(getattr(camp, "user_active", False)) and has_ready_letter)
        is_in_window = bool(
            is_running and _is_now_in_send_window(now_de, getattr(camp, "window", None), global_window_json)
        )
        items.append(
            {
                "campaign_id": int(cid),
                "ui_id": ui,
                "sent_count": sent,
                "sent_count_fmt": f"{sent:,}".replace(",", " "),
                "views_count": views,
                "views_count_fmt": f"{views:,}".replace(",", " "),
                "views_pct": views_pct,
                "is_running": is_running,
                "is_in_window": is_in_window,
                "timeline_rows": _build_timeline_rows(
                    now_de=now_de,
                    camp_window=getattr(camp, "window", None),
                    global_window=global_window_json,
                    sending_interval_ms=getattr(camp, "sending_interval", None),
                    is_running=is_running,
                    is_in_window=is_in_window,
                    planned_contacts=planned_by_campaign.get(int(cid), []),
                    recent_rows=recent_by_campaign.get(int(cid), []),
                ),
            }
        )
    traffic_rows = _overview_site_click_rows(ws_id, limit=8)
    mailing_items = _overview_mailing_items(ws_id)
    return JsonResponse({"ok": True, "items": items, "traffic_rows": traffic_rows, "mailing_items": mailing_items})


def stats_view(request):
    return redirect("stats_clicks")


def _stats_clicks_campaign_context(request, ws_id):
    selected_ui = str(request.GET.get("campaign") or "").strip()
    selected_id = 0
    if selected_ui:
        try:
            selected_id = int(decode_id(selected_ui))
        except Exception:
            selected_id = 0

    campaigns = list(
        Campaign.objects.filter(workspace_id=ws_id)
        .select_related("sending_list")
        .only("id", "title", "archived", "sending_list__type")
        .order_by("archived", "id")
    )

    active_items = []
    archived_items = []
    selected_item = None

    for c in campaigns:
        cid = int(c.id)
        c_type = str(getattr(getattr(c, "sending_list", None), "type", "") or "").strip().lower()
        item = {
            "id": cid,
            "ui_id": encode_id(cid),
            "title": str(getattr(c, "title", "") or "").strip() or f"#{cid}",
            "type": c_type,
            "archived": bool(getattr(c, "archived", False)),
        }
        if item["archived"]:
            archived_items.append(item)
        else:
            active_items.append(item)
        if selected_id > 0 and cid == selected_id:
            selected_item = item

    selected_title = _trans("Все кампании")
    selected_type = "all"
    if selected_item:
        selected_title = str(selected_item["title"])
        selected_type = str(selected_item.get("type") or "").strip().lower() or "all"
        if selected_item["archived"]:
            selected_title = f"{selected_title} ({_trans('архивная')})"
    if selected_type not in {"buy", "sell"}:
        selected_type = "all"

    return {
        "stats_campaign_active": active_items,
        "stats_campaign_archived": archived_items,
        "stats_campaign_selected_ui": (str(selected_item["ui_id"]) if selected_item else ""),
        "stats_campaign_selected_id": int(selected_item["id"]) if selected_item else 0,
        "stats_campaign_selected_title": selected_title,
        "stats_campaign_selected_type": selected_type,
    }


def _stats_stub_view(request, *, section: str):
    if not request.user.is_authenticated:
        return render(request, "public/login.html", status=401)

    ws_id = getattr(request, "workspace_id", None)
    if ws_id is None:
        return render(request, "panels/access_denied.html")

    stats_section = str(section or "").strip().lower()
    if stats_section not in {"clicks", "sending"}:
        stats_section = "clicks"
    ctx = {
        "stats_section": stats_section,
        "stats_section_url_name": ("stats_clicks" if stats_section == "clicks" else "stats_sending"),
    }
    camp_ctx = _stats_clicks_campaign_context(request, ws_id)
    ctx.update(camp_ctx)
    selected_ui = str(camp_ctx.get("stats_campaign_selected_ui") or "")
    selected_campaign_id = int(camp_ctx.get("stats_campaign_selected_id") or 0)
    if selected_campaign_id > 0 and stats_section == "clicks":
        sent_cnt = int(_sent_counts_by_campaign_ids([selected_campaign_id]).get(selected_campaign_id, 0))
        unique_cnt = int(_views_counts_by_campaign_ids([selected_campaign_id]).get(selected_campaign_id, 0))
        ctx["stats_campaign_summary_show"] = True
        ctx["stats_campaign_summary_mode"] = "clicks"
        ctx["stats_campaign_unique_fmt"] = f"{unique_cnt:,}".replace(",", " ")
        ctx["stats_campaign_sent_fmt"] = f"{sent_cnt:,}".replace(",", " ")
        ctx["stats_campaign_ctr"] = _pct_one_decimal(unique_cnt, sent_cnt)
    elif selected_campaign_id > 0 and stats_section == "sending":
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*)::int AS total_cnt,
                  COUNT(*) FILTER (WHERE UPPER(COALESCE(status, '')) = 'SEND')::int AS ok_cnt
                FROM public.sending_log
                WHERE campaign_id = %s
                """,
                [int(selected_campaign_id)],
            )
            row = cur.fetchone() or [0, 0]
        total_cnt = int(row[0] or 0)
        ok_cnt = int(row[1] or 0)
        ctx["stats_campaign_summary_show"] = True
        ctx["stats_campaign_summary_mode"] = "sending"
        ctx["stats_campaign_send_ok_fmt"] = f"{ok_cnt:,}".replace(",", " ")
        ctx["stats_campaign_send_total_fmt"] = f"{total_cnt:,}".replace(",", " ")
        ctx["stats_campaign_send_success_pct"] = _pct_one_decimal(ok_cnt, total_cnt)
    else:
        ctx["stats_campaign_summary_show"] = False
        ctx["stats_campaign_summary_mode"] = ""
        ctx["stats_campaign_unique_fmt"] = "0"
        ctx["stats_campaign_sent_fmt"] = "0"
        ctx["stats_campaign_ctr"] = "0.0"
        ctx["stats_campaign_send_ok_fmt"] = "0"
        ctx["stats_campaign_send_total_fmt"] = "0"
        ctx["stats_campaign_send_success_pct"] = "0.0"
    ctx["stats_clicks_show_all"] = bool(stats_section == "clicks" and not selected_ui)
    ctx["stats_clicks_show_campaign"] = bool(stats_section == "clicks" and selected_ui and selected_campaign_id > 0)
    ctx["stats_sending_show_all"] = bool(stats_section == "sending" and not selected_ui)
    ctx["stats_sending_show_campaign"] = bool(stats_section == "sending" and selected_ui and selected_campaign_id > 0)

    if bool(ctx["stats_clicks_show_all"]):
        page_raw = (request.GET.get("p") or "").strip()
        try:
            page_i = int(page_raw or "1")
        except Exception:
            page_i = 1
        page_data = _stats_site_click_rows_page(ws_id, limit=100, page=page_i)
        ctx["stats_clicks_rows"] = page_data.get("rows") or []
        ctx["stats_clicks_page"] = int(page_data.get("page") or 1)
        ctx["stats_clicks_pages"] = int(page_data.get("pages") or 1)
        ctx["stats_clicks_has_prev"] = bool(page_data.get("has_prev"))
        ctx["stats_clicks_has_next"] = bool(page_data.get("has_next"))
        ctx["stats_clicks_prev_page"] = int(page_data.get("prev_page") or 1)
        ctx["stats_clicks_next_page"] = int(page_data.get("next_page") or 1)
        ctx["stats_clicks_total_display"] = str(page_data.get("total_display") or "0")
        ctx["stats_clicks_page_items"] = page_data.get("page_items") or []
    elif bool(ctx["stats_clicks_show_campaign"]):
        rows = _stats_site_click_rows_for_campaign(ws_id, campaign_id=selected_campaign_id)
        split_at = (len(rows) + 1) // 2
        ctx["stats_clicks_campaign_left_rows"] = rows[:split_at]
        ctx["stats_clicks_campaign_right_rows"] = rows[split_at:]
    elif bool(ctx["stats_sending_show_all"]):
        page_raw = (request.GET.get("p") or "").strip()
        try:
            page_i = int(page_raw or "1")
        except Exception:
            page_i = 1
        page_data = _stats_sending_rows_page(ws_id, limit=100, page=page_i)
        ctx["stats_sending_rows"] = page_data.get("rows") or []
        ctx["stats_sending_page"] = int(page_data.get("page") or 1)
        ctx["stats_sending_pages"] = int(page_data.get("pages") or 1)
        ctx["stats_sending_has_prev"] = bool(page_data.get("has_prev"))
        ctx["stats_sending_has_next"] = bool(page_data.get("has_next"))
        ctx["stats_sending_prev_page"] = int(page_data.get("prev_page") or 1)
        ctx["stats_sending_next_page"] = int(page_data.get("next_page") or 1)
        ctx["stats_sending_total_display"] = str(page_data.get("total_display") or "0")
        ctx["stats_sending_page_items"] = page_data.get("page_items") or []
    elif bool(ctx["stats_sending_show_campaign"]):
        page_raw = (request.GET.get("p") or "").strip()
        try:
            page_i = int(page_raw or "1")
        except Exception:
            page_i = 1
        page_data = _stats_sending_rows_for_campaign_page(ws_id, campaign_id=selected_campaign_id, limit=200, page=page_i)
        rows = page_data.get("rows") or []
        split_at = (len(rows) + 1) // 2
        ctx["stats_sending_campaign_left_rows"] = rows[:split_at]
        ctx["stats_sending_campaign_right_rows"] = rows[split_at:]
        ctx["stats_sending_campaign_page"] = int(page_data.get("page") or 1)
        ctx["stats_sending_campaign_pages"] = int(page_data.get("pages") or 1)
        ctx["stats_sending_campaign_has_prev"] = bool(page_data.get("has_prev"))
        ctx["stats_sending_campaign_has_next"] = bool(page_data.get("has_next"))
        ctx["stats_sending_campaign_prev_page"] = int(page_data.get("prev_page") or 1)
        ctx["stats_sending_campaign_next_page"] = int(page_data.get("next_page") or 1)
        ctx["stats_sending_campaign_total_display"] = str(page_data.get("total_display") or "0")
        ctx["stats_sending_campaign_page_items"] = page_data.get("page_items") or []
    return render(request, "panels/stats.html", ctx)


def stats_clicks_view(request):
    return _stats_stub_view(request, section="clicks")


def stats_sending_view(request):
    return _stats_stub_view(request, section="sending")


def _can_switch_user(request) -> bool:
    if not request.user.is_authenticated:
        return False
    ws = getattr(request.user, "workspace", None)
    return bool(ws and ws.access_type == "super")


def switch_user_modal_view(request):
    if not _can_switch_user(request):
        return render(
            request,
            "panels/modals/switch_user.html",
            {"items": [], "forbidden": True},
        )

    items = []
    qs = (
        ClientUser.objects.select_related("workspace")
        .filter(archived=False, workspace__isnull=False, workspace__archived=False)
        .order_by("workspace__company_name", "email")
    )
    for user in qs:
        ws = user.workspace
        full_name = f"{(user.first_name or '').strip()} {(user.last_name or '').strip()}".strip()
        items.append(
            {
                "ui_id": encode_id(int(user.id)),
                "company_name": (ws.company_name if ws else "") or "",
                "email": user.email or "",
                "full_name": full_name or "-",
                "access": (ws.access_type if ws else "") or "",
            }
        )

    return render(
        request,
        "panels/modals/switch_user.html",
        {"items": items, "forbidden": False},
    )


@require_POST
def switch_user_login_view(request):
    if not _can_switch_user(request):
        return redirect("dashboard")

    raw_uid = (request.POST.get("uid") or "").strip()
    try:
        user_id = int(decode_id(raw_uid))
    except Exception:
        messages.error(request, _trans("Неверный пользователь"))
        return redirect("dashboard")

    target = (
        ClientUser.objects.select_related("workspace")
        .filter(id=user_id, archived=False, workspace__isnull=False, workspace__archived=False)
        .first()
    )
    if not target:
        messages.error(request, _trans("Пользователь недоступен"))
        return redirect("dashboard")

    backend = (
        request.session.get("_auth_user_backend")
        or getattr(request.user, "backend", "")
        or settings.AUTHENTICATION_BACKENDS[0]
    )
    auth_login(request, target, backend=backend)
    return redirect("dashboard")


def contact_modal_view(request):
    def _flat_text(value):
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (int, float, bool)):
            return str(value).strip()
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False, sort_keys=True).strip()
        if isinstance(value, list):
            parts = []
            for item in value:
                text = _flat_text(item)
                if text:
                    parts.append(text)
            return "\n".join(parts).strip()
        return str(value).strip()

    def _text_or_dash(value):
        text = _flat_text(value)
        return text if text else "-"

    def _comma_text_or_dash(value):
        if isinstance(value, list):
            out = []
            for item in value:
                text = _flat_text(item)
                if text and text not in out:
                    out.append(text)
            return ", ".join(out) if out else "-"
        text = _flat_text(value)
        if not text:
            return "-"
        if "\n" not in text:
            return text
        out = []
        for part in text.split("\n"):
            clean = part.strip()
            if clean and clean not in out:
                out.append(clean)
        return ", ".join(out) if out else "-"

    def _format_phone_one(raw: str) -> str:
        text = str(raw or "").strip()
        if not text:
            return ""
        try:
            parsed = phonenumbers.parse(text, "DE")
            if phonenumbers.is_possible_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL).strip()
        except Exception:
            return text
        return text

    def _format_phone_block(value):
        if isinstance(value, list):
            out = []
            for item in value:
                text = _format_phone_one(item)
                if text and text not in out:
                    out.append(text)
            return "\n".join(out) if out else "-"
        text = _flat_text(value)
        if not text:
            return "-"
        parts = [part.strip() for part in text.split(",")]
        if len(parts) <= 1:
            return _format_phone_one(text) or "-"
        out = []
        for part in parts:
            phone = _format_phone_one(part)
            if phone and phone not in out:
                out.append(phone)
        return "\n".join(out) if out else "-"

    def _format_link_block(value):
        if isinstance(value, list):
            out = []
            for item in value:
                text = _flat_text(item)
                if text and text not in out:
                    out.append(text)
            return "\n".join(out) if out else "-"
        text = _flat_text(value)
        if not text:
            return "-"
        parts = [part.strip() for part in text.split(",")]
        if len(parts) <= 1:
            return text
        out = []
        for part in parts:
            if part and part not in out:
                out.append(part)
        return "\n".join(out) if out else "-"

    def _link_items(value):
        def _label(url_text: str) -> str:
            text = str(url_text or "").strip()
            text = text.removeprefix("https://")
            text = text.removeprefix("http://")
            if text.endswith("/"):
                text = text[:-1]
            return text

        items = []
        if isinstance(value, list):
            for item in value:
                href = _flat_text(item)
                if not href:
                    continue
                label = _label(href)
                if not label:
                    continue
                pair = {"href": href, "label": label}
                if pair not in items:
                    items.append(pair)
            return items

        text = _flat_text(value)
        if not text:
            return items
        for part in text.split(","):
            href = part.strip()
            if not href:
                continue
            label = _label(href)
            if not label:
                continue
            pair = {"href": href, "label": label}
            if pair not in items:
                items.append(pair)
        return items

    def _build_empty_context(status_text):
        return {
            "status_class": "YY-STATUS_GRAY",
            "status_text": status_text,
            "contact_id": "-",
            "title_company_name": "-",
            "title_company_names": "-",
            "title_email": "-",
            "title_emails": "-",
            "title_phones": "-",
            "title_fax": "-",
            "title_websites": "-",
            "title_socials": "-",
            "title_address": "-",
            "title_addresses": "-",
            "title_city": "-",
            "title_land": "-",
            "title_categories": "-",
            "title_search_cities": "-",
            "title_search_categories": "-",
            "title_statuses_11880": "-",
            "title_keywords_11880": "-",
            "title_description": "-",
        }

    if not request.user.is_authenticated:
        return render(
            request,
            "panels/modals/contact_from_audience.html",
            _build_empty_context(_trans("Здесь пока нет данных")),
            status=401,
        )

    ws_id = getattr(request, "workspace_id", None)
    if ws_id is None:
        return render(
            request,
            "panels/modals/contact_from_audience.html",
            _build_empty_context(_trans("Здесь пока нет данных")),
            status=403,
        )

    token = (request.GET.get("id") or "").strip()
    if not token:
        return render(
            request,
            "panels/modals/contact_from_audience.html",
            _build_empty_context(_trans("Здесь пока нет данных")),
        )

    try:
        aggr_contact_id = int(decode_id(token))
    except Exception:
        return render(
            request,
            "panels/modals/contact_from_audience.html",
            _build_empty_context(_trans("Здесь пока нет данных")),
        )

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                ac.id::bigint,
                ac.company_name,
                ac.email,
                ac.company_data
            FROM public.aggr_contacts_cb ac
            WHERE ac.id = %s
            LIMIT 1
            """,
            [int(aggr_contact_id)],
        )
        row = cur.fetchone()

    if not row:
        return render(
            request,
            "panels/modals/contact_from_audience.html",
            _build_empty_context(_trans("Здесь пока нет данных")),
        )

    company_data = parse_json_object(row[3], field_name="aggr_contacts_cb.company_data")
    norm = company_data.get("norm") if isinstance(company_data.get("norm"), dict) else {}
    cards = company_data.get("cards") if isinstance(company_data.get("cards"), dict) else {}
    source_urls = []
    for _unused_key, card_wrap in cards.items():
        if not isinstance(card_wrap, dict):
            continue
        src = _flat_text(card_wrap.get("url"))
        if src:
            source_urls.append(src)
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                cp.plz_id,
                cp.branch_id
            FROM public.cb_contacts cc
            JOIN public.cb_crawl_pairs cp
              ON cp.id = cc.cb_id
            WHERE cc.aggr_contact_id = %s
            ORDER BY cc.cb_id DESC
            LIMIT 1
            """,
            [int(row[0])],
        )
        search_rows = cur.fetchall() or []

    search_cities = []
    search_categories = []
    seen_plz_ids = set()
    seen_branch_ids = set()
    for plz_id, branch_id in search_rows:
        if plz_id is not None:
            plz_id_int = int(plz_id)
            if plz_id_int not in seen_plz_ids:
                seen_plz_ids.add(plz_id_int)
                search_cities.append(get_city_title(plz_id_int, request, land=True, plz=False))
        if branch_id is not None:
            branch_id_int = int(branch_id)
            if branch_id_int not in seen_branch_ids:
                seen_branch_ids.add(branch_id_int)
                search_categories.append(get_category_title(branch_id_int, request))

    return render(
        request,
        "panels/modals/contact_from_audience.html",
        {
            "status_class": "YY-STATUS_BLUE",
            "status_text": _trans("Карточка компании"),
            "contact_id": int(row[0]),
            "title_company_name": _text_or_dash(norm.get("company_name")),
            "title_company_names": _text_or_dash(norm.get("company_names")),
            "title_email": _text_or_dash(norm.get("email")),
            "title_emails": _text_or_dash(norm.get("emails")),
            "title_phones": _format_phone_block(norm.get("phones")),
            "title_fax": _format_phone_block(norm.get("fax")),
            "title_websites": _format_link_block(norm.get("websites")),
            "title_socials": _format_link_block(norm.get("socials")),
            "title_websites_items": _link_items(norm.get("websites")),
            "title_socials_items": _link_items(norm.get("socials")),
            "title_source_items": _link_items(source_urls),
            "title_address": _text_or_dash(norm.get("address")),
            "title_addresses": _text_or_dash(norm.get("addresses")),
            "title_city": _text_or_dash(norm.get("city")),
            "title_land": _text_or_dash(norm.get("land")),
            "title_categories": _comma_text_or_dash(norm.get("categories")),
            "title_search_cities": "\n".join(search_cities) if search_cities else "-",
            "title_search_categories": "\n".join(search_categories) if search_categories else "-",
            "title_statuses_11880": _text_or_dash(norm.get("statuses_11880")),
            "title_keywords_11880": _text_or_dash(norm.get("keywords_11880")),
            "title_description": _text_or_dash(norm.get("description")),
            "title_description_web": _text_or_dash(norm.get("description_web")),
        },
    )
