# FILE: web/panel/views.py
# DATE: 2026-03-08
# PURPOSE: panel main views: overview + stats + switch-user.

from __future__ import annotations

import json
from datetime import timedelta

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import login as auth_login
from django.db import connection
from django.shortcuts import redirect
from django.shortcuts import render
from django.views.decorators.http import require_POST
from django.utils import timezone

from mailer_web.access import encode_id, decode_id
from mailer_web.models import ClientUser
from panel.aap_audience.models import AudienceTask
from panel.aap_campaigns.models import Campaign


def dashboard(request):
    if not request.user.is_authenticated:
        return render(request, "public/login.html", status=401)
    ws_id = getattr(request, "workspace_id", None)
    if ws_id is None:
        return render(request, "panels/access_denied.html")

    user = request.user
    company_name = ""
    if getattr(user, "workspace", None):
        company_name = (user.workspace.company_name or "").strip()

    recent_campaigns = list(
        Campaign.objects.filter(workspace_id=ws_id)
        .select_related("mailing_list")
        .prefetch_related("mailing_list__audience_tasks")
        .only("id", "title", "active", "created_at", "end_at", "mailing_list__title", "mailing_list_id", "window")
        .order_by("-created_at")
    )[:3]
    camp_ids = [int(x.id) for x in recent_campaigns]

    global_window = {}
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT value_json
            FROM public.aap_settings_sending_settings
            WHERE workspace_id = %s::uuid
            LIMIT 1
            """,
            [ws_id],
        )
        row = cur.fetchone()
        if row:
            raw = row[0]
            if isinstance(raw, dict):
                global_window = raw
            elif isinstance(raw, str):
                try:
                    parsed = json.loads(raw)
                except Exception:
                    parsed = {}
                if isinstance(parsed, dict):
                    global_window = parsed

    def _parse_hhmm(v: str):
        if not isinstance(v, str) or ":" not in v:
            return None
        try:
            h, m = v.split(":", 1)
            hh = int(h)
            mm = int(m)
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return hh * 60 + mm
        except Exception:
            return None
        return None

    def _iter_slots(day_slots):
        if isinstance(day_slots, list):
            for p in day_slots:
                if isinstance(p, (list, tuple)) and len(p) == 2:
                    yield p[0], p[1]
                elif isinstance(p, dict):
                    a = p.get("from") or p.get("start")
                    b = p.get("to") or p.get("end")
                    if a and b:
                        yield a, b

    def _has_any_slot(win) -> bool:
        if not isinstance(win, dict):
            return False
        for day_key in ("mon", "tue", "wed", "thu", "fri", "sat", "sun", "hol"):
            for _ in _iter_slots(win.get(day_key, [])):
                return True
        return False

    def _effective_window(camp_window):
        if _has_any_slot(camp_window):
            return camp_window
        if _has_any_slot(global_window):
            return global_window
        return {}

    def _window_minutes_for_date(camp_window, day_de) -> int:
        win = _effective_window(camp_window)
        if not isinstance(win, dict):
            return 0
        wd = day_de.weekday()
        key = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")[wd]
        total_minutes = 0
        for a_str, b_str in _iter_slots(win.get(key, [])):
            a = _parse_hhmm(a_str)
            b = _parse_hhmm(b_str)
            if a is None or b is None or b <= a:
                continue
            total_minutes += (b - a)
        return int(total_minutes)

    def _window_minutes_today(camp_window) -> int:
        now_de = timezone.localtime().date()
        return _window_minutes_for_date(camp_window, now_de)

    def _is_now_in_send_window(camp_window):
        now_de = timezone.localtime()
        win = _effective_window(camp_window)
        if not isinstance(win, dict):
            return False
        wd = now_de.weekday()
        key = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")[wd]
        cur = now_de.hour * 60 + now_de.minute
        for a_str, b_str in _iter_slots(win.get(key, [])):
            a = _parse_hhmm(a_str)
            b = _parse_hhmm(b_str)
            if a is None or b is None or b <= a:
                continue
            if a <= cur < b:
                return True
        return False

    campaign_totals: dict[int, int] = {}
    campaign_sent: dict[int, int] = {}
    campaign_delivered: dict[int, int] = {}
    campaign_views: dict[int, int] = {}
    campaign_daily_sent: dict[tuple[int, object], int] = {}
    campaign_daily_delivered: dict[tuple[int, object], int] = {}
    campaign_daily_views: dict[tuple[int, object], int] = {}
    campaign_first_sent: dict[int, object] = {}
    campaign_today_sent: dict[int, int] = {}
    campaign_sent_by_day: dict[tuple[int, object], int] = {}
    mailbox_limit_hour: dict[int, int] = {}
    active_today_by_mailbox: dict[int, int] = {}
    active_by_mailbox_day: dict[tuple[int, object], int] = {}

    if camp_ids:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT c.id, COUNT(lc.id) AS total_cnt
                FROM public.campaigns_campaigns c
                JOIN public.lists_contacts lc
                  ON lc.list_id = c.mailing_list_id
                 AND lc.active = true
                WHERE c.id = ANY(%s)
                GROUP BY c.id
                """,
                [camp_ids],
            )
            for cid, total_cnt in cur.fetchall() or []:
                campaign_totals[int(cid)] = int(total_cnt or 0)

            cur.execute(
                """
                SELECT
                  campaign_id,
                  COUNT(id) AS sent_cnt
                FROM public.mailbox_sent
                WHERE campaign_id = ANY(%s)
                GROUP BY campaign_id
                """,
                [camp_ids],
            )
            for cid, sent_cnt in cur.fetchall() or []:
                campaign_sent[int(cid)] = int(sent_cnt or 0)

            cur.execute(
                """
                SELECT
                  s.campaign_id,
                  COUNT(s.id) AS delivered_cnt
                FROM public.mailbox_sent s
                LEFT JOIN public.rate_contacts rc ON rc.id = s.rate_contact_id
                LEFT JOIN public.mail_blocked_recipients mbr
                  ON mbr.aggr_contact_id = rc.contact_id
                 AND mbr.active = true
                WHERE s.campaign_id = ANY(%s)
                  AND mbr.aggr_contact_id IS NULL
                GROUP BY s.campaign_id
                """,
                [camp_ids],
            )
            for cid, delivered_cnt in cur.fetchall() or []:
                campaign_delivered[int(cid)] = int(delivered_cnt or 0)

            cur.execute(
                """
                SELECT
                  s.campaign_id,
                  COUNT(DISTINCT ms.letter_id) AS views_cnt
                FROM public.mailbox_stats ms
                JOIN public.mailbox_sent s ON s.id = ms.letter_id
                WHERE s.campaign_id = ANY(%s)
                GROUP BY s.campaign_id
                """,
                [camp_ids],
            )
            for cid, views_cnt in cur.fetchall() or []:
                campaign_views[int(cid)] = int(views_cnt or 0)

            cur.execute(
                """
                SELECT campaign_id, MIN(created_at) AS first_sent_at
                FROM public.mailbox_sent
                WHERE campaign_id = ANY(%s)
                GROUP BY campaign_id
                """,
                [camp_ids],
            )
            for cid, first_sent_at in cur.fetchall() or []:
                campaign_first_sent[int(cid)] = first_sent_at

            cur.execute(
                """
                SELECT campaign_id, COUNT(id) AS sent_today_cnt
                FROM public.mailbox_sent
                WHERE campaign_id = ANY(%s)
                  AND (created_at AT TIME ZONE 'Europe/Berlin')::date = (NOW() AT TIME ZONE 'Europe/Berlin')::date
                GROUP BY campaign_id
                """,
                [camp_ids],
            )
            for cid, sent_today_cnt in cur.fetchall() or []:
                campaign_today_sent[int(cid)] = int(sent_today_cnt or 0)

            cur.execute(
                """
                SELECT
                  campaign_id,
                  (created_at AT TIME ZONE 'Europe/Berlin')::date AS day_de,
                  COUNT(id) AS sent_day_cnt
                FROM public.mailbox_sent
                WHERE campaign_id = ANY(%s)
                  AND (created_at AT TIME ZONE 'Europe/Berlin')::date BETWEEN
                      ((NOW() AT TIME ZONE 'Europe/Berlin')::date + 1)
                      AND
                      ((NOW() AT TIME ZONE 'Europe/Berlin')::date + 4)
                GROUP BY campaign_id, day_de
                """,
                [camp_ids],
            )
            for cid, day_de, sent_day_cnt in cur.fetchall() or []:
                campaign_sent_by_day[(int(cid), day_de)] = int(sent_day_cnt or 0)

            cur.execute(
                """
                SELECT
                  campaign_id,
                  (created_at AT TIME ZONE 'Europe/Berlin')::date AS day_de,
                  COUNT(id) AS sent_cnt
                FROM public.mailbox_sent
                WHERE campaign_id = ANY(%s)
                  AND (created_at AT TIME ZONE 'Europe/Berlin')::date BETWEEN
                      ((NOW() AT TIME ZONE 'Europe/Berlin')::date - 4)
                      AND
                      ((NOW() AT TIME ZONE 'Europe/Berlin')::date)
                GROUP BY campaign_id, day_de
                """,
                [camp_ids],
            )
            for cid, day_de, sent_cnt in cur.fetchall() or []:
                key = (int(cid), day_de)
                campaign_daily_sent[key] = int(sent_cnt or 0)

            cur.execute(
                """
                SELECT
                  s.campaign_id,
                  (s.created_at AT TIME ZONE 'Europe/Berlin')::date AS day_de,
                  COUNT(s.id) AS delivered_cnt
                FROM public.mailbox_sent s
                LEFT JOIN public.rate_contacts rc ON rc.id = s.rate_contact_id
                LEFT JOIN public.mail_blocked_recipients mbr
                  ON mbr.aggr_contact_id = rc.contact_id
                 AND mbr.active = true
                WHERE s.campaign_id = ANY(%s)
                  AND mbr.aggr_contact_id IS NULL
                  AND (s.created_at AT TIME ZONE 'Europe/Berlin')::date BETWEEN
                      ((NOW() AT TIME ZONE 'Europe/Berlin')::date - 4)
                      AND
                      ((NOW() AT TIME ZONE 'Europe/Berlin')::date)
                GROUP BY s.campaign_id, day_de
                """,
                [camp_ids],
            )
            for cid, day_de, delivered_cnt in cur.fetchall() or []:
                campaign_daily_delivered[(int(cid), day_de)] = int(delivered_cnt or 0)

            cur.execute(
                """
                SELECT
                  s.campaign_id,
                  (s.created_at AT TIME ZONE 'Europe/Berlin')::date AS day_de,
                  COUNT(DISTINCT ms.letter_id) AS views_cnt
                FROM public.mailbox_stats ms
                JOIN public.mailbox_sent s ON s.id = ms.letter_id
                WHERE s.campaign_id = ANY(%s)
                  AND (s.created_at AT TIME ZONE 'Europe/Berlin')::date BETWEEN
                      ((NOW() AT TIME ZONE 'Europe/Berlin')::date - 4)
                      AND
                      ((NOW() AT TIME ZONE 'Europe/Berlin')::date)
                GROUP BY s.campaign_id, day_de
                """,
                [camp_ids],
            )
            for cid, day_de, views_cnt in cur.fetchall() or []:
                campaign_daily_views[(int(cid), day_de)] = int(views_cnt or 0)

    mailbox_ids = sorted({int(getattr(c, "mailbox_id")) for c in recent_campaigns if getattr(c, "mailbox_id", None) is not None})
    if mailbox_ids:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT mailbox_id, limit_hour_sent
                FROM public.aap_settings_smtp_mailboxes
                WHERE mailbox_id = ANY(%s)
                """,
                [mailbox_ids],
            )
            for mb_id, limit_hour_sent in cur.fetchall() or []:
                mailbox_limit_hour[int(mb_id)] = int(limit_hour_sent or 0)

    today_de = timezone.localtime().date()
    active_for_today = list(
        Campaign.objects.filter(workspace_id=ws_id, active=True)
        .only("id", "mailbox_id", "window", "start_at", "end_at")
    )
    for camp in active_for_today:
        mb_id = getattr(camp, "mailbox_id", None)
        if mb_id is None:
            continue
        start_date = timezone.localtime(camp.start_at).date() if camp.start_at else None
        end_date = timezone.localtime(camp.end_at).date() if camp.end_at else None
        if start_date and start_date > today_de:
            continue
        if end_date and end_date < today_de:
            continue
        if _window_minutes_today(getattr(camp, "window", None)) <= 0:
            continue
        mb_int = int(mb_id)
        active_today_by_mailbox[mb_int] = int(active_today_by_mailbox.get(mb_int, 0)) + 1

    for day_offset in range(1, 5):
        day = today_de + timedelta(days=day_offset)
        for camp in active_for_today:
            mb_id = getattr(camp, "mailbox_id", None)
            if mb_id is None:
                continue
            start_date = timezone.localtime(camp.start_at).date() if camp.start_at else None
            end_date = timezone.localtime(camp.end_at).date() if camp.end_at else None
            if start_date and start_date > day:
                continue
            if end_date and end_date < day:
                continue
            if _window_minutes_for_date(getattr(camp, "window", None), day) <= 0:
                continue
            mb_int = int(mb_id)
            key = (mb_int, day)
            active_by_mailbox_day[key] = int(active_by_mailbox_day.get(key, 0)) + 1

    card_campaigns = []
    card_stats = []
    for camp in recent_campaigns:
        cid = int(camp.id)
        total = int(campaign_totals.get(cid, 0))
        sent = int(campaign_sent.get(cid, 0))
        left = total - sent
        if left < 0:
            left = 0
        sent_pct = (int(round((sent * 100.0) / total)) if total else 0)
        left_pct = (int(round((left * 100.0) / total)) if total else 0)

        today_sent = int(campaign_today_sent.get(cid, 0))
        mb_id = int(getattr(camp, "mailbox_id", 0) or 0)
        mb_limit = int(mailbox_limit_hour.get(mb_id, 0))
        active_mb_cnt = int(active_today_by_mailbox.get(mb_id, 0))
        today_minutes = int(_window_minutes_today(getattr(camp, "window", None)))
        today_hours = float(today_minutes) / 60.0
        today_plan = int((today_hours * float(mb_limit)) / float(active_mb_cnt)) if active_mb_cnt > 0 else 0
        today_total = min(today_plan, left) if left > 0 else 0
        today_left = today_total - today_sent
        if today_left < 0:
            today_left = 0
        today_sent_pct = (int(round((today_sent * 100.0) / today_total)) if today_total else 0)
        today_left_pct = (int(round((today_left * 100.0) / today_total)) if today_total else 0)
        future_rows = []
        camp_end_date = timezone.localtime(camp.end_at).date() if camp.end_at else None
        rolling_left = int(left)
        for day_offset in range(1, 5):
            if rolling_left <= 0:
                break
            day = today_de + timedelta(days=day_offset)
            if camp_end_date and day > camp_end_date:
                break
            day_active_mb_cnt = int(active_by_mailbox_day.get((mb_id, day), 0))
            day_minutes = int(_window_minutes_for_date(getattr(camp, "window", None), day))
            day_hours = float(day_minutes) / 60.0
            day_plan = int((day_hours * float(mb_limit)) / float(day_active_mb_cnt)) if day_active_mb_cnt > 0 else 0
            day_total = min(day_plan, rolling_left) if rolling_left > 0 else 0
            day_sent = int(campaign_sent_by_day.get((cid, day), 0))
            if day_sent > day_total:
                day_sent = day_total
            day_left = day_total - day_sent
            if day_left < 0:
                day_left = 0
            day_sent_pct = (int(round((day_sent * 100.0) / day_total)) if day_total else 0)
            day_left_pct = (int(round((day_left * 100.0) / day_total)) if day_total else 0)
            future_rows.append(
                {
                    "day": day,
                    "sent": day_sent,
                    "left": day_left,
                    "total": day_total,
                    "sent_pct": day_sent_pct,
                    "left_pct": day_left_pct,
                    "total_pct": (100 if day_total > 0 else 0),
                }
            )
            rolling_left = int(day_left)

        valid_cnt = int(campaign_delivered.get(cid, 0))
        views_cnt = int(campaign_views.get(cid, 0))
        pct = int(round((views_cnt * 100.0) / valid_cnt)) if valid_cnt else 0
        stats_days = []
        has_recent_sent = False
        for day_offset in range(0, 5):
            day = today_de - timedelta(days=day_offset)
            day_sent = int(campaign_daily_sent.get((cid, day), 0))
            if day_sent > 0:
                has_recent_sent = True
            day_delivered = int(campaign_daily_delivered.get((cid, day), 0))
            day_views = int(campaign_daily_views.get((cid, day), 0))
            day_pct = int(round((day_views * 100.0) / day_delivered)) if day_delivered else 0
            stats_days.append(
                {
                    "day": day,
                    "is_today": (day_offset == 0),
                    "sent": day_sent,
                    "delivered": day_delivered,
                    "views": day_views,
                    "pct": day_pct,
                }
            )

        card_campaigns.append(
            {
                "title": (camp.title or "").strip() or f"#{cid}",
                "audience": ", ".join(
                    [
                        (t.title or "").strip()
                        for t in (getattr(getattr(camp, "mailing_list", None), "audience_tasks", []).all() if getattr(camp, "mailing_list", None) else [])
                        if (t.title or "").strip()
                    ]
                )
                or "-",
                "mailing_list": (getattr(camp, "mailing_list", None).title if getattr(camp, "mailing_list", None) else "") or "-",
                "start_at": campaign_first_sent.get(cid),
                "end_at": camp.end_at,
                "is_active": bool(camp.active),
                "campaign_status": "Кампания включена" if bool(camp.active) else "Кампания отключена",
                "work_status": ("Идет рассылка" if _is_now_in_send_window(getattr(camp, "window", None)) else "Ждем окно отправки") if bool(camp.active) else "-",
                "sent": sent,
                "left": left,
                "total": total,
                "sent_pct": sent_pct,
                "left_pct": left_pct,
                "total_pct": (100 if total > 0 else 0),
                "today_sent": today_sent,
                "today_left": today_left,
                "today_total": today_total,
                "today_sent_pct": today_sent_pct,
                "today_left_pct": today_left_pct,
                "future_rows": future_rows,
            }
        )
        card_stats.append(
            {
                "title": (camp.title or "").strip() or f"#{cid}",
                "sent": sent,
                "delivered": valid_cnt,
                "views": views_cnt,
                "pct": pct,
                "days": stats_days,
                "has_recent_sent": has_recent_sent,
            }
        )

    recent_tasks = list(
        AudienceTask.objects.filter(
            workspace_id=ws_id,
            archived=False,
        )
        .only("id", "title", "run_processing")
        .order_by("-created_at")
    )[:3]
    task_ids = [int(t.id) for t in recent_tasks]
    aud_by_task: dict[int, dict[str, int]] = {}
    if task_ids:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT
                    task_id::bigint AS task_id,
                    COUNT(*)::int AS total_cnt,
                    SUM(
                        CASE
                            WHEN rate_cl IS NOT NULL
                             AND hash_task IS NOT NULL
                             AND hash_task NOT IN (-1,0,1)
                            THEN 1 ELSE 0
                        END
                    )::int AS rated_cnt,
                    SUM(
                        CASE
                            WHEN rate_cl BETWEEN 1 AND 30
                             AND hash_task IS NOT NULL
                             AND hash_task NOT IN (-1,0,1)
                            THEN 1 ELSE 0
                        END
                    )::int AS b1_cnt,
                    SUM(
                        CASE
                            WHEN rate_cl BETWEEN 31 AND 70
                             AND hash_task IS NOT NULL
                             AND hash_task NOT IN (-1,0,1)
                            THEN 1 ELSE 0
                        END
                    )::int AS b2_cnt,
                    SUM(
                        CASE
                            WHEN rate_cl BETWEEN 71 AND 100
                             AND hash_task IS NOT NULL
                             AND hash_task NOT IN (-1,0,1)
                            THEN 1 ELSE 0
                        END
                    )::int AS b3_cnt
                FROM public.rate_contacts
                WHERE task_id = ANY(%s::bigint[])
                GROUP BY task_id
                """,
                [task_ids],
            )
            for tid, total_cnt, rated_cnt, b1_cnt, b2_cnt, b3_cnt in cur.fetchall() or []:
                aud_by_task[int(tid)] = {
                    "total": int(total_cnt or 0),
                    "rated": int(rated_cnt or 0),
                    "b1": int(b1_cnt or 0),
                    "b2": int(b2_cnt or 0),
                    "b3": int(b3_cnt or 0),
                }

    card_audiences = []
    for t in recent_tasks:
        tid = int(t.id)
        s = aud_by_task.get(tid, {"total": 0, "rated": 0, "b1": 0, "b2": 0, "b3": 0})
        card_audiences.append(
            {
                "title": (t.title or "").strip() or f"#{tid}",
                "status": "Сбор контактов включен" if bool(t.run_processing) else "Сбор контактов отключен",
                "status_on": bool(t.run_processing),
                "total": int(s["total"]),
                "rated": int(s["rated"]),
                "b1": int(s["b1"]),
                "b2": int(s["b2"]),
                "b3": int(s["b3"]),
            }
        )

    return render(
        request,
        "panels/overview.html",
        {
            "card_campaigns": card_campaigns,
            "card_stats": card_stats,
            "card_audiences": card_audiences,
            "lang_code": getattr(request, "LANGUAGE_CODE", "") or "",
        },
    )


def stats_view(request):
    if not request.user.is_authenticated:
        return render(request, "public/login.html", status=401)

    ws_id = getattr(request, "workspace_id", None)
    if ws_id is None:
        return render(request, "panels/access_denied.html")

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              MIN(ms.time)     AS first_time,
              COUNT(*)         AS visits,
              c.id             AS campaign_id,
              c.title          AS campaign_title,
              rc.contact_id    AS aggr_id,
              ag.company_name  AS company_name
            FROM public.mailbox_stats ms
            JOIN public.mailbox_sent s
              ON s.id = ms.letter_id
            JOIN public.campaigns_campaigns c
              ON c.id = s.campaign_id AND c.workspace_id = %s::uuid
            JOIN public.rate_contacts rc
              ON rc.id = s.rate_contact_id
            JOIN public.raw_contacts_aggr ag
              ON ag.id = rc.contact_id
            GROUP BY
              c.id, c.title,
              rc.contact_id,
              ag.company_name
            ORDER BY first_time DESC
            LIMIT 500
            """,
            [ws_id],
        )

        stats = []
        for first_time, visits, campaign_id, campaign_title, aggr_id, company_name in cur.fetchall() or []:
            stats.append(
                {
                    "first_time": first_time,
                    "visits": int(visits),
                    "campaign_id": int(campaign_id),
                    "campaign_title": (campaign_title or "").strip() or str(int(campaign_id)),
                    "aggr_id": int(aggr_id),
                    "company_name": (company_name or "").strip() or str(int(aggr_id)),
                    "ui_id": encode_id(int(aggr_id)),
                }
            )

    return render(request, "panels/stats.html", {"stats": stats})


def stats_campaign_view(request, uid: str):
    if not request.user.is_authenticated:
        return render(request, "public/login.html", status=401)

    ws_id = getattr(request, "workspace_id", None)
    if ws_id is None:
        return render(request, "panels/access_denied.html")

    try:
        campaign_id = int(decode_id(uid))
    except Exception:
        return render(request, "panels/access_denied.html", status=404)

    camp = Campaign.objects.filter(id=campaign_id, workspace_id=ws_id).only("id", "title").first()
    if not camp:
        return render(request, "panels/access_denied.html", status=404)

    sent = 0
    delivered = 0
    views = 0
    days_sent: dict[object, int] = {}
    days_delivered: dict[object, int] = {}
    days_views: dict[object, int] = {}
    visitors_by_day: dict[object, list[dict]] = {}

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(id) AS sent_cnt
            FROM public.mailbox_sent
            WHERE campaign_id = %s
            """,
            [campaign_id],
        )
        row = cur.fetchone()
        sent = int((row[0] if row else 0) or 0)

        cur.execute(
            """
            SELECT COUNT(s.id) AS delivered_cnt
            FROM public.mailbox_sent s
            LEFT JOIN public.rate_contacts rc ON rc.id = s.rate_contact_id
            LEFT JOIN public.mail_blocked_recipients mbr
              ON mbr.aggr_contact_id = rc.contact_id
             AND mbr.active = true
            WHERE s.campaign_id = %s
              AND mbr.aggr_contact_id IS NULL
            """,
            [campaign_id],
        )
        row = cur.fetchone()
        delivered = int((row[0] if row else 0) or 0)

        cur.execute(
            """
            SELECT COUNT(DISTINCT ms.letter_id) AS views_cnt
            FROM public.mailbox_stats ms
            JOIN public.mailbox_sent s ON s.id = ms.letter_id
            WHERE s.campaign_id = %s
            """,
            [campaign_id],
        )
        row = cur.fetchone()
        views = int((row[0] if row else 0) or 0)

        cur.execute(
            """
            SELECT
              (created_at AT TIME ZONE 'Europe/Berlin')::date AS day_de,
              COUNT(id) AS sent_cnt
            FROM public.mailbox_sent
            WHERE campaign_id = %s
            GROUP BY day_de
            """,
            [campaign_id],
        )
        for day_de, sent_cnt in cur.fetchall() or []:
            days_sent[day_de] = int(sent_cnt or 0)

        cur.execute(
            """
            SELECT
              (s.created_at AT TIME ZONE 'Europe/Berlin')::date AS day_de,
              COUNT(s.id) AS delivered_cnt
            FROM public.mailbox_sent s
            LEFT JOIN public.rate_contacts rc ON rc.id = s.rate_contact_id
            LEFT JOIN public.mail_blocked_recipients mbr
              ON mbr.aggr_contact_id = rc.contact_id
             AND mbr.active = true
            WHERE s.campaign_id = %s
              AND mbr.aggr_contact_id IS NULL
            GROUP BY day_de
            """,
            [campaign_id],
        )
        for day_de, delivered_cnt in cur.fetchall() or []:
            days_delivered[day_de] = int(delivered_cnt or 0)

        cur.execute(
            """
            SELECT
              (s.created_at AT TIME ZONE 'Europe/Berlin')::date AS day_de,
              COUNT(DISTINCT ms.letter_id) AS views_cnt
            FROM public.mailbox_stats ms
            JOIN public.mailbox_sent s ON s.id = ms.letter_id
            WHERE s.campaign_id = %s
            GROUP BY day_de
            """,
            [campaign_id],
        )
        for day_de, views_cnt in cur.fetchall() or []:
            days_views[day_de] = int(views_cnt or 0)

        cur.execute(
            """
            SELECT
              (ms.time AT TIME ZONE 'Europe/Berlin')::date AS day_de,
              rc.contact_id::bigint AS aggr_id,
              MIN(ms.time) AS first_time,
              COUNT(*)::int AS visits,
              MAX(COALESCE(ag.company_name, '')) AS company_name,
              MAX(COALESCE((ag.address_list)[1], '')) AS address_line
            FROM public.mailbox_stats ms
            JOIN public.mailbox_sent s ON s.id = ms.letter_id
            JOIN public.rate_contacts rc ON rc.id = s.rate_contact_id
            JOIN public.raw_contacts_aggr ag ON ag.id = rc.contact_id
            WHERE s.campaign_id = %s
            GROUP BY day_de, rc.contact_id
            ORDER BY day_de DESC, first_time DESC
            """,
            [campaign_id],
        )
        visitors_rows = cur.fetchall() or []

        aggr_ids = sorted({int(r[1]) for r in visitors_rows if r[1] is not None})
        branches_by_aggr: dict[int, str] = {}
        if aggr_ids:
            cur.execute(
                """
                SELECT
                  ra.id::bigint AS aggr_id,
                  COALESCE(
                    STRING_AGG(
                      DISTINCT COALESCE(NULLIF(i.name_trans, ''), NULLIF(i.name_original, ''), NULLIF(b.name, ''), '')
                      , ', '
                    ),
                    ''
                  ) AS branches_text
                FROM public.raw_contacts_aggr ra
                LEFT JOIN LATERAL UNNEST(ra.branches) bid ON true
                LEFT JOIN public.gb_branches b ON b.id = bid
                LEFT JOIN public.gb_branch_i18n i ON i.branch_id = b.id AND i.lang = %s
                WHERE ra.id = ANY(%s::bigint[])
                GROUP BY ra.id
                """,
                [((getattr(request, "LANGUAGE_CODE", "") or "ru")[:2]), aggr_ids],
            )
            for aggr_id, branches_text in cur.fetchall() or []:
                branches_by_aggr[int(aggr_id)] = (branches_text or "").strip()

        for day_de, aggr_id, first_time, visits_cnt, company_name, address_line in visitors_rows:
            day_key = day_de
            visitors_by_day.setdefault(day_key, []).append(
                {
                    "first_time": first_time,
                    "visits": int(visits_cnt or 0),
                    "company_name": (company_name or "").strip() or (str(int(aggr_id)) if aggr_id is not None else "-"),
                    "address": (address_line or "").strip(),
                    "branches": branches_by_aggr.get(int(aggr_id), "-") if aggr_id is not None else "-",
                    "ui_id": encode_id(int(aggr_id)) if aggr_id is not None else "",
                }
            )

    pct = int(round((views * 100.0) / delivered)) if delivered else 0
    day_sections = []
    activity_days = sorted(set(days_sent.keys()) | set(days_views.keys()))
    if activity_days:
        day = max(activity_days)
        min_day = min(activity_days)
        today_de = timezone.localtime().date()
        while day >= min_day:
            day_sent = int(days_sent.get(day, 0))
            day_delivered = int(days_delivered.get(day, 0))
            day_views = int(days_views.get(day, 0))
            day_pct = int(round((day_views * 100.0) / day_delivered)) if day_delivered else 0
            day_sections.append(
                {
                    "day": day,
                    "is_today": (day == today_de),
                    "sent": day_sent,
                    "delivered": day_delivered,
                    "views": day_views,
                    "pct": day_pct,
                    "visitors": visitors_by_day.get(day, []),
                }
            )
            day = day - timedelta(days=1)

    return render(
        request,
        "panels/stats_campaign.html",
        {
            "stats_title": (camp.title or "").strip() or f"#{campaign_id}",
            "stats_total": {"sent": sent, "delivered": delivered, "views": views, "pct": pct},
            "day_sections": day_sections,
            "lang_code": getattr(request, "LANGUAGE_CODE", "") or "",
        },
    )


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
                "role": user.role or "",
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
        messages.error(request, "Неверный пользователь")
        return redirect("dashboard")

    target = (
        ClientUser.objects.select_related("workspace")
        .filter(id=user_id, archived=False, workspace__isnull=False, workspace__archived=False)
        .first()
    )
    if not target:
        messages.error(request, "Пользователь недоступен")
        return redirect("dashboard")

    backend = (
        request.session.get("_auth_user_backend")
        or getattr(request.user, "backend", "")
        or settings.AUTHENTICATION_BACKENDS[0]
    )
    auth_login(request, target, backend=backend)
    messages.info(request, f"Вход выполнен как {target.email}")
    return redirect("dashboard")
