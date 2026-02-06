# FILE: web/panel/views.py  (обновлено — 2026-02-06)
# PURPOSE: Overview: DISTINCT по клиентам (aggr_id) + count переходов + время первого посещения.

from __future__ import annotations

from django.db import connection
from django.shortcuts import render

from mailer_web.access import encode_id


def dashboard(request):
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

    return render(request, "panels/overview.html", {"stats": stats})
