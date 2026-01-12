# FILE: web/panel/aap_audience/views/modal_status_task.py  (обновлено — 2026-01-12)
# DATE: 2026-01-12
# CHANGE:
# - Перед build_contact_packet() читаем из public.rate_contacts снапшот rate_cb/rate_cl,
#   и прокидываем их в build_contact_packet(..., rate_cb=..., rate_cl=...),
#   чтобы кеш-ключ учитывал текущие рейтинги (как в lists_list.py).

from __future__ import annotations

from django.db import connection
from django.shortcuts import render

from mailer_web.access import decode_id
from mailer_web.format_data import build_contact_packet


def _fetch_rates_snapshot(rate_contact_id: int):
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT rate_cb, rate_cl
            FROM public.rate_contacts
            WHERE id = %s
            LIMIT 1
            """,
            [int(rate_contact_id)],
        )
        row = cur.fetchone()
        if not row:
            return None, None
        return row[0], row[1]


def modal_status_task_view(request):
    token = (request.GET.get("id") or "").strip()
    if not token:
        return render(request, "panels/aap_audience/modal_status_task.html", {"status": "empty"})

    try:
        rate_contact_id = int(decode_id(token))
    except Exception:
        return render(request, "panels/aap_audience/modal_status_task.html", {"status": "empty"})

    ui_lang = getattr(request, "LANGUAGE_CODE", "") or "ru"

    rate_cb, rate_cl = _fetch_rates_snapshot(int(rate_contact_id))

    packet = build_contact_packet(int(rate_contact_id), ui_lang, rate_cb=rate_cb, rate_cl=rate_cl)
    contact = packet.get("contact")
    ratings = packet.get("ratings")

    status = "done" if (contact and ratings) else "empty"

    return render(
        request,
        "panels/aap_audience/modal_status_task.html",
        {
            "status": status,
            "contact": contact,
            "ratings": ratings,
        },
    )
