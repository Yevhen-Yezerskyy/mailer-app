# FILE: web/panel/aap_audience/views/modal_status_task.py
# DATE: 2026-01-04
# PURPOSE:
#   Modal status-task: минимальная логика.
#   Достаёт пакет {contact, ratings} одной функцией из mailer_web.format_data.

from __future__ import annotations

from django.shortcuts import render

from mailer_web.access import decode_id
from mailer_web.format_data import build_contact_packet


def modal_status_task_view(request):
    token = (request.GET.get("id") or "").strip()
    if not token:
        return render(request, "panels/aap_audience/modal_status_task.html", {"status": "empty"})

    try:
        rate_contact_id = int(decode_id(token))
    except Exception:
        return render(request, "panels/aap_audience/modal_status_task.html", {"status": "empty"})

    ui_lang = getattr(request, "LANGUAGE_CODE", "") or "ru"

    packet = build_contact_packet(rate_contact_id, ui_lang)
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
