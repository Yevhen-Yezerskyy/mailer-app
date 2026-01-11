# FILE: web/panel/aap_lists/views/modal_lists_list.py
# DATE: 2026-01-11
# PURPOSE: Modal для lists/list: рендер contact+ratings из format_data по rate_contacts.id (encode).
# CHANGE: локальная модалка для раздела lists.

from __future__ import annotations

from django.shortcuts import render

from mailer_web.access import decode_id
from mailer_web.format_data import build_contact_packet


def modal_lists_list_view(request):
    token = (request.GET.get("id") or "").strip()
    if not token:
        return render(request, "panels/aap_lists/modal_lists_list.html", {"status": "empty"})

    try:
        rate_contact_id = int(decode_id(token))
    except Exception:
        return render(request, "panels/aap_lists/modal_lists_list.html", {"status": "empty"})

    ui_lang = getattr(request, "LANGUAGE_CODE", "") or "ru"

    packet = build_contact_packet(rate_contact_id, ui_lang)
    contact = packet.get("contact")
    ratings = packet.get("ratings")

    status = "done" if (contact and ratings) else "empty"

    return render(
        request,
        "panels/aap_lists/modal_lists_list.html",
        {
            "status": status,
            "contact": contact,
            "ratings": ratings,
        },
    )
