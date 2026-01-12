# FILE: web/panel/aap_lists/views/modal_contacts.py
# DATE: 2026-01-12
# PURPOSE: /panel/lists/contacts/modal/?id=... — модалка контакта по aggr_id: контакт + аудитории (rate_cl/rate_cb) + списки.
# CHANGE: новый рендер без rate_contact_id (контакт может быть в разных аудиториях).

from __future__ import annotations

from django.db import connection
from django.shortcuts import render

from mailer_web.access import decode_id
from mailer_web.format_data import get_contact


def _rate_cl_bg(rate_cl):
    try:
        v = int(rate_cl)
    except Exception:
        return ""
    if v <= 0:
        return "bg-10"
    if v > 100:
        return "bg-100"
    bucket = ((v - 1) // 10 + 1) * 10
    if bucket < 10:
        bucket = 10
    if bucket > 100:
        bucket = 100
    return f"bg-{bucket}"


def _fetch_lists(ws_id, aggr_id: int) -> list[dict]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT ml.id::bigint, ml.title::text, ml.archived::bool
            FROM public.lists_contacts lc
            JOIN public.aap_lists_mailinglist ml
              ON ml.id = lc.list_id AND ml.workspace_id = %s::uuid
            WHERE lc.contact_id = %s::bigint
            ORDER BY ml.archived ASC, ml.created_at DESC, ml.id DESC
            """,
            [ws_id, int(aggr_id)],
        )
        out = []
        for lid, title, archived in cur.fetchall() or []:
            out.append(
                {
                    "id": int(lid),
                    "title": (title or "").strip() or str(int(lid)),
                    "archived": bool(archived),
                }
            )
        return out


def _fetch_audiences(ws_id, aggr_id: int) -> list[dict]:
    # только таски текущего workspace (user не учитываем)
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              t.id::bigint,
              t.title::text,
              rc.rate_cl,
              rc.rate_cb,
              rc.updated_at
            FROM public.rate_contacts rc
            JOIN public.aap_audience_audiencetask t
              ON t.id = rc.task_id AND t.workspace_id = %s::uuid
            WHERE rc.contact_id = %s::bigint
            ORDER BY t.created_at DESC, t.id DESC
            """,
            [ws_id, int(aggr_id)],
        )
        out = []
        for tid, title, rate_cl, rate_cb, updated_at in cur.fetchall() or []:
            out.append(
                {
                    "task_id": int(tid),
                    "title": (title or "").strip() or str(int(tid)),
                    "rate_cl": rate_cl,
                    "rate_cb": rate_cb,
                    "rate_cl_bg": _rate_cl_bg(rate_cl),
                    "updated_at": updated_at,
                }
            )
        return out


def modal_contacts_view(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return render(request, "panels/aap_lists/modal_contacts.html", {"status": "empty"})

    token = (request.GET.get("id") or "").strip()
    if not token:
        return render(request, "panels/aap_lists/modal_contacts.html", {"status": "empty"})

    try:
        aggr_id = int(decode_id(token))
    except Exception:
        return render(request, "panels/aap_lists/modal_contacts.html", {"status": "empty"})

    ui_lang = getattr(request, "LANGUAGE_CODE", "") or "ru"

    contact = get_contact(int(aggr_id), ui_lang)
    if not contact:
        return render(request, "panels/aap_lists/modal_contacts.html", {"status": "empty"})

    lists = _fetch_lists(ws_id, int(aggr_id))
    audiences = _fetch_audiences(ws_id, int(aggr_id))

    return render(
        request,
        "panels/aap_lists/modal_contacts.html",
        {
            "status": "done",
            "contact": contact,
            "lists": lists,
            "audiences": audiences,
        },
    )
