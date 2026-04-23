# FILE: web/panel/aap_audience/views/modal_edit_contact_rate.py
# DATE: 2026-04-22
# PURPOSE: Modal form for editing contact Con-R rating inside create/edit mailing flow.

from __future__ import annotations

from django.db import connection
from django.http import JsonResponse
from django.shortcuts import render
from django.utils.translation import gettext as _trans

from engine.common.utils import parse_json_object
from mailer_web.access import decode_id
from panel.aap_audience.models import AudienceTask


def _resolve_task(request, token: str):
    if not token:
        return None
    try:
        pk = int(decode_id(token))
    except Exception:
        return None
    return (
        AudienceTask.objects.filter(
            id=pk,
            workspace_id=request.workspace_id,
            archived=False,
        ).first()
    )


def _parse_contact_id(raw: str) -> int:
    try:
        value = int(str(raw or "").strip())
        return value if value > 0 else 0
    except Exception:
        return 0


def _load_contact_row(task_id: int, contact_id: int) -> dict[str, object] | None:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                sl.aggr_contact_cb_id::bigint AS contact_id,
                sl.rate AS contact_rate,
                ac.company_name AS company_name,
                ac.company_data AS company_data
            FROM public.sending_lists sl
            JOIN public.aggr_contacts_cb ac
              ON ac.id = sl.aggr_contact_cb_id
            WHERE sl.task_id = %s
              AND sl.aggr_contact_cb_id = %s
              AND COALESCE(sl.removed, false) = false
            LIMIT 1
            """,
            [int(task_id), int(contact_id)],
        )
        row = cur.fetchone()
    if not row:
        return None

    company_data = parse_json_object(row[3], field_name="aggr_contacts_cb.company_data")
    norm_data = company_data.get("norm") if isinstance(company_data.get("norm"), dict) else {}
    return {
        "contact_id": int(row[0]),
        "contact_rate": row[1],
        "company_name": str(row[2] or "").strip(),
        "address": str(norm_data.get("address") or "").strip(),
    }


def modal_edit_contact_rate_view(request):
    token = (request.POST.get("id") or request.GET.get("id") or "").strip()
    task = _resolve_task(request, token)
    contact_id = _parse_contact_id(request.POST.get("sid") or request.GET.get("sid") or "")

    if request.method == "POST":
        if not task:
            return JsonResponse({"ok": False, "error": str(_trans("Запись не найдена."))}, status=404)
        if not contact_id:
            return JsonResponse({"ok": False, "error": str(_trans("Контакт не найден."))}, status=404)

        try:
            rate = int(str(request.POST.get("rate") or "").strip())
        except Exception:
            return JsonResponse({"ok": False, "error": str(_trans("Введите рейтинг от 1 до 100."))}, status=400)

        if rate < 1 or rate > 100:
            return JsonResponse({"ok": False, "error": str(_trans("Введите рейтинг от 1 до 100."))}, status=400)

        with connection.cursor() as cur:
            cur.execute(
                """
                UPDATE public.sending_lists
                SET rate = %s,
                    updated_at = now()
                WHERE task_id = %s
                  AND aggr_contact_cb_id = %s
                  AND COALESCE(removed, false) = false
                """,
                [int(rate), int(task.id), int(contact_id)],
            )
            changed = int(cur.rowcount or 0)
        if changed <= 0:
            return JsonResponse({"ok": False, "error": str(_trans("Контакт не найден."))}, status=404)

        return JsonResponse({"ok": True, "rate": int(rate)})

    if not task or not contact_id:
        return render(
            request,
            "panels/aap_audience/modal_edit_contact_rate.html",
            {"status": "empty"},
        )

    row = _load_contact_row(int(task.id), int(contact_id))
    if not row:
        return render(
            request,
            "panels/aap_audience/modal_edit_contact_rate.html",
            {"status": "empty"},
        )

    current_rate = row.get("contact_rate")
    return render(
        request,
        "panels/aap_audience/modal_edit_contact_rate.html",
        {
            "status": "ok",
            "type": str(task.type or "").strip(),
            "task_id_token": token,
            "sending_list_id": int(row["contact_id"]),
            "company_name": str(row.get("company_name") or "").strip(),
            "address": str(row.get("address") or "").strip(),
            "current_rate": current_rate,
            "current_rate_display": current_rate if current_rate is not None else "-",
        },
    )
