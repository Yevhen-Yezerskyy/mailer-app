# FILE: web/panel/aap_audience/views/modal_edit_branch_rate.py
# DATE: 2026-03-24
# PURPOSE: Modal form for editing branch rating inside create/edit flow.

from __future__ import annotations

from django.http import JsonResponse
from django.shortcuts import render
from django.utils.translation import gettext as _trans

from mailer_web.access import decode_id
from mailer_web.format_data import get_branches_sys_translations
from panel.aap_audience.models import AudienceTask
from django.db import connection


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


def _parse_ids(raw: str) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for value in str(raw or "").split(","):
        value = value.strip()
        if not value:
            continue
        try:
            branch_id = int(value)
        except Exception:
            continue
        if branch_id in seen:
            continue
        seen.add(branch_id)
        out.append(branch_id)
    return out


def modal_edit_branch_rate_view(request):
    token = (request.POST.get("id") or request.GET.get("id") or "").strip()
    task = _resolve_task(request, token)
    branch_ids = _parse_ids(request.POST.get("ids") or request.GET.get("ids") or "")

    if request.method == "POST":
        if not task:
            return JsonResponse({"ok": False, "error": str(_trans("Запись не найдена."))}, status=404)
        if not branch_ids:
            return JsonResponse({"ok": False, "error": str(_trans("Категория не найдена."))}, status=404)

        try:
            rate = int(str(request.POST.get("rate") or "").strip())
        except Exception:
            return JsonResponse({"ok": False, "error": str(_trans("Введите рейтинг от 1 до 20."))}, status=400)

        if rate < 1 or rate > 20:
            return JsonResponse({"ok": False, "error": str(_trans("Введите рейтинг от 1 до 20."))}, status=400)

        with connection.cursor() as cur:
            cur.execute(
                "UPDATE task_branch_ratings "
                "SET rate = %s "
                "WHERE task_id = %s AND branch_id = ANY(%s)",
                [rate, int(task.id), branch_ids],
            )

        return JsonResponse({"ok": True, "rate": rate})

    rows = []
    if task and branch_ids:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT tbr.branch_id, tbr.rate, bs.branch_name "
                "FROM task_branch_ratings tbr "
                "JOIN branches_sys bs ON bs.id = tbr.branch_id "
                "WHERE tbr.task_id = %s AND tbr.branch_id = ANY(%s) "
                "ORDER BY tbr.branch_id ASC",
                [int(task.id), branch_ids],
            )
            rows = cur.fetchall() or []

    if not rows:
        return render(
            request,
            "panels/aap_audience/modal_edit_branch_rate.html",
            {"status": "empty"},
        )

    branch_name = str(rows[0][2] or "").strip()
    current_rate = rows[0][1]
    translated_name = ""
    if request.ui_lang_code != "de":
        translated = get_branches_sys_translations([int(row[0]) for row in rows], request.ui_lang_code)
        for row in rows:
            value = str(translated.get(int(row[0])) or "").strip()
            if value and value != branch_name:
                translated_name = value
                break

    return render(
        request,
        "panels/aap_audience/modal_edit_branch_rate.html",
        {
            "status": "ok",
            "type": str(task.type or "").strip(),
            "task_id_token": token,
            "ids_csv": ",".join(str(int(row[0])) for row in rows),
            "branch_name": branch_name,
            "translated_name": translated_name,
            "current_rate": current_rate,
            "current_rate_display": current_rate if current_rate is not None else "-",
        },
    )
