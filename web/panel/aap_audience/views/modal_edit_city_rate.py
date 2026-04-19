# FILE: web/panel/aap_audience/views/modal_edit_city_rate.py
# DATE: 2026-03-26
# PURPOSE: Modal form for editing city rating inside create/edit flow.

from __future__ import annotations

from django.db import connection
from django.http import JsonResponse
from django.shortcuts import render
from django.utils.translation import gettext as _trans

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


def _parse_ids(raw: str) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for value in str(raw or "").split(","):
        value = value.strip()
        if not value:
            continue
        try:
            city_id = int(value)
        except Exception:
            continue
        if city_id in seen:
            continue
        seen.add(city_id)
        out.append(city_id)
    return out


def modal_edit_city_rate_view(request):
    token = (request.POST.get("id") or request.GET.get("id") or "").strip()
    task = _resolve_task(request, token)
    city_ids = _parse_ids(request.POST.get("ids") or request.GET.get("ids") or "")

    if request.method == "POST":
        if not task:
            return JsonResponse({"ok": False, "error": str(_trans("Запись не найдена."))}, status=404)
        if not city_ids:
            return JsonResponse({"ok": False, "error": str(_trans("Город не найден."))}, status=404)

        try:
            rate = int(str(request.POST.get("rate") or "").strip())
        except Exception:
            return JsonResponse({"ok": False, "error": str(_trans("Введите рейтинг от 1 до 100."))}, status=400)

        if rate < 1 or rate > 100:
            return JsonResponse({"ok": False, "error": str(_trans("Введите рейтинг от 1 до 100."))}, status=400)

        with connection.cursor() as cur:
            cur.execute(
                "UPDATE task_city_ratings "
                "SET rate = %s "
                "WHERE task_id = %s AND city_id = ANY(%s)",
                [rate, int(task.id), city_ids],
            )

        return JsonResponse({"ok": True, "rate": rate})

    rows = []
    if task and city_ids:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT tcr.city_id, tcr.rate, cs.name, cs.state_name "
                "FROM task_city_ratings tcr "
                "JOIN cities_sys cs ON cs.id = tcr.city_id "
                "WHERE tcr.task_id = %s AND tcr.city_id = ANY(%s) "
                "ORDER BY tcr.city_id ASC",
                [int(task.id), city_ids],
            )
            rows = cur.fetchall() or []

    if not rows:
        return render(
            request,
            "panels/aap_audience/modal_edit_city_rate.html",
            {"status": "empty"},
        )

    city_name = str(rows[0][2] or "").strip()
    state_name = str(rows[0][3] or "").strip()
    current_rate = rows[0][1]

    return render(
        request,
        "panels/aap_audience/modal_edit_city_rate.html",
        {
            "status": "ok",
            "type": str(task.type or "").strip(),
            "task_id_token": token,
            "ids_csv": ",".join(str(int(row[0])) for row in rows),
            "city_name": city_name,
            "state_name": state_name,
            "current_rate": current_rate,
            "current_rate_display": current_rate if current_rate is not None else "-",
        },
    )
