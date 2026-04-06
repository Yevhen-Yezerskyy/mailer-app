# FILE: web/panel/aap_audience/views/modal_pause_info.py
# DATE: 2026-04-06
# PURPOSE: Info modal shown from paused contacts/rating statuses with workspace-specific message.

from __future__ import annotations

from django.shortcuts import render

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


def modal_pause_info_view(request):
    token = (request.GET.get("id") or "").strip()
    task = _resolve_task(request, token)
    if not task:
        return render(
            request,
            "panels/aap_audience/modal_pause_info.html",
            {
                "status": "empty",
            },
        )

    ws = getattr(request.user, "workspace", None)
    ws_access_type = str(getattr(ws, "access_type", "") or "").strip().lower()
    is_test_workspace = ws_access_type == "test"

    if is_test_workspace:
        title = "Ограничение тестового доступа"
        lines = [
            "Ограничение на подбор контактов с успешным рейтингом - 20 для пользователей с тестовым доступом.",
        ]
    else:
        title = "Рассылка на 24 часа обеспечена"
        lines = [
            "Собрано достаточное количество контактов",
            "Положительно отрейтинговано достаточное количество контактов.",
            "Список рассылки на 24 часа сформирован",
        ]

    return render(
        request,
        "panels/aap_audience/modal_pause_info.html",
        {
            "status": "ok",
            "type": str(task.type or "").strip(),
            "title": title,
            "lines": lines,
        },
    )

