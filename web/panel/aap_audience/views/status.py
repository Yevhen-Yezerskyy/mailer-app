# FILE: web/panel/aap_audience/views/status.py  (обновлено — 2025-12-28)
# Смысл: статус-страница показывает ТОЛЬКО задачи с run_processing=true
#        таблица/оформление — на стороне status.html (как низ clar.html), без SQL/статусов/действий.

from __future__ import annotations

from django.shortcuts import render

from mailer_web.access import encode_id
from panel.aap_audience.models import AudienceTask


def status_view(request):
    ws_id = request.workspace_id
    user = request.user

    if not ws_id or not getattr(user, "is_authenticated", False):
        tasks = AudienceTask.objects.none()
    else:
        tasks = (
            AudienceTask.objects.filter(workspace_id=ws_id, user=user, run_processing=True)
            .order_by("-created_at")[:50]
        )

    for t in tasks:
        t.ui_id = encode_id(int(t.id))

    return render(
        request,
        "panels/aap_audience/status.html",
        {
            "tasks": tasks,
        },
    )
