# FILE: web/panel/aap_audience/views/modal_create_task.py
# DATE: 2026-03-16
# PURPOSE: Modal details view for audience create-list task card.

from django.shortcuts import render

from mailer_web.access import decode_id
from panel.aap_audience.models import AudienceTask


def modal_create_task_view(request):
    token = (request.GET.get("id") or "").strip()
    if not token:
        return render(request, "panels/aap_audience/modal_create_task.html", {"task": None})

    try:
        pk = int(decode_id(token))
    except Exception:
        return render(request, "panels/aap_audience/modal_create_task.html", {"task": None})

    task = (
        AudienceTask.objects.filter(
            id=pk,
            workspace_id=request.workspace_id,
        ).first()
    )
    return render(request, "panels/aap_audience/modal_create_task.html", {"task": task})
