# FILE: web/panel/aap_audience/views/create_list.py
# DATE: 2026-03-08
# PURPOSE: "Постановка задачи" list page with create buttons and tasks table (source_* fields).

from django.shortcuts import redirect, render

from mailer_web.access import decode_id, encode_id
from panel.aap_audience.models import AudienceTask


def _get_tasks(request):
    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return []
    tasks = list(
        AudienceTask.objects.filter(workspace_id=ws_id, user=user, archived=False).order_by("-created_at")
    )
    for t in tasks:
        t.ui_id = encode_id(int(t.id))
    return tasks


def create_list_view(request):
    ws_id = request.workspace_id
    user = request.user

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "delete":
            token = (request.POST.get("id") or "").strip()
            try:
                pk = int(decode_id(token))
            except Exception:
                pk = 0
            if pk > 0 and ws_id and getattr(user, "is_authenticated", False):
                AudienceTask.objects.filter(id=pk, workspace_id=ws_id, user=user).update(archived=True)
            return redirect("audience:create_list")

    tasks = _get_tasks(request)
    return render(
        request,
        "panels/aap_audience/create_list.html",
        {
            "tasks": tasks,
        },
    )
