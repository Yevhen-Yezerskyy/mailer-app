# FILE: web/panel/aap_audience/views/modal_edit_rate_limit.py
# DATE: 2026-04-06
# PURPOSE: Modal form for editing task rate_limit (20..60) with is_more_needed cache refresh.

from __future__ import annotations

from django.http import JsonResponse
from django.shortcuts import render
from django.utils.translation import gettext as _

from engine.core_status.is_active import clear_is_more_needed_full_cache, is_more_needed
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


def modal_edit_rate_limit_view(request):
    token = (request.POST.get("id") or request.GET.get("id") or "").strip()
    task = _resolve_task(request, token)

    if request.method == "POST":
        if not task:
            return JsonResponse({"ok": False, "error": str(_("Запись не найдена."))}, status=404)

        try:
            rate_limit = int(str(request.POST.get("rate_limit") or "").strip())
        except Exception:
            return JsonResponse({"ok": False, "error": str(_("Введите лимит от 20 до 60."))}, status=400)

        if rate_limit < 20 or rate_limit > 60:
            return JsonResponse({"ok": False, "error": str(_("Введите лимит от 20 до 60."))}, status=400)

        task.rate_limit = int(rate_limit)
        task.save(update_fields=["rate_limit", "updated_at"])
        try:
            clear_is_more_needed_full_cache(int(task.id))
            is_more_needed(int(task.id), update=True)
        except Exception:
            pass
        return JsonResponse({"ok": True, "rate_limit": int(rate_limit)})

    if not task:
        return render(
            request,
            "panels/aap_audience/modal_edit_rate_limit.html",
            {"status": "empty"},
        )

    return render(
        request,
        "panels/aap_audience/modal_edit_rate_limit.html",
        {
            "status": "ok",
            "type": str(task.type or "").strip(),
            "task_id_token": token,
            "current_rate_limit": int(task.rate_limit or 50),
        },
    )
