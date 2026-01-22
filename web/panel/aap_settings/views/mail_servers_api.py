# FILE: web/panel/aap_settings/views/mail_servers_api.py
# DATE: 2026-01-22
# PURPOSE: AJAX API для "Проверок" в Settings → Mail servers.
# CHANGE: Инфраструктура без реальных проверок: POST action=check_domain (пока один) -> ответ через 1 секунду.

from __future__ import annotations

import time

from django.http import JsonResponse
from django.views.decorators.http import require_POST


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


@require_POST
def mail_servers_api_view(request):
    ws_id = _guard(request)
    if not ws_id:
        return JsonResponse({"ok": False, "error": "auth"}, status=403)

    action = (request.POST.get("action") or "").strip()

    # Пока делаем одну кнопку/действие. Остальные добавим позже.
    if action != "check_domain":
        return JsonResponse({"ok": False, "error": "bad_action"}, status=400)

    # demo delay, чтобы увидеть "крутилку"
    time.sleep(1.0)

    return JsonResponse(
        {
            "ok": True,
            "action": action,
            "message": "DEMO: domain check finished (no real checks yet).",
        }
    )
