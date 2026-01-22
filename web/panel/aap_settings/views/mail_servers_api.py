# FILE: web/panel/aap_settings/views/mail_servers_api.py
# DATE: 2026-01-22
# PURPOSE: AJAX API для "Проверок" в Settings → Mail servers.
# CHANGE: Fix import path to avoid duplicate model registration (use panel.*).

from __future__ import annotations

from django.http import JsonResponse
from django.views.decorators.http import require_POST

from engine.common.mail.smtp_test import smtp_check_and_log
from web.mailer_web.access import decode_id
from panel.aap_settings.models import Mailbox  # <-- FIX (was web.panel.aap_settings.models)


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
    if action != "check_smtp":
        return JsonResponse({"ok": False, "error": "bad_action"}, status=400)

    tok = (request.POST.get("id") or "").strip()
    if not tok:
        return JsonResponse({"ok": False, "error": "missing_id"}, status=400)

    try:
        mailbox_id = decode_id(tok)
    except Exception:
        return JsonResponse({"ok": False, "error": "bad_id"}, status=400)

    if not Mailbox.objects.filter(id=mailbox_id, workspace_id=ws_id).exists():
        return JsonResponse({"ok": False, "error": "not_found"}, status=404)

    r = smtp_check_and_log(int(mailbox_id))

    # UI contract: always compact
    out = {
        "ok": True,
        "action": "SMTP_CHECK",
        "status": r.status,                        # OK / FAIL
        "message": r.user_message or "OK",          # human friendly
        "latency_ms": (r.data or {}).get("latency_ms"),
        "stage": (r.data or {}).get("stage"),
    }

    # Optional verbose dump (for debug button later)
    if (request.POST.get("verbose") or "").strip() == "1":
        out["data"] = r.data

    return JsonResponse(out)
