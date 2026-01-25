# FILE: web/panel/aap_settings/views/mail_servers_api.py
# DATE: 2026-01-25
# PURPOSE: AJAX API для Settings → Mail servers: оставлена только проверка домена (tech+reputation).
# CHANGE:
# - Убраны SMTP/IMAP checks и человеко-отчёты.
# - Принимает JSON body {action,id} (и совместимо с form POST).
# - Возвращает JSON: {ok, action, tech:{action,status,data}, reputation:{action,status,data}}.

from __future__ import annotations

import json
from typing import Any, Dict

from django.http import JsonResponse
from django.views.decorators.http import require_POST

from engine.common.mail.domain_checks import domain_check_reputation, domain_check_tech
from mailer_web.access import decode_id
from panel.aap_settings.models import Mailbox


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


def _read_payload(request) -> Dict[str, Any]:
    # Prefer JSON (fetch), fallback to form POST.
    ct = (request.META.get("CONTENT_TYPE") or "").lower()
    if "application/json" in ct:
        try:
            return json.loads((request.body or b"{}").decode("utf-8"))
        except Exception:
            return {}
    return dict(request.POST.itemsallowlist()) if hasattr(request.POST, "itemsallowlist") else dict(request.POST.items())


@require_POST
def mail_servers_api_view(request):
    ws_id = _guard(request)
    if not ws_id:
        return JsonResponse({"ok": False, "error": "auth"}, status=403)

    payload = _read_payload(request)
    action = (payload.get("action") or "").strip()
    if action != "check_domain":
        return JsonResponse({"ok": False, "error": "bad_action"}, status=400)

    tok = (payload.get("id") or "").strip()
    if not tok:
        return JsonResponse({"ok": False, "error": "missing_id"}, status=400)

    try:
        mailbox_id = int(decode_id(tok))
    except Exception:
        return JsonResponse({"ok": False, "error": "bad_id"}, status=400)

    if not Mailbox.objects.filter(id=mailbox_id, workspace_id=ws_id).exists():
        return JsonResponse({"ok": False, "error": "not_found"}, status=404)

    r_tech = domain_check_tech(mailbox_id)
    r_rep = domain_check_reputation(mailbox_id)

    return JsonResponse(
        {
            "tech": r_tech,           # {action,status,data}
            "reputation": r_rep,      # {action,status,data}
        }
    )
