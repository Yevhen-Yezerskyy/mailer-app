# FILE: web/panel/aap_settings/views/mail_servers_api.py
# DATE: 2026-01-26
# PURPOSE: AJAX API for Mail servers.
# ACTIONS:
# - check_domain
# - check_smtp
# - send_test_mail

from __future__ import annotations

import json
from typing import Any, Dict, Callable

from django.http import JsonResponse
from django.views.decorators.http import require_POST

from engine.common.mail.domain_checks import domain_check_reputation, domain_check_tech
from engine.common.mail.utils import smtp_auth_check, smtp_send_check
from mailer_web.access import decode_id
from panel.aap_settings.models import Mailbox


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


def _read_payload(request) -> Dict[str, Any]:
    ct = (request.META.get("CONTENT_TYPE") or "").lower()
    if "application/json" in ct:
        try:
            return json.loads((request.body or b"{}").decode("utf-8"))
        except Exception:
            return {}
    return dict(request.POST.items())


# -------------------------
# Handlers
# -------------------------

def _handle_check_domain(*, mailbox_id: int, **_) -> Dict[str, Any]:
    return {
        "tech": domain_check_tech(mailbox_id),
        "reputation": domain_check_reputation(mailbox_id),
    }


def _handle_check_smtp(*, mailbox_id: int, **_) -> Dict[str, Any]:
    return smtp_auth_check(mailbox_id)


def _handle_send_test_mail(*, mailbox_id: int, to: str | None = None, **_) -> Dict[str, Any]:
    if not to:
        return {"action": "SMTP_SEND_CHECK", "status": "FAIL", "data": {"error": "missing_to"}}
    return smtp_send_check(mailbox_id, to)


ACTION_HANDLERS: Dict[str, Callable[..., Dict[str, Any]]] = {
    "check_domain": _handle_check_domain,
    "check_smtp": _handle_check_smtp,
    "send_test_mail": _handle_send_test_mail,
}


@require_POST
def mail_servers_api_view(request):
    ws_id = _guard(request)
    if not ws_id:
        return JsonResponse({"error": "auth"}, status=403)

    payload = _read_payload(request)
    action = (payload.get("action") or "").strip()

    handler = ACTION_HANDLERS.get(action)
    if not handler:
        return JsonResponse({"error": "bad_action", "action": action}, status=400)

    tok = (payload.get("id") or "").strip()
    if not tok:
        return JsonResponse({"error": "missing_id"}, status=400)

    try:
        mailbox_id = int(decode_id(tok))
    except Exception:
        return JsonResponse({"error": "bad_id"}, status=400)

    if not Mailbox.objects.filter(id=mailbox_id, workspace_id=ws_id).exists():
        return JsonResponse({"error": "not_found"}, status=404)

    result = handler(mailbox_id=mailbox_id, **payload)

    return JsonResponse(
        {
            "action": action,
            **result,
        }
    )
