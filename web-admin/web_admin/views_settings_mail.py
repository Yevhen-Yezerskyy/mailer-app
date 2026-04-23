# FILE: web-admin/web_admin/views_settings_mail.py
# DATE: 2026-03-07
# PURPOSE: Settings -> system mailbox page + proxy to original SMTP/IMAP/API logic from panel.aap_settings.

from __future__ import annotations

import json
import uuid
from zoneinfo import ZoneInfo

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse

from mailer_web.access import decode_id, encode_id
from panel.aap_settings.models import ImapMailbox, Mailbox, SmtpMailbox
from panel.aap_settings.views import imap_server, mail_servers_api, smtp_server


SERVICE_WORKSPACE_ID = uuid.UUID("00000000-0000-0000-0000-000000000000")
SERVICE_MAILBOX_EMAIL = "service@serenity-mail.de"
_FLAG_ATTR = "_tw_classmap_enabled"


def _flag_request(request: HttpRequest) -> None:
    setattr(request, _FLAG_ATTR, True)


def _mailbox_domain(email: str) -> str:
    em = (email or "").strip().lower()
    if "@" in em:
        return em.split("@", 1)[1].strip()
    return ""


def _ensure_service_mailbox() -> Mailbox:
    mailbox, _unused = Mailbox.objects.get_or_create(
        workspace_id=SERVICE_WORKSPACE_ID,
        email=SERVICE_MAILBOX_EMAIL,
        defaults={
            "domain": _mailbox_domain(SERVICE_MAILBOX_EMAIL),
            "is_active": True,
            "archived": False,
        },
    )
    return mailbox


def _set_service_workspace(request: HttpRequest) -> None:
    request.workspace_id = SERVICE_WORKSPACE_ID


def _mailbox_id_from_token(token: str) -> int | None:
    try:
        return int(decode_id((token or "").strip()))
    except Exception:
        return None


def _is_system_mailbox_token(token: str, mailbox: Mailbox) -> bool:
    mailbox_id = _mailbox_id_from_token(token)
    return mailbox_id is not None and int(mailbox.id) == int(mailbox_id)


def _fmt_dt(dt) -> str:
    try:
        return dt.astimezone(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return "—"


def _domain_from_mailbox(mb: Mailbox) -> str:
    d = (getattr(mb, "domain", "") or "").strip().lower()
    if d:
        return d
    return _mailbox_domain(mb.email)


@login_required(login_url="login")
def service_mail_servers_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
    _set_service_workspace(request)
    mb = _ensure_service_mailbox()

    smtp_exists = SmtpMailbox.objects.filter(mailbox_id=mb.id).exists()
    imap_exists = ImapMailbox.objects.filter(mailbox_id=mb.id).exists()

    from engine.common import db as engine_db

    actions = (
        "SMTP_AUTH_CHECK",
        "SMTP_SEND_CHECK",
        "IMAP_CHECK",
        "DOMAIN_CHECK_TECH",
        "DOMAIN_CHECK_REPUTATION",
    )

    rows = engine_db.fetch_all(
        """
        SELECT action, status, created_at
        FROM mailbox_events
        WHERE mailbox_id = %s
          AND action = ANY(%s)
        ORDER BY action, created_at DESC
        """,
        (int(mb.id), list(actions)),
    ) or []

    status_map: dict[str, dict] = {}
    for action, status, created_at in rows:
        if str(action) in status_map:
            continue
        status_map[str(action)] = {
            "dt": _fmt_dt(created_at),
            "action": str(action),
            "status": str(status),
        }

    mb.ui_id = encode_id(int(mb.id))
    mb.domain_name = _domain_from_mailbox(mb)

    mb.smtp_configured = bool(smtp_exists)
    mb.smtp_auth = status_map.get("SMTP_AUTH_CHECK")
    mb.smtp_send = status_map.get("SMTP_SEND_CHECK")

    mb.imap_configured = bool(imap_exists)
    mb.imap_check = status_map.get("IMAP_CHECK")

    return render(
        request,
        "panels/aap_settings/mail_servers_system.html",
        {
            "section": "service_mailbox",
            "items": [mb],
            "system_mailbox": mb,
        },
    )


@login_required(login_url="login")
def service_mail_servers_smtp_view(request: HttpRequest, id: str) -> HttpResponse:
    _flag_request(request)
    _set_service_workspace(request)
    mb = _ensure_service_mailbox()
    if not _is_system_mailbox_token(id, mb):
        return redirect(reverse("settings:mail_servers"))
    return smtp_server.smtp_server_view(request, id)


@login_required(login_url="login")
def service_mail_servers_smtp_secret_view(request: HttpRequest, id: str) -> HttpResponse:
    _flag_request(request)
    _set_service_workspace(request)
    mb = _ensure_service_mailbox()

    token = (request.GET.get("id") or "").strip() or (id or "").strip()
    if not _is_system_mailbox_token(token, mb):
        return JsonResponse({"ok": False, "error": "not_found"}, status=404)

    return smtp_server.smtp_secret_view(request, id)


@login_required(login_url="login")
def service_mail_servers_imap_view(request: HttpRequest, id: str) -> HttpResponse:
    _flag_request(request)
    _set_service_workspace(request)
    mb = _ensure_service_mailbox()
    if not _is_system_mailbox_token(id, mb):
        return redirect(reverse("settings:mail_servers"))
    return imap_server.imap_server_view(request, id)


@login_required(login_url="login")
def service_mail_servers_imap_secret_view(request: HttpRequest, id: str) -> HttpResponse:
    _flag_request(request)
    _set_service_workspace(request)
    mb = _ensure_service_mailbox()

    token = (request.GET.get("id") or "").strip() or (id or "").strip()
    if not _is_system_mailbox_token(token, mb):
        return JsonResponse({"ok": False, "error": "not_found"}, status=404)

    return imap_server.imap_secret_view(request, id)


@login_required(login_url="login")
def service_mail_servers_api_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
    _set_service_workspace(request)
    mb = _ensure_service_mailbox()

    token = (request.POST.get("id") or "").strip()
    if not token and request.content_type and "application/json" in request.content_type.lower():
        try:
            payload = json.loads((request.body or b"{}").decode("utf-8"))
        except Exception:
            payload = {}
        token = str(payload.get("id") or "").strip()
    if token and not _is_system_mailbox_token(token, mb):
        return JsonResponse({"error": "not_found"}, status=404)

    return mail_servers_api.mail_servers_api_view(request)
