# FILE: web/panel/aap_settings/views/oauth.py
# DATE: 2026-01-23
# PURPOSE: OAuth2 (Google/Microsoft) start+callback for SMTP/IMAP XOAUTH2 connections.
# CHANGE: Implements redirect to provider, exchanges code->tokens, stores refresh_token in secret_enc (encrypted),
#         stores access_token+expires_at in extra_json, with masked logging (no token leaks).

from __future__ import annotations

import time
from typing import Dict, Any
from urllib.parse import urlencode

import httpx

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core import signing
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect
from django.urls import reverse

from engine.common.mail.logs import encrypt_secret, decrypt_secret, log_mail_event

from panel.aap_settings.models import Mailbox, MailboxConnection, ConnKind, OAuthProvider, MailboxOAuthApp


_STATE_SALT = "aap_settings.oauth.state.v1"
_STATE_MAX_AGE_SEC = 15 * 60


_GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_SCOPE = "https://mail.google.com/"  # SMTP+IMAP (XOAUTH2)


_MS_AUTH_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
_MS_TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
# IMAP/SMTP OAuth2 scopes for Outlook/Exchange Online:
_MS_SCOPE = "offline_access https://outlook.office.com/IMAP.AccessAsUser.All https://outlook.office.com/SMTP.Send"


def _sign_state(payload: Dict[str, Any]) -> str:
    return signing.dumps(payload, salt=_STATE_SALT)


def _unsign_state(state: str) -> Dict[str, Any]:
    return signing.loads(state, salt=_STATE_SALT, max_age=_STATE_MAX_AGE_SEC)


def _abs_callback_url(request: HttpRequest, provider: str) -> str:
    return request.build_absolute_uri(reverse("settings:mail_oauth_callback", kwargs={"provider": provider}))


def _abs_next_default() -> str:
    return reverse("settings:mail_servers")


def _get_oauth_app(*, workspace_id, provider: str) -> MailboxOAuthApp | None:
    return (
        MailboxOAuthApp.objects
        .filter(workspace_id=workspace_id, provider=provider, is_active=True)
        .first()
    )


def _safe_log(*, mailbox_id: int, action: str, status: str, message: str = "", data: Any = None) -> None:
    try:
        log_mail_event(mailbox_id=mailbox_id, action=action, status=status, message=message, data=data)
    except Exception:
        # logger должен быть "best effort": UI нельзя ронять логами
        return


@login_required
def mail_oauth_start_view(request: HttpRequest, provider: str, kind: str, mailbox_id: int) -> HttpResponse:
    """
    Start OAuth flow for конкретного mailbox+kind.
    Требование (твоё): connection уже должен существовать; UI создаст/валидирует host/port/security отдельно.
    """
    provider = (provider or "").strip().lower()
    kind = (kind or "").strip().lower()

    if provider not in (OAuthProvider.GOOGLE, OAuthProvider.MICROSOFT):
        messages.error(request, "OAuth: unknown provider.")
        return redirect(_abs_next_default())

    if kind not in (ConnKind.SMTP, ConnKind.IMAP):
        messages.error(request, "OAuth: bad connection kind.")
        return redirect(_abs_next_default())

    ws_id = getattr(request, "workspace_id", None)
    if not ws_id:
        return redirect(_abs_next_default())

    mb = Mailbox.objects.filter(id=int(mailbox_id), workspace_id=ws_id).first()
    if not mb:
        messages.error(request, "OAuth: mailbox not found.")
        return redirect(_abs_next_default())

    conn = MailboxConnection.objects.filter(mailbox=mb, kind=kind).first()
    if not conn:
        messages.error(request, "OAuth: connection does not exist yet (create SMTP/IMAP first).")
        return redirect(_abs_next_default())

    app = _get_oauth_app(workspace_id=ws_id, provider=provider)
    if not app:
        messages.error(request, "OAuth: app credentials are not configured for this workspace.")
        _safe_log(mailbox_id=mb.id, action="OAUTH", status="FAIL", message="no_oauth_app", data={"provider": provider, "kind": kind})
        return redirect(_abs_next_default())

    client_id = (app.client_id or "").strip()
    client_secret = decrypt_secret(app.client_secret_enc or "")

    if not client_id or not client_secret:
        messages.error(request, "OAuth: app credentials are empty.")
        _safe_log(mailbox_id=mb.id, action="OAUTH", status="FAIL", message="empty_oauth_app", data={"provider": provider, "kind": kind})
        return redirect(_abs_next_default())

    next_url = request.GET.get("next") or _abs_next_default()

    state = _sign_state(
        {
            "ws": str(ws_id),
            "mailbox_id": int(mb.id),
            "kind": kind,
            "provider": provider,
            "next": str(next_url),
            "ts": int(time.time()),
        }
    )

    redirect_uri = _abs_callback_url(request, provider)

    if provider == OAuthProvider.GOOGLE:
        params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": _GOOGLE_SCOPE,
            "access_type": "offline",
            "include_granted_scopes": "true",
            # чтобы гарантировать refresh_token (да, может показывать consent чаще)
            "prompt": "consent",
            "state": state,
        }
        return redirect(f"{_GOOGLE_AUTH_URL}?{urlencode(params)}")

    # Microsoft
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "response_mode": "query",
        "scope": _MS_SCOPE,
        "state": state,
    }
    return redirect(f"{_MS_AUTH_URL}?{urlencode(params)}")


@login_required
def mail_oauth_callback_view(request: HttpRequest, provider: str) -> HttpResponse:
    provider = (provider or "").strip().lower()
    if provider not in (OAuthProvider.GOOGLE, OAuthProvider.MICROSOFT):
        messages.error(request, "OAuth: unknown provider.")
        return redirect(_abs_next_default())

    err = (request.GET.get("error") or "").strip()
    if err:
        messages.error(request, f"OAuth error: {err}")
        return redirect(_abs_next_default())

    code = (request.GET.get("code") or "").strip()
    state_raw = (request.GET.get("state") or "").strip()
    if not code or not state_raw:
        messages.error(request, "OAuth: missing code/state.")
        return redirect(_abs_next_default())

    try:
        st = _unsign_state(state_raw)
    except Exception:
        messages.error(request, "OAuth: state is invalid/expired.")
        return redirect(_abs_next_default())

    ws_id_req = getattr(request, "workspace_id", None)
    ws_id_state = st.get("ws")
    if not ws_id_req or str(ws_id_req) != str(ws_id_state):
        messages.error(request, "OAuth: workspace mismatch.")
        return redirect(_abs_next_default())

    mailbox_id = int(st.get("mailbox_id") or 0)
    kind = str(st.get("kind") or "").strip().lower()
    next_url = str(st.get("next") or _abs_next_default())

    mb = Mailbox.objects.filter(id=mailbox_id, workspace_id=ws_id_req).first()
    if not mb:
        messages.error(request, "OAuth: mailbox not found.")
        return redirect(_abs_next_default())

    conn = MailboxConnection.objects.filter(mailbox=mb, kind=kind).first()
    if not conn:
        messages.error(request, "OAuth: connection not found.")
        return redirect(_abs_next_default())

    app = _get_oauth_app(workspace_id=ws_id_req, provider=provider)
    if not app:
        messages.error(request, "OAuth: app credentials are not configured for this workspace.")
        _safe_log(mailbox_id=mb.id, action="OAUTH", status="FAIL", message="no_oauth_app", data={"provider": provider, "kind": kind})
        return redirect(next_url)

    client_id = (app.client_id or "").strip()
    client_secret = decrypt_secret(app.client_secret_enc or "")

    redirect_uri = _abs_callback_url(request, provider)

    try:
        token = _exchange_code_for_tokens(
            provider=provider,
            code=code,
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
        )
    except Exception:
        messages.error(request, "OAuth: token exchange failed.")
        _safe_log(mailbox_id=mb.id, action="OAUTH", status="FAIL", message="token_exchange_failed", data={"provider": provider, "kind": kind})
        return redirect(next_url)

    refresh_token = (token.get("refresh_token") or "").strip()
    access_token = (token.get("access_token") or "").strip()
    expires_in = int(token.get("expires_in") or 0) if str(token.get("expires_in") or "").isdigit() else 0

    if not access_token:
        messages.error(request, "OAuth: access_token is missing.")
        _safe_log(mailbox_id=mb.id, action="OAUTH", status="FAIL", message="no_access_token", data={"provider": provider, "kind": kind})
        return redirect(next_url)

    expires_at = int(time.time()) + max(0, expires_in)

    # Сохраняем:
    # - refresh_token -> secret_enc (obfuscated)
    # - access_token + expires_at -> extra_json
    # refresh_token может отсутствовать (провайдер не выдал) — тогда НЕ затираем существующий
    if refresh_token:
        conn.secret_enc = encrypt_secret(refresh_token)

    ej = dict(conn.extra_json or {})
    ej["access_token"] = access_token
    ej["expires_at"] = expires_at
    conn.extra_json = ej

    # фиксируем auth_type в зависимости от провайдера
    if provider == OAuthProvider.GOOGLE:
        conn.auth_type = "google_oauth2"
    else:
        conn.auth_type = "microsoft_oauth2"

    conn.save(update_fields=["secret_enc", "extra_json", "auth_type", "updated_at"])

    messages.success(request, "Connected.")
    _safe_log(mailbox_id=mb.id, action="OAUTH", status="OK", message="connected", data={"provider": provider, "kind": kind})

    return redirect(next_url)


def _exchange_code_for_tokens(*, provider: str, code: str, client_id: str, client_secret: str, redirect_uri: str) -> Dict[str, Any]:
    if provider == OAuthProvider.GOOGLE:
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        }
        with httpx.Client(timeout=20) as c:
            r = c.post(_GOOGLE_TOKEN_URL, data=data, headers={"Accept": "application/json"})
            r.raise_for_status()
            return dict(r.json() or {})

    # Microsoft
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
        "scope": _MS_SCOPE,
    }
    with httpx.Client(timeout=20) as c:
        r = c.post(_MS_TOKEN_URL, data=data, headers={"Accept": "application/json"})
        r.raise_for_status()
        return dict(r.json() or {})
