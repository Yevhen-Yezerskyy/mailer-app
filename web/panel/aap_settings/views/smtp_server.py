# FILE: web/panel/aap_settings/views/smtp_server.py
# DATE: 2026-01-26
# PURPOSE: Settings → SMTP server (PU).
# CHANGE:
# - ВОССТАНОВЛЕН контракт smtp_secret_view: всегда возвращает {ok: true|false}
# - Глаз (reveal password) снова работает
# - Логика SMTP, форм, checks — без изменений по смыслу

from __future__ import annotations

import json
from typing import Any, Dict, List, Type
from zoneinfo import ZoneInfo

from django.db import connection
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.http import require_GET

from engine.common import db as engine_db
from engine.common.mail import types as mail_types
from engine.common.mail.types import SMTP_CREDENTIALS_FORMAT
from mailer_web.access import decode_id, encode_id
from panel.aap_settings.forms import SmtpServerForm
from panel.aap_settings.models import Mailbox, ProviderPreset, ProviderPresetNoAuth, SmtpMailbox

SECRET_MASK = "********"


def _guard_ws_id(request):
    ws_id = getattr(request, "workspace_id", None)
    if not ws_id or not getattr(request.user, "is_authenticated", False):
        return None
    return ws_id


def _td_keys(td: Any) -> List[str]:
    return list((getattr(td, "__annotations__", None) or {}).keys())


SMTP_AUTH_TYPES = set(SMTP_CREDENTIALS_FORMAT.keys())


def _norm_security(v: Any) -> str:
    s = (v or "").strip().lower()
    if s == "tls":
        return "ssl"
    return s


def _mask_password_widget(form: SmtpServerForm) -> None:
    f = form.fields.get("password")
    if not f:
        return
    w = f.widget
    attrs = getattr(w, "attrs", {}) or {}
    attrs["readonly"] = "readonly"
    attrs["data-yy-masked"] = "1"
    w.attrs = attrs


def _clean_password_input(v: str) -> str:
    v = (v or "").strip()
    if v == SECRET_MASK:
        return ""
    return v


# ============================================================
# SECRET REVEAL (ГЛАЗ) — КОНТРАКТ ВОССТАНОВЛЕН
# ============================================================

@require_GET
def smtp_secret_view(request, id: str):
    ws_id = _guard_ws_id(request)
    if not ws_id:
        return JsonResponse({"ok": False, "error": "auth"}, status=403)

    tok = (request.GET.get("id") or "").strip() or (id or "").strip()
    try:
        mailbox_id = int(decode_id(tok))
    except Exception:
        return JsonResponse({"ok": False, "error": "bad_id"}, status=400)

    kind = (request.GET.get("kind") or "").strip().lower()
    if kind not in ("smtp", "password"):
        return JsonResponse({"ok": False, "error": "bad_kind"}, status=400)

    mb = Mailbox.objects.filter(id=mailbox_id, workspace_id=ws_id).first()
    if not mb:
        return JsonResponse({"ok": False, "error": "not_found"}, status=404)

    smtp = SmtpMailbox.objects.filter(mailbox_id=mb.id).first()
    if not smtp or not isinstance(smtp.credentials_json, dict):
        return JsonResponse({"ok": False, "error": "no_credentials"}, status=404)

    if smtp.auth_type != "LOGIN":
        return JsonResponse({"ok": False, "error": "not_supported"}, status=400)

    try:
        fmt: Type[Any] = SMTP_CREDENTIALS_FORMAT["LOGIN"]
        creds_plain = mail_types.get(dict(smtp.credentials_json), fmt)
        secret = (creds_plain.get("password") or "").strip()
    except Exception:
        return JsonResponse({"ok": False, "error": "decrypt_failed"}, status=400)

    return JsonResponse({"ok": True, "secret": secret})


# ============================================================
# MAIN PU
# ============================================================

def smtp_server_view(request, id: str):
    ws_id = _guard_ws_id(request)
    if not ws_id:
        return redirect("/")

    try:
        mailbox_id = int(decode_id((id or "").strip()))
    except Exception:
        return redirect(reverse("settings:mail_servers"))

    mb = Mailbox.objects.filter(id=mailbox_id, workspace_id=ws_id).first()
    if not mb:
        return redirect(reverse("settings:mail_servers"))

    smtp = SmtpMailbox.objects.filter(mailbox_id=mb.id).first()
    state = "edit" if smtp else "add"

    # -------------------------
    # Presets (UI only)
    # -------------------------
    login_preset_items = list(ProviderPreset.objects.filter(is_active=True).order_by("order", "name"))

    relay_preset_items = []
    try:
        tables = set(connection.introspection.table_names())
    except Exception:
        tables = set()
    if ProviderPresetNoAuth._meta.db_table in tables:
        relay_preset_items = list(ProviderPresetNoAuth.objects.filter(is_active=True).order_by("order", "name"))

    login_presets_map: Dict[str, Dict[str, Any]] = {}
    for p in login_preset_items:
        pj = p.preset_json or {}
        login = ((pj.get("smtp") or {}).get("login") or {}) if isinstance(pj, dict) else {}
        if isinstance(login, dict):
            host = (login.get("host") or "").strip()
            port = login.get("port")
            sec = _norm_security(login.get("security"))
            if host and port and sec:
                login_presets_map[str(p.id)] = {
                    "name": p.name,
                    "host": host,
                    "port": int(port),
                    "security": sec,
                }

    relay_presets_map: Dict[str, Dict[str, Any]] = {}
    for p in relay_preset_items:
        pj = p.preset_json or {}
        relay = ((pj.get("smtp") or {}).get("relay_noauth") or {}) if isinstance(pj, dict) else {}
        if isinstance(relay, dict):
            host = (relay.get("host") or "").strip()
            port = relay.get("port")
            sec = _norm_security(relay.get("security"))
            if host and port and sec:
                relay_presets_map[str(p.id)] = {
                    "name": p.name,
                    "host": host,
                    "port": int(port),
                    "security": sec,
                }

    relay_presets_map.setdefault(
        "builtin_google_relay",
        {
            "name": "Google SMTP Relay",
            "host": "smtp-relay.gmail.com",
            "port": 587,
            "security": "starttls",
        },
    )

    # -------------------------
    # Last checks (mailbox_events)
    # -------------------------
    CHECK_ACTIONS = (
        "DOMAIN_CHECK_TECH",
        "DOMAIN_CHECK_REPUTATION",
        "SMTP_AUTH_CHECK",
        "SMTP_SEND_CHECK",
    )

    rows = engine_db.fetch_all(
        """
        SELECT action, status, created_at, data
        FROM mailbox_events
        WHERE mailbox_id = %s AND action = ANY(%s)
        ORDER BY created_at DESC
        """,
        (mb.id, list(CHECK_ACTIONS)),
    ) or []

    berlin = ZoneInfo("Europe/Berlin")
    seen = set()
    last_checks: List[Dict[str, Any]] = []

    for action, status, created_at, data in rows:
        if action in seen:
            continue
        seen.add(action)

        try:
            dt = created_at.astimezone(berlin)
            dt_s = dt.strftime("%d.%m.%Y %H:%M:%S")
        except Exception:
            dt_s = "—"

        try:
            payload = json.dumps(data or {}, ensure_ascii=False, indent=2)
        except Exception:
            payload = ""

        last_checks.append(
            {
                "action": action,
                "status": status,
                "dt": dt_s,
                "payload": payload,
            }
        )

    # -------------------------
    # Stored → initial
    # -------------------------
    stored_auth_type = (smtp.auth_type if smtp else "LOGIN") or "LOGIN"
    if stored_auth_type not in SMTP_AUTH_TYPES:
        stored_auth_type = "LOGIN"

    stored_creds_enc = smtp.credentials_json if (smtp and isinstance(smtp.credentials_json, dict)) else {}
    stored_password_enc = ""
    if stored_auth_type == "LOGIN" and isinstance(stored_creds_enc, dict):
        stored_password_enc = (stored_creds_enc.get("password") or "").strip()
    require_password = stored_auth_type == "LOGIN" and not bool(stored_password_enc)

    stored_password_plain = ""
    if smtp and stored_auth_type == "LOGIN" and stored_password_enc:
        try:
            fmt0 = SMTP_CREDENTIALS_FORMAT["LOGIN"]
            stored_plain = mail_types.get(dict(stored_creds_enc), fmt0)
            stored_password_plain = (stored_plain.get("password") or "").strip()
        except Exception:
            pass

    initial: Dict[str, Any] = {
        "auth_type": stored_auth_type,
        "sender_name": smtp.sender_name if smtp else "",
        "email": (smtp.from_email if smtp else mb.email or "").strip(),
        "limit_hour_sent": smtp.limit_hour_sent if smtp else 50,
    }

    if stored_auth_type in SMTP_AUTH_TYPES and isinstance(stored_creds_enc, dict):
        td = SMTP_CREDENTIALS_FORMAT[stored_auth_type]
        for k in _td_keys(td):
            if k != "password":
                initial[k] = stored_creds_enc.get(k)

    if state == "add":
        if mb.email:
            initial["username"] = (mb.email or "").strip()
        initial["security"] = "starttls"
        
    if stored_auth_type == "LOGIN" and not require_password:
        initial["password"] = SECRET_MASK

    def _ctx(form_obj: SmtpServerForm) -> Dict[str, Any]:
        return {
            "state": state,
            "mailbox": mb,
            "mailbox_ui_id": encode_id(mb.id),
            "form": form_obj,
            "login_presets_json": json.dumps(login_presets_map, ensure_ascii=False),
            "relay_presets_json": json.dumps(relay_presets_map, ensure_ascii=False),
            "last_checks": last_checks,
        }

    # -------------------------
    # POST
    # -------------------------
    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "close":
            return redirect(reverse("settings:mail_servers"))

        form = SmtpServerForm(request.POST, require_password=require_password)
        if stored_auth_type == "LOGIN" and not require_password:
            _mask_password_widget(form)

        if not form.is_valid():
            return render(
                request,
                "panels/aap_settings/smtp_server.html",
                _ctx(form),
            )

        auth_type = (form.cleaned_data.get("auth_type") or "LOGIN").strip().upper()
        if auth_type not in SMTP_AUTH_TYPES:
            auth_type = "LOGIN"
        td = SMTP_CREDENTIALS_FORMAT[auth_type]

        creds_plain = {}
        for k in _td_keys(td):
            if k == "password":
                v = _clean_password_input(form.cleaned_data.get("password") or "")
                if not v:
                    v = stored_password_plain
                if not v:
                    form.add_error("password", "Пароль обязателен.")
                    return render(
                        request,
                        "panels/aap_settings/smtp_server.html",
                        _ctx(form),
                    )
                creds_plain[k] = v
            else:
                creds_plain[k] = form.cleaned_data.get(k)

        creds_enc = mail_types.put(creds_plain, td)

        if smtp:
            smtp.auth_type = auth_type
            smtp.sender_name = form.cleaned_data["sender_name"]
            smtp.from_email = form.cleaned_data["email"]
            smtp.limit_hour_sent = form.cleaned_data["limit_hour_sent"]
            smtp.credentials_json = creds_enc
            smtp.save()
        else:
            SmtpMailbox.objects.create(
                mailbox=mb,
                auth_type=auth_type,
                sender_name=form.cleaned_data["sender_name"],
                from_email=form.cleaned_data["email"],
                limit_hour_sent=form.cleaned_data["limit_hour_sent"],
                credentials_json=creds_enc,
            )

        return redirect(reverse("settings:mail_servers_smtp", kwargs={"id": encode_id(mb.id)}))

    form = SmtpServerForm(initial=initial, require_password=require_password)
    if stored_auth_type == "LOGIN" and not require_password:
        _mask_password_widget(form)

    return render(
        request,
        "panels/aap_settings/smtp_server.html",
        _ctx(form),
    )
