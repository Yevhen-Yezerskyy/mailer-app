# FILE: web/panel/aap_settings/views/imap_server.py
# DATE: 2026-01-26
# PURPOSE: Settings → IMAP server (PU).
# CHANGE:
# - add-state: username=mailbox.email, security=starttls (жёстко).
# - Добавлен показ последней IMAP_CHECK: дата+статус.
# - Кнопка "Проверить IMAP" ездит через mail_servers_api (action=check_imap).

from __future__ import annotations

import json
from typing import Any, Dict, List, Type
from zoneinfo import ZoneInfo

from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.http import require_GET

from engine.common import db as engine_db
from engine.common.mail import types as mail_types
from engine.common.mail.types import IMAP_CREDENTIALS_FORMAT
from mailer_web.access import decode_id, encode_id
from panel.aap_settings.forms import ImapConnForm
from panel.aap_settings.models import ImapMailbox, Mailbox, ProviderPreset

SECRET_MASK = "********"


def _guard_ws_id(request):
    ws_id = getattr(request, "workspace_id", None)
    if not ws_id or not getattr(request.user, "is_authenticated", False):
        return None
    return ws_id


def _td_keys(td: Any) -> List[str]:
    return list((getattr(td, "__annotations__", None) or {}).keys())


IMAP_AUTH_TYPES = set(IMAP_CREDENTIALS_FORMAT.keys())


def _mask_password_widget(form: ImapConnForm) -> None:
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


def _force_required_login_fields(form: ImapConnForm, require_password: bool) -> None:
    for k in ("host", "port", "security", "username"):
        if k in form.fields:
            form.fields[k].required = True
    if "password" in form.fields:
        form.fields["password"].required = bool(require_password)


def _get_last_imap_check(mailbox_id: int) -> tuple[str | None, str | None]:
    row = engine_db.fetch_one(
        """
        SELECT status, created_at
        FROM mailbox_events
        WHERE mailbox_id = %s AND action = 'IMAP_CHECK'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (int(mailbox_id),),
    )
    if not row:
        return None, None

    status, created_at = row
    try:
        dt = created_at.astimezone(ZoneInfo("Europe/Berlin"))
        dt_s = dt.strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        dt_s = "—"
    return str(status), dt_s


# ============================================================
# SECRET REVEAL (ГЛАЗ) — КОНТРАКТ {ok: true|false}
# ============================================================

@require_GET
def imap_secret_view(request, id: str):
    ws_id = _guard_ws_id(request)
    if not ws_id:
        return JsonResponse({"ok": False, "error": "auth"}, status=403)

    tok = (request.GET.get("id") or "").strip() or (id or "").strip()
    try:
        mailbox_id = int(decode_id(tok))
    except Exception:
        return JsonResponse({"ok": False, "error": "bad_id"}, status=400)

    kind = (request.GET.get("kind") or "").strip().lower()
    if kind not in ("imap", "password"):
        return JsonResponse({"ok": False, "error": "bad_kind"}, status=400)

    mb = Mailbox.objects.filter(id=mailbox_id, workspace_id=ws_id).first()
    if not mb:
        return JsonResponse({"ok": False, "error": "not_found"}, status=404)

    imap = ImapMailbox.objects.filter(mailbox_id=mb.id).first()
    if not imap or not isinstance(imap.credentials_json, dict):
        return JsonResponse({"ok": False, "error": "no_credentials"}, status=404)

    if imap.auth_type != "LOGIN":
        return JsonResponse({"ok": False, "error": "not_supported"}, status=400)

    try:
        fmt: Type[Any] = IMAP_CREDENTIALS_FORMAT["LOGIN"]
        creds_plain = mail_types.get(dict(imap.credentials_json), fmt)
        secret = (creds_plain.get("password") or "").strip()
    except Exception:
        return JsonResponse({"ok": False, "error": "decrypt_failed"}, status=400)

    return JsonResponse({"ok": True, "secret": secret})


# ============================================================
# MAIN PU
# ============================================================

def imap_server_view(request, id: str):
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

    imap = ImapMailbox.objects.filter(mailbox_id=mb.id).first()
    state = "edit" if imap else "add"

    last_check_status, last_check_dt = _get_last_imap_check(mb.id)

    # -------------------------
    # Presets (UI only)
    # -------------------------
    preset_items = list(ProviderPreset.objects.filter(is_active=True).order_by("order", "name"))
    preset_choices = [(str(p.id), p.name) for p in preset_items]

    presets_map: Dict[str, Dict[str, Any]] = {}
    for p in preset_items:
        pj = p.preset_json or {}
        login = ((pj.get("imap") or {}).get("login") or {}) if isinstance(pj, dict) else {}
        if isinstance(login, dict):
            host = (login.get("host") or "").strip()
            port = login.get("port")
            sec = (login.get("security") or "").strip()
            if host and port and sec:
                presets_map[str(p.id)] = {"host": host, "port": int(port), "security": sec}

    # -------------------------
    # Stored → initial
    # -------------------------
    stored_auth_type = (imap.auth_type if imap else "LOGIN") or "LOGIN"
    if stored_auth_type not in IMAP_AUTH_TYPES:
        stored_auth_type = "LOGIN"

    stored_creds_enc = imap.credentials_json if (imap and isinstance(imap.credentials_json, dict)) else {}
    stored_password_enc = (stored_creds_enc.get("password") or "").strip()
    require_password = not bool(stored_password_enc)

    stored_password_plain = ""
    if imap and stored_auth_type == "LOGIN" and stored_password_enc:
        try:
            fmt0 = IMAP_CREDENTIALS_FORMAT["LOGIN"]
            stored_plain = mail_types.get(dict(stored_creds_enc), fmt0)
            stored_password_plain = (stored_plain.get("password") or "").strip()
        except Exception:
            pass

    initial: Dict[str, Any] = {"auth_type": stored_auth_type}

    if imap and stored_auth_type in IMAP_AUTH_TYPES and isinstance(stored_creds_enc, dict):
        td0 = IMAP_CREDENTIALS_FORMAT[stored_auth_type]
        for k in _td_keys(td0):
            if k != "password":
                initial[k] = stored_creds_enc.get(k)

    if state == "add":
        if mb.email:
            initial["username"] = (mb.email or "").strip()
        initial["security"] = "starttls"

    if not require_password:
        initial["password"] = SECRET_MASK

    # -------------------------
    # POST
    # -------------------------
    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "close":
            return redirect(reverse("settings:mail_servers"))

        form = ImapConnForm(request.POST, require_password=require_password, password_masked=False)
        _force_required_login_fields(form, require_password=require_password)
        if not require_password:
            _mask_password_widget(form)

        if not form.is_valid():
            return render(
                request,
                "panels/aap_settings/imap_server.html",
                {
                    "state": state,
                    "mailbox": mb,
                    "mailbox_ui_id": encode_id(mb.id),
                    "form": form,
                    "preset_choices": preset_choices,
                    "presets_json": json.dumps(presets_map, ensure_ascii=False),
                    "last_check_status": last_check_status,
                    "last_check_dt": last_check_dt,
                },
            )

        auth_type = (form.cleaned_data.get("auth_type") or "LOGIN").strip()
        if auth_type not in IMAP_AUTH_TYPES:
            auth_type = "LOGIN"

        td = IMAP_CREDENTIALS_FORMAT[auth_type]

        creds_plain: Dict[str, Any] = {}
        for k in _td_keys(td):
            if k == "password":
                v = _clean_password_input(form.cleaned_data.get("password") or "")
                if not v:
                    v = stored_password_plain
                if not v:
                    form.add_error("password", "Пароль обязателен.")
                    return render(
                        request,
                        "panels/aap_settings/imap_server.html",
                        {
                            "state": state,
                            "mailbox": mb,
                            "mailbox_ui_id": encode_id(mb.id),
                            "form": form,
                            "preset_choices": preset_choices,
                            "presets_json": json.dumps(presets_map, ensure_ascii=False),
                            "last_check_status": last_check_status,
                            "last_check_dt": last_check_dt,
                        },
                    )
                creds_plain[k] = v
            else:
                creds_plain[k] = form.cleaned_data.get(k)

        creds_enc = mail_types.put(creds_plain, td)

        if imap:
            imap.auth_type = auth_type
            imap.credentials_json = creds_enc
            imap.save()
        else:
            ImapMailbox.objects.create(
                mailbox=mb,
                auth_type=auth_type,
                credentials_json=creds_enc,
            )

        return redirect(reverse("settings:mail_servers_imap", kwargs={"id": encode_id(mb.id)}))

    form = ImapConnForm(initial=initial, require_password=require_password, password_masked=False)
    _force_required_login_fields(form, require_password=require_password)
    if not require_password:
        _mask_password_widget(form)

    return render(
        request,
        "panels/aap_settings/imap_server.html",
        {
            "state": state,
            "mailbox": mb,
            "mailbox_ui_id": encode_id(mb.id),
            "form": form,
            "preset_choices": preset_choices,
            "presets_json": json.dumps(presets_map, ensure_ascii=False),
            "last_check_status": last_check_status,
            "last_check_dt": last_check_dt,
        },
    )
