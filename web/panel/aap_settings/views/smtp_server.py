# FILE: web/panel/aap_settings/views/smtp_server.py
# DATE: 2026-01-25
# PURPOSE: Settings → SMTP server (PU).
# CHANGE:
# - При state=="add": подставляем Mailbox.email в from_email (form.email) и в username (если поле есть в types).
# - Сохранение credentials_json теперь через engine.common.mail.types.put (шифрует password).
# - Чтение из БД для reuse пароля (когда пароль не меняли) через engine.common.mail.types.get (расшифровывает password).
# - Добавлен GET endpoint smtp_secret_view для модалки "показать пароль" (используется mail_servers_secret.js).

from __future__ import annotations

import json
from typing import Any, Dict, List, Type

from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.http import require_GET

from engine.common import db as engine_db
from engine.common.mail import types as mail_types
from engine.common.mail.types import SMTP_CREDENTIALS_FORMAT
from mailer_web.access import decode_id, encode_id
from panel.aap_settings.forms import SmtpServerForm
from panel.aap_settings.models import Mailbox, ProviderPreset, SmtpMailbox

SECRET_MASK = "********"


def _guard_ws_id(request):
    ws_id = getattr(request, "workspace_id", None)
    if not ws_id or not getattr(request.user, "is_authenticated", False):
        return None
    return ws_id


def _td_keys(td: Any) -> List[str]:
    return list((getattr(td, "__annotations__", None) or {}).keys())


SMTP_AUTH_TYPES = set(SMTP_CREDENTIALS_FORMAT.keys())


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


@require_GET
def smtp_secret_view(request, id: str):
    """
    Reveal SMTP password for LOGIN.
    Used by web/static/js/aap_settings/mail_servers_secret.js
    GET params: ?id=<token>&kind=smtp  (kind is ignored except validation).
    """
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

    auth_type = (smtp.auth_type or "LOGIN").strip()
    if auth_type != "LOGIN":
        return JsonResponse({"ok": False, "error": "not_supported"}, status=400)

    try:
        fmt: Type[Any] = SMTP_CREDENTIALS_FORMAT["LOGIN"]
        creds_plain = mail_types.get(dict(smtp.credentials_json), fmt)
        secret = (creds_plain.get("password") or "").strip()
    except Exception:
        return JsonResponse({"ok": False, "error": "decrypt_failed"}, status=400)

    return JsonResponse({"ok": True, "secret": secret})


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

    # --- presets (UI only) ---
    preset_items = list(ProviderPreset.objects.filter(is_active=True).order_by("order", "name"))
    preset_choices = [(str(p.id), p.name) for p in preset_items]

    presets_map: Dict[str, Dict[str, Any]] = {}
    for p in preset_items:
        pj = p.preset_json or {}
        login = ((pj.get("smtp") or {}).get("login") or {}) if isinstance(pj, dict) else {}
        if isinstance(login, dict):
            host = (login.get("host") or "").strip()
            port = login.get("port")
            sec = (login.get("security") or "").strip()
            if host and port and sec:
                presets_map[str(p.id)] = {"host": host, "port": int(port), "security": sec}

    # --- last SMTP check (existing DB table) ---
    last_smtp_status = None
    last_smtp_payload = ""
    row = engine_db.fetch_one(
        """
        SELECT status, data
        FROM mailbox_events
        WHERE mailbox_id = %s AND action = 'SMTP_CHECK'
        ORDER BY id DESC
        LIMIT 1
        """,
        (mb.id,),
    )
    if row:
        last_smtp_status = str(row[0] or "")
        try:
            last_smtp_payload = json.dumps(row[1] or {}, ensure_ascii=False, indent=2, sort_keys=True)
        except Exception:
            last_smtp_payload = ""

    # --- stored state ---
    stored_auth_type = (smtp.auth_type if smtp else "LOGIN") or "LOGIN"
    if stored_auth_type not in SMTP_AUTH_TYPES:
        stored_auth_type = "LOGIN"

    stored_creds_enc = smtp.credentials_json if (smtp and isinstance(smtp.credentials_json, dict)) else {}
    stored_password_enc = (stored_creds_enc.get("password") or "").strip() if isinstance(stored_creds_enc, dict) else ""
    require_password = not bool(stored_password_enc)

    stored_password_plain = ""
    if smtp and stored_auth_type == "LOGIN" and stored_password_enc:
        try:
            fmt0: Type[Any] = SMTP_CREDENTIALS_FORMAT["LOGIN"]
            stored_plain = mail_types.get(dict(stored_creds_enc), fmt0)
            stored_password_plain = (stored_plain.get("password") or "").strip()
        except Exception:
            stored_password_plain = ""

    # --- initial ---
    initial: Dict[str, Any] = {
        "auth_type": stored_auth_type,
        "sender_name": (smtp.sender_name if smtp else ""),
        "email": ((smtp.from_email if smtp else "") or "").strip(),
        "limit_hour_sent": (smtp.limit_hour_sent if smtp else 50),
    }

    # init creds block by selected auth_type keyset (except secrets)
    if stored_auth_type in SMTP_AUTH_TYPES and isinstance(stored_creds_enc, dict):
        td: Type[Any] = SMTP_CREDENTIALS_FORMAT[stored_auth_type]
        for k in _td_keys(td):
            if k == "password":
                continue
            initial[k] = stored_creds_enc.get(k, "")

    # IMPORTANT: только на добавление (SMTP ещё нет) — подставляем mb.email в from_email и username
    if state == "add":
        mb_email = (mb.email or "").strip()
        if mb_email:
            initial["email"] = mb_email
            if "username" in initial:
                initial["username"] = mb_email

    if not require_password:
        initial["password"] = SECRET_MASK

    # --- POST ---
    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "close":
            return redirect(reverse("settings:mail_servers"))

        form = SmtpServerForm(request.POST, require_password=require_password)  # preset_choices НЕ передаём
        if not require_password:
            _mask_password_widget(form)

        if not form.is_valid():
            return render(
                request,
                "panels/aap_settings/smtp_server.html",
                {
                    "state": state,
                    "mailbox": mb,
                    "mailbox_ui_id": encode_id(mb.id),
                    "form": form,
                    "preset_choices": preset_choices,
                    "presets_json": json.dumps(presets_map, ensure_ascii=False),
                    "last_smtp_status": last_smtp_status,
                    "last_smtp_payload": last_smtp_payload,
                },
            )

        auth_type = (form.cleaned_data.get("auth_type") or "").strip()
        if auth_type not in SMTP_AUTH_TYPES:
            raise ValueError("Invalid auth_type")

        # OAuth stubs (как сейчас): не сохраняем
        if auth_type != "LOGIN":
            return redirect(reverse("settings:mail_servers"))

        td: Type[Any] = SMTP_CREDENTIALS_FORMAT[auth_type]
        keys = _td_keys(td)

        # собрать plaintext для put(); password берём из формы или (если не меняли) из расшифрованного stored_password_plain
        creds_plain: Dict[str, Any] = {}
        for k in keys:
            if k == "password":
                v = _clean_password_input(str(form.cleaned_data.get("password") or ""))
                if not v:
                    v = stored_password_plain
                if not v:
                    form.add_error("password", "Пароль обязателен.")
                    return render(
                        request,
                        "panels/aap_settings/smtp_server.html",
                        {
                            "state": state,
                            "mailbox": mb,
                            "mailbox_ui_id": encode_id(mb.id),
                            "form": form,
                            "preset_choices": preset_choices,
                            "presets_json": json.dumps(presets_map, ensure_ascii=False),
                            "last_smtp_status": last_smtp_status,
                            "last_smtp_payload": last_smtp_payload,
                        },
                    )
                creds_plain[k] = v
            else:
                creds_plain[k] = form.cleaned_data.get(k)

        # validate + encrypt marked fields (password) for DB
        creds_enc = mail_types.put(creds_plain, td)

        sender_name = (form.cleaned_data.get("sender_name") or "").strip()
        from_email = (form.cleaned_data.get("email") or "").strip()
        limit_hour_sent = int(form.cleaned_data.get("limit_hour_sent") or 0)

        if smtp:
            smtp.auth_type = auth_type
            smtp.sender_name = sender_name
            smtp.from_email = from_email
            smtp.limit_hour_sent = limit_hour_sent
            smtp.credentials_json = creds_enc
            smtp.save(
                update_fields=[
                    "auth_type",
                    "sender_name",
                    "from_email",
                    "limit_hour_sent",
                    "credentials_json",
                    "updated_at",
                ]
            )
        else:
            SmtpMailbox.objects.create(
                mailbox=mb,
                auth_type=auth_type,
                sender_name=sender_name,
                from_email=from_email,
                limit_hour_sent=limit_hour_sent,
                credentials_json=creds_enc,
            )

        return redirect(reverse("settings:mail_servers_smtp", kwargs={"id": encode_id(mb.id)}))

    form = SmtpServerForm(initial=initial, require_password=require_password)
    if not require_password:
        _mask_password_widget(form)

    return render(
        request,
        "panels/aap_settings/smtp_server.html",
        {
            "state": state,
            "mailbox": mb,
            "mailbox_ui_id": encode_id(mb.id),
            "form": form,
            "preset_choices": preset_choices,
            "presets_json": json.dumps(presets_map, ensure_ascii=False),
            "last_smtp_status": last_smtp_status,
            "last_smtp_payload": last_smtp_payload,
        },
    )
