# FILE: web/panel/aap_settings/views/smtp_server.py
# DATE: 2026-01-24
# PURPOSE: Settings → SMTP server (PU).
# - НЕ трогаем Mailbox.email
# - payload формируем строго по engine.common.mail.types (SMTP_CREDENTIALS_FORMAT → TypedDict keyset)
# - OAuth пока заглушка
# - preset_choices/presets_json только для шаблона (НЕ для формы)

from __future__ import annotations

import json
from typing import Any, Dict, List, Type

from django.shortcuts import redirect, render
from django.urls import reverse

from engine.common import db as engine_db
from engine.common.mail.types import SMTP_CREDENTIALS_FORMAT
from mailer_web.access import decode_id, encode_id
from panel.aap_settings.forms import SmtpServerForm
from panel.aap_settings.models import Mailbox, ProviderPreset, SmtpMailbox


def _guard_ws_id(request):
    ws_id = getattr(request, "workspace_id", None)
    if not ws_id or not getattr(request.user, "is_authenticated", False):
        return None
    return ws_id


def _td_keys(td: Any) -> List[str]:
    return list((getattr(td, "__annotations__", None) or {}).keys())


SMTP_AUTH_TYPES = set(SMTP_CREDENTIALS_FORMAT.keys())


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

    stored_creds = smtp.credentials_json if (smtp and isinstance(smtp.credentials_json, dict)) else {}
    stored_password = (stored_creds.get("password") or "").strip() if isinstance(stored_creds, dict) else ""
    require_password = not bool(stored_password)

    # --- initial ---
    initial: Dict[str, Any] = {
        "auth_type": stored_auth_type,
        "sender_name": (smtp.sender_name if smtp else ""),
        "email": ((smtp.from_email if smtp else "") or "").strip(),
        "limit_hour_sent": (smtp.limit_hour_sent if smtp else 50),
    }

    # init creds block by selected auth_type keyset (except secrets where needed)
    if stored_auth_type in SMTP_AUTH_TYPES and isinstance(stored_creds, dict):
        td: Type[Any] = SMTP_CREDENTIALS_FORMAT[stored_auth_type]
        for k in _td_keys(td):
            if k == "password":
                continue
            initial[k] = stored_creds.get(k, "")

    # safe default for username if it exists in this auth_type
    if "username" in initial and not (initial.get("username") or "").strip():
        initial["username"] = (mb.email or "").strip()

    # --- POST ---
    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "close":
            return redirect(reverse("settings:mail_servers"))

        form = SmtpServerForm(request.POST, require_password=require_password)  # <-- preset_choices НЕ передаём
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

        creds: Dict[str, Any] = {}
        for k in keys:
            if k == "password":
                v = (form.cleaned_data.get("password") or "").strip() or stored_password
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
                creds[k] = v
            else:
                creds[k] = form.cleaned_data.get(k)

        sender_name = (form.cleaned_data.get("sender_name") or "").strip()
        from_email = (form.cleaned_data.get("email") or "").strip()
        limit_hour_sent = int(form.cleaned_data.get("limit_hour_sent") or 0)

        if smtp:
            smtp.auth_type = auth_type
            smtp.sender_name = sender_name
            smtp.from_email = from_email
            smtp.limit_hour_sent = limit_hour_sent
            smtp.credentials_json = creds
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
                credentials_json=creds,
            )

        return redirect(reverse("settings:mail_servers_smtp", kwargs={"id": encode_id(mb.id)}))

    form = SmtpServerForm(initial=initial, require_password=require_password)  # <-- preset_choices НЕ передаём

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
