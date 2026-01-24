# FILE: web/panel/aap_settings/views/smtp_server.py
# DATE: 2026-01-24
# PURPOSE: Settings → SMTP server: отдельная страница настройки SMTP для одного Mailbox (LOGIN + OAuth2-заглушки) + пресеты + проверка SMTP (через существующий API).

from __future__ import annotations

import json

from django.shortcuts import redirect, render
from django.urls import reverse

from mailer_web.access import decode_id, encode_id
from panel.aap_settings.forms import SmtpServerForm
from panel.aap_settings.models import Mailbox, ProviderPreset, SmtpMailbox


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


def _pp(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return ""


def smtp_server_view(request, id: str):
    """
    URL: /settings/mail-servers/<ui_id>/smtp/
    """
    from engine.common import db as engine_db

    ws_id = _guard(request)
    if not ws_id:
        return redirect("/")

    try:
        mailbox_id = int(decode_id((id or "").strip()))
    except Exception:
        return redirect(reverse("settings:mail_servers"))

    mb = Mailbox.objects.filter(id=int(mailbox_id), workspace_id=ws_id).first()
    if not mb:
        return redirect(reverse("settings:mail_servers"))

    smtp = SmtpMailbox.objects.filter(mailbox_id=int(mb.id)).first()
    state = "edit" if smtp else "add"

    # --- presets (UI-only): берём только SMTP-login часть и только host/port/security
    preset_items = list(ProviderPreset.objects.filter(is_active=True).order_by("order", "name"))
    preset_choices = [(str(p.id), p.name) for p in preset_items]

    presets_map: dict[str, dict] = {}
    for p in preset_items:
        pj = p.preset_json or {}
        # контракт для этой страницы:
        # preset_json.smtp.login = { host, port, security }
        login = ((pj.get("smtp") or {}).get("login") or {}) if isinstance(pj, dict) else {}
        if isinstance(login, dict):
            host = (login.get("host") or "").strip()
            port = login.get("port")
            sec = (login.get("security") or "").strip()
            if host and port and sec:
                presets_map[str(p.id)] = {"host": host, "port": int(port), "security": sec}

    # --- last SMTP check result (mailbox_events)
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
        (int(mb.id),),
    )
    if row:
        last_smtp_status = str(row[0] or "")
        try:
            last_smtp_payload = _pp(row[1] or {})
        except Exception:
            last_smtp_payload = ""

    # --- initial form
    initial = {
        "auth_type": (smtp.auth_type if smtp else "login"),
        "sender_name": (smtp.sender_name if smtp else ""),
        "limit_hour_sent": (smtp.limit_hour_sent if smtp else 50),
        "host": "",
        "port": "",
        "security": "starttls",
        "username": "",
    }
    if smtp and isinstance(smtp.credentials_json, dict):
        cj = smtp.credentials_json or {}
        initial["host"] = cj.get("host") or ""
        initial["port"] = cj.get("port") or ""
        initial["security"] = cj.get("security") or initial["security"]
        initial["username"] = cj.get("username") or ""

    require_password = (state == "add")  # в edit можно не менять пароль

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "close":
            return redirect(reverse("settings:mail_servers"))

        form = SmtpServerForm(
            request.POST,
            preset_choices=preset_choices,
            require_password=require_password,
            mailbox_email=(mb.email or "").strip(),
        )

        if not form.is_valid():
            return render(
                request,
                "panels/aap_settings/smtp_server.html",
                {
                    "state": state,
                    "mailbox": mb,
                    "mailbox_ui_id": encode_id(int(mb.id)),
                    "form": form,
                    "presets_json": json.dumps(presets_map, ensure_ascii=False),
                    "last_smtp_status": last_smtp_status,
                    "last_smtp_payload": last_smtp_payload,
                },
            )

        # OAuth режимы на этом этапе не сохраняем (кнопка Save в UI скрыта, но сервер тоже защищаемся)
        auth_type = (form.cleaned_data.get("auth_type") or "").strip()
        if auth_type in ("google_oauth2", "microsoft_oauth2"):
            return redirect(reverse("settings:mail_servers"))

        sender_name = (form.cleaned_data["sender_name"] or "").strip()
        limit_hour_sent = int(form.cleaned_data["limit_hour_sent"])

        host = (form.cleaned_data["host"] or "").strip()
        port = int(form.cleaned_data["port"])
        security = (form.cleaned_data["security"] or "").strip()
        username = (form.cleaned_data["username"] or "").strip()
        password = (form.cleaned_data.get("password") or "").strip()

        credentials_json = {
            "host": host,
            "port": port,
            "security": security,
            "username": username,
        }
        if password:
            credentials_json["password"] = password

        if smtp:
            smtp.auth_type = "login"
            smtp.sender_name = sender_name
            smtp.from_email = mb.email
            smtp.limit_hour_sent = limit_hour_sent
            smtp.credentials_json = credentials_json
            smtp.save(update_fields=["auth_type", "sender_name", "from_email", "limit_hour_sent", "credentials_json", "updated_at"])
        else:
            SmtpMailbox.objects.create(
                mailbox=mb,
                auth_type="login",
                sender_name=sender_name,
                from_email=mb.email,
                limit_hour_sent=limit_hour_sent,
                credentials_json=credentials_json,
            )

        # остаёмся на этой странице
        return redirect(reverse("settings:mail_servers_smtp", kwargs={"id": encode_id(int(mb.id))}))

    form = SmtpServerForm(
        initial=initial,
        preset_choices=preset_choices,
        require_password=require_password,
        mailbox_email=(mb.email or "").strip(),
    )

    return render(
        request,
        "panels/aap_settings/smtp_server.html",
        {
            "state": state,
            "mailbox": mb,
            "mailbox_ui_id": encode_id(int(mb.id)),
            "form": form,
            "presets_json": json.dumps(presets_map, ensure_ascii=False),
            "last_smtp_status": last_smtp_status,
            "last_smtp_payload": last_smtp_payload,
        },
    )
