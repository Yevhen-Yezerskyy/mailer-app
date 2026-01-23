# FILE: web/panel/aap_settings/views/mail_servers.py
# DATE: 2026-01-23
# PURPOSE: Settings → Mail servers (split): (1) mailbox list+add, (2) SMTP form, (3) IMAP form.
# CHANGE:
# - Страница разнесена на 3 изолированные формы: список/добавление mailbox; отдельные страницы SMTP и IMAP.
# - limit_hour_sent хранится в SMTP MailboxConnection.extra_json["limit_hour_sent"] (fallback: Mailbox.limit_hour_sent).
# - Domain-check справа показывается только если домен НЕ в whitelist (engine.common.mail.domain_whitelist).
# - reveal secret по кнопке "глаз" через AJAX + confirm modal (decrypt_secret/encrypt_secret).
# - Preset применяется JS-ом (без кнопки), view отдаёт presets json.

from __future__ import annotations

from django.http import HttpResponseRedirect, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse

from engine.common.mail.domain_whitelist import is_domain_whitelisted
from engine.common.mail.logs import decrypt_secret, encrypt_secret
from mailer_web.access import decode_id, encode_id, resolve_pk_or_redirect
from panel.aap_settings.forms import ImapConnForm, MailboxAddForm, SmtpConnForm
from panel.aap_settings.models import Mailbox, MailboxConnection, ProviderPreset


SECRET_MASK = "********"


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


def _preset_choices(kind: str):
    qs = (
        ProviderPreset.objects.filter(is_active=True, kind=kind)
        .values_list("code", "name")
        .distinct()
        .order_by("name", "code")
    )
    return [(str(code), str(name)) for code, name in qs]


def _preset_map(kind: str) -> dict:
    """
    Для JS auto-apply пресета.
    Возвращает:
      { code: {host, ports:[...], security, auth_type} }
    """
    out: dict = {}
    qs = ProviderPreset.objects.filter(is_active=True, kind=kind).only("code", "host", "ports_json", "security", "auth_type")
    for p in qs:
        ports = []
        try:
            ports = p.ports_json or []
        except Exception:
            ports = []
        out[str(p.code)] = {
            "host": str(p.host or ""),
            "ports": ports,
            "security": str(p.security or ""),
            "auth_type": str(p.auth_type or ""),
        }
    return out


def _conn(mailbox_id: int, kind: str) -> MailboxConnection | None:
    return MailboxConnection.objects.filter(mailbox_id=int(mailbox_id), kind=kind).first()


def _norm_secret_from_cleaned(v: str) -> str:
    s = (v or "").strip()
    if s == SECRET_MASK:
        return ""
    return s


def _sender_label(mb: Mailbox, smtp: MailboxConnection | None) -> str:
    ex = smtp.extra_json if (smtp and isinstance(smtp.extra_json, dict)) else {}
    fn = (ex.get("from_name") or "").strip()
    fe = (ex.get("from_email") or "").strip() or (mb.email or "").strip()
    return f"{fn} <{fe}>" if fn else f"<{fe}>"


def _smtp_limit(mb: Mailbox, smtp: MailboxConnection | None) -> int:
    ex = smtp.extra_json if (smtp and isinstance(smtp.extra_json, dict)) else {}
    v = ex.get("limit_hour_sent")
    try:
        if v is not None:
            return int(v)
    except Exception:
        pass
    try:
        return int(getattr(mb, "limit_hour_sent", 50) or 50)
    except Exception:
        return 50


def _domain_from_mailbox(mb: Mailbox) -> str:
    d = (getattr(mb, "domain", "") or "").strip().lower()
    if d:
        return d
    em = (mb.email or "").strip().lower()
    if "@" in em:
        return em.split("@", 1)[1].strip().lower()
    return ""


def mail_server_secret_view(request):
    ws_id = _guard(request)
    if not ws_id:
        return JsonResponse({"ok": False, "error": "auth"}, status=403)

    token = (request.GET.get("id") or "").strip()
    kind = (request.GET.get("kind") or "").strip().lower()

    if not token or kind not in ("smtp", "imap"):
        return JsonResponse({"ok": False, "error": "bad_request"}, status=400)

    try:
        mailbox_id = int(decode_id(token))
    except Exception:
        return JsonResponse({"ok": False, "error": "bad_id"}, status=400)

    mb = Mailbox.objects.filter(id=int(mailbox_id), workspace_id=ws_id).only("id").first()
    if not mb:
        return JsonResponse({"ok": False, "error": "not_found"}, status=404)

    conn = (
        MailboxConnection.objects.filter(mailbox_id=int(mb.id), kind=kind)
        .only("secret_enc")
        .first()
    )
    if not conn or not (conn.secret_enc or "").strip():
        return JsonResponse({"ok": False, "error": "no_secret"}, status=404)

    try:
        plain = decrypt_secret(str(conn.secret_enc))
    except Exception:
        return JsonResponse({"ok": False, "error": "decrypt_failed"}, status=500)

    return JsonResponse({"ok": True, "secret": plain})


def mail_servers_view(request):
    """
    Legacy endpoint: оставлен для обратной совместимости (старые ссылки).
    """
    return redirect(reverse("settings:mail_servers_list"))


def mail_servers_list_view(request):
    """
    (1) Список mailbox + add mailbox (только email) + delete mailbox.
    """
    ws_id = _guard(request)
    if not ws_id:
        return redirect("/")

    items = list(Mailbox.objects.filter(workspace_id=ws_id).order_by("email"))

    mb_ids = [int(m.id) for m in items]
    smtp_qs = (
        MailboxConnection.objects.filter(mailbox_id__in=mb_ids, kind="smtp")
        .only("id", "mailbox_id", "extra_json")
        .order_by("mailbox_id", "-id")
    )
    latest_smtp: dict[int, MailboxConnection] = {}
    for c in smtp_qs:
        mid = int(c.mailbox_id)
        if mid not in latest_smtp:
            latest_smtp[mid] = c

    for it in items:
        it.ui_id = encode_id(int(it.id))
        smtp = latest_smtp.get(int(it.id))
        it.sender_label = _sender_label(it, smtp)
        it.limit_hour_sent = _smtp_limit(it, smtp)

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "delete":
            token = (request.POST.get("id") or "").strip()
            if token:
                q = request.GET.copy()
                q["id"] = token
                request.GET = q

            res = resolve_pk_or_redirect(request, Mailbox, param="id")
            if isinstance(res, HttpResponseRedirect):
                return res

            Mailbox.objects.filter(id=int(res), workspace_id=ws_id).delete()
            return redirect(request.path)

        form = MailboxAddForm(request.POST, workspace_id=ws_id)
        if not form.is_valid():
            return render(request, "panels/aap_settings/mail_servers_list.html", {"form": form, "items": items})

        email = (form.cleaned_data["email"] or "").strip().lower()
        domain = (email.split("@", 1)[1] if "@" in email else "").strip().lower()

        mb = Mailbox.objects.create(workspace_id=ws_id, email=email, domain=domain)
        return redirect(reverse("settings:mail_servers_smtp", kwargs={"id": encode_id(int(mb.id))}))

    form = MailboxAddForm(initial={"email": ""}, workspace_id=ws_id)
    return render(request, "panels/aap_settings/mail_servers_list.html", {"form": form, "items": items})


def mail_servers_smtp_view(request, id: str):
    """
    (2) SMTP form.
    """
    ws_id = _guard(request)
    if not ws_id:
        return redirect("/")

    request.GET = request.GET.copy()
    request.GET["id"] = id
    res = resolve_pk_or_redirect(request, Mailbox, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res

    mb = Mailbox.objects.filter(id=int(res), workspace_id=ws_id).first()
    if not mb:
        return redirect(reverse("settings:mail_servers_list"))

    smtp = _conn(int(mb.id), "smtp")

    preset_choices = _preset_choices("smtp")
    presets = _preset_map("smtp")

    secret_masked = bool(smtp and (smtp.secret_enc or "").strip())

    domain = _domain_from_mailbox(mb)
    domain_whitelisted = is_domain_whitelisted(domain)

    init = {
        "preset_code": "",
        "auth_type": (smtp.auth_type if smtp else "login"),
        "host": (smtp.host if smtp else ""),
        "port": (smtp.port if smtp else ""),
        "security": (smtp.security if smtp else "none"),
        "username": (smtp.username if smtp else (mb.email or "")),
        "from_name": "",
        "limit_hour_sent": _smtp_limit(mb, smtp),
    }

    if smtp and isinstance(smtp.extra_json, dict):
        init["from_name"] = (smtp.extra_json.get("from_name") or "").strip()

    if secret_masked:
        init["secret"] = SECRET_MASK

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "close":
            return redirect(reverse("settings:mail_servers_list"))

        form = SmtpConnForm(
            request.POST,
            preset_choices=preset_choices,
            require_secret=not bool(smtp),
            secret_masked=secret_masked,
        )

        if not form.is_valid():
            return render(
                request,
                "panels/aap_settings/mail_servers_smtp.html",
                {
                    "form": form,
                    "mb": mb,
                    "mb_token": encode_id(int(mb.id)),
                    "presets": presets,
                    "domain_whitelisted": domain_whitelisted,
                },
            )

        auth_type = (form.cleaned_data.get("auth_type") or "login").strip()

        smtp_secret_new = _norm_secret_from_cleaned(form.cleaned_data.get("secret") or "")
        if smtp_secret_new:
            secret_enc_to_store = encrypt_secret(smtp_secret_new)
        else:
            secret_enc_to_store = (smtp.secret_enc or "").strip() if smtp else ""

        ex = dict(smtp.extra_json) if (smtp and isinstance(smtp.extra_json, dict)) else {}
        ex["from_name"] = (form.cleaned_data.get("from_name") or "").strip()
        ex["from_email"] = (mb.email or "").strip()
        try:
            ex["limit_hour_sent"] = int(form.cleaned_data.get("limit_hour_sent") or 50)
        except Exception:
            ex["limit_hour_sent"] = 50

        MailboxConnection.objects.update_or_create(
            mailbox_id=int(mb.id),
            kind="smtp",
            defaults={
                "host": (form.cleaned_data.get("host") or "").strip(),
                "port": int(form.cleaned_data.get("port") or 0),
                "security": form.cleaned_data.get("security") or "none",
                "auth_type": auth_type,
                "username": (form.cleaned_data.get("username") or "").strip(),
                "secret_enc": secret_enc_to_store,
                "extra_json": ex,
            },
        )

        return redirect(request.path)

    form = SmtpConnForm(
        initial=init,
        preset_choices=preset_choices,
        require_secret=not bool(smtp),
        secret_masked=secret_masked,
    )
    return render(
        request,
        "panels/aap_settings/mail_servers_smtp.html",
        {
            "form": form,
            "mb": mb,
            "mb_token": encode_id(int(mb.id)),
            "presets": presets,
            "domain_whitelisted": domain_whitelisted,
        },
    )


def mail_servers_imap_view(request, id: str):
    """
    (3) IMAP form.
    """
    ws_id = _guard(request)
    if not ws_id:
        return redirect("/")

    request.GET = request.GET.copy()
    request.GET["id"] = id
    res = resolve_pk_or_redirect(request, Mailbox, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res

    mb = Mailbox.objects.filter(id=int(res), workspace_id=ws_id).first()
    if not mb:
        return redirect(reverse("settings:mail_servers_list"))

    imap = _conn(int(mb.id), "imap")

    preset_choices = _preset_choices("imap")
    presets = _preset_map("imap")

    secret_masked = bool(imap and (imap.secret_enc or "").strip())

    domain = _domain_from_mailbox(mb)
    domain_whitelisted = is_domain_whitelisted(domain)

    init = {
        "preset_code": "",
        "auth_type": (imap.auth_type if imap else "login"),
        "host": (imap.host if imap else ""),
        "port": (imap.port if imap else ""),
        "security": (imap.security if imap else "none"),
        "username": (imap.username if imap else (mb.email or "")),
    }
    if secret_masked:
        init["secret"] = SECRET_MASK

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "close":
            return redirect(reverse("settings:mail_servers_list"))

        if action == "delete_imap":
            MailboxConnection.objects.filter(mailbox_id=int(mb.id), kind="imap").delete()
            return redirect(request.path)

        form = ImapConnForm(
            request.POST,
            preset_choices=preset_choices,
            require_secret=not bool(imap),
            secret_masked=secret_masked,
        )

        if not form.is_valid():
            return render(
                request,
                "panels/aap_settings/mail_servers_imap.html",
                {
                    "form": form,
                    "mb": mb,
                    "mb_token": encode_id(int(mb.id)),
                    "presets": presets,
                    "domain_whitelisted": domain_whitelisted,
                },
            )

        auth_type = (form.cleaned_data.get("auth_type") or "login").strip()

        imap_secret_new = _norm_secret_from_cleaned(form.cleaned_data.get("secret") or "")
        if imap_secret_new:
            secret_enc_to_store = encrypt_secret(imap_secret_new)
        else:
            secret_enc_to_store = (imap.secret_enc or "").strip() if imap else ""

        MailboxConnection.objects.update_or_create(
            mailbox_id=int(mb.id),
            kind="imap",
            defaults={
                "host": (form.cleaned_data.get("host") or "").strip(),
                "port": int(form.cleaned_data.get("port") or 0),
                "security": form.cleaned_data.get("security") or "none",
                "auth_type": auth_type,
                "username": (form.cleaned_data.get("username") or "").strip(),
                "secret_enc": secret_enc_to_store,
                "extra_json": (imap.extra_json if (imap and isinstance(imap.extra_json, dict)) else {}),
            },
        )

        return redirect(request.path)

    form = ImapConnForm(
        initial=init,
        preset_choices=preset_choices,
        require_secret=not bool(imap),
        secret_masked=secret_masked,
    )
    return render(
        request,
        "panels/aap_settings/mail_servers_imap.html",
        {
            "form": form,
            "mb": mb,
            "mb_token": encode_id(int(mb.id)),
            "presets": presets,
            "domain_whitelisted": domain_whitelisted,
        },
    )
