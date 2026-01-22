# FILE: web/panel/aap_settings/views/mail_servers.py
# DATE: 2026-01-22
# PURPOSE: /panel/settings/mail-servers/ — SMTP обяз., IMAP опц.; apply preset без валидации; reveal secret по кнопке "глаз" через AJAX + confirm modal.
# CHANGE:
# - secret_enc теперь хранится ЗАШИФРОВАННЫМ (encrypt_secret/decrypt_secret из engine.common.mail.logs)
# - secret_view возвращает расшифрованный secret
# - при сохранении: новый секрет шифруется; если секрет не меняли (маска) — сохраняем старый secret_enc как есть

from __future__ import annotations

from django.http import HttpResponseRedirect, JsonResponse
from django.shortcuts import redirect, render

from engine.common.mail.logs import decrypt_secret, encrypt_secret
from mailer_web.access import decode_id, encode_id, resolve_pk_or_redirect
from panel.aap_settings.forms import MailServerForm
from panel.aap_settings.models import Mailbox, MailboxConnection, ProviderPreset


SECRET_MASK = "********"


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


def _preset_choices():
    qs = (
        ProviderPreset.objects.filter(is_active=True)
        .values_list("code", "name")
        .distinct()
        .order_by("name", "code")
    )
    return [(str(code), str(name)) for code, name in qs]


def _get_edit_obj(request, ws_id):
    if request.GET.get("state") != "edit":
        return None
    if not request.GET.get("id"):
        return None

    res = resolve_pk_or_redirect(request, Mailbox, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res

    return Mailbox.objects.filter(id=int(res), workspace_id=ws_id).first()


def _conn_map(mailbox: Mailbox):
    qs = MailboxConnection.objects.filter(mailbox_id=int(mailbox.id))
    out = {}
    for c in qs:
        out[str(c.kind)] = c
    return out


def _imap_any(post) -> bool:
    keys = [
        "imap_host",
        "imap_port",
        "imap_security",
        "imap_auth_type",
        "imap_username",
        "imap_secret",
    ]
    for k in keys:
        v = (post.get(k) or "")
        if str(v).strip():
            return True
    return False


def _apply_email_to_usernames(post):
    email = (post.get("email") or "").strip()
    if not email:
        return post

    if not (post.get("smtp_username") or "").strip():
        post["smtp_username"] = email
    if not (post.get("imap_username") or "").strip():
        post["imap_username"] = email
    return post


def _apply_preset_to_post(post, code: str):
    """
    Apply preset values into POST-like dict (in-place).
    Preset entries are per-kind (smtp/imap).
    """
    smtp = ProviderPreset.objects.filter(code=code, kind="smtp", is_active=True).first()
    imap = ProviderPreset.objects.filter(code=code, kind="imap", is_active=True).first()

    if smtp:
        post["smtp_host"] = smtp.host or post.get("smtp_host", "")
        ports = []
        try:
            ports = smtp.ports_json or []
        except Exception:
            ports = []
        if ports and not (post.get("smtp_port") or "").strip():
            post["smtp_port"] = str(int(ports[0]))
        post["smtp_security"] = smtp.security or post.get("smtp_security", "")
        post["smtp_auth_type"] = smtp.auth_type or post.get("smtp_auth_type", "")

    if imap:
        post["imap_host"] = imap.host or post.get("imap_host", "")
        ports = []
        try:
            ports = imap.ports_json or []
        except Exception:
            ports = []
        if ports and not (post.get("imap_port") or "").strip():
            post["imap_port"] = str(int(ports[0]))
        post["imap_security"] = imap.security or post.get("imap_security", "")
        post["imap_auth_type"] = imap.auth_type or post.get("imap_auth_type", "")

    return post


def _norm_secret_from_cleaned(v: str) -> str:
    s = (v or "").strip()
    if s == SECRET_MASK:
        return ""
    return s


def _attach_sender_fields(items: list[Mailbox]) -> list[Mailbox]:
    """
    Adds:
      - it.sender_name (from SMTP extra_json.from_name)
      - it.sender_label (from_name <from_email>)
    Uses latest SMTP connection per mailbox (by max id).
    """
    if not items:
        return items

    mb_by_id = {int(m.id): m for m in items}
    mb_ids = list(mb_by_id.keys())

    smtp_conns = (
        MailboxConnection.objects.filter(mailbox_id__in=mb_ids, kind="smtp")
        .only("id", "mailbox_id", "extra_json")
        .order_by("mailbox_id", "-id")
    )

    latest_by_mb: dict[int, MailboxConnection] = {}
    for c in smtp_conns:
        mid = int(c.mailbox_id)
        if mid not in latest_by_mb:
            latest_by_mb[mid] = c

    for mid, mb in mb_by_id.items():
        c = latest_by_mb.get(mid)
        extra = c.extra_json if (c and isinstance(c.extra_json, dict)) else {}
        fn = (extra.get("from_name") or "").strip()
        fe = (extra.get("from_email") or "").strip() or (mb.email or "").strip()
        mb.sender_name = fn
        mb.sender_label = f"{fn} <{fe}>" if fn else f"<{fe}>"

    return items


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
    ws_id = _guard(request)
    if not ws_id:
        return redirect("/")

    preset_choices = _preset_choices()

    edit_obj = _get_edit_obj(request, ws_id)
    if isinstance(edit_obj, HttpResponseRedirect):
        return edit_obj

    state = ""
    if request.GET.get("state") == "add":
        state = "add"
    if edit_obj:
        state = "edit"
        edit_obj.ui_id = encode_id(int(edit_obj.id))

    items = list(Mailbox.objects.filter(workspace_id=ws_id).order_by("email"))
    _attach_sender_fields(items)
    for it in items:
        it.ui_id = encode_id(int(it.id))

    init = {
        "email": "",
        "limit_hour_sent": 50,
        "preset_code": "",  # после save — пусто (не "липнет")
        "from_name": "",
    }

    smtp_has_secret = False
    imap_has_secret = False

    if edit_obj:
        init["email"] = edit_obj.email or ""
        init["limit_hour_sent"] = int(getattr(edit_obj, "limit_hour_sent", 50) or 50)

        cm = _conn_map(edit_obj)
        smtp = cm.get("smtp")
        imap = cm.get("imap")

        if smtp:
            init["smtp_host"] = smtp.host
            init["smtp_port"] = smtp.port
            init["smtp_security"] = smtp.security
            init["smtp_auth_type"] = smtp.auth_type
            init["smtp_username"] = smtp.username
            ex = smtp.extra_json or {}
            init["from_name"] = (ex.get("from_name") or "").strip()

            if (smtp.secret_enc or "").strip():
                smtp_has_secret = True
                init["smtp_secret"] = SECRET_MASK

        if imap:
            init["imap_host"] = imap.host
            init["imap_port"] = imap.port
            init["imap_security"] = imap.security
            init["imap_auth_type"] = imap.auth_type
            init["imap_username"] = imap.username

            if (imap.secret_enc or "").strip():
                imap_has_secret = True
                init["imap_secret"] = SECRET_MASK

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "close":
            return redirect(request.path)

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

        post = request.POST.copy()

        if action == "apply_preset":
            code = (post.get("preset_code") or "").strip()
            if code:
                _apply_preset_to_post(post, code)

            _apply_email_to_usernames(post)

            form = MailServerForm(
                post,
                preset_choices=preset_choices,
                require_smtp_secret=not bool(edit_obj),
            )
            return render(
                request,
                "panels/aap_settings/mail_servers.html",
                {"state": state or "add", "form": form, "edit_obj": edit_obj, "items": items},
            )

        if action in ("check_domain", "check_smtp", "check_imap"):
            return redirect(request.get_full_path())

        _apply_email_to_usernames(post)

        form = MailServerForm(
            post,
            preset_choices=preset_choices,
            require_smtp_secret=not bool(edit_obj),
            workspace_id=ws_id,
            mailbox_id=(int(edit_obj.id) if edit_obj else None),
        )

        if not form.is_valid():
            if edit_obj and not getattr(edit_obj, "ui_id", None):
                edit_obj.ui_id = encode_id(int(edit_obj.id))
            return render(
                request,
                "panels/aap_settings/mail_servers.html",
                {"state": state or "add", "form": form, "edit_obj": edit_obj, "items": items},
            )

        email = (form.cleaned_data["email"] or "").strip()
        domain = (email.split("@", 1)[1] if "@" in email else "").strip().lower()
        limit_hour_sent = int(form.cleaned_data.get("limit_hour_sent") or 50)

        if edit_obj:
            mb = edit_obj
            mb.email = email
            mb.domain = domain
            mb.limit_hour_sent = limit_hour_sent
            mb.save(update_fields=["email", "domain", "limit_hour_sent", "updated_at"])
        else:
            mb = Mailbox.objects.create(
                workspace_id=ws_id,
                email=email,
                domain=domain,
                limit_hour_sent=limit_hour_sent,
            )

        cm_now = _conn_map(mb)
        smtp_now = cm_now.get("smtp")
        imap_now = cm_now.get("imap")

        smtp_extra = {}
        fn = (form.cleaned_data.get("from_name") or "").strip()
        smtp_extra["from_name"] = fn
        smtp_extra["from_email"] = email

        smtp_secret_new = _norm_secret_from_cleaned(form.cleaned_data.get("smtp_secret") or "")
        if smtp_secret_new:
            smtp_secret_enc_to_store = encrypt_secret(smtp_secret_new)
        else:
            smtp_secret_enc_to_store = (smtp_now.secret_enc or "").strip() if smtp_now else ""

        MailboxConnection.objects.update_or_create(
            mailbox_id=int(mb.id),
            kind="smtp",
            defaults={
                "host": (form.cleaned_data["smtp_host"] or "").strip(),
                "port": int(form.cleaned_data["smtp_port"]),
                "security": form.cleaned_data["smtp_security"],
                "auth_type": form.cleaned_data["smtp_auth_type"],
                "username": (form.cleaned_data["smtp_username"] or "").strip(),
                "secret_enc": smtp_secret_enc_to_store,
                "extra_json": smtp_extra,
            },
        )

        if _imap_any(post):
            imap_secret_new = _norm_secret_from_cleaned(form.cleaned_data.get("imap_secret") or "")
            if imap_secret_new:
                imap_secret_enc_to_store = encrypt_secret(imap_secret_new)
            else:
                imap_secret_enc_to_store = (imap_now.secret_enc or "").strip() if imap_now else ""

            MailboxConnection.objects.update_or_create(
                mailbox_id=int(mb.id),
                kind="imap",
                defaults={
                    "host": (form.cleaned_data.get("imap_host") or "").strip(),
                    "port": int(form.cleaned_data.get("imap_port") or 0)
                    if str(form.cleaned_data.get("imap_port") or "").strip()
                    else 0,
                    "security": (form.cleaned_data.get("imap_security") or "none"),
                    "auth_type": (form.cleaned_data.get("imap_auth_type") or "login"),
                    "username": (form.cleaned_data.get("imap_username") or "").strip(),
                    "secret_enc": imap_secret_enc_to_store,
                    "extra_json": {},
                },
            )
        else:
            MailboxConnection.objects.filter(mailbox_id=int(mb.id), kind="imap").delete()

        if edit_obj:
            return redirect(f"{request.path}?state=edit&id={encode_id(int(mb.id))}")
        return redirect(request.path)

    form = MailServerForm(
        initial=init,
        preset_choices=preset_choices,
        require_smtp_secret=not bool(edit_obj),
        smtp_masked=smtp_has_secret,
        imap_masked=imap_has_secret,
    )
    return render(
        request,
        "panels/aap_settings/mail_servers.html",
        {"state": state, "form": form, "edit_obj": edit_obj, "items": items},
    )
