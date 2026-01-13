# FILE: web/panel/aap_settings/views/mail_servers.py
# DATE: 2026-01-13
# PURPOSE: /panel/settings/mail-servers/ — список mailboxes + add/edit form + apply preset (без проверок пока).

from __future__ import annotations

from django.shortcuts import redirect, render
from django.http import HttpResponseRedirect

from mailer_web.access import encode_id, resolve_pk_or_redirect
from panel.aap_settings.forms import MailServerForm
from panel.aap_settings.models import Mailbox, MailboxConnection, ProviderPreset


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


def _preset_choices():
    qs = (
        ProviderPreset.objects
        .filter(is_active=True)
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


def _apply_preset_to_post(post, code: str):
    """
    Apply preset values into POST-like dict (in-place).
    Preset entries are per-kind (smtp/imap).
    """
    smtp = ProviderPreset.objects.filter(code=code, kind="smtp", is_active=True).first()
    imap = ProviderPreset.objects.filter(code=code, kind="imap", is_active=True).first()

    if smtp:
        post["smtp_host"] = smtp.host or post.get("smtp_host", "")
        try:
            ports = smtp.ports_json or []
        except Exception:
            ports = []
        if ports and not post.get("smtp_port"):
            post["smtp_port"] = str(int(ports[0]))
        post["smtp_security"] = smtp.security or post.get("smtp_security", "")
        post["smtp_auth_type"] = smtp.auth_type or post.get("smtp_auth_type", "")

    if imap:
        post["has_imap"] = "on"
        post["imap_host"] = imap.host or post.get("imap_host", "")
        try:
            ports = imap.ports_json or []
        except Exception:
            ports = []
        if ports and not post.get("imap_port"):
            post["imap_port"] = str(int(ports[0]))
        post["imap_security"] = imap.security or post.get("imap_security", "")
        post["imap_auth_type"] = imap.auth_type or post.get("imap_auth_type", "")

    return post


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

    # list for bottom table
    items = list(Mailbox.objects.filter(workspace_id=ws_id).order_by("name"))
    for it in items:
        it.ui_id = encode_id(int(it.id))

    # init form (GET)
    init = {
        "name": "",
        "email": "",
        "is_active": True,
        "preset_code": "",
        "has_imap": False,
    }

    if edit_obj:
        init["name"] = edit_obj.name or ""
        init["email"] = edit_obj.email or ""
        init["is_active"] = bool(edit_obj.is_active)

        cm = _conn_map(edit_obj)
        smtp = cm.get("smtp")
        imap = cm.get("imap")

        if smtp:
            init["smtp_host"] = smtp.host
            init["smtp_port"] = smtp.port
            init["smtp_security"] = smtp.security
            init["smtp_auth_type"] = smtp.auth_type
            init["smtp_username"] = smtp.username
            init["smtp_secret"] = ""  # не показываем
            ex = smtp.extra_json or {}
            init["from_email"] = (ex.get("from_email") or "").strip()
            init["from_name"] = (ex.get("from_name") or "").strip()

        if imap:
            init["has_imap"] = True
            init["imap_host"] = imap.host
            init["imap_port"] = imap.port
            init["imap_security"] = imap.security
            init["imap_auth_type"] = imap.auth_type
            init["imap_username"] = imap.username
            init["imap_secret"] = ""  # не показываем

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "close":
            return redirect(request.path)

        # delete
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

            form = MailServerForm(post, preset_choices=preset_choices)
            return render(
                request,
                "panels/aap_settings/mail_servers.html",
                {
                    "state": state or "add",
                    "form": form,
                    "edit_obj": edit_obj,
                    "items": items,
                },
            )

        # checks (placeholders)
        if action in ("check_domain", "check_smtp", "check_imap"):
            # TODO: call engine later
            return redirect(request.get_full_path())

        form = MailServerForm(post, preset_choices=preset_choices)
        if not form.is_valid():
            return render(
                request,
                "panels/aap_settings/mail_servers.html",
                {
                    "state": state or "add",
                    "form": form,
                    "edit_obj": edit_obj,
                    "items": items,
                },
            )

        name = (form.cleaned_data["name"] or "").strip()
        email = (form.cleaned_data["email"] or "").strip()
        domain = (email.split("@", 1)[1] if "@" in email else "").strip().lower()
        is_active = bool(form.cleaned_data.get("is_active"))

        # upsert mailbox
        if edit_obj:
            mb = edit_obj
            mb.name = name
            mb.email = email
            mb.domain = domain
            mb.is_active = is_active
            mb.save(update_fields=["name", "email", "domain", "is_active", "updated_at"])
        else:
            mb = Mailbox.objects.create(
                workspace_id=ws_id,
                name=name,
                email=email,
                domain=domain,
                is_active=is_active,
            )

        # SMTP upsert (required)
        smtp_extra = {}
        fe = (form.cleaned_data.get("from_email") or "").strip()
        fn = (form.cleaned_data.get("from_name") or "").strip()
        if fe:
            smtp_extra["from_email"] = fe
        if fn:
            smtp_extra["from_name"] = fn

        MailboxConnection.objects.update_or_create(
            mailbox_id=int(mb.id),
            kind="smtp",
            defaults={
                "host": (form.cleaned_data["smtp_host"] or "").strip(),
                "port": int(form.cleaned_data["smtp_port"]),
                "security": form.cleaned_data["smtp_security"],
                "auth_type": form.cleaned_data["smtp_auth_type"],
                "username": (form.cleaned_data["smtp_username"] or "").strip(),
                "secret_enc": form.cleaned_data["smtp_secret"],  # TODO: encrypt later
                "extra_json": smtp_extra,
            },
        )

        # IMAP optional: create/update or delete
        if form.cleaned_data.get("has_imap"):
            MailboxConnection.objects.update_or_create(
                mailbox_id=int(mb.id),
                kind="imap",
                defaults={
                    "host": (form.cleaned_data["imap_host"] or "").strip(),
                    "port": int(form.cleaned_data["imap_port"]),
                    "security": form.cleaned_data["imap_security"],
                    "auth_type": form.cleaned_data["imap_auth_type"],
                    "username": (form.cleaned_data["imap_username"] or "").strip(),
                    "secret_enc": form.cleaned_data["imap_secret"],  # TODO: encrypt later
                    "extra_json": {},
                },
            )
        else:
            MailboxConnection.objects.filter(mailbox_id=int(mb.id), kind="imap").delete()

        if edit_obj:
            return redirect(f"{request.path}?state=edit&id={encode_id(int(mb.id))}")
        return redirect(request.path)

    # GET
    form = MailServerForm(initial=init, preset_choices=preset_choices)

    return render(
        request,
        "panels/aap_settings/mail_servers.html",
        {
            "state": state,
            "form": form,
            "edit_obj": edit_obj,
            "items": items,
        },
    )
