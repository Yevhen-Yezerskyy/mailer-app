# FILE: web/panel/aap_settings/views/mail_servers.py
# DATE: 2026-01-24
# PURPOSE: Settings → Mail servers: mailbox list/add/edit/delete (legacy state UX) + domain test.
# CHANGE:
# - Шаблон: mail_servers.html (вместо mail_servers_list.html)
# - Роуты/redirect/reverse: имя страницы 'settings:mail_servers' (а не mail_servers_list)
# - SMTP/IMAP “настроен/не настроен” по наличию SmtpMailbox/ImapMailbox.
# - Статусы проверок берём из mailbox_events (SMTP_CHECK/IMAP_CHECK/DOMAIN_*).

from __future__ import annotations

from django.shortcuts import redirect, render
from django.urls import reverse

from engine.common.mail.domain_whitelist import is_domain_whitelisted
from mailer_web.access import decode_id, encode_id
from panel.aap_settings.forms import MailboxAddForm
from panel.aap_settings.models import ImapMailbox, Mailbox, SmtpMailbox


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


def _domain_from_mailbox(mb: Mailbox) -> str:
    d = (getattr(mb, "domain", "") or "").strip().lower()
    if d:
        return d
    em = (mb.email or "").strip().lower()
    if "@" in em:
        return em.split("@", 1)[1].strip().lower()
    return ""


def mail_servers_view(request):
    """
    (1) Список mailbox + add/edit mailbox (только email) + delete mailbox.
    UX: ?state=add / ?state=edit&id=...
    + нижняя таблица: Domain/SMTP/IMAP со статусами последних проверок и кнопками.
    """
    from engine.common import db as engine_db
    from engine.common.mail.domain_checks_test import domain_reputation_check_and_log, domain_tech_check_and_log

    ws_id = _guard(request)
    if not ws_id:
        return redirect("/")

    state = (request.GET.get("state") or "").strip().lower()
    if state not in ("add", "edit"):
        state = ""

    items = list(Mailbox.objects.filter(workspace_id=ws_id).order_by("email"))
    mb_ids = [int(m.id) for m in items]

    smtp_ids = set(
        SmtpMailbox.objects.filter(mailbox_id__in=mb_ids)
        .values_list("mailbox_id", flat=True)
        .distinct()
    )
    imap_ids = set(
        ImapMailbox.objects.filter(mailbox_id__in=mb_ids)
        .values_list("mailbox_id", flat=True)
        .distinct()
    )

    status_map: dict[tuple[int, str], str] = {}
    if mb_ids:
        rows = engine_db.fetch_all(
            """
            SELECT DISTINCT ON (mailbox_id, action)
                   mailbox_id, action, status
            FROM mailbox_events
            WHERE mailbox_id = ANY(%s)
              AND action IN ('SMTP_CHECK','IMAP_CHECK','DOMAIN_TECH_CHECK','DOMAIN_REPUTATION_CHECK')
            ORDER BY mailbox_id, action, id DESC
            """,
            (mb_ids,),
        ) or []
        for mailbox_id, action, status in rows:
            status_map[(int(mailbox_id), str(action))] = str(status)

    def _apply_ui_fields(mb: Mailbox) -> None:
        mb.ui_id = encode_id(int(mb.id))

        mb.domain_name = _domain_from_mailbox(mb)
        mb.domain_whitelisted = is_domain_whitelisted(mb.domain_name)

        mb.domain_tech_status = status_map.get((int(mb.id), "DOMAIN_TECH_CHECK"))
        mb.domain_rep_status = status_map.get((int(mb.id), "DOMAIN_REPUTATION_CHECK"))
        mb.domain_tested = bool(mb.domain_tech_status or mb.domain_rep_status)

        mb.smtp_configured = int(mb.id) in smtp_ids
        mb.smtp_status = status_map.get((int(mb.id), "SMTP_CHECK"))

        mb.imap_configured = int(mb.id) in imap_ids
        mb.imap_status = status_map.get((int(mb.id), "IMAP_CHECK"))

    for it in items:
        _apply_ui_fields(it)

    edit_obj = None
    if state == "edit":
        token = (request.GET.get("id") or "").strip()
        try:
            mailbox_id = int(decode_id(token))
        except Exception:
            return redirect(reverse("settings:mail_servers"))

        edit_obj = Mailbox.objects.filter(id=int(mailbox_id), workspace_id=ws_id).first()
        if not edit_obj:
            return redirect(reverse("settings:mail_servers"))

        _apply_ui_fields(edit_obj)

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "close":
            return redirect(reverse("settings:mail_servers"))

        if action == "delete":
            token = (request.POST.get("id") or "").strip()
            try:
                mailbox_id = int(decode_id(token))
            except Exception:
                return redirect(reverse("settings:mail_servers"))

            Mailbox.objects.filter(id=int(mailbox_id), workspace_id=ws_id).delete()
            return redirect(reverse("settings:mail_servers"))

        if action == "test_domain":
            token = (request.POST.get("id") or "").strip()
            try:
                mailbox_id = int(decode_id(token))
            except Exception:
                return redirect(reverse("settings:mail_servers"))

            mb = Mailbox.objects.filter(id=int(mailbox_id), workspace_id=ws_id).first()
            if not mb:
                return redirect(reverse("settings:mail_servers"))

            if not is_domain_whitelisted(_domain_from_mailbox(mb)):
                domain_tech_check_and_log(int(mb.id))
                domain_reputation_check_and_log(int(mb.id))

            return redirect(reverse("settings:mail_servers"))

        # save (add/edit)
        mailbox_id = int(edit_obj.id) if (state == "edit" and edit_obj) else None
        form = MailboxAddForm(request.POST, workspace_id=ws_id, mailbox_id=mailbox_id)

        if not form.is_valid():
            return render(
                request,
                "panels/aap_settings/mail_servers.html",
                {
                    "state": state or "add",
                    "form": form,
                    "items": items,
                    "edit_obj": edit_obj,
                },
            )

        email = (form.cleaned_data["email"] or "").strip().lower()
        domain = (email.split("@", 1)[1] if "@" in email else "").strip().lower()

        if mailbox_id is not None:
            Mailbox.objects.filter(id=int(mailbox_id), workspace_id=ws_id).update(email=email, domain=domain)
            tok = encode_id(int(mailbox_id))
            return redirect(reverse("settings:mail_servers") + f"?state=edit&id={tok}")

        mb = Mailbox.objects.create(workspace_id=ws_id, email=email, domain=domain)
        return redirect(reverse("settings:mail_servers") + f"?state=edit&id={encode_id(int(mb.id))}")

    if state == "edit" and edit_obj:
        form = MailboxAddForm(initial={"email": edit_obj.email}, workspace_id=ws_id, mailbox_id=int(edit_obj.id))
    else:
        form = MailboxAddForm(initial={"email": ""}, workspace_id=ws_id)

    return render(
        request,
        "panels/aap_settings/mail_servers.html",
        {
            "state": state,
            "form": form,
            "items": items,
            "edit_obj": edit_obj,
        },
    )
