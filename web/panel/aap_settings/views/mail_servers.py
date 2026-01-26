# FILE: web/panel/aap_settings/views/mail_servers.py
# DATE: 2026-01-26
# PURPOSE: Settings → Mail servers: mailbox list/add/edit/delete + status blocks for last checks.
# CHANGE:
# - SMTP now shows TWO checks: SMTP_AUTH_CHECK + SMTP_SEND_CHECK.
# - Status rendering data is split into 3 fields (dt/action/status) instead of one string.
# - Template contract fields updated: domain_tech/domain_rep/smtp_auth/smtp_send/imap_check.

from __future__ import annotations

from zoneinfo import ZoneInfo

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


def _fmt_dt(dt) -> str:
    try:
        return dt.astimezone(ZoneInfo("Europe/Berlin")).strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return "—"


def mail_servers_view(request):
    """
    (1) Список mailbox + add/edit mailbox (только email) + delete mailbox.
    UX: ?state=add / ?state=edit&id=...
    + нижняя таблица: Domain/SMTP/IMAP со статусами последних проверок и кнопками.
    """
    from engine.common import db as engine_db

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

    ACTIONS = (
        "SMTP_AUTH_CHECK",
        "SMTP_SEND_CHECK",
        "IMAP_CHECK",
        "DOMAIN_CHECK_TECH",
        "DOMAIN_CHECK_REPUTATION",
    )

    status_map: dict[tuple[int, str], dict] = {}
    if mb_ids:
        rows = engine_db.fetch_all(
            """
            SELECT DISTINCT ON (mailbox_id, action)
                   mailbox_id, action, status, created_at
            FROM mailbox_events
            WHERE mailbox_id = ANY(%s)
              AND action = ANY(%s)
            ORDER BY mailbox_id, action, created_at DESC
            """,
            (mb_ids, list(ACTIONS)),
        ) or []
        for mailbox_id, action, status, created_at in rows:
            status_map[(int(mailbox_id), str(action))] = {
                "dt": _fmt_dt(created_at),
                "action": str(action),
                "status": str(status),
            }

    def _rec(mb_id: int, action: str) -> dict | None:
        return status_map.get((int(mb_id), action))

    def _apply_ui_fields(mb: Mailbox) -> None:
        mb.ui_id = encode_id(int(mb.id))

        mb.domain_name = _domain_from_mailbox(mb)
        mb.domain_whitelisted = is_domain_whitelisted(mb.domain_name)

        mb.domain_tech = _rec(int(mb.id), "DOMAIN_CHECK_TECH")
        mb.domain_rep = _rec(int(mb.id), "DOMAIN_CHECK_REPUTATION")
        mb.domain_tested = bool(mb.domain_tech or mb.domain_rep)

        mb.smtp_configured = int(mb.id) in smtp_ids
        mb.smtp_auth = _rec(int(mb.id), "SMTP_AUTH_CHECK")
        mb.smtp_send = _rec(int(mb.id), "SMTP_SEND_CHECK")
        mb.smtp_tested = bool(mb.smtp_auth or mb.smtp_send)

        mb.imap_configured = int(mb.id) in imap_ids
        mb.imap_check = _rec(int(mb.id), "IMAP_CHECK")
        mb.imap_tested = bool(mb.imap_check)

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

        # domain checks должны ездить только через AJAX API — тут ничего не выполняем
        if action == "test_domain":
            return redirect(reverse("settings:mail_servers"))

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
