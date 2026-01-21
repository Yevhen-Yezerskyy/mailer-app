# FILE: web/panel/aap_campaigns/views/campaigns.py
# DATE: 2026-01-21
# PURPOSE: Campaigns page: add/edit + letter-editor init ctx + bottom table.
# CHANGE:
# - Sender label everywhere:  "Имя отправителя" <мыло>
#   берём из MailboxConnection(kind=SMTP).extra_json: from_name/from_email
#   fallback: from_name -> Mailbox.name, from_email -> Mailbox.email
# - tpl select: только Templates.archived=False; если выбран archived/нет — опция "Шаблон удален"
# - bottom table: it.letter_tpl_label через gettext; it.sender_label подготовлен в python
# - FIX: не затираем gettext "_" (get_or_create created)

from __future__ import annotations

import json
from datetime import date, timedelta
from types import SimpleNamespace
from typing import Optional, Union
from uuid import UUID

from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render
from django.utils.translation import gettext as _

from engine.common.email_template import render_html, sanitize
from mailer_web.access import decode_id, encode_id, resolve_pk_or_redirect
from panel.aap_campaigns.models import Campaign, Letter, Templates
from panel.aap_campaigns.template_editor import (
    find_demo_content_from_template,
    letter_editor_extract_content,
    letter_editor_render_html,
    styles_json_to_css,
)
from panel.aap_lists.models import MailingList
from panel.aap_settings.models import ConnKind, Mailbox, MailboxConnection, SendingSettings


def _guard(request) -> tuple[Optional[UUID], Optional[object]]:
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None, None
    return ws_id, user


def _qs(ws_id: UUID):
    return (
        Campaign.objects.filter(workspace_id=ws_id)
        .select_related("mailing_list", "mailbox", "letter", "letter__template")
        .order_by("-updated_at")
    )


def _with_ui_ids(items):
    for it in items:
        it.ui_id = encode_id(int(it.id))
    return items


def _get_state(request) -> str:
    st = (request.GET.get("state") or "").strip()
    return st if st in ("add", "edit", "letter") else ""


def _get_edit_obj(request, ws_id: UUID) -> Union[None, Campaign, HttpResponseRedirect]:
    state = _get_state(request)
    if state not in ("edit", "letter"):
        return None
    if not request.GET.get("id"):
        return None

    res = resolve_pk_or_redirect(request, Campaign, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res

    return Campaign.objects.filter(id=int(res), workspace_id=ws_id).first()


def _ensure_letter(ws_id: UUID, camp: Campaign) -> Letter:
    obj = Letter.objects.filter(workspace_id=ws_id, campaign=camp).first()
    if obj:
        return obj
    return Letter.objects.create(workspace_id=ws_id, campaign=camp)


def _styles_pick_main(styles_obj):
    if not isinstance(styles_obj, dict):
        return {}
    main = styles_obj.get("main")
    return main if isinstance(main, dict) else styles_obj


def _parse_date_from_post(request, prefix: str) -> Optional[date]:
    try:
        dd = int(request.POST.get(f"{prefix}_dd"))
        mm = int(request.POST.get(f"{prefix}_mm"))
        yy = int(request.POST.get(f"{prefix}_yy"))
        return date(yy, mm, dd)
    except Exception:
        return None


def _build_sender_labels(mailboxes: list[Mailbox]) -> dict[int, str]:
    """
    label = "from_name" <from_email>
    from extra_json of latest SMTP connection per mailbox
    """
    if not mailboxes:
        return {}

    mb_by_id = {int(m.id): m for m in mailboxes}
    mb_ids = list(mb_by_id.keys())

    smtp_conns = (
        MailboxConnection.objects.filter(mailbox_id__in=mb_ids, kind=ConnKind.SMTP)
        .only("id", "mailbox_id", "extra_json")
        .order_by("mailbox_id", "-id")
    )

    latest_smtp_by_mb: dict[int, MailboxConnection] = {}
    for c in smtp_conns:
        mid = int(c.mailbox_id)
        if mid not in latest_smtp_by_mb:
            latest_smtp_by_mb[mid] = c

    out: dict[int, str] = {}
    for mid, mb in mb_by_id.items():
        c = latest_smtp_by_mb.get(mid)
        extra = c.extra_json if (c and isinstance(c.extra_json, dict)) else {}
        from_name = (extra.get("from_name") or "").strip() or (mb.name or "").strip() or "—"
        from_email = (extra.get("from_email") or "").strip() or (mb.email or "").strip() or "—"
        out[mid] = f'{from_name} <{from_email}>'
    return out


def campaigns_view(request):
    ws_id, _user = _guard(request)
    if not ws_id:
        return redirect("/")

    state = _get_state(request)

    edit_obj = _get_edit_obj(request, ws_id)
    if isinstance(edit_obj, HttpResponseRedirect):
        return edit_obj

    if edit_obj:
        edit_obj.ui_id = encode_id(int(edit_obj.id))
        edit_obj.letter = (
            Letter.objects.filter(workspace_id=ws_id, campaign=edit_obj)
            .select_related("template")
            .first()
        )

    list_items = MailingList.objects.filter(workspace_id=ws_id, archived=False).order_by("-created_at")
    mb_items = Mailbox.objects.filter(workspace_id=ws_id, is_active=True).order_by("name")

    # templates in select: только не archived
    tpl_items = Templates.objects.filter(
        workspace_id=ws_id,
        is_active=True,
        archived=False,
    ).order_by("order", "template_name")

    for it in list_items:
        it.ui_id = encode_id(int(it.id))

    # sender labels map (для селекта и для таблицы кампаний)
    sender_label_by_mb_id = _build_sender_labels(list(mb_items))

    for it in mb_items:
        it.ui_id = encode_id(int(it.id))
        it.sender_label = sender_label_by_mb_id.get(int(it.id), f"{it.name} <{it.email}>")

    for it in tpl_items:
        it.ui_id = encode_id(int(it.id))

    # edit: если выбран archived/отсутствующий шаблон — добавляем спец-опцию "Шаблон удален"
    deleted_tpl_ui = None
    deleted_tpl_id = None
    if state in ("edit", "add") and edit_obj and getattr(edit_obj, "letter", None) and edit_obj.letter:
        t_id = int(edit_obj.letter.template_id) if edit_obj.letter.template_id else None
        if t_id:
            t = edit_obj.letter.template
            if (not t) or getattr(t, "archived", False):
                deleted_tpl_id = int(t_id)
                deleted_tpl_ui = encode_id(int(t_id))
                tpl_items = list(tpl_items)
                tpl_items.insert(
                    0,
                    SimpleNamespace(
                        id=int(t_id),
                        ui_id=deleted_tpl_ui,
                        template_name=_("Шаблон удален"),
                        is_deleted_option=True,
                    ),
                )

    parent_items = _with_ui_ids(Campaign.objects.filter(workspace_id=ws_id))

    ss, created = SendingSettings.objects.get_or_create(
        workspace_id=ws_id,
        defaults={"value_json": {}},
    )
    global_window_json = ss.value_json or {}

    # ----- letter editor init ctx -----
    letter_obj = None
    letter_init_html = ""
    letter_init_css = ""
    letter_init_subjects = "[]"
    letter_init_headers = "{}"
    letter_template_html = ""

    if state == "letter" and edit_obj:
        letter_obj = _ensure_letter(ws_id, edit_obj)
        letter_obj.ui_id = encode_id(int(letter_obj.id))

        tpl = letter_obj.template if letter_obj.template_id else None
        if tpl:
            letter_template_html = tpl.template_html or ""

            content_html = (letter_obj.html_content or "").strip()
            if not content_html:
                content_html = find_demo_content_from_template(letter_template_html)

            letter_init_html = letter_editor_render_html(letter_template_html, content_html)

            styles_obj = _styles_pick_main(tpl.styles or {})
            letter_init_css = styles_json_to_css(styles_obj) or ""

        try:
            letter_init_subjects = json.dumps(letter_obj.subjects or [], ensure_ascii=False)
        except Exception:
            letter_init_subjects = "[]"

        try:
            letter_init_headers = json.dumps(letter_obj.headers or {}, ensure_ascii=False, indent=2)
        except Exception:
            letter_init_headers = "{}"

    # ---------------- POST ----------------
    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "close":
            return redirect(request.path)

        if action in ("activate", "pause"):
            post_id = (request.POST.get("id") or "").strip()
            if not post_id:
                return redirect(request.get_full_path())

            try:
                pk = int(decode_id(post_id))
            except Exception:
                return redirect(request.get_full_path())

            camp = Campaign.objects.filter(id=pk, workspace_id=ws_id).first()
            if camp:
                camp.active = (action == "activate")
                camp.save(update_fields=["active", "updated_at"])

            return redirect(request.get_full_path())

        if action == "delete":
            post_id = (request.POST.get("id") or "").strip()
            if post_id:
                q = request.GET.copy()
                q["id"] = post_id
                request.GET = q

            res = resolve_pk_or_redirect(request, Campaign, param="id")
            if isinstance(res, HttpResponseRedirect):
                return res

            Campaign.objects.filter(id=int(res), workspace_id=ws_id).delete()
            return redirect(request.path)

        if action in ("add_campaign", "save_campaign", "add_campaign_close", "save_campaign_close"):
            want_close = action in ("add_campaign_close", "save_campaign_close")

            title = (request.POST.get("title") or "").strip()
            mailing_list_ui = (request.POST.get("mailing_list") or "").strip()
            mailbox_ui = (request.POST.get("mailbox") or "").strip()
            template_ui = (request.POST.get("template") or "").strip()

            if not (title and mailing_list_ui and mailbox_ui):
                return redirect(request.path)

            try:
                mailing_list_pk = int(decode_id(mailing_list_ui))
                mailbox_pk = int(decode_id(mailbox_ui))
            except Exception:
                return redirect(request.path)

            template_pk = None
            if template_ui:
                try:
                    template_pk = int(decode_id(template_ui))
                except Exception:
                    template_pk = None

            start_at = _parse_date_from_post(request, "start") or date.today()
            end_at = _parse_date_from_post(request, "end") or (start_at + timedelta(days=90))

            send_after_days = 0
            if edit_obj and edit_obj.campaign_parent_id:
                try:
                    send_after_days = int(request.POST.get("send_after_parent_days") or 0)
                except Exception:
                    send_after_days = 0
                if send_after_days < 0:
                    send_after_days = 0

            use_global_window = True if request.POST.get("use_global_window") else False
            window_raw = (request.POST.get("window") or "").strip()
            window_obj = {}
            if not use_global_window:
                try:
                    parsed = json.loads(window_raw) if window_raw else {}
                    window_obj = parsed if isinstance(parsed, dict) else {}
                except Exception:
                    window_obj = {}

            if action in ("add_campaign", "add_campaign_close"):
                camp = Campaign.objects.create(
                    workspace_id=ws_id,
                    title=title,
                    mailing_list_id=mailing_list_pk,
                    mailbox_id=mailbox_pk,
                    start_at=start_at,
                    end_at=end_at,
                    window=window_obj,
                )
                let = _ensure_letter(ws_id, camp)
                let.template_id = template_pk
                let.save(update_fields=["template", "updated_at"])

                if want_close:
                    return redirect(request.path)
                return redirect(f"{request.path}?state=letter&id={encode_id(int(camp.id))}")

            if not edit_obj:
                return redirect(request.path)

            edit_obj.title = title
            edit_obj.mailing_list_id = mailing_list_pk
            edit_obj.mailbox_id = mailbox_pk
            edit_obj.start_at = start_at
            edit_obj.end_at = end_at
            edit_obj.send_after_parent_days = send_after_days
            edit_obj.window = window_obj
            edit_obj.save(
                update_fields=[
                    "title",
                    "mailing_list",
                    "mailbox",
                    "start_at",
                    "end_at",
                    "send_after_parent_days",
                    "window",
                    "updated_at",
                ]
            )

            let = _ensure_letter(ws_id, edit_obj)
            let.template_id = template_pk
            let.save(update_fields=["template", "updated_at"])

            if want_close:
                return redirect(request.path)
            return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_obj.id))}")

        if action in ("save_letter", "save_ready"):
            if not edit_obj:
                return redirect(request.path)

            let = _ensure_letter(ws_id, edit_obj)

            editor_mode = (request.POST.get("editor_mode") or "user").strip()
            editor_html = request.POST.get("editor_html") or ""
            subjects_json = request.POST.get("subjects_json") or "[]"
            headers_json = (request.POST.get("headers_json") or "").strip() or "{}"

            try:
                subs = json.loads(subjects_json)
                subs = [str(x).strip() for x in subs if str(x).strip()] if isinstance(subs, list) else []
            except Exception:
                subs = []

            try:
                hdrs = json.loads(headers_json) if headers_json else {}
                hdrs = hdrs if isinstance(hdrs, dict) else {}
            except Exception:
                hdrs = {}

            content_html = editor_html
            if editor_mode != "advanced":
                content_html = letter_editor_extract_content(editor_html or "")

            let.html_content = sanitize(content_html or "")
            let.subjects = subs
            let.headers = hdrs
            let.save(update_fields=["html_content", "subjects", "headers", "updated_at"])

            tpl = let.template if let.template_id else None
            if tpl:
                ready = render_html(
                    template_html=tpl.template_html or "",
                    content_html=sanitize(let.html_content or ""),
                    styles=_styles_pick_main(tpl.styles or {}),
                    vars_json=None,
                )
                let.ready_content = ready or ""
                let.save(update_fields=["ready_content", "updated_at"])

            return redirect(f"{request.path}?state=letter&id={encode_id(int(edit_obj.id))}")

        return redirect(request.path)

    # ---------------- GET ----------------
    items = _with_ui_ids(_qs(ws_id))
    for it in items:
        # sender label for bottom table
        it.sender_label = sender_label_by_mb_id.get(int(it.mailbox_id), f"{it.mailbox.name} <{it.mailbox.email}>")

        # template label for bottom table
        tpl = None
        if getattr(it, "letter", None):
            tpl = it.letter.template if it.letter and it.letter.template_id else None

        if tpl and not getattr(tpl, "archived", False):
            it.letter_tpl_label = tpl.template_name
        else:
            it.letter_tpl_label = _("Шаблон удален")

    edit_window_json_str = ""
    if edit_obj and isinstance(edit_obj.window, dict):
        edit_window_json_str = json.dumps(edit_obj.window or {}, ensure_ascii=False)

    ctx = {
        "items": items,
        "state": state,
        "edit_obj": edit_obj,
        "letter_obj": letter_obj,
        "list_items": list_items,
        "mb_items": mb_items,
        "tpl_items": tpl_items,
        "parent_items": parent_items,
        "global_window_json_str": json.dumps(global_window_json or {}, ensure_ascii=False),
        "edit_window_json_str": edit_window_json_str,
        "deleted_tpl_ui": deleted_tpl_ui,
        "deleted_tpl_id": deleted_tpl_id,
        # letter init
        "letter_init_html": letter_init_html,
        "letter_init_css": letter_init_css,
        "letter_init_subjects": letter_init_subjects,
        "letter_init_headers": letter_init_headers,
        "letter_template_html": letter_template_html,
    }
    return render(request, "panels/aap_campaigns/campaigns.html", ctx)
