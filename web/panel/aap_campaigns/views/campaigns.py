# FILE: web/panel/aap_campaigns/views/campaigns.py
# DATE: 2026-01-19
# PURPOSE: /panel/campaigns/campaigns/ — одна страница: list/add/edit/letter через state в GET.
# CHANGE: (new) CRUD кампаний + редактирование письма (TinyMCE + CodeMirror) + ready_content.

from __future__ import annotations

import json
from typing import Optional, Union
from uuid import UUID

from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from engine.common.email_template import render_html, sanitize
from mailer_web.access import decode_id, encode_id, resolve_pk_or_redirect
from panel.aap_campaigns.models import Campaign, Letter, Templates
from panel.aap_lists.models import MailingList
from panel.aap_settings.models import Mailbox, SendingSettings


def _guard(request) -> tuple[Optional[UUID], Optional[object]]:
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None, None
    return ws_id, user


def _qs(ws_id: UUID):
    return Campaign.objects.filter(workspace_id=ws_id).order_by("-updated_at")


def _with_ui_ids(items):
    for it in items:
        it.ui_id = encode_id(int(it.id))
    return items


def _get_state(request) -> str:
    st = (request.GET.get("state") or "").strip()
    return st if st in ("add", "edit", "letter") else ""


def _get_edit_obj(request, ws_id: UUID) -> Union[None, Campaign, HttpResponseRedirect]:
    st = _get_state(request)
    if st not in ("edit", "letter"):
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


def campaigns_view(request):
    ws_id, _user = _guard(request)
    if not ws_id:
        return redirect("/")

    state = _get_state(request)
    edit_obj = _get_edit_obj(request, ws_id)
    if isinstance(edit_obj, HttpResponseRedirect):
        return edit_obj

    # selects
    list_items = MailingList.objects.filter(workspace_id=ws_id, archived=False).order_by("-created_at")
    mb_items = Mailbox.objects.filter(workspace_id=ws_id, is_active=True).order_by("name")
    tpl_items = Templates.objects.filter(workspace_id=ws_id, is_active=True).order_by("order", "template_name")

    for it in list_items:
        it.ui_id = encode_id(int(it.id))
    for it in mb_items:
        it.ui_id = encode_id(int(it.id))
    for it in tpl_items:
        it.ui_id = encode_id(int(it.id))

    parent_items = Campaign.objects.filter(workspace_id=ws_id).order_by("-updated_at")
    parent_items = _with_ui_ids(parent_items)

    # global window for UI
    ss, _created = SendingSettings.objects.get_or_create(
        workspace_id=ws_id,
        defaults={"value_json": {}},
    )
    global_window_json = ss.value_json or {}

    letter_obj = None
    if state == "letter" and edit_obj:
        letter_obj = _ensure_letter(ws_id, edit_obj)
        letter_obj.ui_id = encode_id(int(letter_obj.id))

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "close":
            return redirect(request.path)

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

        # --- add/save campaign ---
        if action in ("add_campaign", "save_campaign"):
            title = (request.POST.get("title") or "").strip()
            mailing_list_ui = (request.POST.get("mailing_list") or "").strip()
            mailbox_ui = (request.POST.get("mailbox") or "").strip()
            template_ui = (request.POST.get("template") or "").strip()

            campaign_parent_ui = (request.POST.get("campaign_parent") or "").strip()
            send_after_parent_days_raw = (request.POST.get("send_after_parent_days") or "").strip()

            start_at = (request.POST.get("start_at") or "").strip()
            end_at = (request.POST.get("end_at") or "").strip()
            active = bool(request.POST.get("active"))

            use_global_window = bool(request.POST.get("use_global_window"))
            window_raw = (request.POST.get("window") or "").strip()

            if not (title and mailing_list_ui and mailbox_ui and start_at):
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

            parent_pk = None
            if campaign_parent_ui:
                try:
                    parent_pk = int(decode_id(campaign_parent_ui))
                except Exception:
                    parent_pk = None

            try:
                send_after_days = int(send_after_parent_days_raw) if send_after_parent_days_raw else 30
            except Exception:
                send_after_days = 30
            if send_after_days < 0:
                send_after_days = 0

            window_obj = {}
            if not use_global_window:
                try:
                    parsed = json.loads(window_raw) if window_raw else {}
                    window_obj = parsed if isinstance(parsed, dict) else {}
                except Exception:
                    window_obj = {}

            if action == "add_campaign":
                camp = Campaign.objects.create(
                    workspace_id=ws_id,
                    title=title,
                    mailing_list_id=mailing_list_pk,
                    mailbox_id=mailbox_pk,
                    campaign_parent_id=parent_pk,
                    send_after_parent_days=send_after_days,
                    start_at=start_at,
                    end_at=end_at or None,
                    active=active,
                    window=window_obj,
                )
                let = _ensure_letter(ws_id, camp)
                let.template_id = template_pk
                let.save(update_fields=["template", "updated_at"])
                return redirect(f"{request.path}?state=letter&id={encode_id(int(camp.id))}")

            # save_campaign
            if not edit_obj:
                return redirect(request.path)

            edit_obj.title = title
            edit_obj.mailing_list_id = mailing_list_pk
            edit_obj.mailbox_id = mailbox_pk
            edit_obj.campaign_parent_id = parent_pk
            edit_obj.send_after_parent_days = send_after_days
            edit_obj.start_at = start_at
            edit_obj.end_at = end_at or None
            edit_obj.active = active
            edit_obj.window = window_obj
            edit_obj.save(
                update_fields=[
                    "title",
                    "mailing_list",
                    "mailbox",
                    "campaign_parent",
                    "send_after_parent_days",
                    "start_at",
                    "end_at",
                    "active",
                    "window",
                    "updated_at",
                ]
            )

            let = _ensure_letter(ws_id, edit_obj)
            let.template_id = template_pk
            let.save(update_fields=["template", "updated_at"])

            return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_obj.id))}")

        # --- save letter / save ready ---
        if action in ("save_letter", "save_ready"):
            if not edit_obj:
                return redirect(request.path)

            let = _ensure_letter(ws_id, edit_obj)

            editor_html = request.POST.get("editor_html") or ""
            subjects_json = request.POST.get("subjects_json") or "[]"

            try:
                subs = json.loads(subjects_json)
                subs = [str(x).strip() for x in (subs or []) if str(x).strip()] if isinstance(subs, list) else []
            except Exception:
                subs = []

            let.html_content = sanitize(editor_html or "")
            let.subjects = subs
            let.save(update_fields=["html_content", "subjects", "updated_at"])

            if action == "save_ready":
                tpl = let.template if let.template_id else None
                if tpl:
                    ready = render_html(
                        template_html=tpl.template_html or "",
                        content_html=sanitize(let.html_content or ""),
                        styles=_styles_pick_main(tpl.styles or {}),
                        vars_json={},
                    )
                    let.ready_content = ready or ""
                    let.save(update_fields=["ready_content", "updated_at"])

            return redirect(f"{request.path}?state=letter&id={encode_id(int(edit_obj.id))}")

        return redirect(request.path)

    items = _with_ui_ids(_qs(ws_id))

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
    }
    return render(request, "panels/aap_campaigns/campaigns.html", ctx)
