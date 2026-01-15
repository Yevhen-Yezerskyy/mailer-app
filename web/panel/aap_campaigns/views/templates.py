# FILE: web/panel/aap_campaigns/views/templates.py  (обновлено — 2026-01-14)
# PURPOSE: /panel/campaigns/templates/ — CRUD шаблонов писем (user + advanced mode).
# CHANGE: Сервер всегда берёт editor_html/css_text (hidden), независимо от режима; пайплайн сохранения одинаковый.

from __future__ import annotations

from typing import Optional, Union
from uuid import UUID

from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from mailer_web.access import encode_id, resolve_pk_or_redirect
from panel.aap_campaigns.models import Templates
from engine.common.email_template import (
    sanitize,
    styles_css_to_json,
    editor_template_parse_html,
)


def _guard(request) -> tuple[Optional[UUID], Optional[object]]:
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None, None
    return ws_id, user


def _qs(ws_id: UUID):
    return Templates.objects.filter(workspace_id=ws_id).order_by("-updated_at")


def _with_ui_ids(items):
    for it in items:
        it.ui_id = encode_id(int(it.id))
    return items


def _get_state(request) -> str:
    st = (request.GET.get("state") or "").strip()
    return st if st in ("add", "edit") else ""


def _get_edit_obj(request, ws_id: UUID) -> Union[None, Templates, HttpResponseRedirect]:
    if _get_state(request) != "edit":
        return None
    if not request.GET.get("id"):
        return None

    res = resolve_pk_or_redirect(request, Templates, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res

    return Templates.objects.filter(id=int(res), workspace_id=ws_id).first()


def templates_view(request):
    ws_id, user = _guard(request)
    if not ws_id:
        return redirect("/")

    state = _get_state(request)
    edit_obj = _get_edit_obj(request, ws_id) if state == "edit" else None
    if isinstance(edit_obj, HttpResponseRedirect):
        return edit_obj

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

            res = resolve_pk_or_redirect(request, Templates, param="id")
            if isinstance(res, HttpResponseRedirect):
                return res

            Templates.objects.filter(id=int(res), workspace_id=ws_id).delete()
            return redirect(request.path)

        template_name = (request.POST.get("template_name") or "").strip()

        # ВАЖНО: независимо от режима — берём hidden (их заполняет JS)
        editor_html = request.POST.get("editor_html") or ""
        css_text = request.POST.get("css_text") or ""

        if not template_name:
            return redirect(request.path)

        clean_html = editor_template_parse_html(editor_html)
        clean_html = sanitize(clean_html)
        styles_obj = styles_css_to_json(css_text)

        if action == "add":
            obj = Templates.objects.create(
                workspace_id=ws_id,
                template_name=template_name,
                template_html=clean_html,
                styles=styles_obj,
            )
            return redirect(f"{request.path}?state=edit&id={encode_id(int(obj.id))}")

        if action == "save":
            post_id = (request.POST.get("id") or "").strip()
            if post_id:
                q = request.GET.copy()
                q["id"] = post_id
                request.GET = q

            res = resolve_pk_or_redirect(request, Templates, param="id")
            if isinstance(res, HttpResponseRedirect):
                return res

            obj = Templates.objects.filter(id=int(res), workspace_id=ws_id).first()
            if obj:
                obj.template_name = template_name
                obj.template_html = clean_html
                obj.styles = styles_obj
                obj.save(update_fields=["template_name", "template_html", "styles", "updated_at"])

            return redirect(f"{request.path}?state=edit&id={encode_id(int(obj.id))}")

        return redirect(request.path)

    items = _with_ui_ids(_qs(ws_id))
    return render(
        request,
        "panels/aap_campaigns/templates.html",
        {
            "items": items,
            "state": state,
            "edit_obj": edit_obj,
        },
    )
