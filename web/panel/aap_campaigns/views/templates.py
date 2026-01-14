# FILE: web/panel/aap_campaigns/views/templates.py
# DATE: 2026-01-14
# PURPOSE: /panel/campaigns/templates/ — простая CRUD-страница шаблонов (add/edit/delete) для модели Templates.
# CHANGE: action=add теперь редиректит на чистый URL (как в aap_lists), без перехода в режим edit.

from __future__ import annotations

import json
from typing import Optional, Union
from uuid import UUID

from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from mailer_web.access import encode_id, resolve_pk_or_redirect
from panel.aap_campaigns.forms import TemplatesForm
from panel.aap_campaigns.models import Templates


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


def _get_edit_obj(request, ws_id: UUID) -> Union[None, Templates, HttpResponseRedirect]:
    if request.GET.get("state") != "edit":
        return None
    if not request.GET.get("id"):
        return None

    res = resolve_pk_or_redirect(request, Templates, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res

    return Templates.objects.filter(id=int(res), workspace_id=ws_id).first()


def _styles_to_text(styles) -> str:
    if not styles:
        return ""
    try:
        return json.dumps(styles, ensure_ascii=False, indent=2)
    except Exception:
        return ""


def templates_view(request):
    ws_id, user = _guard(request)
    if not ws_id:
        return redirect("/")

    edit_obj = _get_edit_obj(request, ws_id)
    if isinstance(edit_obj, HttpResponseRedirect):
        return edit_obj

    state = "edit" if edit_obj else ""

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "cancel":
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

        form = TemplatesForm(request.POST)
        if not form.is_valid():
            items = _with_ui_ids(_qs(ws_id))
            return render(
                request,
                "panels/aap_campaigns/templates.html",
                {
                    "items": items,
                    "state": state,
                    "form": form,
                    "edit_obj": edit_obj,
                },
            )

        data = form.to_model_fields()

        if action == "add":
            Templates.objects.create(workspace_id=ws_id, **data)
            return redirect(request.path)

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
            if obj is None:
                return redirect(request.path)

            obj.template_name = data["template_name"]
            obj.template_html = data["template_html"]
            obj.styles = data["styles"]
            obj.save(update_fields=["template_name", "template_html", "styles", "updated_at"])

            return redirect(f"{request.path}?state=edit&id={encode_id(int(obj.id))}")

        return redirect(request.path)

    # GET
    init = {"template_name": "", "template_html": "", "styles": ""}
    if edit_obj:
        init["template_name"] = edit_obj.template_name or ""
        init["template_html"] = edit_obj.template_html or ""
        init["styles"] = _styles_to_text(edit_obj.styles)

    form = TemplatesForm(initial=init)
    items = _with_ui_ids(_qs(ws_id))

    return render(
        request,
        "panels/aap_campaigns/templates.html",
        {
            "items": items,
            "state": state,
            "form": form,
            "edit_obj": edit_obj,
        },
    )
