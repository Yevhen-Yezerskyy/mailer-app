# FILE: web/panel/aap_campaigns/views/templates.py
# DATE: 2026-01-21
# PURPOSE: /panel/campaigns/templates/ — CRUD шаблонов писем (user + advanced mode).
# CHANGE:
#   - list/edit: показываем/редактируем только Templates.archived=False
#   - delete: вместо удаления — архивирование (archived=True, is_active=False)

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Optional, Union
from uuid import UUID

from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from engine.common.email_template import sanitize
from mailer_web.access import encode_id, resolve_pk_or_redirect
from panel.aap_campaigns.models import Templates
from panel.aap_campaigns.template_editor import editor_template_parse_html, styles_css_to_json
from panel.models import GlobalTemplate


def _guard(request) -> tuple[Optional[UUID], Optional[object]]:
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None, None
    return ws_id, user


def _qs(ws_id: UUID):
    return (
        Templates.objects.filter(workspace_id=ws_id, archived=False)
        .order_by("-updated_at")
    )


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

    return (
        Templates.objects
        .filter(id=int(res), workspace_id=ws_id, archived=False)
        .first()
    )


def _get_gl_tpl_from_query(request) -> int | None:
    raw = (request.GET.get("gl_tpl") or "").strip()
    return int(raw) if raw.isdigit() else None


def _pick_random_active_gl_tpl_id() -> int | None:
    obj = GlobalTemplate.objects.filter(is_active=True).order_by("?").first()
    return int(obj.id) if obj else None


def _extract_global_template_id_from_first_tag(template_html: str) -> int | None:
    s = (template_html or "").lstrip()
    if not s:
        return None

    m_tag = re.search(r"(?is)<\s*([a-zA-Z][a-zA-Z0-9:_-]*)([^>]*)>", s)
    if not m_tag:
        return None

    attrs = m_tag.group(2) or ""
    m_class = re.search(r"""(?is)\bclass\s*=\s*(?P<q>["'])(?P<v>.*?)(?P=q)""", attrs)
    if not m_class:
        return None

    class_value = (m_class.group("v") or "").strip()
    if not class_value:
        return None

    for token in class_value.split():
        if token.startswith("id-"):
            tail = token[3:]
            if tail.isdigit():
                return int(tail)
    return None


def _global_style_keys_by_gid(gid: int | None) -> tuple[int | None, list[str], list[str]]:
    if not gid:
        return None, [], []

    gt = GlobalTemplate.objects.filter(id=int(gid), is_active=True).first()
    if not gt or not isinstance(gt.styles, dict):
        return None, [], []

    colors = gt.styles.get("colors")
    fonts = gt.styles.get("fonts")

    c_keys = sorted([k for k in (colors or {}).keys() if isinstance(k, str)]) if isinstance(colors, dict) else []
    f_keys = sorted([k for k in (fonts or {}).keys() if isinstance(k, str)]) if isinstance(fonts, dict) else []
    return int(gt.id), c_keys, f_keys


def _build_global_tpl_items(current_gid: int | None):
    out = []
    qs = GlobalTemplate.objects.filter(is_active=True).order_by("order", "template_name")
    for gt in qs:
        out.append(
            SimpleNamespace(
                id=int(gt.id),
                template_name=gt.template_name,
                is_current=bool(current_gid and int(current_gid) == int(gt.id)),
            )
        )
    return out


def templates_view(request):
    ws_id, _user = _guard(request)
    if not ws_id:
        return redirect("/")

    state = _get_state(request)
    edit_obj = _get_edit_obj(request, ws_id) if state == "edit" else None
    if isinstance(edit_obj, HttpResponseRedirect):
        return edit_obj

    # ADD: если state=add и нет gl_tpl в URL — редирект на случайный активный GlobalTemplate
    if request.method == "GET" and state == "add":
        if not _get_gl_tpl_from_query(request):
            rid = _pick_random_active_gl_tpl_id()
            if not rid:
                return redirect(request.path)
            return redirect(f"{request.path}?state=add&gl_tpl={rid}")

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

            Templates.objects.filter(
                id=int(res),
                workspace_id=ws_id,
                archived=False,
            ).update(
                archived=True,
                is_active=False,
            )
            return redirect(request.path)

        template_name = (request.POST.get("template_name") or "").strip()
        editor_html = request.POST.get("editor_html") or ""
        css_text = request.POST.get("css_text") or ""

        if not template_name:
            return redirect(request.path)

        clean_html = sanitize(editor_template_parse_html(editor_html))
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

            obj = Templates.objects.filter(
                id=int(res),
                workspace_id=ws_id,
                archived=False,
            ).first()
            if obj:
                obj.template_name = template_name
                obj.template_html = clean_html
                obj.styles = styles_obj
                obj.save(update_fields=["template_name", "template_html", "styles", "updated_at"])
                return redirect(f"{request.path}?state=edit&id={encode_id(int(obj.id))}")

            return redirect(request.path)

        return redirect(request.path)

    # active GlobalTemplate: приоритет ?gl_tpl=..., иначе edit -> id-<N> из HTML
    current_gid = _get_gl_tpl_from_query(request)
    if not current_gid and state == "edit" and edit_obj:
        current_gid = _extract_global_template_id_from_first_tag(edit_obj.template_html or "")

    global_style_gid, global_colors, global_fonts = _global_style_keys_by_gid(current_gid)
    global_tpl_items = _build_global_tpl_items(current_gid)

    items = _with_ui_ids(_qs(ws_id))
    return render(
        request,
        "panels/aap_campaigns/templates.html",
        {
            "items": items,
            "state": state,
            "edit_obj": edit_obj,
            "global_style_gid": global_style_gid,
            "global_colors": global_colors,
            "global_fonts": global_fonts,
            "global_tpl_items": global_tpl_items,
        },
    )
