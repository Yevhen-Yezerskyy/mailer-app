# FILE: web/panel/aap_campaigns/views/templates.py
# DATE: 2026-01-18
# PURPOSE: /panel/campaigns/templates/ — CRUD шаблонов писем (user + advanced mode).
# CHANGE:
#   - add-mode: используем gl_tpl из URL (?gl_tpl=<id>), иначе редиректим на случайный активный GlobalTemplate.
#   - в контекст прокидываем gl_tpl (id) + списки keys для colors/fonts (для кнопок, которые рендерит Python).

from __future__ import annotations

import re
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


def _get_gl_tpl_from_query(request) -> int | None:
    raw = (request.GET.get("gl_tpl") or "").strip()
    return int(raw) if raw.isdigit() else None


def _pick_random_active_gl_tpl_id() -> int | None:
    obj = GlobalTemplate.objects.filter(is_active=True).order_by("?").first()
    return int(obj.id) if obj else None


def _extract_gl_tpl_from_template_html(template_html: str) -> int | None:
    """
    id-<N> ищем в class первого HTML-тега.
    """
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


def _gl_tpl_keys(gl_tpl_id: int | None) -> tuple[list[str], list[str]]:
    if not gl_tpl_id:
        return [], []
    gt = GlobalTemplate.objects.filter(id=int(gl_tpl_id), is_active=True).first()
    if not gt or not isinstance(gt.styles, dict):
        return [], []
    colors = gt.styles.get("colors")
    fonts = gt.styles.get("fonts")
    colors_keys = sorted(list(colors.keys())) if isinstance(colors, dict) else []
    fonts_keys = sorted(list(fonts.keys())) if isinstance(fonts, dict) else []
    return colors_keys, fonts_keys


def templates_view(request):
    ws_id, user = _guard(request)
    if not ws_id:
        return redirect("/")

    state = _get_state(request)
    edit_obj = _get_edit_obj(request, ws_id) if state == "edit" else None
    if isinstance(edit_obj, HttpResponseRedirect):
        return edit_obj

    # --- ADD: обязателен gl_tpl в URL (иначе редирект на случайный активный) ---
    if request.method == "GET" and state == "add":
        gl_tpl = _get_gl_tpl_from_query(request)
        if not gl_tpl:
            rid = _pick_random_active_gl_tpl_id()
            if not rid:
                # нет глобальных шаблонов — просто в список
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

            Templates.objects.filter(id=int(res), workspace_id=ws_id).delete()
            return redirect(request.path)

        template_name = (request.POST.get("template_name") or "").strip()

        # независимо от режима — берём hidden (их заполняет JS)
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

    # --- контекст для кнопок (python-render) ---
    gl_tpl: int | None = None
    if state == "edit" and edit_obj:
        gl_tpl = _extract_gl_tpl_from_template_html(edit_obj.template_html or "")
    elif state == "add":
        gl_tpl = _get_gl_tpl_from_query(request)

    colors_keys, fonts_keys = _gl_tpl_keys(gl_tpl)

    items = _with_ui_ids(_qs(ws_id))
    return render(
        request,
        "panels/aap_campaigns/templates.html",
        {
            "items": items,
            "state": state,
            "edit_obj": edit_obj,
            "gl_tpl": gl_tpl,
            "gl_colors": colors_keys,
            "gl_fonts": fonts_keys,
        },
    )
