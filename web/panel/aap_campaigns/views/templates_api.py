# FILE: web/panel/aap_campaigns/views/templates_api.py
# DATE: 2026-01-18
# PURPOSE: API для TinyMCE/advanced switch + preview + overlays.
# CHANGE:
#   - Demo content: по id-<N> из class первого тега берём GlobalTemplate.html_content.
#   - Новый endpoint: templates/_global-style-css/?gid=<N>&type=colors|fonts&name=<KEY> -> CSS overlay (только блок).

from __future__ import annotations

import json
import re

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from engine.common.email_template import render_html
from mailer_web.access import decode_id
from panel.aap_campaigns.models import Templates
from panel.aap_campaigns.template_editor import (
    editor_template_parse_html,
    editor_template_render_html,
    styles_css_to_json,
    styles_json_to_css,
)
from panel.models import GlobalTemplate

_DEMO_FALLBACK_HTML = "<p>[DEMO CONTENT]</p>"


def _load_obj_by_ui_id(ui_id: str) -> Templates | None:
    ui_id = (ui_id or "").strip()
    if not ui_id:
        return None
    try:
        pk = int(decode_id(ui_id))
    except Exception:
        return None
    return Templates.objects.filter(id=pk).first()


def _extract_global_template_id_from_first_tag(template_html: str) -> int | None:
    s = (template_html or "").lstrip()
    if not s:
        return None

    m_tag = re.search(r"<\s*([a-zA-Z][a-zA-Z0-9:_-]*)([^>]*)>", s)
    if not m_tag:
        return None

    attrs = m_tag.group(2) or ""
    m_class = re.search(r"""\bclass\s*=\s*(?P<q>["'])(?P<v>.*?)(?P=q)""", attrs, flags=re.IGNORECASE | re.DOTALL)
    if not m_class:
        return None

    class_value = (m_class.group("v") or "").strip()
    if not class_value:
        return None

    for token in class_value.split():
        if token.startswith("id-"):
            tail = token[3:]
            if tail.isdigit():
                try:
                    return int(tail)
                except Exception:
                    return None
    return None


def _find_demo_content(template_html: str) -> str:
    pk = _extract_global_template_id_from_first_tag(template_html)
    if not pk:
        return _DEMO_FALLBACK_HTML

    obj = GlobalTemplate.objects.filter(id=pk).first()
    if not obj:
        return _DEMO_FALLBACK_HTML

    html = (obj.html_content or "").strip()
    return html or _DEMO_FALLBACK_HTML


def _build_demo_vars(template_html: str) -> dict:
    return {}


def _read_json(request: HttpRequest) -> dict:
    try:
        raw = (request.body or b"").decode("utf-8", "ignore")
        obj = json.loads(raw) if raw else {}
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


@require_GET
@csrf_exempt
def templates__render_user_html_view(request: HttpRequest) -> HttpResponse:
    obj = _load_obj_by_ui_id(request.GET.get("id") or "")
    if not obj:
        return HttpResponse("", content_type="text/html; charset=utf-8")

    demo_html = _find_demo_content(obj.template_html or "")
    html = editor_template_render_html(
        template_html=obj.template_html or "",
        content_html=demo_html,
    )
    return HttpResponse(html, content_type="text/html; charset=utf-8")


@require_GET
@csrf_exempt
def templates__render_user_css_view(request: HttpRequest) -> HttpResponse:
    obj = _load_obj_by_ui_id(request.GET.get("id") or "")
    if not obj:
        return HttpResponse("", content_type="text/plain; charset=utf-8")

    css = styles_json_to_css(obj.styles or {})
    return HttpResponse(css or "", content_type="text/plain; charset=utf-8")


@require_POST
@csrf_exempt
def templates__parse_editor_html_view(request: HttpRequest) -> JsonResponse:
    data = _read_json(request)
    editor_html = data.get("editor_html") or ""
    template_html = editor_template_parse_html(editor_html)
    return JsonResponse({"ok": True, "template_html": template_html})


@require_POST
@csrf_exempt
def templates__render_editor_html_view(request: HttpRequest) -> JsonResponse:
    data = _read_json(request)
    template_html = data.get("template_html") or ""
    demo_html = _find_demo_content(template_html)
    editor_html = editor_template_render_html(template_html=template_html, content_html=demo_html)
    return JsonResponse({"ok": True, "editor_html": editor_html})


@require_GET
@csrf_exempt
def templates__preview_modal_by_id_view(request: HttpRequest) -> HttpResponse:
    obj = _load_obj_by_ui_id(request.GET.get("id") or "")
    if not obj:
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    tpl = obj.template_html or ""
    content = _find_demo_content(tpl)
    vars_json = _build_demo_vars(tpl)

    email_html = render_html(
        template_html=tpl,
        content_html=content,
        styles=obj.styles or {},
        vars_json=vars_json,
    )

    return render(
        request,
        "panels/aap_campaigns/modal_preview.html",
        {"status": "done", "email_html": email_html or ""},
    )


@require_POST
@csrf_exempt
def templates__preview_modal_from_editor_view(request: HttpRequest) -> HttpResponse:
    data = _read_json(request)
    mode = (data.get("mode") or "").strip()

    css_text = data.get("css_text") or ""
    styles_obj = styles_css_to_json(css_text)

    if mode == "advanced":
        tpl = data.get("template_html") or ""
    else:
        editor_html = data.get("editor_html") or ""
        tpl = editor_template_parse_html(editor_html)

    content = _find_demo_content(tpl)
    vars_json = _build_demo_vars(tpl)

    email_html = render_html(
        template_html=tpl,
        content_html=content,
        styles=styles_obj,
        vars_json=vars_json,
    )

    return render(
        request,
        "panels/aap_campaigns/modal_preview.html",
        {"status": "done", "email_html": email_html or ""},
    )


@require_GET
@csrf_exempt
def templates__global_style_css_view(request: HttpRequest) -> HttpResponse:
    gid_raw = (request.GET.get("gid") or "").strip()
    typ = (request.GET.get("type") or "").strip()
    name = (request.GET.get("name") or "").strip()

    if not (gid_raw.isdigit() and typ in ("colors", "fonts") and name):
        return HttpResponse("", content_type="text/plain; charset=utf-8")

    gt = GlobalTemplate.objects.filter(id=int(gid_raw)).first()
    if not gt:
        return HttpResponse("", content_type="text/plain; charset=utf-8")

    styles = gt.styles or {}
    group = styles.get(typ)
    if not isinstance(group, dict):
        return HttpResponse("", content_type="text/plain; charset=utf-8")

    block = group.get(name)
    if not isinstance(block, dict):
        return HttpResponse("", content_type="text/plain; charset=utf-8")

    css = styles_json_to_css(block)
    return HttpResponse(css or "", content_type="text/plain; charset=utf-8")
