# FILE: web/panel/aap_campaigns/views/templates_api.py  (обновлено — 2026-01-15)
# PURPOSE: API для User-editor (TinyMCE) + переключение advanced<->user через сервер.
# CHANGE:
#   - GET: _render-user-html/_render-user-css (как было, но с underscore URL).
#   - POST: _parse-editor-html (TinyMCE html -> template_html with {{ ..content.. }})
#           _render-editor-html (template_html -> TinyMCE html with demo-content wrapped)
#   - JSON ответы: {"ok": true, ...}

from __future__ import annotations

import json

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.http import require_GET, require_POST
from django.views.decorators.csrf import csrf_exempt

from mailer_web.access import decode_id
from panel.aap_campaigns.models import Templates
from engine.common.email_template import (
    styles_json_to_css,
    editor_template_render_html,
    editor_template_parse_html,
)


def _load_obj_by_ui_id(ui_id: str) -> Templates | None:
    ui_id = (ui_id or "").strip()
    if not ui_id:
        return None
    try:
        pk = int(decode_id(ui_id))
    except Exception:
        return None
    return Templates.objects.filter(id=pk).first()


def _find_demo_content(template_html: str) -> str:
    # ВРЕМЕННО.
    return "<p>[DEMO CONTENT]</p>"


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
