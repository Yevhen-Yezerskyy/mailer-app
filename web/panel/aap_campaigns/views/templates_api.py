# FILE: web/panel/aap_campaigns/views/templates_api.py  (обновлено — 2026-01-17)
# PURPOSE: API для TinyMCE/advanced switch + preview.
# CHANGE:
#   - Preview модалка: GET templates/preview/modal/?id=... (из БД) -> HTML модалки.
#   - Preview модалка: POST templates/preview/modal-from-editor/ (из редактора) -> HTML модалки.
#   - _build_demo_vars(): заглушка (пока пусто).
# NOTE: финальный HTML письма делается через engine.common.email_template.render_html().

from __future__ import annotations

import json

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from mailer_web.access import decode_id
from engine.common.email_template import render_html
from panel.aap_campaigns.models import Templates
from panel.aap_campaigns.template_editor import (
    editor_template_parse_html,
    editor_template_render_html,
    styles_css_to_json,
    styles_json_to_css,
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


def _build_demo_vars(template_html: str) -> dict:
    # ВРЕМЕННО: заглушка под {{ var }}.
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
