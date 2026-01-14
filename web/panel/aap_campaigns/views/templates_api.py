# FILE: web/panel/aap_campaigns/views/templates_api.py
# DATE: 2026-01-14
# PURPOSE: API для User-editor (TinyMCE).
# CHANGE:
#   - добавлена функция поиска demo-content (пока заглушка)
#   - render-user-html использует editor_template_render_html(template_html, demo_html)

from __future__ import annotations

from django.http import HttpRequest, HttpResponse
from django.views.decorators.http import require_GET
from django.views.decorators.csrf import csrf_exempt

from mailer_web.access import decode_id
from panel.aap_campaigns.models import Templates
from engine.common.email_template import (
    styles_json_to_css,
    editor_template_render_html,
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
    """
    ВРЕМЕННО.
    Ищет/генерит demo-контент для user-editor на основе template_html.
    Потом будет нормальная логика (по маркерам / типу шаблона / etc).
    """
    return "<p>[DEMO CONTENT]</p>"


@require_GET
@csrf_exempt
def templates_render_user_html_view(request: HttpRequest) -> HttpResponse:
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
def templates_render_user_css_view(request: HttpRequest) -> HttpResponse:
    obj = _load_obj_by_ui_id(request.GET.get("id") or "")
    if not obj:
        return HttpResponse("", content_type="text/plain; charset=utf-8")

    css = styles_json_to_css(obj.styles or {})
    return HttpResponse(css or "", content_type="text/plain; charset=utf-8")
