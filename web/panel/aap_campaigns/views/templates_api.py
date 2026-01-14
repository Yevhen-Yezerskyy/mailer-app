# FILE: web/panel/aap_campaigns/views/templates_api.py
# DATE: 2026-01-14
# PURPOSE: API для User-editor (Quill).
# CHANGE: GET:
#   - render-user-html(id=ui_id)  -> хранимый template_html (без <style>)
#   - render-user-css(id=ui_id)   -> CSS-текст, полученный из styles JSON (python JSON->CSS)

from __future__ import annotations

from django.http import HttpRequest, HttpResponse
from django.views.decorators.http import require_GET
from django.views.decorators.csrf import csrf_exempt

from mailer_web.access import decode_id
from panel.aap_campaigns.models import Templates
from engine.common.email_template import styles_json_to_css


def _load_obj_by_ui_id(ui_id: str) -> Templates | None:
    ui_id = (ui_id or "").strip()
    if not ui_id:
        return None
    try:
        pk = int(decode_id(ui_id))
    except Exception:
        return None
    return Templates.objects.filter(id=pk).first()


@require_GET
@csrf_exempt
def templates_render_user_html_view(request: HttpRequest) -> HttpResponse:
    obj = _load_obj_by_ui_id(request.GET.get("id") or "")
    if not obj:
        return HttpResponse("")
    return HttpResponse(obj.template_html or "")


@require_GET
@csrf_exempt
def templates_render_user_css_view(request: HttpRequest) -> HttpResponse:
    obj = _load_obj_by_ui_id(request.GET.get("id") or "")
    if not obj:
        return HttpResponse("", content_type="text/plain; charset=utf-8")
    css = styles_json_to_css(obj.styles or {})
    return HttpResponse(css or "", content_type="text/plain; charset=utf-8")
