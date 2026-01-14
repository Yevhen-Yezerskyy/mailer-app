# FILE: web/panel/aap_campaigns/views/templates_api.py  (новое)
# DATE: 2026-01-14
# PURPOSE: Серверные endpoint'ы для редактора шаблонов:
#          - render_user: (template_html, styles) -> html_for_editor (вставки+style-tag на сервере)
#          - normalize: (editor_html) -> clean_template_html + styles_json (из style-tag)
#          - preview: (template_html, styles, mode) -> html (для модалки)

from __future__ import annotations

import json

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.http import require_POST

from engine.common.email_template import normalize_for_store, render_for_editor, render_for_preview


def _demo_block() -> str:
    # demo кусок для user-mode (этап-1)
    return (
        "<table class=\"yy-demo\" style=\"border:1px dashed #999; width:100%;\">"
        "<tr><td>"
        "<p><i>[DEMO CONTENT]</i></p>"
        "<p>Тут будет {{ ..content.. }} (этап-2).</p>"
        "</td></tr></table>"
    )


@require_POST
def templates_render_user_view(request: HttpRequest) -> JsonResponse:
    template_html = (request.POST.get("template_html") or "").strip()
    styles = (request.POST.get("styles") or "").strip()

    res = render_for_editor(template_html, styles, demo_html=_demo_block())
    return JsonResponse({"html": res.html_for_editor})


@require_POST
def templates_normalize_view(request: HttpRequest) -> JsonResponse:
    editor_html = request.POST.get("editor_html") or ""
    res = normalize_for_store(editor_html)

    # styles отдаём как “красивый” JSON-стринг (для формы)
    styles_text = json.dumps(res.styles_json_obj or {}, ensure_ascii=False, indent=2)

    return JsonResponse(
        {
            "template_html": res.clean_template_html,
            "styles": styles_text,
        }
    )


@require_POST
def templates_preview_view(request: HttpRequest) -> HttpResponse:
    template_html = request.POST.get("template_html") or ""
    styles = request.POST.get("styles") or "{}"
    mode = (request.POST.get("mode") or "style_tag").strip()
    if mode not in ("style_tag", "inline"):
        mode = "style_tag"

    vars_json = None
    vars_text = (request.POST.get("vars_json") or "").strip()
    if vars_text:
        try:
            v = json.loads(vars_text)
            if isinstance(v, dict):
                vars_json = v
        except Exception:
            vars_json = None

    html = render_for_preview(template_html, styles, mode=mode, vars_json=vars_json)

    # “рамка” под модалку (твои базовые стили центра)
    out = (
        "<div class='YY-CARD_WHITE'>"
        "<div class='YY-STATUS_BLUE mb-3'>Preview</div>"
        "<div style='border:1px solid #022a39;margin-top:30px;padding:12px;'>"
        f"{html}"
        "</div>"
        "</div>"
    )
    return HttpResponse(out)
