# FILE: web/panel/aap_campaigns/views/campaigns_api.py
# DATE: 2026-01-20
# PURPOSE: Letter editor API (как в templates): extract content / render editor_html + preview (user/advanced).
# CHANGE:
# - preview/from-editor: принимает {id, editor_mode, editor_html}; если mode=user => extract content через python.
# - NEW: /campaigns/letter/_extract-content/ и /campaigns/letter/_render-editor-html/

from __future__ import annotations

from typing import Any, Dict
from uuid import UUID

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from engine.common.email_template import render_html, sanitize
from mailer_web.access import decode_id, resolve_pk_or_redirect
from panel.aap_campaigns.models import Campaign, Letter
from panel.aap_campaigns.template_editor import (
    letter_editor_extract_content,
    letter_editor_render_html,
)


def _guard_ws(request: HttpRequest) -> UUID | None:
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


def _styles_pick_main(styles_obj: Any) -> Dict[str, Any]:
    if not isinstance(styles_obj, dict):
        return {}
    main = styles_obj.get("main")
    return main if isinstance(main, dict) else styles_obj


def _load_campaign_by_ui_id(ws_id: UUID, ui_id: str) -> Campaign | None:
    try:
        pk = int(decode_id(ui_id))
    except Exception:
        return None
    return Campaign.objects.filter(id=pk, workspace_id=ws_id).first()


def _ensure_letter(ws_id: UUID, camp: Campaign) -> Letter:
    obj = Letter.objects.filter(workspace_id=ws_id, campaign=camp).select_related("template").first()
    if obj:
        return obj
    return Letter.objects.create(workspace_id=ws_id, campaign=camp)


def _read_json_body(request: HttpRequest) -> Dict[str, Any]:
    try:
        import json

        return json.loads((request.body or b"{}").decode("utf-8"))
    except Exception:
        return {}


@require_POST
@csrf_exempt
def campaigns__letter_extract_content_view(request: HttpRequest) -> JsonResponse:
    ws_id = _guard_ws(request)
    if not ws_id:
        return JsonResponse({"ok": False})

    data = _read_json_body(request)
    editor_html = data.get("editor_html") or ""
    content_html = letter_editor_extract_content(editor_html or "")
    return JsonResponse({"ok": True, "content_html": content_html})


@require_POST
@csrf_exempt
def campaigns__letter_render_editor_html_view(request: HttpRequest) -> JsonResponse:
    ws_id = _guard_ws(request)
    if not ws_id:
        return JsonResponse({"ok": False})

    data = _read_json_body(request)
    ui_id = (data.get("id") or "").strip()
    content_html = data.get("content_html") or ""

    camp = _load_campaign_by_ui_id(ws_id, ui_id)
    if not camp:
        return JsonResponse({"ok": False})

    let = _ensure_letter(ws_id, camp)
    tpl = let.template if let and let.template_id else None
    if not tpl:
        return JsonResponse({"ok": False})

    editor_html = letter_editor_render_html(tpl.template_html or "", sanitize(content_html or ""))
    return JsonResponse({"ok": True, "editor_html": editor_html or ""})


@require_GET
@csrf_exempt
def campaigns__preview_modal_by_id_view(request: HttpRequest) -> HttpResponse:
    ws_id = _guard_ws(request)
    if not ws_id:
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    res = resolve_pk_or_redirect(request, Campaign, param="id")
    if not isinstance(res, int):
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    camp = Campaign.objects.filter(id=int(res), workspace_id=ws_id).first()
    if not camp:
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    let = _ensure_letter(ws_id, camp)
    tpl = let.template if let and let.template_id else None
    if not tpl:
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    email_html = render_html(
        template_html=tpl.template_html or "",
        content_html=sanitize(let.html_content or ""),
        styles=_styles_pick_main(tpl.styles or {}),
        vars_json={},
    )
    return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "done", "email_html": email_html or ""})


@require_POST
@csrf_exempt
def campaigns__preview_modal_from_editor_view(request: HttpRequest) -> HttpResponse:
    ws_id = _guard_ws(request)
    if not ws_id:
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    data = _read_json_body(request)
    ui_id = (data.get("id") or "").strip()
    editor_mode = (data.get("editor_mode") or "user").strip()
    editor_html = data.get("editor_html") or ""

    camp = _load_campaign_by_ui_id(ws_id, ui_id)
    if not camp:
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    let = _ensure_letter(ws_id, camp)
    tpl = let.template if let and let.template_id else None
    if not tpl:
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    # user => editor_html == visual => extract content in python
    content_html = editor_html or ""
    if editor_mode != "advanced":
        content_html = letter_editor_extract_content(editor_html or "")

    email_html = render_html(
        template_html=tpl.template_html or "",
        content_html=sanitize(content_html or ""),
        styles=_styles_pick_main(tpl.styles or {}),
        vars_json={},
    )
    return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "done", "email_html": email_html or ""})
