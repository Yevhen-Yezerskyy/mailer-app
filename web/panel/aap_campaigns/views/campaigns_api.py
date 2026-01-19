# FILE: web/panel/aap_campaigns/views/campaigns_api.py
# DATE: 2026-01-19
# PURPOSE: Preview modal для письма кампании: by id (из БД) и from-editor (из текущего редактора).
# CHANGE: (new) endpoints для YYModal.

from __future__ import annotations

from typing import Any, Dict
from uuid import UUID

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from engine.common.email_template import render_html, sanitize
from mailer_web.access import decode_id, resolve_pk_or_redirect
from panel.aap_campaigns.models import Campaign, Letter, Templates


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
    obj = Letter.objects.filter(workspace_id=ws_id, campaign=camp).first()
    if obj:
        return obj
    return Letter.objects.create(workspace_id=ws_id, campaign=camp)


@require_GET
@csrf_exempt
def campaigns__preview_modal_by_id_view(request: HttpRequest) -> HttpResponse:
    ws_id = _guard_ws(request)
    if not ws_id:
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    # enforce workspace via central resolver
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

    # JSON body
    try:
        import json

        data = json.loads((request.body or b"{}").decode("utf-8"))
    except Exception:
        data = {}

    ui_id = (data.get("id") or "").strip()
    editor_html = data.get("editor_html") or ""

    camp = _load_campaign_by_ui_id(ws_id, ui_id)
    if not camp:
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    let = _ensure_letter(ws_id, camp)
    tpl = let.template if let and let.template_id else None
    if not tpl:
        return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "empty", "email_html": ""})

    email_html = render_html(
        template_html=tpl.template_html or "",
        content_html=sanitize(editor_html or ""),
        styles=_styles_pick_main(tpl.styles or {}),
        vars_json={},
    )
    return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "done", "email_html": email_html or ""})
