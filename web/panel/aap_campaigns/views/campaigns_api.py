# FILE: web/panel/aap_campaigns/views/campaigns_api.py
# DATE: 2026-01-22
# PURPOSE: Letter editor API (как в templates): extract content / render editor_html + preview (user/advanced).
# CHANGE:
# - Campaigns preview: отдельная модалка modal_full_preview.html (VIEW / HTML / HTML EMAIL).
# - HTML tab: 3 блока (внутренний content_html, CSS, внешний template_html).
# - VIEW: render_html + demo vars (default_template_vars()) в iframe srcdoc (изоляция стилей).
# - HTML EMAIL: render_html без vars_json ({}).
# - NEW: buttons-by-template: вытащить GlobalTemplate.buttons по id-<N> из первого тега template_html.

from __future__ import annotations

import re
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
    default_template_vars,
    letter_editor_extract_content,
    letter_editor_render_html,
    styles_json_to_css,
)
from panel.models import GlobalTemplate


def _extract_global_template_id_from_first_tag(template_html: str) -> int | None:
    s = (template_html or "").lstrip()
    if not s:
        return None

    m_tag = re.search(r"(?is)<\s*([a-zA-Z][a-zA-Z0-9:_-]*)([^>]*)>", s)
    if not m_tag:
        return None

    attrs = m_tag.group(2) or ""
    m_class = re.search(
        r"""\bclass\s*=\s*(?P<q>["'])(?P<v>.*?)(?P=q)""",
        attrs,
        flags=re.IGNORECASE | re.DOTALL,
    )
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


def _build_preview_bundle(template_html: str, styles_obj: Any, content_html: str) -> Dict[str, str]:
    styles_main = _styles_pick_main(styles_obj or {})
    content_used = sanitize(content_html or "")
    tpl_html = template_html or ""

    # VIEW — с demo vars (реалистичное превью)
    view_html = render_html(
        template_html=tpl_html,
        content_html=content_used,
        styles=styles_main,
        vars_json=default_template_vars(),
    ) or ""

    # HTML EMAIL — без vars
    email_html = render_html(
        template_html=tpl_html,
        content_html=content_used,
        styles=styles_main,
        vars_json={},
    ) or ""

    return {
        "view_html": view_html,
        "content_html": content_used,
        "raw_css": styles_json_to_css(styles_main) or "",
        "template_html": tpl_html,
        "email_html": email_html,
    }


# ==================== LETTER EDITOR (python-only HTML ops) ====================

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


@require_POST
@csrf_exempt
def campaigns__letter_buttons_by_template_view(request: HttpRequest) -> JsonResponse:
    """Return buttons dict for current template_html (via id-<N> in first tag class).

    Payload: {"template_html": "<...>"}
    Response: {"ok": true, "buttons": {"NAME": "<html>"}}
    """
    ws_id = _guard_ws(request)
    if not ws_id:
        return JsonResponse({"ok": False})

    data = _read_json_body(request)
    template_html = data.get("template_html") or ""

    pk = _extract_global_template_id_from_first_tag(template_html or "")
    if not pk:
        return JsonResponse({"ok": True, "buttons": {}})

    gt = GlobalTemplate.objects.filter(id=int(pk), is_active=True).first()
    if not gt:
        return JsonResponse({"ok": True, "buttons": {}})

    btn = gt.buttons if isinstance(gt.buttons, dict) else {}
    out: Dict[str, str] = {}
    for k, v in btn.items():
        key = str(k).strip()
        if not key:
            continue
        out[key] = str(v or "")

    return JsonResponse({"ok": True, "buttons": out})


# ==================== PREVIEW (Campaigns / Letter) ====================

_EMPTY_CTX = {"status": "empty", "view_html": "", "content_html": "", "raw_css": "", "template_html": "", "email_html": ""}


@require_GET
@csrf_exempt
def campaigns__preview_modal_by_id_view(request: HttpRequest) -> HttpResponse:
    ws_id = _guard_ws(request)
    if not ws_id:
        return render(request, "panels/aap_campaigns/modal_full_preview.html", _EMPTY_CTX)

    res = resolve_pk_or_redirect(request, Campaign, param="id")
    if not isinstance(res, int):
        return render(request, "panels/aap_campaigns/modal_full_preview.html", _EMPTY_CTX)

    camp = Campaign.objects.filter(id=int(res), workspace_id=ws_id).first()
    if not camp:
        return render(request, "panels/aap_campaigns/modal_full_preview.html", _EMPTY_CTX)

    let = _ensure_letter(ws_id, camp)
    tpl = let.template if let and let.template_id else None
    if not tpl:
        return render(request, "panels/aap_campaigns/modal_full_preview.html", _EMPTY_CTX)

    bundle = _build_preview_bundle(
        template_html=tpl.template_html or "",
        styles_obj=tpl.styles or {},
        content_html=(let.html_content or ""),
    )
    return render(request, "panels/aap_campaigns/modal_full_preview.html", {"status": "done", **bundle})


@require_POST
@csrf_exempt
def campaigns__preview_modal_from_editor_view(request: HttpRequest) -> HttpResponse:
    ws_id = _guard_ws(request)
    if not ws_id:
        return render(request, "panels/aap_campaigns/modal_full_preview.html", _EMPTY_CTX)

    data = _read_json_body(request)
    ui_id = (data.get("id") or "").strip()
    editor_mode = (data.get("editor_mode") or "user").strip()
    editor_html = data.get("editor_html") or ""

    camp = _load_campaign_by_ui_id(ws_id, ui_id)
    if not camp:
        return render(request, "panels/aap_campaigns/modal_full_preview.html", _EMPTY_CTX)

    let = _ensure_letter(ws_id, camp)
    tpl = let.template if let and let.template_id else None
    if not tpl:
        return render(request, "panels/aap_campaigns/modal_full_preview.html", _EMPTY_CTX)

    # user => visual editor => extract content in python
    content_html = editor_html or ""
    if editor_mode != "advanced":
        content_html = letter_editor_extract_content(editor_html or "")

    bundle = _build_preview_bundle(
        template_html=tpl.template_html or "",
        styles_obj=tpl.styles or {},
        content_html=content_html or "",
    )
    return render(request, "panels/aap_campaigns/modal_full_preview.html", {"status": "done", **bundle})
