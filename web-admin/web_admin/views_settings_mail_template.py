# FILE: web-admin/web_admin/views_settings_mail_template.py
# DATE: 2026-03-07
# PURPOSE: Settings -> single system mail template editor + API compatible with campaign_templates JS paths.

from __future__ import annotations

import json
import re
from typing import Optional

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.translation import gettext as _
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from engine.common.email_template import render_html, sanitize
from mailer_web.access import encode_id
from mailer_web.models import MailTemplate
from panel.aap_campaigns.template_editor import (
    default_template_vars,
    editor_template_parse_html,
    editor_template_render_html,
    styles_css_to_json,
    styles_json_to_css,
)
from panel.models import GlobalTemplate


SYSTEM_TEMPLATE_DEFAULT_NAME = "Системный шаблон"
_FLAG_ATTR = "_tw_classmap_enabled"
_DEMO_FALLBACK_HTML = "<p>[DEMO CONTENT]</p>"


def _flag_request(request: HttpRequest) -> None:
    setattr(request, _FLAG_ATTR, True)


def _ensure_system_template() -> MailTemplate:
    obj = MailTemplate.objects.order_by("id").first()
    if obj:
        return obj
    return MailTemplate.objects.create(
        template_name=SYSTEM_TEMPLATE_DEFAULT_NAME,
        template_html="",
        styles={},
    )


def _extract_global_template_id_from_first_tag(template_html: str) -> int | None:
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


def _global_style_keys_by_gid(gid: int | None) -> tuple[int | None, list[str], list[str]]:
    if not gid:
        return None, [], []

    gt = GlobalTemplate.objects.filter(id=int(gid), is_active=True).first()
    if not gt or not isinstance(gt.styles, dict):
        return None, [], []

    colors = gt.styles.get("colors")
    fonts = gt.styles.get("fonts")

    c_keys = sorted([k for k in (colors or {}).keys() if isinstance(k, str)]) if isinstance(colors, dict) else []
    f_keys = sorted([k for k in (fonts or {}).keys() if isinstance(k, str)]) if isinstance(fonts, dict) else []

    return int(gt.id), c_keys, f_keys


def _build_global_tpl_items(current_gid: int | None):
    out = []
    qs = GlobalTemplate.objects.filter(is_active=True).order_by("order", "template_name")
    for gt in qs:
        out.append(
            {
                "id": int(gt.id),
                "template_name": gt.template_name,
                "is_current": bool(current_gid and int(current_gid) == int(gt.id)),
            }
        )
    return out


def _pick_random_active_gl_tpl_id() -> int | None:
    obj = GlobalTemplate.objects.filter(is_active=True).order_by("?").first()
    return int(obj.id) if obj else None


def _find_demo_content(template_html: str) -> str:
    pk = _extract_global_template_id_from_first_tag(template_html or "")
    if not pk:
        return _DEMO_FALLBACK_HTML

    obj = GlobalTemplate.objects.filter(id=pk).first()
    if not obj:
        return _DEMO_FALLBACK_HTML

    html = (obj.html_content or "").strip()
    return html or _DEMO_FALLBACK_HTML


def _styles_pick_main(styles_obj) -> dict:
    if not isinstance(styles_obj, dict):
        return {}
    main = styles_obj.get("main")
    return main if isinstance(main, dict) else styles_obj


def _build_preview_bundle(template_html: str, styles_obj, content_html: str) -> dict:
    tpl = template_html or ""
    styles_main = _styles_pick_main(styles_obj or {})
    raw_css = styles_json_to_css(styles_main) or ""

    view_html = render_html(
        template_html=tpl,
        content_html=content_html or "",
        styles=styles_main,
        vars_json=default_template_vars(),
    ) or ""

    email_html = render_html(
        template_html=tpl,
        content_html=content_html or "",
        styles=styles_main,
        vars_json={},
    ) or ""

    return {
        "view_html": view_html,
        "raw_html": tpl,
        "raw_css": raw_css,
        "email_html": email_html,
    }


def _read_json(request: HttpRequest) -> dict:
    try:
        raw = (request.body or b"").decode("utf-8", "ignore")
        obj = json.loads(raw) if raw else {}
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


@login_required(login_url="login")
def system_mail_template_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
    obj = _ensure_system_template()
    obj.ui_id = encode_id(int(obj.id))
    return render(
        request,
        "panels/aap_settings/system_mail_template.html",
        {
            "section": "mail_template",
            "items": [obj],
        },
    )


@login_required(login_url="login")
def system_mail_template_edit_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
    obj = _ensure_system_template()
    obj.ui_id = encode_id(int(obj.id))

    state_q = (request.GET.get("state") or "").strip()
    id_q = (request.GET.get("id") or "").strip()
    gl_q = (request.GET.get("gl_tpl") or "").strip()

    need_redirect = False
    q = request.GET.copy()
    if state_q != "edit":
        q["state"] = "edit"
        need_redirect = True
    if id_q != obj.ui_id:
        q["id"] = obj.ui_id
        need_redirect = True
    if not (obj.template_html or "").strip() and not gl_q:
        rid = _pick_random_active_gl_tpl_id()
        if rid:
            q["gl_tpl"] = str(rid)
            need_redirect = True
    if need_redirect and request.method == "GET":
        return redirect(f"{reverse('settings:mail_template_edit')}?{q.urlencode()}")

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "close":
            return redirect("settings:mail_template")

        if action == "save":
            template_name = (request.POST.get("template_name") or "").strip()
            editor_html = request.POST.get("editor_html") or ""
            css_text = request.POST.get("css_text") or ""

            if not template_name:
                storage = messages.get_messages(request)
                storage.used = True
                messages.error(request, _("Имя шаблона обязательно."))
                return redirect(request.get_full_path())

            clean_html = sanitize(editor_template_parse_html(editor_html))
            styles_obj = styles_css_to_json(css_text)

            obj.template_name = template_name
            obj.template_html = clean_html
            obj.styles = styles_obj
            obj.save(update_fields=["template_name", "template_html", "styles", "updated_at"])

            return redirect(f"{reverse('settings:mail_template_edit')}?state=edit&id={obj.ui_id}")

    gl_tpl = (request.GET.get("gl_tpl") or "").strip()
    current_gid: Optional[int] = None
    if gl_tpl.isdigit():
        current_gid = int(gl_tpl)
    if not current_gid:
        current_gid = _extract_global_template_id_from_first_tag(obj.template_html)

    global_style_gid, global_colors, global_fonts = _global_style_keys_by_gid(current_gid)

    items = [obj]

    return render(
        request,
        "panels/aap_settings/system_mail_template_edit.html",
        {
            "section": "mail_template",
            "state": "edit",
            "edit_obj": obj,
            "items": items,
            "global_tpl_items": _build_global_tpl_items(current_gid),
            "global_style_gid": global_style_gid,
            "global_colors": global_colors,
            "global_fonts": global_fonts,
        },
    )


@login_required(login_url="login")
@require_GET
@csrf_exempt
def system_templates__render_user_html_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
    obj = _ensure_system_template()

    gl_tpl = (request.GET.get("gl_tpl") or "").strip()
    if gl_tpl.isdigit():
        gt = GlobalTemplate.objects.filter(id=int(gl_tpl), is_active=True).first()
        if gt:
            tpl = gt.html_template or ""
            content = (gt.html_content or "").strip() or _DEMO_FALLBACK_HTML
            html = editor_template_render_html(template_html=tpl, content_html=content)
            return HttpResponse(html, content_type="text/html; charset=utf-8")

    html = editor_template_render_html(template_html=obj.template_html or "", content_html=_find_demo_content(obj.template_html or ""))
    return HttpResponse(html, content_type="text/html; charset=utf-8")


@login_required(login_url="login")
@require_GET
@csrf_exempt
def system_templates__render_user_css_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
    obj = _ensure_system_template()

    gl_tpl = (request.GET.get("gl_tpl") or "").strip()
    if gl_tpl.isdigit():
        gt = GlobalTemplate.objects.filter(id=int(gl_tpl), is_active=True).first()
        if gt:
            css = styles_json_to_css(_styles_pick_main(gt.styles or {}))
            return HttpResponse(css or "", content_type="text/plain; charset=utf-8")

    css = styles_json_to_css(_styles_pick_main(obj.styles or {}))
    return HttpResponse(css or "", content_type="text/plain; charset=utf-8")


@login_required(login_url="login")
@require_POST
@csrf_exempt
def system_templates__parse_editor_html_view(request: HttpRequest) -> JsonResponse:
    _flag_request(request)
    data = _read_json(request)
    editor_html = data.get("editor_html") or ""
    template_html = editor_template_parse_html(editor_html)
    return JsonResponse({"ok": True, "template_html": template_html})


@login_required(login_url="login")
@require_POST
@csrf_exempt
def system_templates__render_editor_html_view(request: HttpRequest) -> JsonResponse:
    _flag_request(request)
    data = _read_json(request)
    template_html = data.get("template_html") or ""
    demo_html = _find_demo_content(template_html)
    editor_html = editor_template_render_html(template_html=template_html, content_html=demo_html)
    return JsonResponse({"ok": True, "editor_html": editor_html})


@login_required(login_url="login")
@require_GET
@csrf_exempt
def system_templates__preview_modal_by_id_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
    obj = _ensure_system_template()
    bundle = _build_preview_bundle(
        template_html=obj.template_html or "",
        styles_obj=obj.styles or {},
        content_html=_find_demo_content(obj.template_html or ""),
    )
    return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "done", **bundle})


@login_required(login_url="login")
@require_POST
@csrf_exempt
def system_templates__preview_modal_from_editor_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
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
    bundle = _build_preview_bundle(template_html=tpl, styles_obj=styles_obj, content_html=content)

    return render(request, "panels/aap_campaigns/modal_preview.html", {"status": "done", **bundle})


@login_required(login_url="login")
@require_GET
@csrf_exempt
def system_templates__global_style_css_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
    gid_raw = (request.GET.get("gid") or "").strip()
    typ = (request.GET.get("type") or "").strip()
    name = (request.GET.get("name") or "").strip()

    if not (gid_raw.isdigit() and typ in ("colors", "fonts") and name):
        return HttpResponse("", content_type="text/plain; charset=utf-8")

    gt = GlobalTemplate.objects.filter(id=int(gid_raw), is_active=True).first()
    if not gt:
        return HttpResponse("", content_type="text/plain; charset=utf-8")

    styles = gt.styles or {}
    group = styles.get(typ) if isinstance(styles, dict) else None
    if not isinstance(group, dict):
        return HttpResponse("", content_type="text/plain; charset=utf-8")

    block = group.get(name)
    if not isinstance(block, dict):
        return HttpResponse("", content_type="text/plain; charset=utf-8")

    css = styles_json_to_css(block)
    return HttpResponse(css or "", content_type="text/plain; charset=utf-8")
