# FILE: web-admin/web_admin/views_settings_mail_letters.py
# DATE: 2026-03-07
# PURPOSE: Settings -> mail letters management (list/create/edit + per-language editor/preview).

from __future__ import annotations

import json
import re

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from engine.common.email_template import render_html, sanitize
from engine.common.prompts.process import translate_text
from mailer_web.access import decode_id, encode_id
from mailer_web.models import MailLetter, MailLetterLang, MailTemplate
from panel.aap_campaigns.template_editor import (
    default_template_vars,
    letter_editor_extract_content,
    letter_editor_render_html,
    styles_json_to_css,
)
from panel.models import GlobalTemplate

from .forms import MailLetterForm

_FLAG_ATTR = "_tw_classmap_enabled"
_DEMO_FALLBACK_HTML = "<p>[DEMO CONTENT]</p>"


def _flag_request(request: HttpRequest) -> None:
    setattr(request, _FLAG_ATTR, True)


def _styles_pick_main(styles_obj) -> dict:
    if not isinstance(styles_obj, dict):
        return {}
    main = styles_obj.get("main")
    return main if isinstance(main, dict) else styles_obj


def _system_template() -> MailTemplate | None:
    return MailTemplate.objects.order_by("id").first()


def _template_for_letter(letter: MailLetter, *, attach_default: bool = False) -> MailTemplate | None:
    if getattr(letter, "template_id", None):
        tpl = MailTemplate.objects.filter(id=int(letter.template_id)).first()
        if tpl:
            return tpl

    sys_tpl = _system_template()
    if attach_default and sys_tpl and not getattr(letter, "template_id", None):
        letter.template_id = int(sys_tpl.id)
        letter.save(update_fields=["template", "updated_at"])
    return sys_tpl


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


def _find_demo_content(template_html: str) -> str:
    pk = _extract_global_template_id_from_first_tag(template_html or "")
    if not pk:
        return _DEMO_FALLBACK_HTML

    gt = GlobalTemplate.objects.filter(id=pk, is_active=True).first()
    if not gt:
        return _DEMO_FALLBACK_HTML

    html = (gt.html_content or "").strip()
    return html or _DEMO_FALLBACK_HTML


def _render_send_html(*, tpl_html: str, tpl_styles: dict, content_html: str) -> str:
    return render_html(
        template_html=tpl_html or "",
        content_html=sanitize(content_html or ""),
        styles=_styles_pick_main(tpl_styles or {}),
        vars_json=None,
    ) or ""


def _lang_codes() -> list[tuple[str, str]]:
    return [(str(c), str(n)) for c, n in (getattr(settings, "LANGUAGES", []) or [])]


def _is_lang_row_empty(row: MailLetterLang | None) -> bool:
    if not row:
        return True
    return not bool(((row.letter_html or "").strip() or (row.send_html or "").strip()))


def _pick_source_lang_row(letter: MailLetter, target_lang: str) -> MailLetterLang | None:
    rows = list(
        MailLetterLang.objects.filter(letter=letter)
        .exclude(lang=target_lang)
        .order_by("id")
    )
    rows = [r for r in rows if not _is_lang_row_empty(r)]
    if not rows:
        return None

    by_lang = {(r.lang or "").strip().lower(): r for r in rows}
    preferred = ["ru", "de", "en", "uk"]
    for code in preferred:
        if code == (target_lang or "").strip().lower():
            continue
        if code in by_lang:
            return by_lang[code]

    return rows[0]


@login_required(login_url="login")
def mail_letters_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)

    langs = _lang_codes()
    letters = list(MailLetter.objects.order_by("name", "id"))
    ids = [int(x.id) for x in letters]

    lang_rows = MailLetterLang.objects.filter(letter_id__in=ids)
    by_key = {(int(r.letter_id), str(r.lang)): r for r in lang_rows}

    for lt in letters:
        _template_for_letter(lt, attach_default=True)
        lt.ui_id = encode_id(int(lt.id))
        lt.lang_cells = []
        for code, label in langs:
            row = by_key.get((int(lt.id), code))
            lt.lang_cells.append(
                {
                    "code": code,
                    "label": label,
                    "row": row,
                    "has_data": not _is_lang_row_empty(row),
                }
            )

    return render(
        request,
        "panels/aap_settings/mail_letters.html",
        {
            "section": "mail_letters",
            "items": letters,
            "langs": langs,
        },
    )


@login_required(login_url="login")
def mail_letter_add_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
    if request.method == "POST":
        form = MailLetterForm(request.POST)
        if form.is_valid():
            obj = form.save(commit=False)
            tpl = _system_template()
            if tpl:
                obj.template = tpl
            obj.save()
            return redirect(reverse("settings:mail_letters"))
    else:
        form = MailLetterForm()

    return render(
        request,
        "panels/aap_settings/mail_letter_edit.html",
        {
            "section": "mail_letters",
            "form": form,
            "obj": None,
            "is_create": True,
        },
    )


@login_required(login_url="login")
def mail_letter_edit_view(request: HttpRequest, pk: int) -> HttpResponse:
    _flag_request(request)
    obj = get_object_or_404(MailLetter, pk=pk)

    if request.method == "POST":
        form = MailLetterForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            return redirect(reverse("settings:mail_letters"))
    else:
        form = MailLetterForm(instance=obj)

    return render(
        request,
        "panels/aap_settings/mail_letter_edit.html",
        {
            "section": "mail_letters",
            "form": form,
            "obj": obj,
            "is_create": False,
        },
    )


@login_required(login_url="login")
def mail_letter_lang_edit_view(request: HttpRequest, pk: int, lang: str) -> HttpResponse:
    _flag_request(request)
    letter = get_object_or_404(MailLetter, pk=pk)
    tpl = _template_for_letter(letter, attach_default=True)
    if not tpl:
        return redirect(reverse("settings:mail_template"))

    row, _ = MailLetterLang.objects.get_or_create(letter=letter, lang=lang)
    row.ui_id = encode_id(int(row.id))

    tpl_html = tpl.template_html or ""
    tpl_styles = tpl.styles or {}

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "close":
            return redirect(reverse("settings:mail_letters"))

        if action == "save":
            editor_mode = (request.POST.get("editor_mode") or "user").strip()
            editor_html = request.POST.get("editor_html") or ""
            subject = (request.POST.get("subject") or "").strip()

            content_html = editor_html
            if editor_mode != "advanced":
                content_html = letter_editor_extract_content(editor_html or "")

            row.subject = subject
            row.letter_html = sanitize(content_html or "")
            row.send_html = _render_send_html(
                tpl_html=tpl_html,
                tpl_styles=tpl_styles,
                content_html=row.letter_html,
            )
            row.save(update_fields=["subject", "letter_html", "send_html"])
            return redirect(reverse("settings:mail_letter_lang_edit", kwargs={"pk": int(letter.id), "lang": lang}))

    init_content = (row.letter_html or "").strip() or _find_demo_content(tpl_html)
    letter_init_html = letter_editor_render_html(tpl_html, init_content)
    letter_init_css = styles_json_to_css(_styles_pick_main(tpl_styles or {})) or ""

    return render(
        request,
        "panels/aap_settings/mail_letter_lang_edit.html",
        {
            "section": "mail_letters",
            "letter": letter,
            "lang": lang,
            "lang_row": row,
            "letter_init_html": letter_init_html,
            "letter_init_css": letter_init_css,
            "letter_template_html": tpl_html,
        },
    )


@login_required(login_url="login")
def mail_letter_lang_preview_view(request: HttpRequest, pk: int, lang: str) -> HttpResponse:
    _flag_request(request)
    letter = get_object_or_404(MailLetter, pk=pk)
    row = MailLetterLang.objects.filter(letter=letter, lang=lang).first()
    if not row:
        return render(
            request,
            "panels/aap_campaigns/modal_full_preview.html",
            {"status": "empty", "view_html": "", "content_html": "", "raw_css": "", "template_html": "", "email_html": ""},
        )

    tpl = _template_for_letter(letter, attach_default=True)
    tpl_html = (tpl.template_html if tpl else "") or ""
    styles_main = _styles_pick_main((tpl.styles if tpl else {}) or {})
    content_html = sanitize(row.letter_html or "")

    view_html = render_html(
        template_html=tpl_html,
        content_html=content_html,
        styles=styles_main,
        vars_json=default_template_vars(),
    ) or ""

    email_html = (row.send_html or "") or render_html(
        template_html=tpl_html,
        content_html=content_html,
        styles=styles_main,
        vars_json={},
    ) or ""

    return render(
        request,
        "panels/aap_campaigns/modal_full_preview.html",
        {
            "status": "done",
            "view_html": view_html,
            "content_html": content_html,
            "raw_css": styles_json_to_css(styles_main) or "",
            "template_html": tpl_html,
            "email_html": email_html,
        },
    )


@require_POST
@login_required(login_url="login")
def mail_letter_lang_translate_view(request: HttpRequest, pk: int, lang: str) -> HttpResponse:
    _flag_request(request)
    letter = get_object_or_404(MailLetter, pk=pk)
    tpl = _template_for_letter(letter, attach_default=True)
    if not tpl:
        return redirect(reverse("settings:mail_template"))

    target, _ = MailLetterLang.objects.get_or_create(letter=letter, lang=lang)
    if not _is_lang_row_empty(target):
        return redirect(reverse("settings:mail_letter_lang_edit", kwargs={"pk": int(letter.id), "lang": lang}))

    src = _pick_source_lang_row(letter, lang)
    if not src:
        messages.error(request, "Нет заполненного текста на другом языке для перевода.")
        return redirect(reverse("settings:mail_letter_lang_edit", kwargs={"pk": int(letter.id), "lang": lang}))

    source_subject = (src.subject or "").strip()
    source_html = (src.letter_html or "").strip()
    translated_subject = (translate_text(source_subject, lang) or "").strip() if source_subject else ""
    translated_html = (translate_text(source_html, lang) or "").strip() if source_html else ""

    if not translated_html:
        messages.error(request, "Перевод не получен. Проверьте настройки переводчика.")
        return redirect(reverse("settings:mail_letter_lang_edit", kwargs={"pk": int(letter.id), "lang": lang}))

    target.subject = translated_subject
    target.letter_html = sanitize(translated_html)
    target.send_html = _render_send_html(
        tpl_html=(tpl.template_html or ""),
        tpl_styles=(tpl.styles or {}),
        content_html=(target.letter_html or ""),
    )
    target.save(update_fields=["subject", "letter_html", "send_html"])
    messages.success(request, "Перевод выполнен.")
    return redirect(reverse("settings:mail_letter_lang_edit", kwargs={"pk": int(letter.id), "lang": lang}))


def _lang_row_by_ui_id(ui_id: str) -> MailLetterLang | None:
    try:
        pk = int(decode_id(ui_id))
    except Exception:
        return None
    return MailLetterLang.objects.select_related("letter").filter(id=pk).first()


@require_POST
@csrf_exempt
@login_required(login_url="login")
def letters__extract_content_view(request: HttpRequest) -> JsonResponse:
    _flag_request(request)
    try:
        data = json.loads((request.body or b"{}").decode("utf-8"))
    except Exception:
        data = {}
    editor_html = data.get("editor_html") or ""
    return JsonResponse({"ok": True, "content_html": letter_editor_extract_content(editor_html or "")})


@require_POST
@csrf_exempt
@login_required(login_url="login")
def letters__render_editor_html_view(request: HttpRequest) -> JsonResponse:
    _flag_request(request)
    try:
        data = json.loads((request.body or b"{}").decode("utf-8"))
    except Exception:
        data = {}

    ui_id = (data.get("id") or "").strip()
    content_html = data.get("content_html") or ""
    row = _lang_row_by_ui_id(ui_id)
    if not row:
        return JsonResponse({"ok": False})

    tpl = _template_for_letter(row.letter, attach_default=True)
    tpl_html = (tpl.template_html if tpl else "") or ""
    editor_html = letter_editor_render_html(tpl_html, sanitize(content_html or ""))
    return JsonResponse({"ok": True, "editor_html": editor_html or ""})


@require_POST
@csrf_exempt
@login_required(login_url="login")
def letters__buttons_by_template_view(request: HttpRequest) -> JsonResponse:
    _flag_request(request)
    try:
        data = json.loads((request.body or b"{}").decode("utf-8"))
    except Exception:
        data = {}
    template_html = data.get("template_html") or ""
    pk = _extract_global_template_id_from_first_tag(template_html or "")
    if not pk:
        return JsonResponse({"ok": True, "buttons": {}})

    gt = GlobalTemplate.objects.filter(id=int(pk), is_active=True).first()
    if not gt:
        return JsonResponse({"ok": True, "buttons": {}})

    btn = gt.buttons if isinstance(gt.buttons, dict) else {}
    out = {str(k).strip(): str(v or "") for k, v in btn.items() if str(k).strip()}
    return JsonResponse({"ok": True, "buttons": out})


@require_POST
@csrf_exempt
@login_required(login_url="login")
def letters__preview_modal_from_editor_view(request: HttpRequest) -> HttpResponse:
    _flag_request(request)
    try:
        data = json.loads((request.body or b"{}").decode("utf-8"))
    except Exception:
        data = {}

    ui_id = (data.get("id") or "").strip()
    editor_mode = (data.get("editor_mode") or "user").strip()
    editor_html = data.get("editor_html") or ""

    row = _lang_row_by_ui_id(ui_id)
    if not row:
        return render(
            request,
            "panels/aap_campaigns/modal_full_preview.html",
            {"status": "empty", "view_html": "", "content_html": "", "raw_css": "", "template_html": "", "email_html": ""},
        )

    tpl = _template_for_letter(row.letter, attach_default=True)
    tpl_html = (tpl.template_html if tpl else "") or ""
    styles_main = _styles_pick_main((tpl.styles if tpl else {}) or {})

    content_html = editor_html
    if editor_mode != "advanced":
        content_html = letter_editor_extract_content(editor_html or "")
    content_html = sanitize(content_html or "")

    view_html = render_html(
        template_html=tpl_html,
        content_html=content_html,
        styles=styles_main,
        vars_json=default_template_vars(),
    ) or ""
    email_html = render_html(
        template_html=tpl_html,
        content_html=content_html,
        styles=styles_main,
        vars_json={},
    ) or ""

    return render(
        request,
        "panels/aap_campaigns/modal_full_preview.html",
        {
            "status": "done",
            "view_html": view_html,
            "content_html": content_html,
            "raw_css": styles_json_to_css(styles_main) or "",
            "template_html": tpl_html,
            "email_html": email_html,
        },
    )
