# FILE: web/mailer_web/letter_sender.py
# DATE: 2026-03-07
# PURPOSE: send system letters by slug+lang through configured system SMTP mailbox.

from __future__ import annotations

from dataclasses import dataclass
from string import Formatter
from typing import Any

from django.db import transaction

from engine.common import db
from engine.common.email_template import render_html, sanitize
from engine.common.mail.smtp import SMTPConn
from engine.common.prompts.process import translate_text
from mailer_web.models import MailLetter, MailLetterLang, MailTemplate


class LetterSenderError(Exception):
    """Base sender exception."""


class LetterNotFoundError(LetterSenderError):
    """Letter slug was not found."""


class LetterLangNotFoundError(LetterSenderError):
    """No source language row found to build target language."""


class LetterTemplateEmptyError(LetterSenderError):
    """Letter content is empty for selected language."""


class LetterRenderError(LetterSenderError):
    """Substitution/render failed."""


class SystemMailboxNotFoundError(LetterSenderError):
    """No active system mailbox with SMTP config."""


class SystemMailboxSendError(LetterSenderError):
    """SMTP send failed on system mailbox."""


@dataclass(frozen=True)
class SentLetter:
    slug: str
    lang: str
    to_email: str
    subject: str
    html: str
    mailbox_id: int


class _SafeDict(dict):
    def __getitem__(self, key):
        if dict.__contains__(self, key):
            return dict.__getitem__(self, key)
        alt = str(key).strip()
        if dict.__contains__(self, alt):
            return dict.__getitem__(self, alt)
        return self.__missing__(key)

    def __missing__(self, key):  # pragma: no cover
        return "{" + key + "}"


def _norm_lang(lang: str | None) -> str:
    raw = (lang or "").strip().lower()
    if not raw:
        return "de"
    return raw.split("-", 1)[0]


def _styles_pick_main(styles_obj) -> dict:
    if not isinstance(styles_obj, dict):
        return {}
    main = styles_obj.get("main")
    return main if isinstance(main, dict) else styles_obj


def _template_for_letter(letter: MailLetter) -> MailTemplate | None:
    if getattr(letter, "template_id", None):
        tpl = MailTemplate.objects.filter(id=int(letter.template_id)).first()
        if tpl:
            return tpl
    return MailTemplate.objects.order_by("id").first()


def _render_send_html(*, tpl_html: str, tpl_styles: dict, content_html: str) -> str:
    return (
        render_html(
            template_html=tpl_html or "",
            content_html=sanitize(content_html or ""),
            styles=_styles_pick_main(tpl_styles or {}),
            vars_json=None,
        )
        or ""
    )


def _pick_source_lang_row(letter: MailLetter, target_lang: str) -> MailLetterLang | None:
    rows = list(
        MailLetterLang.objects.filter(letter=letter)
        .exclude(lang=target_lang)
        .order_by("id")
    )
    rows = [r for r in rows if (r.letter_html or "").strip()]
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


def _ensure_lang_row(letter: MailLetter, lang: str) -> MailLetterLang:
    wanted = _norm_lang(lang)
    row = MailLetterLang.objects.filter(letter=letter, lang=wanted).first()
    if row:
        return row

    src = _pick_source_lang_row(letter, wanted)
    if not src:
        raise LetterLangNotFoundError(
            f"No source language with letter_html for letter slug='{letter.slug}'"
        )

    tpl = _template_for_letter(letter)
    tpl_html = (tpl.template_html if tpl else "") or ""
    tpl_styles = (tpl.styles if tpl else {}) or {}

    source_subject = (src.subject or "").strip()
    source_html = (src.letter_html or "").strip()

    translated_subject = (translate_text(source_subject, wanted) or "").strip() if source_subject else ""
    translated_html = (translate_text(source_html, wanted) or "").strip() if source_html else ""

    if not translated_html:
        raise LetterLangNotFoundError(
            f"Translation failed for slug='{letter.slug}' to lang='{wanted}'"
        )

    with transaction.atomic():
        row, _ = MailLetterLang.objects.get_or_create(letter=letter, lang=wanted)
        row.subject = translated_subject
        row.letter_html = sanitize(translated_html)
        row.send_html = _render_send_html(
            tpl_html=tpl_html,
            tpl_styles=tpl_styles,
            content_html=row.letter_html,
        )
        row.save(update_fields=["subject", "letter_html", "send_html"])

    return row


def _render_with_context(template: str, context: dict[str, Any]) -> str:
    src = template or ""
    if not src:
        return ""
    try:
        safe = _SafeDict(**(context or {}))
        for k, v in list((context or {}).items()):
            sk = str(k).strip()
            if sk and not dict.__contains__(safe, sk):
                safe[sk] = v
        return src.format_map(safe)
    except Exception as exc:
        raise LetterRenderError(f"Render failed: {exc}") from exc


def _collect_required_placeholders(subject: str, html: str) -> set[str]:
    out: set[str] = set()
    fmt = Formatter()
    for part in (subject or "", html or ""):
        for _lit, field_name, _fmt_spec, _conv in fmt.parse(part):
            if field_name:
                key = str(field_name).split(".", 1)[0].split("[", 1)[0]
                if key:
                    out.add(key)
    return out


def _pick_system_mailbox_id() -> int:
    row = db.fetch_one(
        """
        SELECT m.id
        FROM public.aap_settings_mailboxes m
        JOIN public.aap_settings_smtp_mailboxes s ON s.mailbox_id = m.id
        WHERE m.workspace_id = '00000000-0000-0000-0000-000000000000'
          AND m.is_active = true
          AND m.archived = false
          AND s.is_active = true
        ORDER BY m.id
        LIMIT 1
        """,
        [],
    )
    if not row or row[0] is None:
        raise SystemMailboxNotFoundError("No active system mailbox with SMTP config")
    return int(row[0])


def send_letter_by_slug(
    *,
    slug: str,
    to_email: str,
    lang: str | None = None,
    context: dict[str, Any] | None = None,
    mailbox_id: int | None = None,
) -> SentLetter:
    letter = MailLetter.objects.filter(slug=(slug or "").strip()).first()
    if not letter:
        raise LetterNotFoundError(f"Letter slug='{slug}' not found")

    row = _ensure_lang_row(letter, _norm_lang(lang))

    raw_subject = (row.subject or "").strip()
    raw_html = (row.send_html or "").strip()

    # send_html is mandatory for send path; regenerate if missing from letter_html/template.
    if not raw_html and (row.letter_html or "").strip():
        tpl = _template_for_letter(letter)
        raw_html = _render_send_html(
            tpl_html=((tpl.template_html if tpl else "") or ""),
            tpl_styles=((tpl.styles if tpl else {}) or {}),
            content_html=(row.letter_html or ""),
        )
        row.send_html = raw_html
        row.save(update_fields=["send_html"])

    if not raw_subject and not raw_html:
        raise LetterTemplateEmptyError(
            f"Letter slug='{slug}' lang='{row.lang}' has empty subject and send_html"
        )

    ctx = dict(context or {})
    _collect_required_placeholders(raw_subject, raw_html)

    subject = _render_with_context(raw_subject, ctx)
    html = _render_with_context(raw_html, ctx)

    mb_id = int(mailbox_id) if mailbox_id is not None else _pick_system_mailbox_id()
    smtp = SMTPConn(mb_id)
    ok = smtp.send_mail(
        to_email,
        subject,
        body_text="",
        body_html=html,
        headers=None,
    )
    if not ok:
        raise SystemMailboxSendError(f"SMTP send failed for mailbox_id={mb_id}")

    return SentLetter(
        slug=letter.slug,
        lang=row.lang,
        to_email=to_email,
        subject=subject,
        html=html,
        mailbox_id=mb_id,
    )
