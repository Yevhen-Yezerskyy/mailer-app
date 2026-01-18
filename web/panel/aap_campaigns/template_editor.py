# FILE: web/panel/aap_campaigns/template_editor.py
# DATE: 2026-01-18
# PURPOSE: Хелперы для user-editor (TinyMCE) и advanced<->user: placeholder + wrap/unwrap контента,
#          editor_html <-> template_html, CSS<->JSON.
# CHANGE:
#   - Добавлена подстановка {{ var }} по словарю (дефолты + date_time по Europe/Berlin).
#   - Добавлена расстановка Tiny-классов: по умолчанию mceNonEditable на все теги,
#     а для тегов с class TemplateEdit — mceEditable. Без дублей, с удалением конфликтующего класса.

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

from engine.common.email_template import PLACEHOLDER, StylesJSON, sanitize

# ---- Wrapper (Tiny-safe) ----

_EDITOR_WRAP_CLASS = "yy_content_wrap"

# ---- Default vars (for {{ var }}) ----

_BERLIN_TZ = ZoneInfo("Europe/Berlin")

_DEFAULT_TEMPLATE_VARS: Dict[str, str] = {
    "company_name": "Unternehmen Adressat GmbH",
    "city_land": "Köln, Nordrhein-Westfalen",
}

_TINY_CLASS_STRIP_RE = re.compile(r'(?is)\bclass\s*=\s*(?P<q>["\'])(?P<v>.*?)(?P=q)')


def _strip_tiny_edit_classes(html: str) -> str:
    if not html:
        return ""

    def repl(m: re.Match) -> str:
        q = m.group("q")
        v = (m.group("v") or "").strip()
        if not v:
            return f'class={q}{q}'
        parts = [c for c in v.split() if c and c not in ("mceNonEditable", "mceEditable")]
        return f'class={q}{" ".join(parts)}{q}'

    return _TINY_CLASS_STRIP_RE.sub(repl, html)


def _default_date_time_berlin() -> str:
    # format: "18:45 04.03.2023" (Europe/Berlin)
    return datetime.now(_BERLIN_TZ).strftime("%H:%M %d.%m.%Y")


def wrap_editor_content(inner_html: str) -> str:
    return (
        f'<table class="{_EDITOR_WRAP_CLASS}">'
        f"<tr><td>{inner_html or ''}</td></tr>"
        f"</table>"
    )


def _find_wrapper_table_span(html_text: str) -> Optional[Tuple[int, int]]:
    """
    Находит диапазон [start:end) для <table ... class="...yy_content_wrap..." ...> ... </table>,
    корректно учитывая вложенные <table> внутри.
    Возвращает None, если wrapper не найден или не удалось найти закрывающий </table>.
    """
    s = html_text or ""
    if not s:
        return None

    # 1) Находим стартовый <table ... class="...yy_content_wrap..." ...>
    m = re.search(
        rf'(?is)<table\b[^>]*\bclass\s*=\s*["\'][^"\']*\b{re.escape(_EDITOR_WRAP_CLASS)}\b[^"\']*["\'][^>]*>',
        s,
    )
    if not m:
        return None

    start = m.start()
    pos = m.end()

    # 2) Считаем вложенность table-тегов, начиная с 1 (мы уже внутри wrapper-table)
    depth = 1
    token_re = re.compile(r"(?is)<table\b[^>]*>|</table\s*>")

    while True:
        t = token_re.search(s, pos)
        if not t:
            return None

        tok = t.group(0).lower()
        if tok.startswith("</table"):
            depth -= 1
            if depth == 0:
                end = t.end()
                return (start, end)
        else:
            depth += 1

        pos = t.end()


def unwrap_editor_content(html_text: str) -> str:
    """
    Достаём inner_html из wrapper-table:
      <table class="yy_content_wrap"><tr><td>INNER</td></tr></table>
    Вложенные таблицы внутри INNER не ломают поиск wrapper (span по depth).
    """
    span = _find_wrapper_table_span(html_text or "")
    if not span:
        return ""

    wrapper = (html_text or "")[span[0] : span[1]]

    m = re.search(r"(?is)<td\b[^>]*>(.*?)</td>", wrapper)
    return m.group(1) if m else ""


# ---- JSON <-> CSS ----

def _parse_styles_json(styles: StylesJSON) -> Dict[str, Dict[str, Any]]:
    if styles is None:
        return {}
    if isinstance(styles, dict):
        return styles
    if isinstance(styles, str):
        try:
            v = json.loads(styles.strip())
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def styles_json_to_css(styles: StylesJSON) -> str:
    obj = _parse_styles_json(styles)
    out: list[str] = []
    for sel in sorted(obj):
        rules = obj.get(sel)
        if not isinstance(rules, dict):
            continue
        decls = [f"{k}:{v};" for k, v in sorted(rules.items()) if v is not None]
        if decls:
            out.append(f"{sel}{{{''.join(decls)}}}")
    return "\n".join(out)


def styles_css_to_json(css_text: str) -> Dict[str, Dict[str, str]]:
    css_text = (css_text or "").strip()
    out: Dict[str, Dict[str, str]] = {}
    for m in re.finditer(r"(?s)([^{}]+)\{([^}]*)\}", css_text):
        sel = m.group(1).strip()
        body = m.group(2).strip()
        rules: Dict[str, str] = {}
        for part in body.split(";"):
            if ":" in part:
                k, v = part.split(":", 1)
                rules[k.strip()] = v.strip()
        if rules:
            out[sel] = rules
    return out


# ---- helpers: {{ var }} ----

_VAR_RE = re.compile(r"(?s)\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")


def _apply_template_vars(html: str, vars_dict: Dict[str, str]) -> str:
    if not html:
        return ""

    def repl(m: re.Match) -> str:
        key = (m.group(1) or "").strip()
        if not key:
            return m.group(0)
        v = vars_dict.get(key)
        return str(v) if v is not None else m.group(0)

    return _VAR_RE.sub(repl, html)


# ---- helpers: Tiny editability ----

_TINY_NONEDIT = "mceNonEditable"
_TINY_EDIT = "mceEditable"
_TEMPLATE_EDIT_CLASS = "TemplateEdit"

# Rough but practical for our sanitized email HTML:
_TAG_RE = re.compile(r"(?is)<([a-zA-Z][a-zA-Z0-9:_-]*)([^<>]*?)>")


_VOID_TAGS = {
    "br", "hr", "meta"
}

def _apply_tiny_editability(html: str) -> str:
    if not html:
        return ""

    def update_tag(m: re.Match) -> str:
        tag = m.group(0)
        name = (m.group(1) or "").lower()
        attrs = m.group(2) or ""

        # не трогаем script/style и void-теги
        if name in ("script", "style") or name in _VOID_TAGS:
            return tag

        m_class = re.search(r'(?is)\bclass\s*=\s*(?P<q>["\'])(?P<v>.*?)(?P=q)', attrs)
        classes: list[str] = []
        if m_class:
            classes = [c for c in (m_class.group("v") or "").split() if c]

        want_edit = _TEMPLATE_EDIT_CLASS in classes

        def ensure(cls: str):
            if cls not in classes:
                classes.append(cls)

        def drop(cls: str):
            while cls in classes:
                classes.remove(cls)

        if want_edit:
            ensure(_TINY_EDIT)
            drop(_TINY_NONEDIT)
        else:
            ensure(_TINY_NONEDIT)
            drop(_TINY_EDIT)

        new_class_value = " ".join(classes).strip()

        if m_class:
            q = m_class.group("q")
            new_attrs = attrs[: m_class.start()] + f'class={q}{new_class_value}{q}' + attrs[m_class.end() :]
        else:
            new_attrs = attrs if (attrs or "").startswith(" ") else (" " + (attrs or ""))
            new_attrs = f' class="{new_class_value}"' + new_attrs

        return f"<{m.group(1)}{new_attrs}>"

    return _TAG_RE.sub(update_tag, html)

# ---- editor helpers ----

def editor_template_render_html(template_html: str, content_html: str) -> str:
    """
    Порядок:
      1) вставка wrapped content в PLACEHOLDER
      2) подмена переменных {{ var }} (по дефолтному словарю)
      3) sanitize
      4) расстановка Tiny классов mceNonEditable/mceEditable (TemplateEdit -> editable)
    """
    html0 = template_html or ""

    # 1) вставка wrapped content
    wrapped = wrap_editor_content(content_html or "")
    html1 = html0.replace(PLACEHOLDER, wrapped, 1)

    # 2) vars
    vars_dict = dict(_DEFAULT_TEMPLATE_VARS)
    vars_dict["date_time"] = _default_date_time_berlin()
    html2 = _apply_template_vars(html1, vars_dict)

    # 3) sanitize
    html3 = sanitize(html2)

    # 4) Tiny editability classes
    return _apply_tiny_editability(html3)


def editor_template_parse_html(editor_html: str) -> str:
    """
    Заменяем wrapper-table (class содержит yy_content_wrap) обратно на PLACEHOLDER.
    Порядок:
      1) strip Tiny классов (mceNonEditable/mceEditable)
      2) wrapper -> PLACEHOLDER (по span)
      3) sanitize
    """
    raw = _strip_tiny_edit_classes(editor_html or "")

    span = _find_wrapper_table_span(raw)
    if not span:
        return sanitize(raw)

    replaced = raw[: span[0]] + PLACEHOLDER + raw[span[1] :]
    return sanitize(replaced)
