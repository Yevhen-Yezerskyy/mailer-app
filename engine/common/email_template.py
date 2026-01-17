# FILE: engine/common/email_template.py  (обновлено — 2026-01-17)
# PURPOSE: Финальный рендер HTML-писем: склейка template+content (body-фрагмент), vars, sanitize (whitelist),
#          inline styles из styles_json (tag + .class override), Outlook-safe p->table/tr/td.
# CHANGE: Убраны html/head/body из whitelist и спец-обработка meta.
#         Финальная обёртка <html><head><meta...></head><body>...</body></html> хардкодится в render_html.

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional, Union

StylesJSON = Union[str, Dict[str, Dict[str, Any]], None]

# ---- whitelist ----

ALLOWED_TAGS = {
    "table", "tbody", "thead", "tfoot", "tr", "td", "th",
    "p", "br", "hr",
    "h1", "h2", "h3", "h4",
    "strong", "em", "a",
}

ALLOWED_ATTRS = {
    "class",
    "href",
    "colspan",
    "rowspan",
}

# ---- placeholder ----

PLACEHOLDER = "{{ ..content.. }}"

# ---- styles ----

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


# ---- sanitize (плоский, линейный) ----

_TAG_RE = re.compile(r"(?is)<(/?)([a-z0-9]+)([^>]*)>")
_ATTR_RE = re.compile(r'([a-z0-9_-]+)\s*=\s*(".*?"|\'.*?\'|[^\s>]+)', re.I)

def _escape_text_minimal(s: str) -> str:
    # ВАЖНО: по договорённости — только "<" и ">"
    return (s or "").replace("<", "&lt;").replace(">", "&gt;")


def sanitize(html_text: str) -> str:
    html_text = html_text or ""
    out: list[str] = []
    pos = 0

    for m in _TAG_RE.finditer(html_text):
        if m.start() > pos:
            out.append(_escape_text_minimal(html_text[pos:m.start()]))

        slash, tag, attr_text = m.groups()
        tag = tag.lower()

        if tag not in ALLOWED_TAGS:
            pos = m.end()
            continue

        if slash:
            out.append(f"</{tag}>")
            pos = m.end()
            continue

        attrs_out: list[str] = []
        for am in _ATTR_RE.finditer(attr_text or ""):
            k, v = am.group(1).lower(), am.group(2)
            if k not in ALLOWED_ATTRS:
                continue
            if v and v[0] in "\"'" and v[-1] == v[0]:
                v = v[1:-1]
            # по договорённости: НЕ html-escape значения атрибутов
            attrs_out.append(f'{k}="{v}"')

        if attrs_out:
            out.append(f"<{tag} " + " ".join(attrs_out) + ">")
        else:
            out.append(f"<{tag}>")

        pos = m.end()

    if pos < len(html_text):
        out.append(_escape_text_minimal(html_text[pos:]))

    return "".join(out)


# ---- inline procedure (one-pass tags) ----

_STYLE_ATTR_RE = re.compile(r'(?is)\sstyle\s*=\s*(".*?"|\'.*?\'|[^\s>]+)')

def _extract_classes_from_attrs(attr_text: str) -> list[str]:
    m = re.search(r'(?is)\bclass\s*=\s*"([^"]*)"', attr_text or "")
    if not m:
        return []
    raw = (m.group(1) or "").strip()
    if not raw:
        return []
    return [c for c in raw.split() if c]


def _drop_style_attr(attr_text: str) -> str:
    # входной style игнорируем
    return _STYLE_ATTR_RE.sub("", attr_text or "")


def _drop_class_attr(attr_text: str) -> str:
    return re.sub(r'(?is)\sclass\s*=\s*"[^"]*"', "", attr_text or "")


def _style_str_from_rules(rules: Dict[str, Any]) -> str:
    if not isinstance(rules, dict) or not rules:
        return ""
    return "".join(f"{k}:{v};" for k, v in rules.items() if v is not None)


def _merged_rules(styles_obj: Dict[str, Dict[str, Any]], tag: str, classes: list[str]) -> Dict[str, Any]:
    rules: Dict[str, Any] = dict(styles_obj.get(tag.lower(), {}) or {})
    for cls in classes:
        rules.update(styles_obj.get(f".{cls}", {}) or {})
    return rules


def _inline_one_pass(html0: str, styles_obj: Dict[str, Dict[str, Any]]) -> str:
    out: list[str] = []
    pos = 0

    p_wrap_depth = 0
    table_style = "width:100%;border-collapse:collapse;border-spacing:0;"

    for m in _TAG_RE.finditer(html0):
        if m.start() > pos:
            out.append(html0[pos:m.start()])

        slash, tag, attr_text = m.groups()
        tag = (tag or "").lower()
        attr_text = attr_text or ""

        # закрывающие: не трогаем, кроме </p> если мы открывали p-wrap
        if slash:
            if tag == "p" and p_wrap_depth > 0:
                out.append("</td></tr></table>")
                p_wrap_depth -= 1
            else:
                out.append(m.group(0))
            pos = m.end()
            continue

        # открывающий <p> => table/tr/td
        if tag == "p":
            classes = _extract_classes_from_attrs(attr_text)
            rules = _merged_rules(styles_obj, "p", classes)
            td_style = _style_str_from_rules(rules)
            td_attr = f' style="{td_style}"' if td_style else ""
            out.append(f'<table style="{table_style}"><tr><td{td_attr}>')
            p_wrap_depth += 1
            pos = m.end()
            continue

        # default: любой другой открывающий тег => style по tag + .classes
        classes2 = _extract_classes_from_attrs(attr_text)
        rules2 = _merged_rules(styles_obj, tag, classes2)
        style2 = _style_str_from_rules(rules2)

        attrs2 = _drop_style_attr(attr_text)
        attrs2 = _drop_class_attr(attrs2)  # классы удаляем здесь

        if style2:
            out.append(f'<{tag}{attrs2} style="{style2}">')
        else:
            out.append(f"<{tag}{attrs2}>")

        pos = m.end()

    if pos < len(html0):
        out.append(html0[pos:])

    while p_wrap_depth > 0:
        out.append("</td></tr></table>")
        p_wrap_depth -= 1

    return "".join(out)


# ---- final render ----

def render_html(
    template_html: str,
    content_html: str,
    styles: StylesJSON,
    vars_json: Optional[Dict[str, Any]] = None,
) -> str:
    # 1) template + content (это body-фрагмент)
    body0 = (template_html or "").replace(PLACEHOLDER, content_html or "", 1)

    # 2) vars substitution (до sanitize, по договорённости)
    if vars_json:
        for k, v in vars_json.items():
            body0 = body0.replace(f"{{{{ {k} }}}}", "" if v is None else str(v))

    # 3) sanitize (по body-фрагменту)
    body0 = sanitize(body0)

    # 4) inline procedure (one pass)
    styles_obj = _parse_styles_json(styles)
    body0 = _inline_one_pass(body0, styles_obj)

    # 5) финальная обёртка (хардкод)
    return (
        "<html>"
        "<head>"
        '<meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
        "</head>"
        "<body>"
        + body0 +
        "</body>"
        "</html>"
    )
