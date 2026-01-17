# FILE: engine/common/email_template.py  (обновлено — 2026-01-17)
# PURPOSE: Детерминированный финальный рендер HTML-писем: склейка template+content, подмена переменных,
#          sanitize (whitelist), разворачивание styles_json в inline, удаление class-атрибутов.
# CHANGE: Вынесены editor-специфичные штуки (PLACEHOLDER/wrap/parse/CSS<->JSON helpers) в aap_campaigns.
#         В тексте sanitize делает только замену "<"->"&lt;" и ">"->"&gt;" (без html.escape).

from __future__ import annotations

import html as _html
import json
import re
from typing import Any, Dict, Optional, Union

StylesJSON = Union[str, Dict[str, Dict[str, Any]], None]

# ---- whitelist ----

ALLOWED_TAGS = {
    "html", "head", "body",
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
            # attrs: экранируем для корректного HTML (не текст-нод)
            attrs_out.append(f'{k}="{_html.escape(v, quote=True)}"')

        if attrs_out:
            out.append(f"<{tag} " + " ".join(attrs_out) + ">")
        else:
            out.append(f"<{tag}>")

        pos = m.end()

    if pos < len(html_text):
        out.append(_escape_text_minimal(html_text[pos:]))

    return "".join(out)


# ---- inline styles helpers ----

_SEL_TAG_RE = re.compile(r"^[a-z0-9]+$", re.I)
_SEL_CLASS_RE = re.compile(r"^\.[a-z0-9_-]+$", re.I)


def _merge_style_attr(attrs_text: str, add_style: str) -> str:
    """
    attrs_text — это часть внутри тега между '<tag' и '>'.
    add_style — уже собранная строка 'k:v;k2:v2;'
    """
    add_style = (add_style or "").strip()
    if not add_style:
        return attrs_text

    m = re.search(r'(?is)\bstyle\s*=\s*"([^"]*)"', attrs_text or "")
    if not m:
        return (attrs_text or "") + f' style="{_html.escape(add_style, quote=True)}"'

    old = m.group(1) or ""
    merged = old + ("" if old.endswith(";") or not old else ";") + add_style
    merged_esc = _html.escape(merged, quote=True)
    return (attrs_text or "")[: m.start()] + f'style="{merged_esc}"' + (attrs_text or "")[m.end() :]


def _apply_inline_for_tag(html0: str, tag: str, style: str) -> str:
    tag_l = tag.lower()

    pat = re.compile(rf'(?is)<{re.escape(tag_l)}\b([^>]*)>')
    def repl(m: re.Match) -> str:
        attrs = m.group(1) or ""
        new_attrs = _merge_style_attr(attrs, style)
        return f"<{tag_l}{new_attrs}>"

    return pat.sub(repl, html0)


def _apply_inline_for_class(html0: str, cls: str, style: str) -> str:
    cls = cls.strip().lstrip(".")
    if not cls:
        return html0

    # Ищем любой открывающий тег с class="... cls ..."
    pat = re.compile(
        rf'(?is)<([a-z0-9]+)\b([^>]*\bclass\s*=\s*"[^"]*\b{re.escape(cls)}\b[^"]*"[^>]*)>'
    )

    def repl(m: re.Match) -> str:
        tag = (m.group(1) or "").lower()
        attrs = m.group(2) or ""
        new_attrs = _merge_style_attr(attrs, style)
        return f"<{tag}{new_attrs}>"

    return pat.sub(repl, html0)


def _inline_style_apply(html0: str, selector: str, rules: Dict[str, Any]) -> str:
    if not selector or not isinstance(rules, dict):
        return html0

    style = "".join(f"{k}:{v};" for k, v in rules.items() if v is not None)
    if not style:
        return html0

    sel = selector.strip()
    if _SEL_TAG_RE.match(sel):
        return _apply_inline_for_tag(html0, sel, style)

    if _SEL_CLASS_RE.match(sel):
        return _apply_inline_for_class(html0, sel, style)

    # Остальные селекторы (p strong, #id, [attr]) — пока игнорируем сознательно.
    return html0


# ---- final render ----

def render_html(
    template_html: str,
    content_html: str,
    styles: StylesJSON,
    vars_json: Optional[Dict[str, Any]] = None,
) -> str:
    # 1) template + content
    html0 = (template_html or "").replace(PLACEHOLDER, content_html or "", 1)

    # 2) vars substitution (до sanitize, по договорённости)
    if vars_json:
        for k, v in vars_json.items():
            html0 = html0.replace(f"{{{{ {k} }}}}", "" if v is None else str(v))

    # 3) sanitize
    html0 = sanitize(html0)

    # 4) inline styles
    styles_obj = _parse_styles_json(styles)
    for sel, rules in styles_obj.items():
        html0 = _inline_style_apply(html0, str(sel), rules)

    # 5) drop class attributes
    html0 = re.sub(r'\sclass="[^"]*"', "", html0)

    return html0
