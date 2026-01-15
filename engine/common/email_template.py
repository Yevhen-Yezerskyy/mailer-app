# FILE: engine/common/email_template.py  (обновлено — 2026-01-15)
# PURPOSE: Детерминированный пайплайн HTML-шаблонов писем.
# CHANGE: разрешён тег <content>; обёртка TinyMCE заменена table-><content class="yy_content_wrap">..</content>.

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
    "strong", "i", "a",
}

ALLOWED_ATTRS = {
    "class",
    "href",
    "colspan",
    "rowspan",
}

# ---- placeholder ----

PLACEHOLDER = "{{ ..content.. }}"

# ---- TinyMCE-safe wrapper ----

_EDITOR_WRAP_CLASS = "yy_content_wrap"


def _wrap_editor_content(inner_html: str) -> str:
    return (
        f'<table class="{_EDITOR_WRAP_CLASS}">'
        f"<tr><td>{inner_html}</td></tr>"
        f"</table>"
    )


def _unwrap_editor_content(html_text: str) -> str:
    m = re.search(
        rf'(?is)<table\b[^>]*class=["\'][^"\']*{_EDITOR_WRAP_CLASS}[^"\']*["\'][^>]*>'
        r'.*?<td>(.*?)</td>.*?</table>',
        html_text or "",
    )
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


# ---- sanitize (плоский, линейный) ----

_TAG_RE = re.compile(r"(?is)<(/?)([a-z0-9]+)([^>]*)>")
_ATTR_RE = re.compile(r'([a-z0-9_-]+)\s*=\s*(".*?"|\'.*?\'|[^\s>]+)', re.I)


def sanitize(html_text: str) -> str:
    html_text = html_text or ""
    out: list[str] = []
    pos = 0

    for m in _TAG_RE.finditer(html_text):
        if m.start() > pos:
            out.append(_html.escape(html_text[pos:m.start()], quote=False))

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
            attrs_out.append(f'{k}="{_html.escape(v, quote=True)}"')

        if attrs_out:
            out.append(f"<{tag} " + " ".join(attrs_out) + ">")
        else:
            out.append(f"<{tag}>")

        pos = m.end()

    if pos < len(html_text):
        out.append(_html.escape(html_text[pos:], quote=False))

    return "".join(out)


# ---- editor helpers ----

def editor_template_render_html(template_html: str, content_html: str) -> str:
    html0 = sanitize(template_html or "")
    wrapped = _wrap_editor_content(content_html or "")
    return html0.replace(PLACEHOLDER, wrapped, 1)


def editor_template_parse_html(editor_html: str) -> str:
    # FIX: не полагаемся на точный порядок атрибутов/пробелов
    base = sanitize(editor_html or "")
    base = re.sub(
        rf'(?is)<table\b[^>]*class=["\'][^"\']*{_EDITOR_WRAP_CLASS}[^"\']*["\'][^>]*>.*?</table>',
        PLACEHOLDER,
        base,
        count=1,
    )
    return base


def editor_render_html(html_text: str) -> str:
    return sanitize(html_text or "")


def editor_parse_html(html_text: str) -> str:
    return sanitize(html_text or "")


# ---- final render ----

def render_html(
    template_html: str,
    content_html: str,
    styles: StylesJSON,
    vars_json: Optional[Dict[str, Any]] = None,
) -> str:
    html0 = (template_html or "").replace(PLACEHOLDER, content_html or "", 1)
    html0 = sanitize(html0)

    if vars_json:
        for k, v in vars_json.items():
            html0 = html0.replace(f"{{{{ {k} }}}}", "" if v is None else str(v))

    styles_obj = _parse_styles_json(styles)
    for sel, rules in styles_obj.items():
        if not isinstance(rules, dict):
            continue
        style = "".join(f"{k}:{v};" for k, v in rules.items() if v is not None)
        if not style:
            continue
        html0 = re.sub(
            rf'(<{sel}\b[^>]*)(>)',
            rf'\1 style="{_html.escape(style, quote=True)}"\2',
            html0,
            flags=re.I,
        )

    html0 = re.sub(r'\sclass="[^"]*"', "", html0)
    return html0
