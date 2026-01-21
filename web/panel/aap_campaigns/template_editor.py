# FILE: web/panel/aap_campaigns/template_editor.py
# DATE: 2026-01-21
# PURPOSE: Единый центр HTML-операций для editors (Templates + Letters).
# CHANGE:
# - Расширен _DEFAULT_TEMPLATE_VARS (company_name/city_land/date_time/city/UTM) для demo-подстановки.
# - FIX: letter_editor_extract_content теперь делает reverse-vars: demo-значения -> {{ var }} (как в шаблонах),
#        чтобы при сохранении не терялись переменные.

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional, Tuple

from engine.common.email_template import PLACEHOLDER, StylesJSON, sanitize
from panel.models import GlobalTemplate

# ---- Wrapper (Tiny-safe) ----

_EDITOR_WRAP_CLASS = "yy_content_wrap"

# ---- Default vars (for {{ var }}) ----

_DEFAULT_TEMPLATE_VARS: Dict[str, str] = {
    "company_name": "Unternehmen Adressat GmbH",
    "city_land": "Köln, Nordrhein-Westfalen",
    "date_time": "12:00 21.01.2028",
    "city": "Hauptstadt",
    "UTM": "smrel=132246897659",
}


def default_template_vars() -> Dict[str, str]:
    return dict(_DEFAULT_TEMPLATE_VARS)


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


def _unapply_template_vars(html: str, vars_dict: Dict[str, str]) -> str:
    """
    Reverse: demo-строки -> {{ var }}.
    Делает простые replace, длинные значения — первыми.
    """
    s = html or ""
    if not s or not isinstance(vars_dict, dict) or not vars_dict:
        return s

    pairs = [(k, str(v)) for k, v in vars_dict.items() if k and v is not None and str(v)]
    pairs.sort(key=lambda kv: len(kv[1]), reverse=True)

    for key, val in pairs:
        s = s.replace(val, f"{{{{ {key} }}}}")
    return s


def wrap_editor_content(inner_html: str) -> str:
    return (
        f'<table class="{_EDITOR_WRAP_CLASS}">'
        f"<tr><td>{inner_html or ''}</td></tr>"
        f"</table>"
    )


def _find_wrapper_table_span(html_text: str) -> Optional[Tuple[int, int]]:
    s = html_text or ""
    if not s:
        return None

    m = re.search(
        rf'(?is)<table\b[^>]*\bclass\s*=\s*["\'][^"\']*\b{re.escape(_EDITOR_WRAP_CLASS)}\b[^"\']*["\'][^>]*>',
        s,
    )
    if not m:
        return None

    start = m.start()
    pos = m.end()

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
                return (start, t.end())
        else:
            depth += 1

        pos = t.end()


def _find_first_td_inner(wrapper_html: str) -> str:
    s = wrapper_html or ""
    if not s:
        return ""

    m = re.search(r"(?is)<td\b[^>]*>", s)
    if not m:
        return ""

    inner_start = m.end()
    pos = inner_start

    depth = 1
    token_re = re.compile(r"(?is)<td\b[^>]*>|</td\s*>")

    while True:
        t = token_re.search(s, pos)
        if not t:
            return ""

        tok = t.group(0).lower()
        if tok.startswith("</td"):
            depth -= 1
            if depth == 0:
                inner_end = t.start()
                return s[inner_start:inner_end]
        else:
            depth += 1

        pos = t.end()


def unwrap_editor_content(html_text: str) -> str:
    span = _find_wrapper_table_span(html_text or "")
    if not span:
        return ""

    wrapper = (html_text or "")[span[0] : span[1]]
    return _find_first_td_inner(wrapper)


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

_TAG_RE = re.compile(r"(?is)<([a-zA-Z][a-zA-Z0-9:_-]*)([^<>]*?)>")

_VOID_TAGS = {"br", "hr", "meta"}


def _apply_tiny_editability(html: str) -> str:
    if not html:
        return ""

    def update_tag(m: re.Match) -> str:
        tag = m.group(0)
        name = (m.group(1) or "").lower()
        attrs = m.group(2) or ""

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


def _apply_tiny_force(html: str, mode: str) -> str:
    if not html:
        return ""

    want = _TINY_EDIT if mode == "edit" else _TINY_NONEDIT
    drop = _TINY_NONEDIT if want == _TINY_EDIT else _TINY_EDIT

    def update_tag(m: re.Match) -> str:
        tag = m.group(0)
        name = (m.group(1) or "").lower()
        attrs = m.group(2) or ""

        if name in ("script", "style") or name in _VOID_TAGS:
            return tag

        m_class = re.search(r'(?is)\bclass\s*=\s*(?P<q>["\'])(?P<v>.*?)(?P=q)', attrs)
        classes: list[str] = []
        if m_class:
            classes = [c for c in (m_class.group("v") or "").split() if c]

        classes = [c for c in classes if c not in (drop, want)]
        classes.append(want)

        new_class_value = " ".join(classes).strip()

        if m_class:
            q = m_class.group("q")
            new_attrs = attrs[: m_class.start()] + f'class={q}{new_class_value}{q}' + attrs[m_class.end() :]
        else:
            new_attrs = attrs if (attrs or "").startswith(" ") else (" " + (attrs or ""))
            new_attrs = f' class="{new_class_value}"' + new_attrs

        return f"<{m.group(1)}{new_attrs}>"

    return _TAG_RE.sub(update_tag, html)


# ---- Templates editor helpers ----

def editor_template_render_html(template_html: str, content_html: str) -> str:
    wrapped = wrap_editor_content(content_html or "")
    html1 = (template_html or "").replace(PLACEHOLDER, wrapped, 1)

    vars_dict = dict(_DEFAULT_TEMPLATE_VARS)
    html2 = _apply_template_vars(html1, vars_dict)

    html3 = sanitize(html2)
    return _apply_tiny_editability(html3)


def editor_template_parse_html(editor_html: str) -> str:
    raw0 = _strip_tiny_edit_classes(editor_html or "")
    raw = _unapply_template_vars(raw0, _DEFAULT_TEMPLATE_VARS)

    span = _find_wrapper_table_span(raw)
    if not span:
        return sanitize(raw)

    replaced = raw[: span[0]] + PLACEHOLDER + raw[span[1] :]
    return sanitize(replaced)


# ==================== Letter editor helpers ====================

_DEMO_FALLBACK_HTML = "<p>[DEMO CONTENT]</p>"


def _extract_global_template_id_from_first_tag(template_html: str) -> int | None:
    s = (template_html or "").lstrip()
    if not s:
        return None

    m_tag = re.search(r"(?is)<\s*([a-zA-Z][a-zA-Z0-9:_-]*)([^>]*)>", s)
    if not m_tag:
        return None

    attrs = m_tag.group(2) or ""
    m_class = re.search(r"""\bclass\s*=\s*(?P<q>["'])(?P<v>.*?)(?P=q)""", attrs, flags=re.IGNORECASE | re.DOTALL)
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


def find_demo_content_from_template(template_html: str) -> str:
    pk = _extract_global_template_id_from_first_tag(template_html or "")
    if not pk:
        return _DEMO_FALLBACK_HTML

    gt = GlobalTemplate.objects.filter(id=pk, is_active=True).first()
    if not gt:
        return _DEMO_FALLBACK_HTML

    html = (gt.html_content or "").strip()
    return html or _DEMO_FALLBACK_HTML


def letter_editor_render_html(template_html: str, content_html: str) -> str:
    wrapped = wrap_editor_content(content_html or "")
    html1 = (template_html or "").replace(PLACEHOLDER, wrapped, 1)

    vars_dict = dict(_DEFAULT_TEMPLATE_VARS)
    html2 = _apply_template_vars(html1, vars_dict)

    raw = sanitize(html2)

    span = _find_wrapper_table_span(raw)
    if not span:
        return _apply_tiny_force(raw, "edit")

    before = raw[: span[0]]
    mid = raw[span[0] : span[1]]
    after = raw[span[1] :]

    return _apply_tiny_force(before, "nonedit") + _apply_tiny_force(mid, "edit") + _apply_tiny_force(after, "nonedit")


def letter_editor_extract_content(editor_html: str) -> str:
    raw0 = _strip_tiny_edit_classes(editor_html or "")
    inner0 = unwrap_editor_content(raw0) or ""

    # FIX: reverse demo-vars обратно в {{ var }} (как в templates)
    inner1 = _unapply_template_vars(inner0, _DEFAULT_TEMPLATE_VARS)

    return sanitize(inner1 or "")
