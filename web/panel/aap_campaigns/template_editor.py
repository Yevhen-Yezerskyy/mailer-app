# FILE: web/panel/aap_campaigns/template_editor.py  (новое — 2026-01-17)
# PURPOSE: Хелперы для user-editor (TinyMCE) и advanced<->user: placeholder, wrap/unwrap контента,
#          парсинг editor_html -> template_html, рендер template_html -> editor_html, CSS<->JSON.
# CHANGE: Вынесено из engine/common/email_template.py (engine/common оставляем только финальный рендер).

from __future__ import annotations

import json
import re
from typing import Any, Dict, Union

from engine.common.email_template import PLACEHOLDER, StylesJSON, sanitize

# ---- TinyMCE-safe wrapper ----

_EDITOR_WRAP_CLASS = "yy_content_wrap"


def wrap_editor_content(inner_html: str) -> str:
    # Сейчас используем table-обёртку (mceNonEditable).
    # Если решишь вернуться к <content> — меняем тут и regex ниже.
    return (
        f'<table class="{_EDITOR_WRAP_CLASS} mceNonEditable">'
        f"<tr><td>{inner_html or ''}</td></tr>"
        f"</table>"
    )


def unwrap_editor_content(html_text: str) -> str:
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


# ---- editor helpers ----

def editor_template_render_html(template_html: str, content_html: str) -> str:
    html0 = sanitize(template_html or "")
    wrapped = wrap_editor_content(content_html or "")
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
