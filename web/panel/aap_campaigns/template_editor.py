# FILE: web/panel/aap_campaigns/template_editor.py
# DATE: 2026-01-18
# PURPOSE: Хелперы для user-editor (TinyMCE) и advanced<->user: placeholder + wrap/unwrap контента,
#          editor_html <-> template_html, CSS<->JSON.
# CHANGE:
#   - FIX: Wrapper-таблица идентифицируется по class="yy_content_wrap" (то, что реально сохраняет Tiny/sanitize).
#   - FIX: Замена wrapper -> PLACEHOLDER выполняется ДО sanitize(), чтобы sanitize не ломал якоря/структуру.
#   - FIX nested <table>: конец wrapper определяется подсчётом вложенных <table>...</table>, а не regex'ом по </table>.

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional, Tuple

from engine.common.email_template import PLACEHOLDER, StylesJSON, sanitize

# ---- Wrapper (Tiny-safe) ----

_EDITOR_WRAP_CLASS = "yy_content_wrap"


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


# ---- editor helpers ----

def editor_template_render_html(template_html: str, content_html: str) -> str:
    # sanitize каркаса можно делать тут (PLACEHOLDER всё равно строкой заменяем)
    html0 = sanitize(template_html or "")
    wrapped = wrap_editor_content(content_html or "")
    return html0.replace(PLACEHOLDER, wrapped, 1)


def editor_template_parse_html(editor_html: str) -> str:
    """
    Заменяем wrapper-table (class содержит yy_content_wrap) обратно на PLACEHOLDER.
    Важно: сначала делаем замену (по span), и только потом sanitize().
    """
    raw = editor_html or ""

    span = _find_wrapper_table_span(raw)
    if not span:
        # fallback: хотя бы sanitize, чтобы не тащить мусор
        return sanitize(raw)

    replaced = raw[: span[0]] + PLACEHOLDER + raw[span[1] :]
    return sanitize(replaced)
