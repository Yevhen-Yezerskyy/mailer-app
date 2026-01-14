# FILE: engine/common/email_template.py  (новое)
# DATE: 2026-01-14
# PURPOSE: Единый прозрачный пайплайн для писем/редактора:
#          - styles_json <-> css (в нашем <style data-yy-style="1">)
#          - render_for_editor: вставка style-tag + demo-блок вместо {{ ..content.. }} по меткам
#          - render_for_preview: style-tag или inline (tag + class override)
#          - normalize_for_store: вырезает служебное, возвращает {{ ..content.. }}, жёстко санитайзит HTML по whitelist.

from __future__ import annotations

import html as _html
import json
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, Optional, Tuple, Union

StylesJSON = Union[str, Dict[str, Dict[str, Any]], None]

# ---- правила (утверждено) ----

ALLOWED_TAGS = {
    "html",
    "head",
    "body",
    "table",
    "tbody",
    "thead",
    "tfoot",
    "tr",
    "td",
    "th",
    "p",
    "br",
    "hr",
    "h1",
    "h2",
    "h3",
    "h4",
    "strong",
    "i",
    "a",
}

# общие атрибуты
_ALLOWED_ATTRS_COMMON = {"class"}

# точечные атрибуты
_ALLOWED_ATTRS_BY_TAG = {
    "td": {"colspan", "rowspan"},
    "th": {"colspan", "rowspan"},
    "a": {"href"},
}

# ---- служебные метки/теги ----

STYLE_TAG_ATTR = "data-yy-style"
STYLE_TAG_ATTR_VAL = "1"

YY_CONTENT_BEGIN = "<!--YY:CONTENT:BEGIN-->"
YY_CONTENT_END = "<!--YY:CONTENT:END-->"
YY_STYLE_BEGIN = "<!--YY:STYLE:BEGIN-->"
YY_STYLE_END = "<!--YY:STYLE:END-->"

# placeholder
PLACEHOLDER_CANON = "{{ ..content.. }}"
_PLACEHOLDER_RE = re.compile(r"\{\{\s*\.\.content\.\.\s*\}\}")

# ---- JSON <-> CSS ----

def _parse_styles_json(styles: StylesJSON) -> Dict[str, Dict[str, Any]]:
    if styles is None:
        return {}
    if isinstance(styles, dict):
        return styles if isinstance(styles, dict) else {}
    if isinstance(styles, str):
        s = styles.strip()
        if not s:
            return {}
        try:
            v = json.loads(s)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def styles_json_to_css(styles: StylesJSON) -> str:
    """
    Детерминированный CSS из JSON:
    { "p": {"color":"red","font-size":"12px"}, ".x": {...} } -> "p{color:red;font-size:12px;}\n.x{...}"
    """
    obj = _parse_styles_json(styles)
    out: list[str] = []

    for sel in sorted(obj.keys()):
        rules = obj.get(sel)
        if not isinstance(sel, str) or not sel.strip():
            continue
        if not isinstance(rules, dict) or not rules:
            continue

        decls: list[str] = []
        for prop in sorted(rules.keys()):
            val = rules.get(prop)
            if not isinstance(prop, str) or not prop.strip():
                continue
            if val is None:
                continue
            decls.append(f"{prop}:{val};")

        if decls:
            out.append(f"{sel}{{{''.join(decls)}}}")

    return "\n".join(out)


_css_block_re = re.compile(r"(?s)(?P<sel>[^{]+)\{(?P<body>[^}]*)\}")


def styles_css_to_json(css_text: str) -> Dict[str, Dict[str, str]]:
    """
    Парсим только наш простой формат: selector{prop:val;prop2:val2;}
    Всё “сложное” игнорируем.
    """
    css_text = (css_text or "").strip()
    if not css_text:
        return {}

    out: Dict[str, Dict[str, str]] = {}
    for m in _css_block_re.finditer(css_text):
        sel = (m.group("sel") or "").strip()
        body = (m.group("body") or "").strip()
        if not sel or not body:
            continue

        rules: Dict[str, str] = {}
        for part in body.split(";"):
            part = part.strip()
            if not part:
                continue
            if ":" not in part:
                continue
            k, v = part.split(":", 1)
            k = k.strip()
            v = v.strip()
            if not k:
                continue
            rules[k] = v

        if rules:
            out[sel] = rules

    return out


def wrap_editor_style_tag(css_text: str) -> str:
    css_text = (css_text or "").strip()
    if not css_text:
        return ""
    return f"<style {STYLE_TAG_ATTR}=\"{STYLE_TAG_ATTR_VAL}\">\n{css_text}\n</style>"


def extract_editor_style_tag(html_text: str) -> Tuple[str, str]:
    """
    Вырезает <style data-yy-style="1">...</style> и возвращает (html_без_него, css_text).
    Если нет — css_text="".
    """
    html_text = html_text or ""
    # очень простая вырезалка (мы генерим сами, этого хватает)
    re_style = re.compile(
        rf"(?is)<style\b[^>]*\b{re.escape(STYLE_TAG_ATTR)}\s*=\s*[\"']{re.escape(STYLE_TAG_ATTR_VAL)}[\"'][^>]*>(?P<css>.*?)</style>"
    )
    m = re_style.search(html_text)
    if not m:
        return html_text, ""
    css = (m.group("css") or "").strip()
    html_wo = html_text[: m.start()] + html_text[m.end() :]
    return html_wo, css


# ---- vars подстановка (опционально; прозрачная) ----

_VAR_RE = re.compile(r"\{\{\s*(?P<key>[^}]+?)\s*\}\}")

def subst_vars(html_text: str, vars_json: Optional[Dict[str, Any]]) -> str:
    if not html_text or not vars_json:
        return html_text or ""

    def repl(m: re.Match) -> str:
        key = (m.group("key") or "").strip()
        if key in vars_json:
            v = vars_json[key]
            return "" if v is None else str(v)
        return m.group(0)

    return _VAR_RE.sub(repl, html_text)


# ---- sanitizer (жёсткий whitelist) ----

class _WhitelistSanitizer(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self._out: list[str] = []
        self._stack: list[str] = []

    def handle_starttag(self, tag: str, attrs: Iterable[Tuple[str, Optional[str]]]):
        t = (tag or "").lower()
        if t not in ALLOWED_TAGS:
            return

        allowed = set(_ALLOWED_ATTRS_COMMON)
        allowed |= _ALLOWED_ATTRS_BY_TAG.get(t, set())

        # собираем attrs: только разрешённые
        parts: list[str] = []
        for k, v in attrs:
            if not k:
                continue
            kk = k.lower()
            if kk not in allowed:
                continue
            if v is None:
                continue
            vv = _html.escape(str(v), quote=True)
            parts.append(f'{kk}="{vv}"')

        if parts:
            self._out.append(f"<{t} " + " ".join(parts) + ">")
        else:
            self._out.append(f"<{t}>")

        # void tags
        if t in ("br", "hr"):
            return

        self._stack.append(t)

    def handle_endtag(self, tag: str):
        t = (tag or "").lower()
        if t in ("br", "hr"):
            return
        # закрываем только если реально был открыт
        for i in range(len(self._stack) - 1, -1, -1):
            if self._stack[i] == t:
                self._stack.pop(i)
                self._out.append(f"</{t}>")
                return

    def handle_data(self, data: str):
        if not data:
            return
        self._out.append(_html.escape(data, quote=False))

    def handle_entityref(self, name: str):
        if name:
            self._out.append(f"&{name};")

    def handle_charref(self, name: str):
        if name:
            self._out.append(f"&#{name};")

    # комментарии/decl/PI просто игнорируем (и это хорошо)

    def get_html(self) -> str:
        # на всякий: закрываем всё, что осталось (в правильном порядке)
        while self._stack:
            t = self._stack.pop()
            self._out.append(f"</{t}>")
        return "".join(self._out)


def sanitize_stored_html(html_text: str) -> str:
    p = _WhitelistSanitizer()
    p.feed(html_text or "")
    return p.get_html()


# ---- inline styles (прозрачно: tag + class override) ----

def _style_dict_to_css_inline(d: Dict[str, Any]) -> str:
    out: list[str] = []
    for k, v in d.items():
        if not isinstance(k, str) or not k.strip():
            continue
        if v is None:
            continue
        out.append(f"{k}:{v};")
    return "".join(out)


class _InlineStyler(HTMLParser):
    """
    Применяет inline по правилам:
    - selector "p" -> всем <p>
    - selector ".cls" -> override для тегов с class containing cls
    - если есть и tag, и class — tag сначала, потом class (оверрайд)
    """
    def __init__(self, styles_obj: Dict[str, Dict[str, Any]]):
        super().__init__(convert_charrefs=False)
        self._out: list[str] = []
        self._stack: list[str] = []
        self.styles_obj = styles_obj or {}

        self.tag_styles: Dict[str, str] = {}
        self.class_styles: Dict[str, str] = {}

        for sel, rules in (self.styles_obj or {}).items():
            if not isinstance(sel, str) or not isinstance(rules, dict):
                continue
            sel = sel.strip()
            if not sel:
                continue
            css_inline = _style_dict_to_css_inline(rules)
            if not css_inline:
                continue
            if sel.startswith(".") and " " not in sel and len(sel) > 1:
                self.class_styles[sel[1:]] = css_inline
            elif sel.isidentifier():
                self.tag_styles[sel.lower()] = css_inline

    def handle_starttag(self, tag: str, attrs: Iterable[Tuple[str, Optional[str]]]):
        t = (tag or "").lower()
        if t not in ALLOWED_TAGS:
            return

        # берём только class/colspan/rowspan/href, НО style будем добавлять сами (в preview/mail)
        allowed = set(_ALLOWED_ATTRS_COMMON)
        allowed |= _ALLOWED_ATTRS_BY_TAG.get(t, set())

        attrs_map: Dict[str, str] = {}
        for k, v in attrs:
            if not k or v is None:
                continue
            kk = k.lower()
            if kk in allowed:
                attrs_map[kk] = str(v)

        # inline style = tag + class override
        style = self.tag_styles.get(t, "")
        cls = attrs_map.get("class", "")
        if cls:
            for c in cls.split():
                if c in self.class_styles:
                    style += self.class_styles[c]

        # соберём атрибуты: разрешённые + style (если есть)
        parts: list[str] = []
        for kk in sorted(attrs_map.keys()):
            vv = _html.escape(attrs_map[kk], quote=True)
            parts.append(f'{kk}="{vv}"')
        if style:
            parts.append(f'style="{_html.escape(style, quote=True)}"')

        if parts:
            self._out.append(f"<{t} " + " ".join(parts) + ">")
        else:
            self._out.append(f"<{t}>")

        if t in ("br", "hr"):
            return
        self._stack.append(t)

    def handle_endtag(self, tag: str):
        t = (tag or "").lower()
        if t in ("br", "hr"):
            return
        for i in range(len(self._stack) - 1, -1, -1):
            if self._stack[i] == t:
                self._stack.pop(i)
                self._out.append(f"</{t}>")
                return

    def handle_data(self, data: str):
        if data:
            self._out.append(_html.escape(data, quote=False))

    def handle_entityref(self, name: str):
        if name:
            self._out.append(f"&{name};")

    def handle_charref(self, name: str):
        if name:
            self._out.append(f"&#{name};")

    def get_html(self) -> str:
        while self._stack:
            t = self._stack.pop()
            self._out.append(f"</{t}>")
        return "".join(self._out)


def render_inline(html_text: str, styles: StylesJSON) -> str:
    obj = _parse_styles_json(styles)
    p = _InlineStyler(obj)
    p.feed(html_text or "")
    return p.get_html()


# ---- editor render / normalize ----

@dataclass
class EditorRenderResult:
    html_for_editor: str
    css_text: str


def render_for_editor(template_html: str, styles: StylesJSON, demo_html: str) -> EditorRenderResult:
    """
    1) Берём хранимый template_html (уже "чистый")
    2) Заменяем {{..content..}} на YY-метки+demo
    3) Вставляем <style data-yy-style="1"> из JSON (в начало)
    """
    css = styles_json_to_css(styles)
    st = wrap_editor_style_tag(css)

    html0 = template_html or ""
    if _PLACEHOLDER_RE.search(html0):
        block = YY_CONTENT_BEGIN + (demo_html or "") + YY_CONTENT_END
        html0 = _PLACEHOLDER_RE.sub(block, html0, count=1)
    else:
        # если плейсхолдера нет — просто дописываем demo в конец (чтобы user-mode видел)
        html0 = html0 + YY_CONTENT_BEGIN + (demo_html or "") + YY_CONTENT_END

    # style-tag вставляем самым простым образом: префикс
    html_for_editor = (st + "\n" if st else "") + html0
    return EditorRenderResult(html_for_editor=html_for_editor, css_text=css)


@dataclass
class NormalizeResult:
    clean_template_html: str
    styles_json_obj: Dict[str, Dict[str, str]]


def normalize_for_store(editor_html: str) -> NormalizeResult:
    """
    Вход: HTML из user-mode (Quill), содержащий:
    - <style data-yy-style="1">...</style> (наш)
    - YY:CONTENT:BEGIN/END (наш)
    Выход:
    - чистый хранимый template_html (только whitelist + class/colspan/rowspan/href, без стилей)
    - styles_json (восстановленный из style-tag)
    """
    s = editor_html or ""

    # 1) забираем CSS из нашего style-tag и удаляем его из html
    s, css_text = extract_editor_style_tag(s)
    styles_obj = styles_css_to_json(css_text)

    # 2) заменить участок между метками на {{ ..content.. }}
    if YY_CONTENT_BEGIN in s and YY_CONTENT_END in s:
        re_block = re.compile(re.escape(YY_CONTENT_BEGIN) + r"(?s).*?" + re.escape(YY_CONTENT_END))
        s = re_block.sub(PLACEHOLDER_CANON, s, count=1)

    # 3) убрать любые YY-метки (на всякий)
    s = s.replace(YY_CONTENT_BEGIN, "").replace(YY_CONTENT_END, "")
    s = s.replace(YY_STYLE_BEGIN, "").replace(YY_STYLE_END, "")

    # 4) жёсткий sanitizer
    clean = sanitize_stored_html(s)

    return NormalizeResult(clean_template_html=clean, styles_json_obj=styles_obj)


def render_for_preview(template_html: str, styles: StylesJSON, mode: str, vars_json: Optional[Dict[str, Any]] = None) -> str:
    """
    mode:
      - "style_tag": вставить <style data-yy-style="1"> (для превью)
      - "inline": применить inline (tag + class override)
    vars_json: опциональная простая подстановка {{ key }} -> value.
    """
    html0 = template_html or ""
    html0 = subst_vars(html0, vars_json)

    # Превью тоже показывает demo вместо плейсхолдера (если он есть)
    if _PLACEHOLDER_RE.search(html0):
        html0 = _PLACEHOLDER_RE.sub("<p>[CONTENT]</p>", html0, count=1)

    if mode == "inline":
        return render_inline(html0, styles)

    css = styles_json_to_css(styles)
    st = wrap_editor_style_tag(css)
    return (st + "\n" if st else "") + html0
