# FILE: engine/common/email_template.py  (обновлено — 2026-01-30)
# PURPOSE: Финальный рендер HTML-писем + (NEW) общий хелпер праздников DE для send-window.
# CHANGE: Добавлен импорт holidays БЕЗ try/except (если пакета нет — падаем), кеш праздников и _is_de_public_holiday().

from __future__ import annotations

import html as _html
import json
import re
import textwrap
from datetime import date
from datetime import datetime
from typing import Any, Dict, Optional, Union

import holidays  # если пакета нет — пусть валится нах
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

StylesJSON = Union[str, Dict[str, Dict[str, Any]], None]

# -------------------------
# holidays (DE-wide)
# -------------------------

_HOL_DE_CACHE: Dict[int, set[date]] = {}
_TZ = ZoneInfo("Europe/Berlin")


def _get_de_wide_holidays_for_year(y: int) -> set[date]:
    if y in _HOL_DE_CACHE:
        return _HOL_DE_CACHE[y]
    h = holidays.country_holidays("DE", years=[y])
    out = {d for d in h.keys() if isinstance(d, date)}
    _HOL_DE_CACHE[y] = set(out)
    return _HOL_DE_CACHE[y]


def _is_de_public_holiday(d: date) -> bool:
    return d in _get_de_wide_holidays_for_year(d.year)


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
    "align",
    "width", 
    "cellspacing", 
    "cellpadding",
    "border",
    "role",
}

# ---- placeholder ----

PLACEHOLDER = "{{ ..content.. }}"

# ---- vars/tools for message construction ----

DEFAULT_VARS: Dict[str, str] = {
    "company_name": "Unternehmen Adressat GmbH",
    "company_address": "Adressatenstraße 12, 40213 Düsseldorf, Nordrhein-Westfalen",
    "city": "Düsseldorf",
    "land": "Nordrhein-Westfalen",
    "city_land": "Düsseldorf, Nordrhein-Westfalen",
    "date_time": "12:00 21.01.2028",
    "date": "21.01.2028",
    "date_plus_1m": "21.02.2028",
    "date_plus_3m": "21.04.2028",
    "company_email": "adressat@unternehmen.de",
    "UTM": "smrel=0",
}

_RE_VAR = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")
_RE_A_HREF = re.compile(r'(<a\b[^>]*\bhref\s*=\s*)(["\'])([^"\']*)(\2)', re.IGNORECASE)
_RE_HAS_SMREL = re.compile(r"(^|[?&])smrel=", re.IGNORECASE)
_RE_TEXT_TAG = re.compile(r"<[^>]+>")
_RE_TEXT_BR = re.compile(r"<\s*br\s*/?\s*>", re.IGNORECASE)
_RE_TEXT_BLOCK_END = re.compile(
    r"</\s*(p|div|tr|table|ul|ol|li|h1|h2|h3|h4|h5|h6)\s*>",
    re.IGNORECASE,
)
_RE_TEXT_A = re.compile(
    r'<a\b[^>]*\bhref\s*=\s*(["\'])([^"\']*)\1[^>]*>(.*?)</a\s*>',
    re.IGNORECASE | re.DOTALL,
)
_RE_TEXT_WS = re.compile(r"[ \t]+")


def _safe_text(v: Any) -> str:
    return str(v or "").strip()


def _preview_vars(utm_value: Optional[str] = None) -> Dict[str, str]:
    out = dict(DEFAULT_VARS)
    if utm_value is not None:
        out["UTM"] = str(utm_value)
    return out


def build_send_vars(
    *,
    company_name: Any,
    company_email: Any,
    city: Any,
    land: Any,
    company_address: Any,
    utm: str,
    now: Optional[datetime] = None,
) -> Dict[str, str]:
    now_dt = now or datetime.now(tz=_TZ)
    d0 = now_dt.date()
    city_s = _safe_text(city)
    land_s = _safe_text(land)
    city_land = ", ".join([part for part in [city_s, land_s] if part]).strip()
    return {
        "company_name": _safe_text(company_name),
        "company_email": _safe_text(company_email),
        "city": city_s,
        "land": land_s,
        "city_land": city_land,
        "company_address": _safe_text(company_address),
        "date_time": now_dt.strftime("%H:%M %d.%m.%Y"),
        "date": now_dt.strftime("%d.%m.%Y"),
        "date_plus_1m": (d0 + relativedelta(months=+1)).strftime("%d.%m.%Y"),
        "date_plus_3m": (d0 + relativedelta(months=+3)).strftime("%d.%m.%Y"),
        "UTM": str(utm),
    }


def build_send_vars_from_contact(
    *,
    contact: Dict[str, Any],
    utm: str,
    now: Optional[datetime] = None,
) -> Dict[str, str]:
    data = contact if isinstance(contact, dict) else {}
    norm = data.get("norm")
    norm_obj = norm if isinstance(norm, dict) else {}
    return build_send_vars(
        company_name=data.get("company_name"),
        company_email=data.get("email"),
        city=norm_obj.get("city"),
        land=norm_obj.get("land"),
        company_address=norm_obj.get("address"),
        utm=utm,
        now=now,
    )


def apply_vars(
    html: str,
    rate_contact_id: Optional[int] = None,
    *,
    utm_value: Optional[str] = None,
    vars_map: Optional[Dict[str, Any]] = None,
) -> str:
    _ = rate_contact_id
    s = str(html or "")
    if vars_map is None:
        local_vars = _preview_vars(utm_value=utm_value)
    else:
        local_vars = {str(k): _safe_text(v) for k, v in vars_map.items() if str(k or "").strip()}
        if utm_value is not None:
            local_vars["UTM"] = str(utm_value)

    def rep(m: re.Match) -> str:
        key = m.group(1)
        return str(local_vars.get(key, m.group(0)))

    return _RE_VAR.sub(rep, s)


def unapply_vars(html: str, vars_map: Dict[str, str]) -> str:
    s = str(html or "")
    items = [(k, v) for k, v in (vars_map or {}).items() if k and v]
    items.sort(key=lambda x: len(str(x[1])), reverse=True)
    for k, v in items:
        s = re.sub(re.escape(str(v)), f"{{{{ {k} }}}}", s)
    return s


def add_smrel_to_links(html: str, utm: str) -> str:
    def rep(m: re.Match) -> str:
        url = m.group(3)
        if not url.startswith("http"):
            return m.group(0)
        if _RE_HAS_SMREL.search(url):
            return m.group(0)
        sep = "&" if "?" in url else "?"
        return f"{m.group(1)}{m.group(2)}{url}{sep}{utm}{m.group(2)}"

    return _RE_A_HREF.sub(rep, str(html or ""))


def html_to_text(s: str) -> str:
    def a_rep(m: re.Match) -> str:
        inner = _RE_TEXT_TAG.sub("", m.group(3) or "")
        inner = _html.unescape(inner).strip()
        url = _html.unescape(m.group(2) or "")
        return f"{inner} ({url})" if inner and url else inner or url

    s1 = _RE_TEXT_A.sub(a_rep, str(s or ""))
    s1 = _RE_TEXT_BR.sub("\n", s1)
    s1 = _RE_TEXT_BLOCK_END.sub("\n\n", s1)
    s1 = _RE_TEXT_TAG.sub("", s1)
    s1 = _html.unescape(s1)
    s1 = _RE_TEXT_WS.sub(" ", s1)
    return textwrap.fill(s1.strip(), width=78)


def build_send_bodies(html_template: str, vars_map: Dict[str, Any], utm: str) -> tuple[str, str]:
    html1 = apply_vars(str(html_template or ""), vars_map=vars_map)
    html2 = add_smrel_to_links(html1, str(utm or "").strip())
    text = html_to_text(html2)
    return html2, text

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
            out.append(f'<table style="{table_style}" width="100%" cellspacing="0" cellpadding="0" border="0" role="presentation"><tr><td{td_attr}>')
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
        '<body style="margin:0;padding:0;">'
        + body0 +
        "</body>"
        "</html>"
    )
