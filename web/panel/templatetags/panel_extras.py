# FILE: web/panel/templatetags/panel_extras.py
# DATE: 2026-03-08
# PURPOSE: Numeric formatting helpers for panel templates.

from __future__ import annotations

from django import template

register = template.Library()


@register.filter(name="group_int")
def group_int(value, lang_code: str = "") -> str:
    try:
        n = int(value)
    except Exception:
        return "0"
    sep = "." if str(lang_code or "").lower().startswith("de") else " "
    sign = "-" if n < 0 else ""
    s = str(abs(n))
    parts = []
    while s:
        parts.append(s[-3:])
        s = s[:-3]
    return sign + sep.join(reversed(parts or ["0"]))

