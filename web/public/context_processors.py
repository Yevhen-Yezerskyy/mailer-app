# FILE: web/public/context_processors.py
# DATE: 2026-03-07
# PURPOSE: language switch data for public templates.

from __future__ import annotations

from django.conf import settings


def language_switcher(request):
    current = (
        getattr(request, "ui_lang_code", "")
        or (getattr(request, "LANGUAGE_CODE", "") or "").split("-", 1)[0].lower()
    )
    available = {str(code).lower() for code, _name in getattr(settings, "LANGUAGES", [])}
    display_order = tuple(getattr(settings, "PUBLIC_LANG_SWITCH_ORDER", ()))
    labels = dict(getattr(settings, "UI_LANGUAGE_META", {}))

    items = []
    for code in display_order:
        if code not in available:
            continue
        items.append(
            {
                "code": code,
                "label": str(labels.get(code, {}).get("switch_label") or code.upper()),
                "active": code == current,
            }
        )

    return {"language_switcher": items}
