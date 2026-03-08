# FILE: web/public/context_processors.py
# DATE: 2026-03-07
# PURPOSE: language switch data for public templates.

from __future__ import annotations

from django.conf import settings

_DISPLAY_ORDER = ("de", "uk", "ru", "en")
_DISPLAY_LABELS = {
    "de": "DEU",
    "uk": "UKR",
    "ru": "RUS",
    "en": "ENG",
}


def language_switcher(request):
    current = (getattr(request, "LANGUAGE_CODE", "") or "").split("-", 1)[0].lower()
    available = {str(code).lower() for code, _name in getattr(settings, "LANGUAGES", [])}

    items = []
    for code in _DISPLAY_ORDER:
        if code not in available:
            continue
        items.append(
            {
                "code": code,
                "label": _DISPLAY_LABELS.get(code, code.upper()),
                "active": code == current,
            }
        )

    return {"language_switcher": items}
