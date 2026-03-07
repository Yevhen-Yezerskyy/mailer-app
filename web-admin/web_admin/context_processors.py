# FILE: web-admin/web_admin/context_processors.py
# DATE: 2026-03-07
# PURPOSE: Build managed sidebar menu and page title for admin contour panel templates.

from django.urls import NoReverseMatch, reverse

from .menu import PANEL_MENU


def _safe_reverse(name: str) -> str:
    try:
        return reverse(name)
    except NoReverseMatch:
        return "#"


def _starts_with(path: str, prefixes: list[str]) -> bool:
    return any(path.startswith(p) for p in prefixes)


def panel_context(request):
    path = request.path or ""
    page_title = None

    menu = []
    for section in PANEL_MENU:
        sec = dict(section)
        sec["open"] = _starts_with(path, sec.get("open_prefixes", []))

        items = []
        for item in sec["items"]:
            it = dict(item)
            it["url"] = _safe_reverse(it["url_name"])
            it["active"] = _starts_with(path, it.get("active_prefixes", []))

            if it["active"] and not page_title:
                page_title = it.get("page_title")

            items.append(it)

        sec["items"] = items
        menu.append(sec)

    return {
        "panel_menu": menu,
        "page_title": page_title or "АДМИН-ПАНЕЛЬ",
    }
