# FILE: web/panel/context_processors.py  (обновлено — 2025-12-18)
# Смысл: подготовка меню панели + вычисление active/open + page_title

from django.urls import reverse, NoReverseMatch
from django.utils.translation import gettext as _trans

from .menu import PANEL_MENU


def _safe_reverse(name):
    try:
        return reverse(name)
    except NoReverseMatch:
        return "#"


def _starts_with(path, prefixes):
    return any(path.startswith(p) for p in prefixes)


def panel_context(request):
    path = request.path or ""
    page_title = None

    menu = []
    for section in PANEL_MENU:
        sec = dict(section)
        sec["open"] = _starts_with(path, sec.get("open_prefixes", []))
        if sec.get("url"):
            sec["url"] = sec["url"]
        elif sec.get("url_name"):
            sec["url"] = _safe_reverse(sec["url_name"])

        items = []
        for item in sec["items"]:
            it = dict(item)
            if it.get("url"):
                it["url"] = it["url"]
            else:
                it["url"] = _safe_reverse(it["url_name"])
            it["active"] = _starts_with(path, it.get("active_prefixes", []))

            if it["active"] and not page_title:
                page_title = it.get("page_title")

            items.append(it)

        sec["items"] = items
        sec["open"] = sec.get("open") or any(bool(it.get("active")) for it in items)
        menu.append(sec)

    return {
        "panel_menu": menu,
        "page_title": page_title or _trans("SERENITY PANEL"),
    }
