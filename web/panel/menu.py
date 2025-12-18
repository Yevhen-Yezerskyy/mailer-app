# FILE: web/panel/menu.py  (новое — 2025-12-18)
# Смысл: единый источник правды для меню панели (порядок, i18n, title, urls)

from django.utils.translation import gettext_lazy as _

PANEL_MENU = [
    {
        # DASHBOARD (корень панели)
        "title": _("Dashboard"),
        "page_title": _("DASHBOARD : ОБЗОР"),
        "open_prefixes": ["/panel/"],
        "items": [
            {
                "title": _("Обзор"),
                "page_title": _("DASHBOARD : ОБЗОР"),
                "url_name": "dashboard",
                "active_prefixes": ["/panel/", "/panel/overview/"],
            },
        ],
    },
    {
        # AUDIENCE
        "title": _("Аудитория"),
        "open_prefixes": ["/panel/audience/"],
        "items": [
            {
                "title": _("Подбор"),
                "page_title": _("AUDIENCE : ПОДБОР"),
                "url_name": "audience:index",
                "active_prefixes": ["/panel/audience/"],
            },
            {
                "title": _("Списки"),
                "page_title": _("AUDIENCE : СПИСКИ"),
                "url_name": "audience:lists",
                "active_prefixes": ["/panel/audience/lists/"],
            },
            {
                "title": _("Импорт"),
                "page_title": _("AUDIENCE : ИМПОРТ"),
                "url_name": "audience:import",
                "active_prefixes": ["/panel/audience/import/"],
            },
        ],
    },
]
