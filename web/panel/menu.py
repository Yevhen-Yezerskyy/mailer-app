# FILE: web/panel/menu.py  (новое — 2025-12-18)
# Смысл: единый источник правды для меню панели (порядок, i18n, title, urls)

from django.utils.translation import gettext_lazy as _

PANEL_MENU = [
    {
        # DASHBOARD (корень панели)
        "title": _("Панель управления"),
        "page_title": _("Панель управления"),
        "open_prefixes": ["/panel/overview"],
        "items": [
            {
                "title": _("Обзор"),
                "page_title": _("Обзор : Панель управления"),
                "url_name": "dashboard",
                "active_prefixes": ["/panel/overview/"],
            },
        ],
    },
    {
        # AUDIENCE
        "title": _("Аудитория"),
        "open_prefixes": ["/panel/audience/"],
        "items": [
            {
                "title": _("Критерии выбора"),
                "page_title": _("Аудитория : Критерии выбора"),
                "url_name": "audience:index",
                "active_prefixes": ["/panel/audience/how/"],
            },
            {
                "title": _("Построение аудитории"),
                "page_title": _("Аудитория : Построение аудитории"),
                "url_name": "audience:clar",
                "active_prefixes": ["/panel/audience/clar/"],
            },
            {
                "title": _("Собранные компании"),
                "page_title": _("Аудитория : Собранные компании"),
                "url_name": "audience:status",
                "active_prefixes": ["/panel/audience/status/"],
            },
        ],
    },
    {
        # AUDIENCE
    "title": _("Настройки"),
        "open_prefixes": ["/panel/settings/"],
        "items": [
            {
                "title": _("Отправка писем"),
                "page_title": _("Настройки : Отправка писем"),
                "url_name": "settings:sending",
                "active_prefixes": ["/panel/settings/sending/"],
            },
            {
                "title": _("Почтовые серверы"),
                "page_title": _("Настройки : Почтовые серверы"),
                "url_name": "settings:mail_servers",
                "active_prefixes": ["/panel/settings/mail-servers/"],
            },

        ],
    },
]
