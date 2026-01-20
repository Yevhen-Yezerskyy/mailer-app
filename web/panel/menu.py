# FILE: web/panel/menu.py
# DATE: 2026-01-14
# PURPOSE: Единый источник правды для меню панели.
# CHANGE: Добавлен раздел "Кампании" с пунктами "Кампании" и "Шаблоны писем".

from django.utils.translation import gettext_lazy as _

PANEL_MENU = [
    {
        # DASHBOARD
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
        "title": _("Аудитории"),
        "open_prefixes": ["/panel/audience/"],
        "items": [
            {
                "title": _("Создание аудиторий"),
                "page_title": _("Аудитории : создание"),
                "url_name": "audience:index",
                "active_prefixes": ["/panel/audience/how/"],
            },
            {
                "title": _("Города и категории"),
                "page_title": _("Города и бизнес-категории"),
                "url_name": "audience:clar",
                "active_prefixes": ["/panel/audience/clar/"],
            },
            {
                "title": _("Сбор контактов / Рейтинг"),
                "page_title": _("Аудитория : Собранные компании"),
                "url_name": "audience:status",
                "active_prefixes": ["/panel/audience/status/"],
            },
        ],
    },
    {
        # CONTACTS / LISTS
        "title": _("Контакты"),
        "open_prefixes": ["/panel/lists/"],
        "items": [
            {
                "title": _("Списки рассылок"),
                "page_title": _("Контакты : Списки рассылок"),
                "url_name": "lists:lists",
                "active_prefixes": ["/panel/lists/lists/"],
            },
            {
                "title": _("Все контакты"),
                "page_title": _("Контакты : Все контакты"),
                "url_name": "lists:contacts",
                "active_prefixes": ["/panel/lists/contacts/"],
            },
        ],
    },
    {
        # CAMPAIGNS
        "title": _("Кампании"),
        "open_prefixes": ["/panel/campaigns/"],
        "items": [
            {
                "title": _("Кампании"),
                "page_title": _("Кампании"),
                "url_name": "campaigns:campaigns",
                "active_prefixes": ["/panel/campaigns/campaigns/"],
            },
            {
                "title": _("Шаблоны писем"),
                "page_title": _("Кампании : Шаблоны писем"),
                "url_name": "campaigns:templates",
                "active_prefixes": ["/panel/campaigns/templates/"],
            },
        ],
    },
    {
        # SETTINGS
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
