# FILE: web/panel/menu.py
# DATE: 2026-03-08
# PURPOSE: panel sidebar sections/items.

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
        # STATS
        "title": _("Статистика"),
        "page_title": _("Статистика"),
        "open_prefixes": ["/panel/stats/"],
        "items": [],
        "dynamic_stats_campaigns": True,
    },
    {
        # AUDIENCE
        "title": _("Управление аудиториями"),
        "open_prefixes": ["/panel/audience/"],
        "items": [
            {
                "title": _("Списки рассылок"),
                "page_title": _("Списки рассылок"),
                "url_name": "audience:create_list",
                "active_prefixes": ["/panel/audience/create/"],
            },
        ],
    },
    {
        # CAMPAIGNS
        "title": _("Кампании и рассылки"),
        "open_prefixes": ["/panel/campaigns/"],
        "items": [
            {
                "title": _("Кампании / письма"),
                "page_title": _("Кампании : рассылки и письма"),
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
        "url_name": "settings:mail_servers",
        "open_prefixes": ["/panel/settings/"],
        "items": [
            {
                "title": _("Почтовые серверы"),
                "page_title": _("Настройки : Почтовые серверы"),
                "url_name": "settings:mail_servers",
                "active_prefixes": ["/panel/settings/mail-servers/"],
            },
            {
                "title": _("'Окна' отправки"),
                "page_title": _("Настройки : 'Окна' отправки"),
                "url_name": "settings:sending",
                "active_prefixes": ["/panel/settings/sending/"],
            },
            {
                "title": _("Учет переходов"),
                "page_title": _("Настройки : Учет переходов"),
                "url_name": "settings:url_stats",
                "active_prefixes": ["/panel/settings/url-stats/"],
            },
        ],
    },
]
