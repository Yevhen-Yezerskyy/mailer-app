# FILE: web/panel/menu.py
# DATE: 2026-03-08
# PURPOSE: panel sidebar sections/items.

from django.utils.translation import gettext_lazy as _trans

PANEL_MENU = [
    {
        # DASHBOARD
        "title": _trans("Начало"),
        "page_title": _trans("Начало"),
        "url_name": "dashboard",
        "open_prefixes": ["/panel/overview"],
        "items": [
            {
                "title": _trans("Обзор"),
                "page_title": _trans("Обзор : Начало"),
                "url_name": "dashboard",
                "active_prefixes": ["/panel/overview/"],
            },
        ],
    },
    {
        # AUDIENCE
        "title": _trans("Списки рассылок"),
        "url_name": "audience:create_list",
        "open_prefixes": ["/panel/audience/"],
        "items": [
            {
                "title": _trans("Списки и рейтинг"),
                "page_title": _trans("Списки и рейтинг"),
                "url_name": "audience:create_list",
                "active_prefixes": ["/panel/audience/create/"],
            },
            {
                "title": _trans("Блокировка кантактов"),
                "page_title": _trans("Блокировка кантактов"),
                "url_name": "audience:contacts_manage",
                "active_prefixes": ["/panel/audience/contacts-manage/"],
            },
        ],
    },
    {
        # CAMPAIGNS
        "title": _trans("Кампании рассылок"),
        "url_name": "campaigns:campaigns",
        "open_prefixes": ["/panel/campaigns/"],
        "items": [
            {
                "title": _trans("Кампании и письма"),
                "page_title": _trans("Кампании : рассылки и письма"),
                "url_name": "campaigns:campaigns",
                "active_prefixes": ["/panel/campaigns/campaigns/"],
            },
            {
                "title": _trans("Шаблоны писем"),
                "page_title": _trans("Кампании : Шаблоны писем"),
                "url_name": "campaigns:templates",
                "active_prefixes": ["/panel/campaigns/templates/"],
            },
        ],
    },
    {
        # STATS
        "title": _trans("Статистика"),
        "page_title": _trans("Статистика"),
        "url_name": "stats_clicks",
        "open_prefixes": ["/panel/stats/"],
        "items": [
            {
                "title": _trans("Переходы"),
                "page_title": _trans("Статистика : Переходы"),
                "url_name": "stats_clicks",
                "active_prefixes": ["/panel/stats/clicks/"],
            },
            {
                "title": _trans("Отправка"),
                "page_title": _trans("Статистика : Отправка"),
                "url_name": "stats_sending",
                "active_prefixes": ["/panel/stats/sending/"],
            },
        ],
    },
    {
        # SETTINGS
        "title": _trans("Настройки"),
        "url_name": "settings:mail_servers",
        "open_prefixes": ["/panel/settings/"],
        "items": [
            {
                "title": _trans("Почтовые серверы"),
                "page_title": _trans("Настройки : Почтовые серверы"),
                "url_name": "settings:mail_servers",
                "active_prefixes": ["/panel/settings/mail-servers/"],
            },
            {
                "title": _trans("'Окна' отправки"),
                "page_title": _trans("Настройки : 'Окна' отправки"),
                "url_name": "settings:sending",
                "active_prefixes": ["/panel/settings/sending/"],
            },
            {
                "title": _trans("Учет переходов"),
                "page_title": _trans("Настройки : Учет переходов"),
                "url_name": "settings:url_stats",
                "active_prefixes": ["/panel/settings/url-stats/"],
            },
        ],
    },
]
