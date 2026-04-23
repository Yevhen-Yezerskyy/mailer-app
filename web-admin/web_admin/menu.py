# FILE: web-admin/web_admin/menu.py
# DATE: 2026-03-07
# PURPOSE: Managed left menu for internal admin contour.

from django.utils.translation import gettext_lazy as _trans


PANEL_MENU = [
    {
        "title": _trans("Пользователи"),
        "open_prefixes": ["/companies/", "/users/", "/dashboard/"],
        "items": [
            {
                "title": _trans("Компании и пользователи"),
                "page_title": _trans("Компании и пользователи"),
                "url_name": "companies",
                "active_prefixes": ["/companies/", "/users/", "/dashboard/"],
            },
        ],
    },
    {
        "title": _trans("Настройки"),
        "open_prefixes": ["/settings/"],
        "items": [
            {
                "title": _trans("Почтовый ящик"),
                "page_title": _trans("Почтовый ящик"),
                "url_name": "settings:mail_servers",
                "active_prefixes": ["/settings/mail-servers/"],
            },
            {
                "title": _trans("Шаблон системного письма"),
                "page_title": _trans("Шаблон системного письма"),
                "url_name": "settings:mail_template",
                "active_prefixes": ["/settings/mail-template/", "/panel/campaigns/templates/"],
            },
            {
                "title": _trans("Письма"),
                "page_title": _trans("Письма"),
                "url_name": "settings:mail_letters",
                "active_prefixes": ["/settings/mail-letters/"],
            },
        ],
    },
    {
        "title": _trans("Лимиты"),
        "open_prefixes": ["/limits/"],
        "items": [
            {
                "title": _trans("Типы доступа"),
                "page_title": _trans("Типы доступа"),
                "url_name": "limits_access_types",
                "active_prefixes": ["/limits/access-types/"],
            },
            {
                "title": _trans("Спец.лимиты"),
                "page_title": _trans("Спец.лимиты"),
                "url_name": "limits_special",
                "active_prefixes": ["/limits/special/"],
            },
        ],
    },
]
