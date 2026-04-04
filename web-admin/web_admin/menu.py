# FILE: web-admin/web_admin/menu.py
# DATE: 2026-03-07
# PURPOSE: Managed left menu for internal admin contour.

from django.utils.translation import gettext_lazy as _


PANEL_MENU = [
    {
        "title": _("Пользователи"),
        "open_prefixes": ["/companies/", "/users/", "/dashboard/"],
        "items": [
            {
                "title": _("Компании и пользователи"),
                "page_title": _("Компании и пользователи"),
                "url_name": "companies",
                "active_prefixes": ["/companies/", "/users/", "/dashboard/"],
            },
        ],
    },
    {
        "title": _("Настройки"),
        "open_prefixes": ["/settings/"],
        "items": [
            {
                "title": _("Почтовый ящик"),
                "page_title": _("Почтовый ящик"),
                "url_name": "settings:mail_servers",
                "active_prefixes": ["/settings/mail-servers/"],
            },
            {
                "title": _("Шаблон системного письма"),
                "page_title": _("Шаблон системного письма"),
                "url_name": "settings:mail_template",
                "active_prefixes": ["/settings/mail-template/", "/panel/campaigns/templates/"],
            },
            {
                "title": _("Письма"),
                "page_title": _("Письма"),
                "url_name": "settings:mail_letters",
                "active_prefixes": ["/settings/mail-letters/"],
            },
        ],
    },
    {
        "title": _("Лимиты"),
        "open_prefixes": ["/limits/"],
        "items": [
            {
                "title": _("Типы доступа"),
                "page_title": _("Типы доступа"),
                "url_name": "limits_access_types",
                "active_prefixes": ["/limits/access-types/"],
            },
            {
                "title": _("Спец.лимиты"),
                "page_title": _("Спец.лимиты"),
                "url_name": "limits_special",
                "active_prefixes": ["/limits/special/"],
            },
        ],
    },
]
