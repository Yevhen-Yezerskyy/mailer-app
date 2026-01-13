# FILE: web/panel/aap_settings/views/sending.py
# DATE: 2026-01-13
# PURPOSE: Заглушка страницы "Отправка писем" (settings:sending). Пока без логики.

from django.http import HttpResponse


def sending_settings_view(request):
    return HttpResponse("Settings / Sending: TODO", content_type="text/plain; charset=utf-8")
