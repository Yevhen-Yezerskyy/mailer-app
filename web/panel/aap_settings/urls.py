# FILE: web/panel/aap_settings/urls.py
# DATE: 2026-01-13
# PURPOSE: URL-ы раздела Settings: sending (пока заглушка), mail-servers (реализация).

from django.urls import path
from django.views.generic import RedirectView

from .views import mail_servers, sending

app_name = "settings"

urlpatterns = [
    path("", RedirectView.as_view(url="mail-servers/", permanent=False)),
    path("sending/", sending.sending_settings_view, name="sending"),
    path("mail-servers/", mail_servers.mail_servers_view, name="mail_servers"),
]
