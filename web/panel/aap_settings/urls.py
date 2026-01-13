# FILE: web/panel/aap_settings/urls.py
# DATE: 2026-01-13
# PURPOSE: URL-ы раздела Settings: sending, mail-servers + endpoint раскрытия секрета (AJAX).
# CHANGE:
# - add: mail-servers/secret/ -> mail_server_secret_view

from django.urls import path
from django.views.generic import RedirectView

from .views import mail_servers, sending

app_name = "settings"

urlpatterns = [
    path("", RedirectView.as_view(url="mail-servers/", permanent=False)),
    path("sending/", sending.sending_settings_view, name="sending"),
    path("mail-servers/", mail_servers.mail_servers_view, name="mail_servers"),
    path("mail-servers/secret/", mail_servers.mail_server_secret_view, name="mail_server_secret"),
]
