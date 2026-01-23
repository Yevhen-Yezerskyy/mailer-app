# FILE: web/panel/aap_settings/urls.py
# DATE: 2026-01-23
# PURPOSE: URL-ы Settings: sending, mail-servers (split) + AJAX endpoints (secret reveal, checks API).
# CHANGE:
# - mail-servers/ -> список + add mailbox
# - mail-servers/<id>/smtp/ -> SMTP форма
# - mail-servers/<id>/imap/ -> IMAP форма

from django.urls import path
from django.views.generic import RedirectView

from .views import mail_servers, mail_servers_api, sending

app_name = "settings"

urlpatterns = [
    path("", RedirectView.as_view(url="mail-servers/", permanent=False)),
    path("sending/", sending.sending_settings_view, name="sending"),

    path("mail-servers/", mail_servers.mail_servers_list_view, name="mail_servers_list"),
    path("mail-servers/legacy/", mail_servers.mail_servers_view, name="mail_servers"),
    path("mail-servers/<str:id>/smtp/", mail_servers.mail_servers_smtp_view, name="mail_servers_smtp"),
    path("mail-servers/<str:id>/imap/", mail_servers.mail_servers_imap_view, name="mail_servers_imap"),

    path("mail-servers/secret/", mail_servers.mail_server_secret_view, name="mail_server_secret"),
    path("mail-servers/api/", mail_servers_api.mail_servers_api_view, name="mail_servers_api"),
]
