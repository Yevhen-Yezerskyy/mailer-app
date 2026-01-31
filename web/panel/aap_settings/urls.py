# FILE: web/panel/aap_settings/urls.py
# DATE: 2026-01-31
# CHANGE:
# - добавлен endpoint для учета переходов URL (url_stats)

from django.urls import path
from django.views.generic import RedirectView

from .views import imap_server, mail_servers, mail_servers_api, sending, smtp_server, url_stats

app_name = "settings"

urlpatterns = [
    path("", RedirectView.as_view(url="mail-servers/", permanent=False)),
    path("sending/", sending.sending_settings_view, name="sending"),
    path("mail-servers/", mail_servers.mail_servers_view, name="mail_servers"),
    path("mail-servers/api/", mail_servers_api.mail_servers_api_view, name="mail_servers_api"),
    path("mail-servers/<str:id>/smtp/", smtp_server.smtp_server_view, name="mail_servers_smtp"),
    path("mail-servers/<str:id>/smtp/secret/", smtp_server.smtp_secret_view, name="mail_servers_smtp_secret"),
    path("mail-servers/<str:id>/imap/", imap_server.imap_server_view, name="mail_servers_imap"),
    path("mail-servers/<str:id>/imap/secret/", imap_server.imap_secret_view, name="mail_servers_imap_secret"),
    path("url-stats/", url_stats.url_stats_view, name="url_stats"),
]
