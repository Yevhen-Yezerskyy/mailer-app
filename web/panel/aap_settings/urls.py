# FILE: web/panel/aap_settings/urls.py
# DATE: 2026-01-24
# PURPOSE: Settings urls.
# CHANGE: добавлены back-compat routes mail_servers_smtp/mail_servers_imap (редирект на mail-servers edit).

from django.urls import path
from django.views.generic import RedirectView
from django.shortcuts import redirect
from django.urls import reverse

from .views import mail_servers, mail_servers_api, sending

app_name = "settings"


def _redir_mail_servers_edit(request, id: str):
    # id уже ui_id (encode_id), просто прокидываем в edit state
    return redirect(reverse("settings:mail_servers") + f"?state=edit&id={id}")


urlpatterns = [
    path("", RedirectView.as_view(url="mail-servers/", permanent=False)),
    path("sending/", sending.sending_settings_view, name="sending"),

    path("mail-servers/", mail_servers.mail_servers_view, name="mail_servers"),
    path("mail-servers/api/", mail_servers_api.mail_servers_api_view, name="mail_servers_api"),

    # back-compat: шаблон/старые ссылки
    path("mail-servers/<str:id>/smtp/", _redir_mail_servers_edit, name="mail_servers_smtp"),
    path("mail-servers/<str:id>/imap/", _redir_mail_servers_edit, name="mail_servers_imap"),
]
