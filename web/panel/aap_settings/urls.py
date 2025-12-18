# FILE: web/aap_settings/urls.py

from django.urls import path
from django.shortcuts import redirect
from .views import sending_settings, mail_servers

app_name = "settings"

urlpatterns = [
    path("", lambda r: redirect("settings:sending"), name="index"),
    path("sending/",      sending_settings, name="sending"),
    path("mail-servers/", mail_servers,     name="mail_servers"),
]
