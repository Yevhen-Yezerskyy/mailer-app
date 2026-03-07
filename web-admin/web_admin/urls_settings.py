# FILE: web-admin/web_admin/urls_settings.py
# DATE: 2026-03-07
# PURPOSE: Namespaced routes for Settings -> system mailbox (mail servers, SMTP/IMAP, API).

from django.urls import path

from .views_settings_mail import (
    service_mail_servers_api_view,
    service_mail_servers_imap_secret_view,
    service_mail_servers_imap_view,
    service_mail_servers_smtp_secret_view,
    service_mail_servers_smtp_view,
    service_mail_servers_view,
)
from .views_settings_mail_letters import (
    mail_letter_add_view,
    mail_letter_edit_view,
    mail_letter_lang_edit_view,
    mail_letter_lang_preview_view,
    mail_letter_lang_translate_view,
    mail_letters_view,
)
from .views_settings_mail_template import system_mail_template_edit_view, system_mail_template_view

app_name = "settings"

urlpatterns = [
    path("mailbox/", service_mail_servers_view, name="mailbox"),
    path("mail-servers/", service_mail_servers_view, name="mail_servers"),
    path("mail-template/", system_mail_template_view, name="mail_template"),
    path("mail-template/edit/", system_mail_template_edit_view, name="mail_template_edit"),
    path("mail-letters/", mail_letters_view, name="mail_letters"),
    path("mail-letters/add/", mail_letter_add_view, name="mail_letter_add"),
    path("mail-letters/<int:pk>/edit/", mail_letter_edit_view, name="mail_letter_edit"),
    path("mail-letters/<int:pk>/lang/<str:lang>/edit/", mail_letter_lang_edit_view, name="mail_letter_lang_edit"),
    path("mail-letters/<int:pk>/lang/<str:lang>/preview/", mail_letter_lang_preview_view, name="mail_letter_lang_preview"),
    path("mail-letters/<int:pk>/lang/<str:lang>/translate/", mail_letter_lang_translate_view, name="mail_letter_lang_translate"),
    path("mail-servers/api/", service_mail_servers_api_view, name="mail_servers_api"),
    path("mail-servers/<str:id>/smtp/", service_mail_servers_smtp_view, name="mail_servers_smtp"),
    path("mail-servers/<str:id>/smtp/secret/", service_mail_servers_smtp_secret_view, name="mail_servers_smtp_secret"),
    path("mail-servers/<str:id>/imap/", service_mail_servers_imap_view, name="mail_servers_imap"),
    path("mail-servers/<str:id>/imap/secret/", service_mail_servers_imap_secret_view, name="mail_servers_imap_secret"),
]
