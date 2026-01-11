# FILE: web/panel/aap_lists/urls.py  (обновлено — 2026-01-10)
# PURPOSE: URL-ы раздела "Контакты" панели (lists / contacts).

from django.urls import path
from django.views.generic import RedirectView
from .views import lists, contacts

app_name = "lists"

urlpatterns = [
    path("", RedirectView.as_view(url="lists/", permanent=False)),
    path("lists/", lists.lists_view, name="lists"),
    path("contacts/", contacts.contacts_view, name="contacts"),
]