# FILE: web/panel/aap_lists/urls.py
# DATE: 2026-01-11
# PURPOSE: URL-ы раздела "Lists" панели (lists / list-manage / contacts).
# CHANGE:
# - добавлена управлялка списка: /panel/lists/lists/list/?id=...
# - добавлена модалка: /panel/lists/lists/list/modal/?id=... (rate_contacts.id, encode)

from django.urls import path
from django.views.generic import RedirectView

from .views import contacts, lists, lists_list, modal_lists_list

app_name = "lists"

urlpatterns = [
    path("", RedirectView.as_view(url="lists/", permanent=False)),
    path("lists/", lists.lists_view, name="lists"),
    path("lists/list/", lists_list.lists_list_view, name="lists_list"),
    path("lists/list/modal/", modal_lists_list.modal_lists_list_view, name="lists_list_modal"),
    path("contacts/", contacts.contacts_view, name="contacts"),
]