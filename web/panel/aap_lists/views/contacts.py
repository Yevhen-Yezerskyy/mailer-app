# FILE: web/panel/aap_lists/views/contacts.py  (новое — 2026-01-10)
# PURPOSE: все контакты (заглушка).

from django.http import HttpResponse

def contacts_view(request):
    return HttpResponse("Lists: all contacts")