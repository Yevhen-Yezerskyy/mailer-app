# FILE: web/panel/aap_settings/views/imap_server.py
# DATE: 2026-01-24
# PURPOSE: Settings → IMAP server: отдельная страница (stub на будущее).
# CHANGE: Пока редиректит в старый UX (?state=edit), но URL уже фиксированный /mail-servers/<id>/imap/.

from __future__ import annotations

from django.shortcuts import redirect
from django.urls import reverse


def imap_server_view(request, id: str):
    return redirect(reverse("settings:mail_servers") + f"?state=edit&id={id}")
