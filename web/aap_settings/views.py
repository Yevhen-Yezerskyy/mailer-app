# FILE: web/aap_settings/views.py

from django.contrib.auth.decorators import login_required
from django.shortcuts import render


@login_required
def sending_settings(request):
    return render(request, "panels/aap_settings/sending_settings.html")


@login_required
def mail_servers(request):
    return render(request, "panels/aap_settings/mail_servers.html")
