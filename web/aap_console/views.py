# FILE: web/aap_console/views.py

from django.contrib.auth.decorators import login_required
from django.shortcuts import render


@login_required
def dashboard(request):
    if request.workspace_id is None:
        return render(request, "panels/access_denied.html")

    return render(request, "panels/dashboard.html", {
        "user_id": request.user.id,
        "username": request.user.username,
        "workspace_id": request.workspace_id,
    })
