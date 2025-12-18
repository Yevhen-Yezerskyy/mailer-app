# FILE: web/aap_settings/views.py

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render, redirect

from .models import MailConnection
from .forms import MailConnectionForm


@login_required
def sending_settings(request: HttpRequest) -> HttpResponse:
    """
    Страница общих настроек отправки (пока заглушка).
    """
    if getattr(request, "workspace_id", None) is None:
        return render(request, "panels/access_denied.html")

    return render(request, "panels/aap_settings/sending_settings.html")


@login_required
def mail_servers(request: HttpRequest) -> HttpResponse:
    """
    Управлялка почтовыми серверами:
    - список подключений для текущего воркспейса;
    - форма добавления нового подключения.
    """
    workspace_id = getattr(request, "workspace_id", None)
    if workspace_id is None:
        return render(request, "panels/access_denied.html")

    connections = (
        MailConnection.objects
        .filter(workspace_id=workspace_id, soft_deleted=False)
        .order_by("name")
    )

    if request.method == "POST":
        form = MailConnectionForm(request.POST, workspace_id=workspace_id)
        if form.is_valid():
            form.save()
            return redirect("settings:mail_servers")
    else:
        form = MailConnectionForm(workspace_id=workspace_id)

    return render(
        request,
        "panels/aap_settings/mail_servers.html",
        {
            "connections": connections,
            "form": form,
        },
    )
