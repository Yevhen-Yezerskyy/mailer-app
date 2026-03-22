# FILE: web/panel/aap_settings/views/domains.py
# DATE: 2026-03-19
# PURPOSE: Settings -> domains management for current workspace with automatic client-subsites directories.

from __future__ import annotations

from django.contrib import messages
from django.db import transaction
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.http import require_POST

from panel.aap_settings.client_subsites import client_subsite_relpath, delete_client_subsite_dir, ensure_client_subsite_dir
from panel.aap_settings.forms import WorkspaceDomainForm
from panel.aap_settings.models import WorkspaceDomain


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None
    return ws_id


def domains_view(request):
    ws_id = _guard(request)
    if not ws_id:
        return redirect("/")

    if request.method == "POST":
        form = WorkspaceDomainForm(request.POST, workspace_id=ws_id)
        if form.is_valid():
            with transaction.atomic():
                obj = form.save()
                ensure_client_subsite_dir(obj.domain)
            messages.success(request, "Домен добавлен.")
            return redirect(reverse("settings:domains"))
    else:
        form = WorkspaceDomainForm(workspace_id=ws_id)

    items = list(
        WorkspaceDomain.objects
        .filter(workspace_id=ws_id)
        .order_by("domain", "id")
    )
    for item in items:
        item.relpath = client_subsite_relpath(item.domain)

    return render(
        request,
        "panels/aap_settings/domains.html",
        {
            "form": form,
            "items": items,
        },
    )


@require_POST
def domain_delete_view(request, pk: int):
    ws_id = _guard(request)
    if not ws_id:
        return redirect("/")

    obj = get_object_or_404(WorkspaceDomain, pk=pk, workspace_id=ws_id)
    domain = obj.domain

    with transaction.atomic():
        obj.delete()
        delete_client_subsite_dir(domain)

    messages.success(request, "Домен удалён.")
    return redirect(reverse("settings:domains"))
