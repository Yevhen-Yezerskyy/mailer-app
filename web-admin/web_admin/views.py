# FILE: web-admin/web_admin/views.py
# DATE: 2026-03-07
# PURPOSE: Custom admin contour views: login, dashboard, companies and users management.

from django.contrib.auth import login as auth_login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import AuthenticationForm
from django.db.models.functions import Lower
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse

from mailer_web.models_accounts import ClientUser, Workspace

from .forms import ClientUserForm, WorkspaceForm


def index_view(request: HttpRequest) -> HttpResponse:
    if request.user.is_authenticated:
        return redirect("companies")
    return redirect("login")


def login_view(request: HttpRequest) -> HttpResponse:
    if request.user.is_authenticated:
        return redirect("companies")

    if request.method == "POST":
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            auth_login(request, form.get_user())
            next_url = (request.POST.get("next") or "").strip()
            if next_url:
                return redirect(next_url)
            return redirect("companies")
    else:
        form = AuthenticationForm(request)
    form.fields["username"].label = "Email"

    return render(request, "public/login.html", {"form": form})


@login_required(login_url="login")
def dashboard_view(request: HttpRequest) -> HttpResponse:
    return render(request, "panels/dashboard.html", {"section": "dashboard"})


@login_required(login_url="login")
def companies_view(request: HttpRequest) -> HttpResponse:
    show_archived = (request.GET.get("archived") or "").strip() in ("1", "true", "yes")
    items = (
        Workspace.objects
        .filter(archived=show_archived)
        .prefetch_related("users")
        .order_by(Lower("company_name"), "created_at")
    )
    return render(
        request,
        "panels/companies.html",
        {"section": "companies", "items": items, "show_archived": show_archived},
    )


@login_required(login_url="login")
def company_add_view(request: HttpRequest) -> HttpResponse:
    if request.method == "POST":
        form = WorkspaceForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect(reverse("companies"))
    else:
        form = WorkspaceForm()

    return render(
        request,
        "panels/company_edit.html",
        {"section": "companies", "obj": None, "form": form, "is_create": True},
    )


@login_required(login_url="login")
def company_delete_view(request: HttpRequest, pk) -> HttpResponse:
    if request.method != "POST":
        return redirect(reverse("companies"))

    obj = get_object_or_404(Workspace, pk=pk, archived=False)
    ClientUser.objects.filter(workspace=obj).update(archived=True)
    obj.archived = True
    obj.save(update_fields=["archived", "updated_at"])
    return redirect(reverse("companies"))


@login_required(login_url="login")
def company_restore_view(request: HttpRequest, pk) -> HttpResponse:
    if request.method != "POST":
        return redirect(reverse("companies") + "?archived=1")

    obj = get_object_or_404(Workspace, pk=pk, archived=True)
    obj.archived = False
    obj.save(update_fields=["archived", "updated_at"])
    ClientUser.objects.filter(workspace=obj, archived=True).update(archived=False)
    return redirect(reverse("companies") + "?archived=1")


@login_required(login_url="login")
def company_edit_view(request: HttpRequest, pk) -> HttpResponse:
    obj = get_object_or_404(Workspace, pk=pk, archived=False)
    if request.method == "POST":
        form = WorkspaceForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            return redirect(reverse("companies"))
    else:
        form = WorkspaceForm(instance=obj)

    return render(
        request,
        "panels/company_edit.html",
        {"section": "companies", "obj": obj, "form": form},
    )


@login_required(login_url="login")
def company_modal_view(request: HttpRequest, pk) -> HttpResponse:
    obj = (
        Workspace.objects
        .prefetch_related("users")
        .filter(pk=pk)
        .first()
    )
    obj = obj or get_object_or_404(Workspace, pk=pk)
    return render(request, "panels/modals/company_info.html", {"obj": obj})


@login_required(login_url="login")
def users_view(request: HttpRequest) -> HttpResponse:
    items = (
        ClientUser.objects
        .filter(archived=False, workspace__archived=False)
        .select_related("workspace")
        .order_by("email", "id")
    )
    return render(request, "panels/users.html", {"section": "users", "items": items})


@login_required(login_url="login")
def user_add_view(request: HttpRequest) -> HttpResponse:
    ws_q = (request.GET.get("workspace") or "").strip()
    initial = {}
    if ws_q:
        ws = Workspace.objects.filter(id=ws_q, archived=False).only("id").first()
        if ws:
            initial["workspace"] = ws.id

    if request.method == "POST":
        form = ClientUserForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect(reverse("companies"))
    else:
        form = ClientUserForm(initial=initial)

    return render(
        request,
        "panels/user_edit.html",
        {"section": "users", "obj": None, "form": form, "is_create": True},
    )


@login_required(login_url="login")
def user_edit_view(request: HttpRequest, pk: int) -> HttpResponse:
    obj = get_object_or_404(
        ClientUser.objects.filter(archived=False, workspace__archived=False).select_related("workspace"),
        pk=pk,
    )
    if request.method == "POST":
        form = ClientUserForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            return redirect(reverse("companies"))
    else:
        form = ClientUserForm(instance=obj)

    return render(
        request,
        "panels/user_edit.html",
        {"section": "users", "obj": obj, "form": form, "is_create": False},
    )


@login_required(login_url="login")
def user_modal_view(request: HttpRequest, pk: int) -> HttpResponse:
    obj = get_object_or_404(ClientUser.objects.select_related("workspace"), pk=pk)
    return render(request, "panels/modals/user_info.html", {"obj": obj})
