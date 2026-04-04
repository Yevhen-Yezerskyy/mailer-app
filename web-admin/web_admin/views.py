# FILE: web-admin/web_admin/views.py
# DATE: 2026-03-07
# PURPOSE: Custom admin contour views: login, dashboard, companies and users management.

from django.contrib.auth import login as auth_login
from django.contrib.auth.decorators import login_required
from django.contrib.auth.forms import AuthenticationForm
from django.db import transaction
from django.db.models.functions import Lower
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse

from mailer_web.models_accounts import ClientUser, Workspace, WorkspaceLimits, WORKSPACE_ACCESS_TYPES

from .forms import ClientUserForm, WorkspaceForm, WorkspaceLimitsForm


LIMIT_FIELD_NAMES = (
    "sending_workspace_limit",
    "sending_task_limit",
    "active_tasks_limit",
)
LIMITS_ACCESS_TYPES = tuple(
    access_type
    for access_type in WORKSPACE_ACCESS_TYPES.keys()
    if access_type not in {"closed", "custom"}
)


def _limits_initial(obj: WorkspaceLimits | None) -> dict[str, int | None]:
    if not obj:
        return {}
    return {
        name: getattr(obj, name)
        for name in LIMIT_FIELD_NAMES
    }


def _apply_limits(obj: WorkspaceLimits, cleaned_data: dict) -> None:
    for name in LIMIT_FIELD_NAMES:
        setattr(obj, name, cleaned_data.get(name))


def _workspace_limits_has_values(cleaned_data: dict) -> bool:
    return any(cleaned_data.get(name) is not None for name in LIMIT_FIELD_NAMES)


def _get_workspace_custom_limits(workspace_id) -> WorkspaceLimits | None:
    return (
        WorkspaceLimits.objects.filter(workspace_id=workspace_id, type="custom").order_by("id").first()
        or WorkspaceLimits.objects.filter(workspace_id=workspace_id).order_by("id").first()
    )


def _save_workspace_form(form: WorkspaceForm) -> Workspace:
    with transaction.atomic():
        workspace = form.save()
        if form.cleaned_data.get("access_type") != "custom":
            return workspace

        limits_obj = _get_workspace_custom_limits(workspace.id)
        if limits_obj is None and not _workspace_limits_has_values(form.cleaned_data):
            return workspace

        limits_obj = limits_obj or WorkspaceLimits(workspace_id=workspace.id, type="custom")
        limits_obj.workspace_id = workspace.id
        limits_obj.type = "custom"
        _apply_limits(limits_obj, form.cleaned_data)
        limits_obj.save()
        return workspace


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
            _save_workspace_form(form)
            return redirect(reverse("companies"))
    else:
        form = WorkspaceForm()

    return render(
        request,
        "panels/company_edit.html",
        {
            "section": "companies",
            "obj": None,
            "form": form,
            "is_create": True,
            "show_custom_limits": form["access_type"].value() == "custom",
        },
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
    limits_obj = _get_workspace_custom_limits(obj.id)
    if request.method == "POST":
        form = WorkspaceForm(request.POST, instance=obj)
        if form.is_valid():
            _save_workspace_form(form)
            return redirect(reverse("companies"))
    else:
        form = WorkspaceForm(instance=obj, initial=_limits_initial(limits_obj))

    return render(
        request,
        "panels/company_edit.html",
        {
            "section": "companies",
            "obj": obj,
            "form": form,
            "show_custom_limits": form["access_type"].value() == "custom",
        },
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


@login_required(login_url="login")
def limits_access_types_view(request: HttpRequest) -> HttpResponse:
    access_types = list(LIMITS_ACCESS_TYPES)
    existing_rows = {
        item.type: item
        for item in WorkspaceLimits.objects
        .filter(workspace_id__isnull=True, type__in=access_types)
        .order_by("type", "id")
    }

    rows = []
    all_valid = True
    for access_type in access_types:
        obj = existing_rows.get(access_type)
        form = WorkspaceLimitsForm(
            request.POST if request.method == "POST" else None,
            initial=_limits_initial(obj),
            prefix=f"type-{access_type}",
        )
        rows.append({"access_type": access_type, "form": form, "obj": obj})
        if request.method == "POST" and not form.is_valid():
            all_valid = False

    if request.method == "POST" and all_valid:
        with transaction.atomic():
            for row in rows:
                obj = row["obj"]
                form = row["form"]
                access_type = row["access_type"]
                if form.has_values():
                    obj = obj or WorkspaceLimits(workspace_id=None, type=access_type)
                    obj.workspace_id = None
                    obj.type = access_type
                    _apply_limits(obj, form.cleaned_data)
                    obj.save()
                elif obj:
                    obj.delete()
        return redirect(reverse("limits_access_types"))

    return render(
        request,
        "panels/limits_access_types.html",
        {"section": "limits_access_types", "rows": rows},
    )


@login_required(login_url="login")
def limits_special_view(request: HttpRequest) -> HttpResponse:
    items = list(
        WorkspaceLimits.objects
        .filter(workspace_id__isnull=False)
        .order_by("workspace_id", "id")
    )
    workspace_map = {
        ws.id: ws
        for ws in Workspace.objects
        .filter(id__in=[item.workspace_id for item in items])
        .only("id", "company_name")
    }

    rows = []
    all_valid = True
    for obj in items:
        form = WorkspaceLimitsForm(
            request.POST if request.method == "POST" else None,
            initial=_limits_initial(obj),
            prefix=f"workspace-{obj.id}",
        )
        rows.append({"workspace": workspace_map.get(obj.workspace_id), "form": form, "obj": obj})
        if request.method == "POST" and not form.is_valid():
            all_valid = False

    if request.method == "POST" and all_valid:
        with transaction.atomic():
            for row in rows:
                obj = row["obj"]
                form = row["form"]
                if form.has_values():
                    _apply_limits(obj, form.cleaned_data)
                    obj.save()
                else:
                    obj.delete()
        return redirect(reverse("limits_special"))

    return render(
        request,
        "panels/limits_special.html",
        {"section": "limits_special", "rows": rows},
    )
