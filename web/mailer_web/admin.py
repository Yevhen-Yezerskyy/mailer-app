# FILE: web/mailer_web/admin.py
# DATE: 2026-02-23
# PURPOSE: Django admin registration for client users and workspaces.

from django.contrib import admin

from .models_accounts import ClientUser, UserWorkspace


@admin.register(ClientUser)
class ClientUserAdmin(admin.ModelAdmin):
    list_display = ("id", "username", "email", "is_active", "date_joined")
    list_filter = ("is_active",)
    search_fields = ("username", "email", "first_name", "last_name")
    ordering = ("id",)


@admin.register(UserWorkspace)
class UserWorkspaceAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "workspace_id")
    search_fields = ("user__username", "user__email", "workspace_id")
    ordering = ("id",)
