# FILE: web/mailer_web/admin.py  (новое — 2025-12-19)
# CHANGE: регистрируем account-модели в Django admin

from django.contrib import admin

from .models_accounts import FrontUser, UserWorkspace


@admin.register(FrontUser)
class FrontUserAdmin(admin.ModelAdmin):
    list_display = ("id", "user")
    search_fields = ("user__username", "user__email")


@admin.register(UserWorkspace)
class UserWorkspaceAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "workspace_id")
    search_fields = ("user__username", "user__email", "workspace_id")
