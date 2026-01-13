# FILE: web/panel/aap_settings/admin.py
# DATE: 2026-01-13
# PURPOSE: Регистрация моделей почтовых настроек в Django Admin.

from django.contrib import admin

from .models import Mailbox, MailboxConnection, ProviderPreset


@admin.register(Mailbox)
class MailboxAdmin(admin.ModelAdmin):
    list_display = ("id", "workspace_id", "name", "email", "domain", "is_active")
    list_filter = ("is_active",)
    search_fields = ("name", "email", "domain")
    ordering = ("name",)


@admin.register(MailboxConnection)
class MailboxConnectionAdmin(admin.ModelAdmin):
    list_display = ("id", "mailbox", "kind", "host", "port", "security", "auth_type")
    list_filter = ("kind", "security", "auth_type")
    search_fields = ("host", "username", "mailbox__email")
    ordering = ("mailbox", "kind")


@admin.register(ProviderPreset)
class ProviderPresetAdmin(admin.ModelAdmin):
    list_display = ("id", "code", "name", "kind", "host", "is_active", "order")
    list_filter = ("kind", "is_active")
    search_fields = ("code", "name", "host")
    ordering = ("order", "name")