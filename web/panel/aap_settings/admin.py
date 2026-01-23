# FILE: web/panel/aap_settings/admin.py
# DATE: 2026-01-23
# PURPOSE: Django Admin for mail settings (updated models).
# CHANGE:
# - Убран MailboxConnection.
# - Добавлены SmtpMailbox и ImapMailbox.
# - ProviderPreset упрощён (name + preset_json).
# - Никаких ссылок на удалённые поля.

from django.contrib import admin

from .models import (
    Mailbox,
    SmtpMailbox,
    ImapMailbox,
    ProviderPreset,
    MailboxOAuthApp,
)


@admin.register(Mailbox)
class MailboxAdmin(admin.ModelAdmin):
    list_display = ("id", "workspace_id", "email", "domain", "is_active")
    list_filter = ("is_active",)
    search_fields = ("email", "domain")
    ordering = ("email",)


@admin.register(SmtpMailbox)
class SmtpMailboxAdmin(admin.ModelAdmin):
    list_display = ("id", "mailbox", "auth_type", "from_email", "limit_hour_sent", "is_active")
    list_filter = ("auth_type", "is_active")
    search_fields = ("mailbox__email", "from_email")
    ordering = ("mailbox",)


@admin.register(ImapMailbox)
class ImapMailboxAdmin(admin.ModelAdmin):
    list_display = ("id", "mailbox", "auth_type", "is_active")
    list_filter = ("auth_type", "is_active")
    search_fields = ("mailbox__email",)
    ordering = ("mailbox",)


@admin.register(MailboxOAuthApp)
class MailboxOAuthAppAdmin(admin.ModelAdmin):
    list_display = ("id", "workspace_id", "provider", "is_active")
    list_filter = ("provider", "is_active")
    search_fields = ("workspace_id", "provider")
    ordering = ("workspace_id", "provider")


@admin.register(ProviderPreset)
class ProviderPresetAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "is_active", "order")
    list_filter = ("is_active",)
    search_fields = ("name",)
    ordering = ("order", "name")
