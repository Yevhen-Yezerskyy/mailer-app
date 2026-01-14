# FILE: web/panel/aap_campaigns/admin.py  (новое — 2026-01-14)
# PURPOSE: Регистрация моделей aap_campaigns в Django admin.
# CHANGE: Добавлен Templates с базовыми настройками списка/поиска/фильтров.

from __future__ import annotations

from django.contrib import admin

from panel.aap_campaigns.models import Templates


@admin.register(Templates)
class TemplatesAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "workspace_id",
        "template_name",
        "is_active",
        "order",
        "updated_at",
    )
    list_filter = ("is_active",)
    search_fields = ("template_name", "workspace_id")
    ordering = ("workspace_id", "order", "template_name")
    readonly_fields = ("created_at", "updated_at")