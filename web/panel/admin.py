# FILE: web/panel/admin.py
# DATE: 2026-01-14
# PURPOSE: Регистрация справочников в Django admin.
# CHANGE: Добавлен админ для GlobalTemplate.

from __future__ import annotations

from django.contrib import admin

from panel.models import GlobalTemplate


@admin.register(GlobalTemplate)
class GlobalTemplateAdmin(admin.ModelAdmin):
    list_display = ("id", "template_name", "is_active", "order", "updated_at")
    list_filter = ("is_active",)
    search_fields = ("template_name",)
    ordering = ("order", "template_name")
    readonly_fields = ("created_at", "updated_at")