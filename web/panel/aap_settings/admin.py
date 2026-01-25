# FILE: web/panel/aap_settings/admin.py
# DATE: 2026-01-25
# PURPOSE: Django Admin registrations for aap_settings.
# CHANGE: Register ONLY ProviderPreset (UI-only presets). Nothing else is exposed in admin.

from __future__ import annotations

from django.contrib import admin

from .models import ProviderPreset


@admin.register(ProviderPreset)
class ProviderPresetAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "is_active", "order")
    list_filter = ("is_active",)
    search_fields = ("name",)
    ordering = ("order", "name", "id")