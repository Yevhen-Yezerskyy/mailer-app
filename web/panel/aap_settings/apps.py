# FILE: web/panel/aap_settings/apps.py  (обновлено — 2025-12-18)
# FIX: правильный dotted path, иначе Django ищет aap_settings.

from django.apps import AppConfig


class AapSettingsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "panel.aap_settings"
