# FILE: web/panel/apps.py  (обновлено — 2025-12-18)
# PURPOSE: корневой app панели (dashboard + include под-аппы)

from django.apps import AppConfig


class PanelConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "panel"
