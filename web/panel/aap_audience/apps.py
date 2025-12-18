# FILE: web/panel/aap_audience/apps.py  (обновлено — 2025-12-18)
# FIX: корректный dotted path для Django app registry.

from django.apps import AppConfig


class AapAudienceConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "panel.aap_audience"
