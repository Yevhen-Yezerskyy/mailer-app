# FILE: web/panel/aap_lists/apps.py  (обновлено — 2026-01-10)
# PURPOSE: правильный dotted-path приложения внутри пакета panel.*.

from django.apps import AppConfig


class AapListsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "panel.aap_lists"