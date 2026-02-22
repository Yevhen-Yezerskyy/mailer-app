# FILE: web-admin/admin_portal/apps.py
# DATE: 2026-02-22
# PURPOSE: App config for web-admin portal.

from django.apps import AppConfig


class AdminPortalConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "admin_portal"

