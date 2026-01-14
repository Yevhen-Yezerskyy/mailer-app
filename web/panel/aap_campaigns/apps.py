# FILE: web/panel/aap_campaigns/apps.py
# DATE: 2026-01-14
# PURPOSE: AppConfig для panel.aap_campaigns.
# CHANGE: (new) корректный name для вложенного app внутри panel.

from django.apps import AppConfig


class AapCampaignsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "panel.aap_campaigns"
    label = "aap_campaigns"
    verbose_name = "Campaigns"