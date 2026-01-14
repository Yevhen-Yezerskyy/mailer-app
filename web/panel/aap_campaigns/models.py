# FILE: web/panel/aap_campaigns/models.py
# DATE: 2026-01-14
# PURPOSE: Модели раздела Campaigns.
# CHANGE: workspace_id: BigIntegerField -> UUIDField (совместимо с accounts_userworkspace.workspace_id).

from __future__ import annotations

from django.db import models


class Templates(models.Model):
    workspace_id = models.UUIDField(db_index=True)

    template_name = models.CharField(max_length=255, help_text="Имя/код шаблона")
    template_html = models.TextField(blank=True, default="", help_text="HTML шаблона (большой)")

    styles = models.JSONField(default=dict, blank=True, help_text="Стили/настройки (JSON)")

    is_active = models.BooleanField(default=True)
    order = models.IntegerField(default=0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_campaigns"
        db_table = "campaigns_templates"
        ordering = ["order", "template_name"]
        constraints = [
            models.UniqueConstraint(
                fields=["workspace_id", "template_name"],
                name="uq_campaigns_templates_ws_name",
            ),
        ]
        indexes = [
            models.Index(fields=["workspace_id", "is_active", "order"]),
        ]

    def __str__(self) -> str:
        return f"{self.workspace_id}:{self.template_name}"
