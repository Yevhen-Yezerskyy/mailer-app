# FILE: web/panel/models.py  (обновлено — 2026-01-21)
# PURPOSE: Модели "глобальных справочников" (редактируются через Django admin).
# CHANGE: GlobalTemplate: добавлено поле buttons (JSON).

from __future__ import annotations

from django.db import models


class GlobalTemplate(models.Model):
    template_name = models.CharField(max_length=255, unique=True, help_text="Имя/код шаблона (уникально)")

    html_template = models.TextField(blank=True, default="", help_text="HTML-шаблон (большой)")
    html_content = models.TextField(blank=True, default="", help_text="HTML-контент (большой)")

    styles = models.JSONField(default=dict, blank=True, help_text="Стили/настройки (JSON)")
    buttons = models.JSONField(default=dict, blank=True, help_text="Кнопки/настройки (JSON)")

    is_active = models.BooleanField(default=True)
    order = models.IntegerField(default=0)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "panel"
        db_table = "panel_global_templates"
        ordering = ["order", "template_name"]
        indexes = [
            models.Index(fields=["is_active", "order"]),
        ]

    def __str__(self) -> str:
        return self.template_name