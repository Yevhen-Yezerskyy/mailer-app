# FILE: web/panel/aap_campaigns/models.py
# DATE: 2026-01-19
# PURPOSE: Модели Campaigns: Templates + Campaign + Letter.
# CHANGE: (new) добавлены Campaign и Letter, Templates не тронут.

from __future__ import annotations

from django.db import models


class Templates(models.Model):
    workspace_id = models.UUIDField(db_index=True)

    template_name = models.CharField(max_length=255, help_text="Имя/код шаблона")
    template_html = models.TextField(blank=True, default="", help_text="HTML шаблона")

    styles = models.JSONField(default=dict, blank=True, help_text="Стили (JSON)")

    is_active = models.BooleanField(default=True)
    archived = models.BooleanField(default=False)  # NEW
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


class Campaign(models.Model):
    workspace_id = models.UUIDField(db_index=True)

    title = models.CharField(max_length=255, help_text="Название кампании")

    mailing_list = models.ForeignKey(
        "aap_lists.MailingList",
        on_delete=models.PROTECT,
        related_name="campaigns",
    )

    campaign_parent = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="children",
    )
    send_after_parent_days = models.PositiveIntegerField(default=30)

    start_at = models.DateTimeField()
    end_at = models.DateTimeField(null=True, blank=True)

    active = models.BooleanField(default=False)

    window = models.JSONField(
        default=dict,
        blank=True,
        help_text="Окно отправки (override глобальных настроек)",
    )

    mailbox = models.ForeignKey(
        "aap_settings.Mailbox",
        on_delete=models.PROTECT,
        related_name="campaigns",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_campaigns"
        db_table = "campaigns_campaigns"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["workspace_id", "active", "start_at"]),
            models.Index(fields=["workspace_id", "campaign_parent"]),
        ]

    def __str__(self) -> str:
        return self.title


class Letter(models.Model):
    workspace_id = models.UUIDField(db_index=True)

    campaign = models.OneToOneField(
        Campaign,
        on_delete=models.CASCADE,
        related_name="letter",
    )

    template = models.ForeignKey(
        Templates,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="letters",
    )

    html_content = models.TextField(blank=True, default="")
    subjects = models.JSONField(default=list, blank=True)
    headers = models.JSONField(default=dict, blank=True)

    ready_content = models.TextField(blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_campaigns"
        db_table = "campaigns_letters"
        indexes = [
            models.Index(fields=["workspace_id"]),
        ]

    def __str__(self) -> str:
        return f"Letter[{self.campaign_id}]"
