# FILE: web/panel/aap_lists/models.py  (обновлено — 2026-01-11)
# PURPOSE: добавить soft-archive для списков рассылок (не удаляем, а скрываем).

from django.conf import settings
from django.db import models


class MailingList(models.Model):
    workspace_id = models.UUIDField(db_index=True)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)

    title = models.CharField(max_length=255)

    audience_tasks = models.ManyToManyField(
        "aap_audience.AudienceTask",
        related_name="mailing_lists",
        blank=True,
    )

    archived = models.BooleanField(default=False)  # NEW

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_lists"
        db_table = "aap_lists_mailinglist"
        ordering = ["-created_at"]

    def __str__(self):
        return self.title