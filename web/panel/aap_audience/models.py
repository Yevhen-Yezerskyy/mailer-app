# FILE: web/panel/aap_audience/models.py  (обновлено — 2025-12-18)
# Смысл: AudienceTask физически живёт в panel/aap_audience,
# но сохраняет старые app_label и db_table, БД и миграции не трогаем.

from django.conf import settings
from django.db import models


class AudienceTask(models.Model):
    workspace_id = models.UUIDField(db_index=True)  # из UserWorkspace
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)

    task = models.TextField()               # WHAT + WHO + GEO
    title = models.CharField(max_length=255)
    task_branches = models.TextField()
    task_geo = models.TextField()
    task_client = models.TextField(blank=True, default="")

    run_processing = models.BooleanField(default=False)
    subscribers_limit = models.IntegerField(default=500)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_audience"
        db_table = "aap_audience_audiencetask"
        ordering = ["-created_at"]

    def __str__(self):
        return self.title
