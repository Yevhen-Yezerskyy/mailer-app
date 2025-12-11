# FILE: aap_audience/models.py  (новое) 2025-12-11
from django.conf import settings
from django.db import models


class AudienceTask(models.Model):
    workspace_id = models.UUIDField(db_index=True)  # из UserWorkspace
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)

    task = models.TextField()               # WHAT + WHO + GEO
    title = models.CharField(max_length=255)
    task_branches = models.TextField()
    task_geo = models.TextField()
    task_client = models.TextField(blank=True, default="")  # <-- НОВОЕ

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return self.title
