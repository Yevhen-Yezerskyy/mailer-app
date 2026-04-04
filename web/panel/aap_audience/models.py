# FILE: web/panel/aap_audience/models.py
# DATE: 2026-01-01

from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models


class AudienceTask(models.Model):
    workspace_id = models.UUIDField(db_index=True)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)

    task = models.TextField()
    title = models.CharField(max_length=255)
    task_branches = models.TextField()
    task_geo = models.TextField()
    task_client = models.TextField(blank=True, default="")
    source_product = models.TextField(blank=True, default="")
    source_company = models.TextField(blank=True, default="")
    source_geo = models.TextField(blank=True, default="")
    rating_city_hash = models.BigIntegerField(blank=True, null=True)
    rating_branch_hash = models.BigIntegerField(blank=True, null=True)

    type = models.CharField(
        max_length=4,
        choices=[("buy", "buy"), ("sell", "sell")],
        default="sell",
    )

    collected = models.BooleanField(default=False)
    ready = models.BooleanField(default=False)
    archived = models.BooleanField(default=False)  # NEW
    active = models.BooleanField(default=False)
    user_active = models.BooleanField(default=True)

    run_processing = models.BooleanField(default=False)
    subscribers_limit = models.IntegerField(default=0)
    rate_limit = models.IntegerField(
        default=50,
        validators=[MinValueValidator(20), MaxValueValidator(60)],
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_audience"
        db_table = "aap_audience_audiencetask"
        ordering = ["-created_at"]

    def __str__(self):
        return self.title
