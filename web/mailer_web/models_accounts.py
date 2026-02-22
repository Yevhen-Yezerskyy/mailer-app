# FILE: web/mailer_web/models_accounts.py  (новое — 2025-12-18)
# Смысл: user-related модели, физически живут в mailer_web, но сохраняют старые app_label/db_table,
# чтобы БД/таблицы/контент-тайпы не “переехали”.

from uuid import uuid4

from django.conf import settings
from django.db import models


class FrontUser(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="front",
    )

    class Meta:
        db_table = "accounts_frontuser"

    def __str__(self) -> str:
        return self.user.username


class UserWorkspace(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="workspace_link",
    )

    # быстрый UUID, генерируется автоматически
    workspace_id = models.UUIDField(
        default=uuid4,
        db_index=True,
        editable=False,
    )

    class Meta:
        db_table = "accounts_userworkspace"

    def __str__(self) -> str:
        return f"{self.user.username} @ {self.workspace_id}"
