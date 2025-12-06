from django.db import models
from django.contrib.auth.models import User
from uuid import uuid4


class FrontUser(models.Model):
    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name="front",
    )

    def __str__(self):
        return self.user.username


class UserWorkspace(models.Model):
    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name="workspace_link",
    )

    # быстрый UUID, генерируется автоматически
    workspace_id = models.UUIDField(
        default=uuid4,      # ← правильная генерация UUID
        db_index=True,
        editable=False,     # ← скрыть в админке
    )

    def __str__(self):
        return f"{self.user.username} @ {self.workspace_id}"
