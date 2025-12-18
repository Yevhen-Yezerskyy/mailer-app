# FILE: web/panel/aap_settings/models.py  (обновлено — 2025-12-18)
# CHANGE: модель переехала под panel/, сохранены старые app_label и db_table,
#         БД/таблицы/миграции не меняем.

from django.db import models


class MailConnection(models.Model):
    """
    Подключение почтового ящика в рамках воркспейса.
    Все статусы служебные, меняются только кодом.
    """

    workspace_id = models.UUIDField(
        db_index=True,
        help_text="UUID воркспейса (как в workspace_link у пользователя)",
    )

    name = models.CharField(
        max_length=255,
        help_text="Название подключения, видно только внутри панели",
    )

    soft_deleted = models.BooleanField(
        default=False,
        help_text="Если включено — подключение скрыто, но не удалено",
    )

    smtp_config = models.JSONField(default=dict, blank=True)
    smtp_status = models.CharField(max_length=32, default="not_checked")

    imap_config = models.JSONField(default=dict, blank=True)
    imap_status = models.CharField(max_length=32, default="not_checked")

    from_email = models.EmailField(max_length=254)
    from_name = models.CharField(max_length=255)

    status = models.CharField(max_length=32, default="not_checked")

    last_check_payload = models.JSONField(default=dict, blank=True)
    last_checked_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_mailconnection"
        ordering = ["name"]
        indexes = [
            models.Index(fields=["workspace_id", "soft_deleted"]),
        ]

    def __str__(self):
        return f"{self.name} <{self.from_email}>"

    STATUS_NOT_CHECKED = "not_checked"
    STATUS_OK = "ok"
    STATUS_PARTIAL = "partial"
    STATUS_ERROR = "error"
