# FILE: web/aap_settings/models.py

from django.db import models


class MailConnection(models.Model):
    """
    Подключение почтового ящика в рамках воркспейса.
    Все статусы служебные, меняются только кодом.
    """

    # Воркспейс, в рамках которого живёт это подключение
    workspace_id = models.UUIDField(
        db_index=True,
        help_text="UUID воркспейса (такой же, как в workspace_link у пользователя)",
    )

    # Человекочитаемое имя подключения
    name = models.CharField(
        max_length=255,
        help_text="Название подключения, видно только внутри панели",
    )

    # Мягкое удаление / архив
    soft_deleted = models.BooleanField(
        default=False,
        help_text="Если включено — подключение скрыто, но не удалено",
    )

    # SMTP-конфиг и статус (служебное поле, правится только кодом)
    smtp_config = models.JSONField(
        default=dict,
        blank=True,
        help_text="Настройки SMTP (host, port, encryption, username, password, ...)",
    )
    # варианты значения: not_checked / ok / warning / error
    smtp_status = models.CharField(
        max_length=32,
        default="not_checked",
    )

    # IMAP-конфиг и статус (служебное поле, правится только кодом)
    imap_config = models.JSONField(
        default=dict,
        blank=True,
        help_text="Настройки IMAP (host, port, encryption, username, password, folders, ...)",
    )
    # варианты значения: not_checked / ok / warning / error
    imap_status = models.CharField(
        max_length=32,
        default="not_checked",
    )

    # От кого отправляем (один набор на запись)
    from_email = models.EmailField(
        max_length=254,
        help_text="E-mail отправителя (From:)",
    )
    from_name = models.CharField(
        max_length=255,
        help_text="Отображаемое имя отправителя",
    )

    # Общий статус подключения (служебное поле)
    # варианты: not_checked / ok / partial / error
    status = models.CharField(
        max_length=32,
        default="not_checked",
        help_text="Общий статус подключения (меняется только кодом)",
    )

    # Сырой результат последней проверки (DKIM/SPF, ошибки и пр.)
    last_check_payload = models.JSONField(
        default=dict,
        blank=True,
        help_text="Сырой JSON результата последней проверки",
    )
    last_checked_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Когда последний раз проверяли подключение",
    )

    # Служебные таймстемпы
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]
        indexes = [
            models.Index(fields=["workspace_id", "soft_deleted"]),
        ]

    def __str__(self):
        return f"{self.name} <{self.from_email}>"

    # служебные константы, в БД не лезут
    STATUS_NOT_CHECKED = "not_checked"
    STATUS_OK = "ok"
    STATUS_PARTIAL = "partial"
    STATUS_ERROR = "error"
