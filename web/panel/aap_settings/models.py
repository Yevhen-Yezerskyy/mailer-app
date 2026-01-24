# FILE: web/panel/aap_settings/models.py
# DATE: 2026-01-24
# PURPOSE: aap_settings models (FINAL, CLEAN) — восстановлено как в архиве 2026-01-23, чтобы вернуть SendingSettings/MailboxOAuthApp и снять ImportError.
# CHANGE: ВОССТАНОВЛЕНО исходное содержимое файла (без самодеятельных добавлений/удалений).

from __future__ import annotations

from django.db import models


class AuthType(models.TextChoices):
    LOGIN = "login", "Login"
    GOOGLE_OAUTH2 = "google_oauth2", "Google OAuth2"
    MICROSOFT_OAUTH2 = "microsoft_oauth2", "Microsoft OAuth2"


class Mailbox(models.Model):
    workspace_id = models.UUIDField(db_index=True)
    email = models.EmailField(max_length=254)
    domain = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_mailboxes"
        ordering = ["email"]
        constraints = [
            models.UniqueConstraint(
                fields=["workspace_id", "email"],
                name="aap_settings_mailbox_ws_email_uniq",
            ),
        ]
        indexes = [
            models.Index(fields=["workspace_id", "is_active"]),
            models.Index(fields=["email"]),
        ]

    def __str__(self) -> str:
        return f"<{self.email}>"


class SmtpMailbox(models.Model):
    mailbox = models.OneToOneField(
        Mailbox,
        on_delete=models.CASCADE,
        related_name="smtp",
    )

    auth_type = models.CharField(
        max_length=32,
        choices=AuthType.choices,
        default=AuthType.LOGIN,
    )

    # ВСЕ данные логина / OAuth (host/port/tls/user/secret или oauth-поля)
    credentials_json = models.JSONField(default=dict)

    # Отправитель (НЕ логин)
    sender_name = models.CharField(max_length=255, blank=True, default="")
    from_email = models.EmailField()

    # Лимиты
    limit_hour_sent = models.PositiveIntegerField(default=50)

    # Доп. заголовки писем (BCC, Reply-To, etc.)
    extra_headers_json = models.JSONField(default=dict, blank=True)

    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_smtp_mailboxes"


class ImapMailbox(models.Model):
    mailbox = models.OneToOneField(
        Mailbox,
        on_delete=models.CASCADE,
        related_name="imap",
    )

    auth_type = models.CharField(
        max_length=32,
        choices=AuthType.choices,
        default=AuthType.LOGIN,
    )

    # ВСЕ данные логина / OAuth
    credentials_json = models.JSONField(default=dict)

    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_imap_mailboxes"


class MailboxOAuthApp(models.Model):
    """
    Workspace-scoped OAuth client credentials (Google / Microsoft).
    Используется SMTP и IMAP, не хранит user-токены.
    """

    workspace_id = models.UUIDField(db_index=True)
    provider = models.CharField(
        max_length=32,
        choices=[
            ("google", "Google"),
            ("microsoft", "Microsoft"),
        ],
    )

    client_id = models.CharField(max_length=255)
    client_secret_enc = models.TextField()

    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_mailbox_oauth_apps"
        constraints = [
            models.UniqueConstraint(
                fields=["workspace_id", "provider"],
                name="aap_settings_oauth_app_ws_provider_uniq",
            ),
        ]
        indexes = [
            models.Index(fields=["workspace_id", "provider", "is_active"]),
        ]


class ProviderPreset(models.Model):
    """
    UI-only presets.
    Никакой логики, только подсказки для автозаполнения форм.
    """

    name = models.CharField(max_length=255)

    preset_json = models.JSONField(
        default=dict,
        help_text="Произвольные подсказки для UI (host/ports/tls/oauth_hint/etc.)",
    )

    is_active = models.BooleanField(default=True)
    order = models.IntegerField(default=0)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_provider_presets"
        ordering = ["order", "name"]

    def __str__(self) -> str:
        return self.name


class SendingSettings(models.Model):
    workspace_id = models.UUIDField(unique=True, db_index=True)
    value_json = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_sending_settings"
