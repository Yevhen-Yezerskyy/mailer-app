# FILE: web/panel/aap_settings/models.py
# DATE: 2026-01-23
# PURPOSE: aap_settings models.
# CHANGE: Add MailboxOAuthApp (workspace-scoped OAuth client credentials for Google/Microsoft).

from __future__ import annotations

from django.db import models


class ConnKind(models.TextChoices):
    SMTP = "smtp", "SMTP"
    IMAP = "imap", "IMAP"


class Security(models.TextChoices):
    NONE = "none", "None"
    SSL = "ssl", "SSL"
    STARTTLS = "starttls", "STARTTLS"


class AuthType(models.TextChoices):
    LOGIN = "login", "Login"
    GOOGLE_OAUTH2 = "google_oauth2", "Google OAuth2"
    MICROSOFT_OAUTH2 = "microsoft_oauth2", "Microsoft OAuth2"


class OAuthProvider(models.TextChoices):
    GOOGLE = "google", "Google"
    MICROSOFT = "microsoft", "Microsoft"


class Mailbox(models.Model):
    workspace_id = models.UUIDField(db_index=True, help_text="UUID воркспейса")
    email = models.EmailField(max_length=254, help_text="Полный email адрес")
    domain = models.CharField(max_length=255, help_text="Домен email (для DNS-проверок)")
    is_active = models.BooleanField(default=True, help_text="Если false — ящик полностью выключен")

    limit_hour_sent = models.IntegerField(default=50, help_text="Лимит исходящих писем в час")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_mailboxes"
        ordering = ["email"]
        constraints = [
            models.UniqueConstraint(fields=["email"], name="aap_settings_mailbox_email_uniq"),
        ]
        indexes = [
            models.Index(fields=["workspace_id", "is_active"]),
            models.Index(fields=["email"]),
        ]

    def __str__(self) -> str:
        return f"<{self.email}>"


class MailboxConnection(models.Model):
    mailbox = models.ForeignKey(Mailbox, on_delete=models.CASCADE, related_name="connections")

    kind = models.CharField(max_length=8, choices=ConnKind.choices)
    host = models.CharField(max_length=255)
    port = models.IntegerField()

    security = models.CharField(max_length=16, choices=Security.choices, default=Security.NONE)
    auth_type = models.CharField(max_length=16, choices=AuthType.choices, default=AuthType.LOGIN)

    username = models.CharField(max_length=255)
    secret_enc = models.TextField(help_text="Зашифрованный пароль или refresh_token")

    extra_json = models.JSONField(
        default=dict,
        blank=True,
        help_text="SMTP/IMAP-специфичные опции (SMTP: from_email/from_name; IMAP: folders/idle и т.п.)",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_mailbox_connections"
        constraints = [
            models.UniqueConstraint(fields=["mailbox", "kind"], name="aap_settings_mailbox_conn_kind_uniq"),
        ]
        indexes = [
            models.Index(fields=["mailbox", "kind"]),
        ]

    def __str__(self) -> str:
        return f"{self.mailbox.email} [{self.kind}]"


class MailboxOAuthApp(models.Model):
    """
    Workspace-scoped OAuth client credentials for SMTP/IMAP OAuth2 (XOAUTH2).
    Stores client_secret in *obfuscated* form (encrypt_secret), like other secrets.
    """
    workspace_id = models.UUIDField(db_index=True, help_text="UUID воркспейса")
    provider = models.CharField(max_length=16, choices=OAuthProvider.choices)

    client_id = models.CharField(max_length=255)
    client_secret_enc = models.TextField(help_text="Зашифрованный client_secret")

    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_mailbox_oauth_apps"
        constraints = [
            models.UniqueConstraint(fields=["workspace_id", "provider"], name="aap_settings_oauth_app_ws_provider_uniq"),
        ]
        indexes = [
            models.Index(fields=["workspace_id", "provider", "is_active"]),
        ]

    def __str__(self) -> str:
        return f"OAuthApp[{self.workspace_id}/{self.provider}]"


class ProviderPreset(models.Model):
    code = models.CharField(max_length=64)
    name = models.CharField(max_length=255)

    kind = models.CharField(max_length=8, choices=ConnKind.choices)
    host = models.CharField(max_length=255)

    ports_json = models.JSONField()
    security = models.CharField(max_length=16, choices=Security.choices, default=Security.NONE)
    auth_type = models.CharField(max_length=16, choices=AuthType.choices, default=AuthType.LOGIN)

    extra_json = models.JSONField(default=dict, blank=True)

    is_active = models.BooleanField(default=True)
    order = models.IntegerField(default=0)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_provider_presets"
        ordering = ["order", "name"]
        constraints = [
            models.UniqueConstraint(fields=["code", "kind"], name="aap_settings_provider_preset_code_kind_uniq"),
        ]
        indexes = [
            models.Index(fields=["is_active", "order"]),
            models.Index(fields=["code", "kind"]),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.code}/{self.kind})"


class SendingSettings(models.Model):
    workspace_id = models.UUIDField(
        unique=True,
        db_index=True,
        help_text="UUID workspace (1 запись на workspace)",
    )

    value_json = models.JSONField(
        default=dict,
        blank=True,
        help_text="Глобальные настройки отправки (JSON)",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_sending_settings"

    def __str__(self) -> str:
        return f"SendingSettings[{self.workspace_id}]"
