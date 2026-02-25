# FILE: web/panel/aap_settings/models.py  (обновлено — 2026-01-27)
# PURPOSE: aap_settings models: add Mailbox.archived to soft-delete mailboxes (hide from UI, keep DB rows).
# CHANGE: Mailbox now has archived=BooleanField(default=False). Other fields/choices unchanged.

from __future__ import annotations

from django.db import models

from engine.common.mail.types import IMAP_CREDENTIALS_FORMAT, SMTP_CREDENTIALS_FORMAT


AUTH_TYPE_CHOICES = [(k, k) for k in SMTP_CREDENTIALS_FORMAT.keys()]
AUTH_TYPE_DEFAULT = "LOGIN"


class Mailbox(models.Model):
    workspace_id = models.UUIDField(db_index=True)
    email = models.EmailField(max_length=254)
    domain = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    archived = models.BooleanField(default=False)

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
        choices=AUTH_TYPE_CHOICES,
        default=AUTH_TYPE_DEFAULT,
    )

    # credentials_json строго соответствует engine/common/mail/types.py (SMTP_* formats)
    credentials_json = models.JSONField(default=dict)

    sender_name = models.CharField(max_length=255, blank=True, default="")
    from_email = models.EmailField()

    limit_hour_sent = models.PositiveIntegerField(default=50)
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
        choices=[(k, k) for k in IMAP_CREDENTIALS_FORMAT.keys()],
        default=AUTH_TYPE_DEFAULT,
    )

    # credentials_json строго соответствует engine/common/mail/types.py (IMAP_* formats)
    credentials_json = models.JSONField(default=dict)

    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_imap_mailboxes"


class ProviderPreset(models.Model):
    """
    UI-only presets. preset_json может содержать:
    {
      "smtp": {"login": {"host": "...", "port": 587, "security": "starttls"}, "auth_type": "LOGIN", ...},
      "imap": {"login": {"host": "...", "port": 993, "security": "ssl"}, "auth_type": "LOGIN", ...},
    }
    """

    name = models.CharField(max_length=255)
    preset_json = models.JSONField(
        default=dict,
        help_text="Произвольные подсказки для UI (smtp/imap host/ports/security/auth_type/oauth_hint/etc.)",
    )

    is_active = models.BooleanField(default=True)
    order = models.IntegerField(default=0)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_provider_presets"
        ordering = ["order", "name"]

    def __str__(self) -> str:
        return self.name


class ProviderPresetNoAuth(models.Model):
    """
    UI-only presets for SMTP relay without authentication.
    preset_json may contain:
    {
      "smtp": {"relay_noauth": {"host": "...", "port": 587, "security": "starttls"}, "auth_type": "RELAY_NOAUTH"},
    }
    """

    name = models.CharField(max_length=255)
    preset_json = models.JSONField(
        default=dict,
        help_text="Произвольные подсказки для UI (smtp relay noauth host/ports/security/auth_type/etc.)",
    )

    is_active = models.BooleanField(default=True)
    order = models.IntegerField(default=0)

    class Meta:
        app_label = "aap_settings"
        db_table = "aap_settings_provider_presets_noauth"
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
