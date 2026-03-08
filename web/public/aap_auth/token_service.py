# FILE: web/public/aap_auth/token_service.py
# DATE: 2026-03-07
# PURPOSE: issue/validate/consume user action tokens (email_confirm, password_reset).

from __future__ import annotations

import secrets
from datetime import timedelta

from django.utils import timezone

from mailer_web.models import UserActionToken, ClientUser


ACTION_EMAIL_CONFIRM = "email_confirm"
ACTION_PASSWORD_RESET = "password_reset"


def issue_token(*, user: ClientUser, action: str, ttl_hours: int = 24, meta: dict | None = None) -> UserActionToken:
    return UserActionToken.objects.create(
        user=user,
        action=action,
        token=secrets.token_urlsafe(16),
        expires_at=timezone.now() + timedelta(hours=ttl_hours),
        meta=meta or {},
    )


def get_active_token(*, token: str, action: str | None = None) -> UserActionToken | None:
    qs = UserActionToken.objects.select_related("user").filter(
        token=token,
        used_at__isnull=True,
        expires_at__gt=timezone.now(),
    )
    if action:
        qs = qs.filter(action=action)
    return qs.first()


def consume_token(token_obj: UserActionToken) -> UserActionToken:
    if token_obj.used_at is None:
        token_obj.used_at = timezone.now()
        token_obj.save(update_fields=["used_at"])
    return token_obj
