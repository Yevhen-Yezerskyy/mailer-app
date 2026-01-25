# FILE: engine/common/mail/types.py
# DATE: 2026-01-24
# PURPOSE: Single source of truth for SMTP/IMAP credentials_json formats
#          and explicit binding auth_type -> expected payload shape.
# CHANGE:
# - SMTP and IMAP logically separated.
# - auth_type lives outside credentials_json.
# - No unions, no inheritance except strict 1:1 aliases.
# - Binding dicts define which connection types exist and which format they require.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypedDict


# =========================
# Shared primitives
# =========================

ConnSecurity = Literal["none", "ssl", "starttls"]


# =========================
# SMTP credentials_json formats
# =========================

class SmtpCredsLogin(TypedDict):
    host: str
    port: int
    security: ConnSecurity
    username: str
    password: str


class SmtpCredsGoogleOAuth2(TypedDict):
    host: str
    port: int
    security: ConnSecurity
    email: str
    access_token: str
    refresh_token_enc: str
    expires_at: int  # unix epoch seconds


class SmtpCredsMicrosoftOAuth2(TypedDict):
    host: str
    port: int
    security: ConnSecurity
    email: str
    tenant: str
    access_token: str
    refresh_token_enc: str
    expires_at: int  # unix epoch seconds


# =========================
# IMAP credentials_json formats
# Strict 1:1 aliases of SMTP formats (identical today)
# =========================

ImapCredsLogin = SmtpCredsLogin
ImapCredsGoogleOAuth2 = SmtpCredsGoogleOAuth2
ImapCredsMicrosoftOAuth2 = SmtpCredsMicrosoftOAuth2


# =========================
# auth_type -> format binding
# THE canonical list of supported connection types
# =========================

SMTP_CREDENTIALS_FORMAT = {
    "LOGIN": SmtpCredsLogin,
    "GOOGLE_OAUTH_2_0": SmtpCredsGoogleOAuth2,
    "MICROSOFT_OAUTH_2_0": SmtpCredsMicrosoftOAuth2,
}

IMAP_CREDENTIALS_FORMAT = {
    "LOGIN": ImapCredsLogin,
    "GOOGLE_OAUTH_2_0": ImapCredsGoogleOAuth2,
    "MICROSOFT_OAUTH_2_0": ImapCredsMicrosoftOAuth2,
}
