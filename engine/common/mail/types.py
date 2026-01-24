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


# =========================
# Logging contract (ACTIONS / STATUSES)
# =========================

MAIL_SPECS: Dict[str, Dict[str, Any]] = {
    "SMTP_CHECK": {
        "statuses": ["OK", "FAIL"],
        "comment": "Binary SMTP connectivity/auth check.",
    },
    "IMAP_CHECK": {
        "statuses": ["OK", "FAIL"],
        "comment": "Binary IMAP connectivity/auth check.",
    },
    "IMAP_LIST_FOLDERS": {
        "statuses": ["OK", "FAIL"],
        "comment": "IMAP folders list (LIST).",
    },
    "DOMAIN_TECH_CHECK": {
        "statuses": ["GOOD", "BAD", "CHECK_FAILED"],
        "comment": "Domain DNS technical check (SPF + DMARC).",
    },
    "DOMAIN_REPUTATION_CHECK": {
        "statuses": ["NORMAL", "QUESTIONABLE", "CHECK_FAILED"],
        "comment": "Domain reputation check via Spamhaus DBL (DQS).",
    },
}


# =========================
# Runtime types (used by smtp.py / imap.py)
# =========================

AuthType = Literal["login", "oauth2"]


@dataclass(frozen=True)
class SmtpCfg:
    mailbox_id: int
    email: str
    domain: str

    host: str
    port: int
    security: ConnSecurity
    auth_type: AuthType

    username: str
    secret: str
    extra: Dict[str, Any] = field(default_factory=dict)

    timeout_sec: float = 10.0


@dataclass(frozen=True)
class ImapCfg:
    mailbox_id: int
    email: str
    domain: str

    host: str
    port: int
    security: ConnSecurity
    auth_type: AuthType

    username: str
    secret: str
    extra: Dict[str, Any] = field(default_factory=dict)

    timeout_sec: float = 10.0


@dataclass
class MailResult:
    """Generic raw result for mail actions (smtp/imap/dns/send/read)."""

    ok: bool
    action: str
    stage: str

    code: str = ""
    message: str = ""

    details: Dict[str, Any] = field(default_factory=dict)
    latency_ms: Optional[int] = None

    message_id: str = ""


# =========================
# UI-level compact result
# =========================

@dataclass
class MailUiResult:
    """Compact result ready to show to user (no parsing needed)."""

    status: str
    user_message: str = ""
    data: Dict[str, Any] = field(default_factory=dict)