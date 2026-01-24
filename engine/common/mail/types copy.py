# FILE: engine/common/mail/types.py
# DATE: 2026-01-24
# PURPOSE: Mail domain types: runtime dataclasses + logging specs (MAIL_SPECS) + JSON contracts for SMTP/IMAP credentials_json.
# CHANGE:
# - Add single source of truth for connection auth_type values (ConnAuthType).
# - Add explicit TypedDict contracts for credentials_json (SMTP/IMAP × LOGIN/GOOGLE_OAUTH_2_0/MICROSOFT_OAUTH_2_0).
# - Keep existing runtime dataclasses + MAIL_SPECS unchanged (only moved below the auth_type section).

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional, TypedDict, Union


# =========================
# Connection JSON: source of truth (AAP Settings credentials_json)
# =========================

ConnSecurity = Literal["none", "ssl", "starttls"]

ConnAuthType = Literal[
    "LOGIN",
    "GOOGLE_OAUTH_2_0",
    "MICROSOFT_OAUTH_2_0",
]

# SMTP credentials_json (3 variants)

class SmtpCredsLogin(TypedDict):
    auth_type: Literal["LOGIN"]
    host: str
    port: int
    security: ConnSecurity
    username: str
    password: str


class SmtpCredsGoogleOAuth2(TypedDict):
    auth_type: Literal["GOOGLE_OAUTH_2_0"]
    host: str
    port: int
    security: ConnSecurity
    email: str
    access_token: str
    refresh_token_enc: str
    expires_at: int  # unix epoch seconds


class SmtpCredsMicrosoftOAuth2(TypedDict):
    auth_type: Literal["MICROSOFT_OAUTH_2_0"]
    host: str
    port: int
    security: ConnSecurity
    email: str
    tenant: str
    access_token: str
    refresh_token_enc: str
    expires_at: int  # unix epoch seconds


SmtpCredentialsJson = Union[SmtpCredsLogin, SmtpCredsGoogleOAuth2, SmtpCredsMicrosoftOAuth2]

# IMAP credentials_json (3 variants)

class ImapCredsLogin(TypedDict):
    auth_type: Literal["LOGIN"]
    host: str
    port: int
    security: ConnSecurity
    username: str
    password: str


class ImapCredsGoogleOAuth2(TypedDict):
    auth_type: Literal["GOOGLE_OAUTH_2_0"]
    host: str
    port: int
    security: ConnSecurity
    email: str
    access_token: str
    refresh_token_enc: str
    expires_at: int  # unix epoch seconds


class ImapCredsMicrosoftOAuth2(TypedDict):
    auth_type: Literal["MICROSOFT_OAUTH_2_0"]
    host: str
    port: int
    security: ConnSecurity
    email: str
    tenant: str
    access_token: str
    refresh_token_enc: str
    expires_at: int  # unix epoch seconds


ImapCredentialsJson = Union[ImapCredsLogin, ImapCredsGoogleOAuth2, ImapCredsMicrosoftOAuth2]


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
