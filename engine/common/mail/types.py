# FILE: engine/common/mail/types.py
# DATE: 2026-01-22
# PURPOSE: Mail domain types: runtime dataclasses + logging specs (MAIL_SPECS).
# CHANGE:
# - Add DOMAIN_TECH_CHECK (GOOD/BAD) and DOMAIN_REPUTATION_CHECK (NORMAL/QUESTIONABLE).
# - Make MailUiResult.status generic (str) to support non-OK/FAIL statuses.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional


# =========================
# Logging contract (ACTIONS / STATUSES)
# =========================

MAIL_SPECS: Dict[str, Dict[str, Any]] = {
    "SMTP_CHECK": {
        "statuses": ["OK", "FAIL"],
        "comment": "Binary SMTP connectivity/auth check.",
    },
    "DOMAIN_TECH_CHECK": {
        "statuses": ["GOOD", "BAD"],
        "comment": "Domain DNS technical check (SPF + DMARC).",
    },
    "DOMAIN_REPUTATION_CHECK": {
        "statuses": ["NORMAL", "QUESTIONABLE"],
        "comment": "Domain reputation check via Spamhaus DBL (DQS).",
    },
}


# =========================
# Runtime types (used by smtp.py)
# =========================

ConnSecurity = Literal["none", "ssl", "starttls"]
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
