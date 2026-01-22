# FILE: engine/common/mail/types.py
# DATE: 2026-01-22
# PURPOSE: Shared datatypes/contracts for engine/common/mail actions.
# CHANGE: (new) Minimal stable result + SMTP config types.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional


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
    """Generic result for mail actions (smtp/imap/dns/send/read).

    NOTE: mail-layer does not log anywhere; callers decide how/where to persist.
    """

    ok: bool
    action: str
    stage: str

    code: str = ""
    message: str = ""

    details: Dict[str, Any] = field(default_factory=dict)
    latency_ms: Optional[int] = None

    # optionally populated by send
    message_id: str = ""
