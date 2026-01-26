# FILE: engine/common/mail/logs.py
# DATE: 2026-01-24
# PURPOSE:
# - Strict mailbox_events logger with hard action/status contract.
# - TEMP reversible obfuscation helpers (moved to end).
# NOTES:
# - NO normalization, NO trimming, NO auto-fixes.
# - Wrong input → exception.

from __future__ import annotations

import base64
from typing import Any, Dict

from psycopg.types.json import Json
from engine.common import db


# =========================
# (A) mailbox_events format (single source of truth)
# =========================

MAIL_ACTIONS_FORMAT: Dict[str, tuple[str, ...]] = {
    #DOMAINS
    "DOMAIN_CHECK_TECH": (
        "GOOD",
        "NORMAL",
        "BAD",
        "TRUSTED",
        "CHECK_FAILED",
    ),
    
    "DOMAIN_CHECK_REPUTATION": (
        "NORMAL",
        "QUESTIONABLE",
        "TRUSTED",
        "CHECK_FAILED",
    ),

    #SMTP
    "SMTP_AUTH_CHECK": (
        "SUCCESS",
        "FAIL",
    ),

    "SMTP_SEND_CHECK": (
        "SUCCESS",
        "FAIL",
    ),

    #IMAP
    "IMAP_CHECK": (
        "SUCCESS",
        "FAIL",
    ),
}


# =========================
# (B) mailbox_events logger (strict)
# =========================

def log_mail_event(
    *,
    mailbox_id: int,
    action: str,
    status: str,
    payload_json: Dict[str, Any],
) -> None:
    """
    Append-only logger for mailbox_events.

    Contract:
      - action MUST be exact key from MAIL_ACTIONS_FORMAT
      - status MUST be one of allowed statuses for action
      - payload_json MUST be dict
    """
    _validate_action_status(action, status)

    if not isinstance(payload_json, dict):
        raise ValueError("mail_bad_payload:payload_json_must_be_dict")

    db.execute(
        """
        INSERT INTO mailbox_events (mailbox_id, action, status, data)
        VALUES (%s, %s, %s, %s)
        """,
        (int(mailbox_id), action, status, Json(payload_json)),
    )


def _validate_action_status(action: str, status: str) -> None:
    if action not in MAIL_ACTIONS_FORMAT:
        raise ValueError(f"mail_bad_action:{action}")

    if status not in MAIL_ACTIONS_FORMAT[action]:
        raise ValueError(f"mail_bad_status:{action}:{status}")


