# FILE: engine/common/mail/logs.py
# DATE: 01-29
# PURPOSE:
# - Strict mailbox_events logger (actions + statuses)
# - Strict mailbox_sent logger (statuses only)
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
    # DOMAINS
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
    # SMTP
    "SMTP_AUTH_CHECK": (
        "SUCCESS",
        "FAIL",
    ),
    "SMTP_SEND_CHECK": (
        "SUCCESS",
        "FAIL_TMP",
        "FAIL",
    ),
    # IMAP
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


# =========================
# (C) mailbox_sent format (single source of truth)
# =========================

MAILBOX_SENT_STATUSES: tuple[str, ...] = (
    "SEND",
    "BAD_ADDRESS",
    "REPUTATION",
    "OTHER",
)


# =========================
# (D) mailbox_sent logger (strict)
# =========================

def log_mailbox_sent(
    *,
    campaign_id: int,
    list_id: int,
    rate_contact_id: int,
    status: str,
    payload_json: Dict[str, Any],
) -> None:
    """
    Insert single row into mailbox_sent.

    Contract:
      - status MUST be one of MAILBOX_SENT_STATUSES
      - payload_json MUST be dict
      - all ids MUST be int-like
      - processed=true, processed_at=now() are set here
    """
    _validate_sent_status(status)

    if not isinstance(payload_json, dict):
        raise ValueError("mail_bad_payload:payload_json_must_be_dict")

    db.execute(
        """
        INSERT INTO mailbox_sent (
            campaign_id,
            list_id,
            rate_contact_id,
            processed,
            status,
            data,
            processed_at
        )
        VALUES (%s, %s, %s, true, %s, %s, now())
        """,
        (
            int(campaign_id),
            int(list_id),
            int(rate_contact_id),
            status,
            Json(payload_json),
        ),
    )


def _validate_sent_status(status: str) -> None:
    if status not in MAILBOX_SENT_STATUSES:
        raise ValueError(f"mail_bad_sent_status:{status}")
