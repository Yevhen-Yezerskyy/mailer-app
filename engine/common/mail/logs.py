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
# (C) TEMP secret obfuscation (isolated, dumb)
# =========================

# TEMP key (соль, не безопасность)
_KEY = b"serenity-mail-secret-key"


def _xor(data: bytes, key: bytes) -> bytes:
    klen = len(key)
    return bytes(b ^ key[i % klen] for i, b in enumerate(data))


def encrypt_secret(plain: str) -> str:
    if not plain:
        return ""
    raw = plain.encode("utf-8")
    x = _xor(raw, _KEY)
    return base64.urlsafe_b64encode(x).decode("ascii")


def decrypt_secret(secret_enc: str) -> str:
    if not secret_enc:
        return ""
    try:
        raw = base64.urlsafe_b64decode(secret_enc.encode("ascii"))
        plain = _xor(raw, _KEY)
        return plain.decode("utf-8", errors="strict")
    except Exception as e:
        raise ValueError("secret_decrypt_failed") from e
