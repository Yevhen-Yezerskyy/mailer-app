# FILE: engine/common/mail/logs.py
# DATE: 2026-01-22
# PURPOSE:
# - TEMP reversible obfuscation for mail secrets (encrypt_secret/decrypt_secret).
# - Append-only mailbox_events logger: validate -> mask -> INSERT (raises on invalid).
# CHANGE:
# - Use psycopg3 Json() adapter for dict->jsonb.
# - Keep logs.py as "dumb logger": no scenarios, no returns.

from __future__ import annotations

import base64
from typing import Any, Dict, Iterable

from psycopg.types.json import Json

from engine.common import db
from engine.common.mail.types import MAIL_SPECS


# =========================
# (A) TEMP secret obfuscation
# =========================

# TEMP key (просто соль, не безопасность)
_KEY = b"serenity-mail-secret-key"


def _xor(data: bytes, key: bytes) -> bytes:
    klen = len(key)
    return bytes(b ^ key[i % klen] for i, b in enumerate(data))


def encrypt_secret(plain: str) -> str:
    s = (plain or "")
    if not s:
        return ""
    raw = s.encode("utf-8")
    x = _xor(raw, _KEY)
    return base64.urlsafe_b64encode(x).decode("ascii")


def decrypt_secret(secret_enc: str) -> str:
    s = (secret_enc or "")
    if not s:
        return ""
    try:
        raw = base64.urlsafe_b64decode(s.encode("ascii"))
        plain = _xor(raw, _KEY)
        return plain.decode("utf-8", errors="strict")
    except Exception as e:
        raise ValueError("secret_decrypt_failed") from e


# =========================
# (B) mailbox_events logger
# =========================

_SENSITIVE_KEY_PARTS: Iterable[str] = (
    "secret",
    "password",
    "passwd",
    "token",
    "access_token",
    "refresh_token",
    "authorization",
    "xoauth2",
)


def log_mail_event(*, mailbox_id: int, action: str, status: str, message: str = "", data: Any = None) -> None:
    """
    Append-only logger for mailbox_events.

    Rules:
      - action/status are normalized to UPPERCASE
      - (action,status) MUST match MAIL_SPECS, otherwise raises
      - data is masked (secrets/tokens) and stored as jsonb
    """
    a = (action or "").strip().upper()
    s = (status or "").strip().upper()
    _validate_action_status(a, s)

    payload = _mask_sensitive(_normalize_data(data))

    db.execute(
        """
        INSERT INTO mailbox_events (mailbox_id, action, status, message, data)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (int(mailbox_id), a, s, str(message or ""), Json(payload)),
    )


def _validate_action_status(action: str, status: str) -> None:
    spec = MAIL_SPECS.get(action)
    if not spec:
        raise ValueError(f"mail_bad_action:{action}")

    statuses = spec.get("statuses") or []
    if status not in statuses:
        raise ValueError(f"mail_bad_status:{action}:{status}")


def _normalize_data(data: Any) -> Dict[str, Any]:
    if data is None:
        return {}
    if isinstance(data, dict):
        return data
    return {"value": str(data)}


def _mask_sensitive(x: Any) -> Any:
    if isinstance(x, dict):
        out: Dict[str, Any] = {}
        for k, v in x.items():
            kk = str(k)
            out[kk] = "***" if _is_sensitive_key(kk) else _mask_sensitive(v)
        return out
    if isinstance(x, list):
        return [_mask_sensitive(v) for v in x]
    return x


def _is_sensitive_key(k: str) -> bool:
    kl = (k or "").strip().lower()
    return any(part in kl for part in _SENSITIVE_KEY_PARTS)
