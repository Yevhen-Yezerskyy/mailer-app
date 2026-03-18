# FILE: engine/core_imap/imap_message.py
# DATE: 2026-03-05
# PURPOSE: Read/parse/classify one IMAP message and upsert blocked recipient when needed.

from __future__ import annotations

import re
from email import policy
from email.message import Message
from email.parser import BytesParser
from email.utils import parseaddr
from typing import Any, Dict, Optional

from engine.common import db

_SYSTEM_FROM_MARKERS = (
    "mailer-daemon",
    "mail delivery subsystem",
    "mail delivery system",
    "mail delivery",
    "postmaster",
)
_DSN_CONTENT_TYPES = ("message/delivery-status", "message/rfc822")
_EMAIL_WRONG_STATUS_RETURNED_MAIL = "RETURNED MAIL"


def _extract_email(value: str) -> Optional[str]:
    raw = (value or "").strip()
    if ";" in raw:
        raw = raw.split(";", 1)[1].strip()
    _name, addr = parseaddr(raw)
    addr = (addr or "").strip().lower()
    if not addr or "@" not in addr or " " in addr:
        return None
    local, _, domain = addr.partition("@")
    if not local or not domain or "." not in domain:
        return None
    return addr


def _has_dsn_part(msg: Message) -> bool:
    for part in msg.walk():
        ctype = (part.get_content_type() or "").lower()
        if ctype in _DSN_CONTENT_TYPES:
            return True
    return False


def _is_candidate_message(msg: Message) -> bool:
    from_hdr = (msg.get("From") or "").strip().lower()
    from_name, from_email = parseaddr(from_hdr)
    from_mix = f"{from_name} {from_email}".strip().lower()

    if any(marker in from_mix for marker in _SYSTEM_FROM_MARKERS):
        return True
    return _has_dsn_part(msg)


def _parse_dsn_fields(raw_text: str) -> Dict[str, Optional[str]]:
    action: Optional[str] = None
    status: Optional[str] = None
    failed_email: Optional[str] = None

    for line in (raw_text or "").splitlines():
        low = line.strip().lower()
        if not low:
            continue

        if low.startswith("action:") and action is None:
            action = low.split(":", 1)[1].strip()
            continue

        if low.startswith("status:") and status is None:
            status = low.split(":", 1)[1].strip()
            continue

        if failed_email is None and low.startswith("final-recipient:"):
            failed_email = _extract_email(low.split(":", 1)[1])
            continue

        if failed_email is None and low.startswith("original-recipient:"):
            failed_email = _extract_email(low.split(":", 1)[1])
            continue

        if failed_email is None and low.startswith("x-failed-recipients:"):
            failed_email = _extract_email(low.split(":", 1)[1].split(",", 1)[0])

    return {"action": action, "status": status, "failed_email": failed_email}


def _classify_from_dsn(dsn: Dict[str, Optional[str]]) -> tuple[Optional[str], bool]:
    action = (dsn.get("action") or "").lower()
    status = (dsn.get("status") or "").lower()

    if (action == "failed" and status.startswith("5.")) or status.startswith("5."):
        return "HARD_BOUNCE_OTHER", True

    if status.startswith("4.") or action == "delayed":
        return "TEMP_BOUNCE", False

    return None, False


def _classify_message(msg: Message, raw_text: str) -> tuple[Optional[str], bool, Optional[str]]:
    # Stage 1: candidate filter (system sender or DSN attachment only).
    if not _is_candidate_message(msg):
        return None, False, None

    # Stage 2: deterministic DSN parse -> classify.
    dsn = _parse_dsn_fields(raw_text)
    reason_code, should_block = _classify_from_dsn(dsn)
    if reason_code is not None:
        return reason_code, should_block, dsn.get("failed_email")

    # Candidate mail without clear DSN status is service noise.
    return "MAILER_NOISE", False, dsn.get("failed_email")


def _unique_aggr_contact_id_by_email(email_value: str) -> Optional[int]:
    rows = db.fetch_all(
        """
        SELECT id
        FROM public.raw_contacts_aggr
        WHERE lower(email) = lower(%s)
        ORDER BY id
        LIMIT 2
        """,
        [email_value],
    )
    if len(rows) != 1 or rows[0][0] is None:
        return None
    return int(rows[0][0])


def _upsert_blocked_recipient(
    *,
    aggr_contact_id: int,
    mailbox_sent_id: Optional[int],
    reason_code: str,
    source_eml: str,
) -> None:
    db.execute(
        """
        INSERT INTO public.mail_blocked_recipients (
            aggr_contact_id,
            mailbox_sent_id,
            reason_code,
            source_eml,
            active,
            created_at,
            updated_at,
            hits_count
        )
        VALUES (%s, %s, %s, %s, true, now(), now(), 1)
        ON CONFLICT (aggr_contact_id) DO UPDATE
        SET mailbox_sent_id = COALESCE(EXCLUDED.mailbox_sent_id, public.mail_blocked_recipients.mailbox_sent_id),
            reason_code = EXCLUDED.reason_code,
            source_eml = EXCLUDED.source_eml,
            active = true,
            updated_at = now(),
            hits_count = public.mail_blocked_recipients.hits_count + 1
        """,
        [int(aggr_contact_id), mailbox_sent_id, reason_code, source_eml],
    )


def _mark_aggr_email_wrong(*, aggr_contact_id: int, status: str) -> None:
    db.execute(
        """
        UPDATE public.raw_contacts_aggr
        SET email_wrong = true,
            email_wrong_status = %s,
            updated_at = now()
        WHERE id = %s
        """,
        [str(status or "").strip() or _EMAIL_WRONG_STATUS_RETURNED_MAIL, int(aggr_contact_id)],
    )


def process_imap_message(mailbox_id: int, folder: str, uid: str, raw_msg: bytes) -> Dict[str, Any]:
    raw_text = raw_msg.decode("utf-8", errors="replace")
    msg = BytesParser(policy=policy.default).parsebytes(raw_msg)
    reason_code, should_block, parsed_failed_email = _classify_message(msg, raw_text)
    if reason_code is None:
        return {"kind": "skip", "moved": False, "blocked": False}

    failed_email = parsed_failed_email

    aggr_contact_id: Optional[int] = None
    if failed_email:
        aggr_contact_id = _unique_aggr_contact_id_by_email(failed_email)

    if should_block and aggr_contact_id is not None:
        _mark_aggr_email_wrong(
            aggr_contact_id=int(aggr_contact_id),
            status=_EMAIL_WRONG_STATUS_RETURNED_MAIL,
        )
        _upsert_blocked_recipient(
            aggr_contact_id=aggr_contact_id,
            mailbox_sent_id=None,
            reason_code=reason_code,
            source_eml=raw_text,
        )

    return {
        "kind": reason_code,
        "moved": True,
        "blocked": bool(should_block and aggr_contact_id is not None),
        "mailbox_sent_id": None,
        "aggr_contact_id": aggr_contact_id,
        "failed_email": failed_email,
        "folder": folder,
        "uid": uid,
    }
