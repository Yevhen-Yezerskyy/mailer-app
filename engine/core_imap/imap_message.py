# FILE: engine/core_imap/imap_message.py
# DATE: 2026-03-05
# PURPOSE: Read/parse/classify one IMAP message and upsert blocked recipient when needed.

from __future__ import annotations

import re
from email import policy
from email.parser import BytesParser
from typing import Any, Dict, Optional

from engine.common import db

_RE_EMAIL = re.compile(r"(?i)\b([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})\b")
_RE_X_MAILER_ID = re.compile(r"(?im)^x-mailer-id:\s*([0-9]+)\s*$")
_RE_SMREL = re.compile(r"(?i)(?:\bsmrel=)([0-9]+)")
_RE_DSN_EMAIL_PATTERNS = (
    re.compile(r"(?im)^final-recipient:\s*rfc822;\s*([^\s<>;]+@[^\s<>;]+)\s*$"),
    re.compile(r"(?im)^original-recipient:\s*rfc822;\s*([^\s<>;]+@[^\s<>;]+)\s*$"),
    re.compile(r"(?im)^x-failed-recipients:\s*([^\s,<>;]+@[^\s,<>;]+)\s*$"),
)
_RE_DSN_ACTION = re.compile(r"(?im)^action:\s*([a-z]+)\s*$")
_RE_DSN_STATUS = re.compile(r"(?im)^status:\s*([245]\.[0-9]+\.[0-9]+)\s*$")

_AUTO_PATTERNS = (
    re.compile(r"(?i)\bout of office\b"),
    re.compile(r"(?i)\bauto(?:matic)? reply\b"),
    re.compile(r"(?i)\bautoreply\b"),
    re.compile(r"(?i)\bvacation\b"),
    re.compile(r"(?i)\babwesen"),
)
_TEMP_PATTERNS = (
    re.compile(r"(?i)\bmailbox full\b"),
    re.compile(r"(?i)\bover quota\b"),
    re.compile(r"(?i)\btemporar(?:y|ily)\b"),
    re.compile(r"(?i)\bgreylist"),
)
_HARD_RULES = (
    (
        "USER_UNKNOWN",
        (
            re.compile(r"(?i)\buser unknown\b"),
            re.compile(r"(?i)\bunknown user\b"),
            re.compile(r"(?i)\bno such user\b"),
            re.compile(r"(?i)\bunknown recipient\b"),
            re.compile(r"(?i)\brecipient address rejected\b"),
            re.compile(r"(?i)\bmailbox not found\b"),
            re.compile(r"(?i)\bdoes not exist\b"),
            re.compile(r"(?i)\b5\.1\.1\b"),
        ),
    ),
    (
        "MAILBOX_DISABLED",
        (
            re.compile(r"(?i)\bmailbox disabled\b"),
            re.compile(r"(?i)\baccount (?:has been )?disabled\b"),
            re.compile(r"(?i)\baccount closed\b"),
            re.compile(r"(?i)\bno longer accepts mail\b"),
            re.compile(r"(?i)\bmailbox unavailable\b"),
            re.compile(r"(?i)\brecipient inactive\b"),
        ),
    ),
)
_BOUNCE_FROM_PATTERNS = (
    re.compile(r"(?i)mailer-daemon"),
    re.compile(r"(?i)postmaster"),
)
_BOUNCE_SUBJECT_PATTERNS = (
    re.compile(r"(?i)delivery status notification"),
    re.compile(r"(?i)undeliver"),
    re.compile(r"(?i)failure notice"),
    re.compile(r"(?i)returned mail"),
)


def _extract_x_mailer_id(raw_text: str) -> Optional[int]:
    m = _RE_X_MAILER_ID.search(raw_text)
    if m:
        return int(m.group(1))
    m = _RE_SMREL.search(raw_text)
    if m:
        return int(m.group(1))
    return None


def _extract_failed_email(raw_text: str) -> Optional[str]:
    for rx in _RE_DSN_EMAIL_PATTERNS:
        m = rx.search(raw_text)
        if m:
            return m.group(1).strip().lower()
    hinted = re.search(
        r"(?is)(?:user unknown|unknown user|no such user|recipient(?: address)? rejected|mailbox unavailable).*?([a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,})",
        raw_text,
    )
    if hinted:
        return hinted.group(1).strip().lower()
    emails = sorted({m.group(1).strip().lower() for m in _RE_EMAIL.finditer(raw_text)})
    if len(emails) == 1:
        return emails[0]
    return None


def _classify_message(msg, raw_text: str) -> tuple[Optional[str], bool]:
    subj = (msg.get("Subject") or "").strip()
    from_hdr = (msg.get("From") or "").strip()
    auto_submitted = (msg.get("Auto-Submitted") or "").strip().lower()
    low = raw_text.lower()

    for reason_code, rules in _HARD_RULES:
        for rx in rules:
            if rx.search(raw_text):
                return reason_code, True

    action_m = _RE_DSN_ACTION.search(raw_text)
    status_m = _RE_DSN_STATUS.search(raw_text)
    status_code = status_m.group(1) if status_m else ""
    if action_m and action_m.group(1).lower() == "failed" and status_code.startswith("5."):
        return "HARD_BOUNCE_OTHER", True
    if status_code.startswith("5."):
        return "HARD_BOUNCE_OTHER", True

    if auto_submitted and auto_submitted != "no":
        return "AUTO_REPLY", False
    for rx in _AUTO_PATTERNS:
        if rx.search(subj) or rx.search(raw_text):
            return "AUTO_REPLY", False

    if status_code.startswith("4."):
        return "TEMP_BOUNCE", False
    if action_m and action_m.group(1).lower() == "delayed":
        return "TEMP_BOUNCE", False
    for rx in _TEMP_PATTERNS:
        if rx.search(raw_text):
            return "TEMP_BOUNCE", False

    if any(rx.search(from_hdr) for rx in _BOUNCE_FROM_PATTERNS) or any(rx.search(subj) for rx in _BOUNCE_SUBJECT_PATTERNS):
        return "MAILER_NOISE", False
    if "message/delivery-status" in low:
        return "MAILER_NOISE", False

    return None, False


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


def _resolve_mailbox_sent(match_mailbox_sent_id: int, mailbox_id: int) -> tuple[Optional[int], Optional[int]]:
    row = db.fetch_one(
        """
        SELECT rc.contact_id, ms.id
        FROM public.mailbox_sent ms
        JOIN public.rate_contacts rc
          ON rc.id = ms.rate_contact_id
        WHERE ms.id = %s
          AND COALESCE(ms.data->>'mailbox_id', '') ~ '^[0-9]+$'
          AND (ms.data->>'mailbox_id')::bigint = %s
        LIMIT 1
        """,
        [int(match_mailbox_sent_id), int(mailbox_id)],
    )
    if not row:
        return None, None
    aggr_contact_id = int(row[0]) if row[0] is not None else None
    mailbox_sent_id = int(row[1]) if row[1] is not None else None
    return aggr_contact_id, mailbox_sent_id


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


def process_imap_message(mailbox_id: int, folder: str, uid: str, raw_msg: bytes) -> Dict[str, Any]:
    raw_text = raw_msg.decode("utf-8", errors="replace")
    msg = BytesParser(policy=policy.default).parsebytes(raw_msg)
    reason_code, should_block = _classify_message(msg, raw_text)
    if reason_code is None:
        return {"kind": "skip", "moved": False, "blocked": False}

    x_mailer_id = _extract_x_mailer_id(raw_text)
    failed_email = _extract_failed_email(raw_text)

    aggr_contact_id: Optional[int] = None
    mailbox_sent_id: Optional[int] = None

    if x_mailer_id is not None:
        aggr_contact_id, mailbox_sent_id = _resolve_mailbox_sent(x_mailer_id, mailbox_id)

    if aggr_contact_id is None and failed_email:
        aggr_contact_id = _unique_aggr_contact_id_by_email(failed_email)

    if should_block and aggr_contact_id is not None:
        _upsert_blocked_recipient(
            aggr_contact_id=aggr_contact_id,
            mailbox_sent_id=mailbox_sent_id,
            reason_code=reason_code,
            source_eml=raw_text,
        )

    return {
        "kind": reason_code,
        "moved": True,
        "blocked": bool(should_block and aggr_contact_id is not None),
        "mailbox_sent_id": mailbox_sent_id,
        "aggr_contact_id": aggr_contact_id,
        "failed_email": failed_email,
        "folder": folder,
        "uid": uid,
    }
