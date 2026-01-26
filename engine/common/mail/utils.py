# FILE: engine/common/mail/utils.py
# PATH: engine/common/mail/utils.py
# DATE: 2026-01-26
# SUMMARY:
# - SMTP utilities: auth check and send test mail.
# - Uses SMTPConn as-is (types.get untouched).
# - Logs results directly via log_mail_event (NO helper abstractions).
# - Returns strict JSON {action, status, data}.

from __future__ import annotations

from typing import Any, Dict

from engine.common.mail.smtp import SMTPConn, STATUS_OK
from engine.common.mail.logs import log_mail_event


def smtp_auth_check(mailbox_id: int) -> Dict[str, Any]:
    action = "SMTP_AUTH_CHECK"
    conn = SMTPConn(mailbox_id)

    try:
        ok = conn.conn()
        data = conn.log or {}
        status = "SUCCESS" if ok and data.get("status") == STATUS_OK else "FAIL"

        log_mail_event(
            mailbox_id=mailbox_id,
            action=action,
            status=status,
            payload_json=data,
        )
        return {"action": action, "status": status, "data": data}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def smtp_send_check(mailbox_id: int, to: str) -> Dict[str, Any]:
    action = "SMTP_SEND_CHECK"
    conn = SMTPConn(mailbox_id)

    try:
        if not conn.conn():
            data = conn.log or {}
            status = "FAIL"
            log_mail_event(
                mailbox_id=mailbox_id,
                action=action,
                status=status,
                payload_json=data,
            )
            return {"action": action, "status": status, "data": data}

        ok = conn._send_mail(
            to,
            subject="SMTP test message",
            body_text="This is a test message sent by Serenity Mailer.",
            body_html="",
        )
        data = conn.log or {}
        status = "SUCCESS" if ok and data.get("status") == STATUS_OK else "FAIL"

        log_mail_event(
            mailbox_id=mailbox_id,
            action=action,
            status=status,
            payload_json=data,
        )
        return {"action": action, "status": status, "data": data}
    finally:
        try:
            conn.close()
        except Exception:
            pass
