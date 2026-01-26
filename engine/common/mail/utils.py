# FILE: engine/common/mail/utils.py
# PATH: engine/common/mail/utils.py
# DATE: 2026-01-26
# SUMMARY:
# - SMTP utilities: auth check and send test mail.
# - IMAP utility: check connection + list folders.
# - Logs results via log_mail_event (strict).
# - Returns strict JSON {action, status, data}.

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from engine.common.mail.imap import IMAPConn, STATUS_OK as IMAP_STATUS_OK
from engine.common.mail.logs import log_mail_event
from engine.common.mail.smtp import SMTPConn, STATUS_OK as SMTP_STATUS_OK


def smtp_auth_check(mailbox_id: int) -> Dict[str, Any]:
    action = "SMTP_AUTH_CHECK"
    conn = SMTPConn(mailbox_id)

    try:
        ok = conn.conn()
        data = conn.log or {}
        status = "SUCCESS" if ok and data.get("status") == SMTP_STATUS_OK else "FAIL"

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
        status = "SUCCESS" if ok and data.get("status") == SMTP_STATUS_OK else "FAIL"

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


def imap_check(mailbox_id: int) -> Dict[str, Any]:
    action = "IMAP_CHECK"
    conn = IMAPConn(mailbox_id)

    def _b2s(x: Any) -> Any:
        if isinstance(x, list):
            return [_b2s(v) for v in x]
        if isinstance(x, (bytes, bytearray)):
            return x.decode("utf-8", errors="replace")
        return x

    try:
        ok = conn.conn()
        folders: List[str] = []
        list_reply: Dict[str, Any] | None = None
        list_error: str | None = None

        if ok and conn.conn_obj:
            try:
                typ, data = conn.conn_obj.list()
                list_reply = {"typ": typ, "data": _b2s(data)}
                if typ == "OK":
                    folders = [str(x) for x in (_b2s(data) or [])]
                else:
                    list_error = "list_failed"
            except Exception as e:
                list_error = str(e)

        data_out: Dict[str, Any] = {
            "log": conn.log or {},
            "folders": folders,
        }
        if list_reply is not None:
            data_out["list_reply"] = list_reply
        if list_error:
            data_out["list_error"] = list_error

        status = "SUCCESS" if ok and (conn.log or {}).get("status") == IMAP_STATUS_OK and not list_error else "FAIL"

        log_mail_event(
            mailbox_id=mailbox_id,
            action=action,
            status=status,
            payload_json=data_out,
        )
        return {"action": action, "status": status, "data": data_out}
    finally:
        try:
            conn.close()
        except Exception:
            pass
