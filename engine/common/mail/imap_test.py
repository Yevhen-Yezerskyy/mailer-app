# FILE: engine/common/mail/imap_test.py
# DATE: 2026-01-23
# PURPOSE: IMAP scenarios: run check+list in one connection, log, return UI-ready compact result.
# CHANGE:
# - Replace double-login flow with imap_check_and_list_folders (one auth).
# - Keep old functions names for callers: imap_check_and_log now includes folders.

from __future__ import annotations

from engine.common.mail.imap import imap_check_and_list_folders
from engine.common.mail.logs import log_mail_event
from engine.common.mail.types import MailResult, MailUiResult


def imap_check_and_log(mailbox_id: int) -> MailUiResult:
    folders, r = imap_check_and_list_folders(mailbox_id)

    status = "OK" if r.ok else "FAIL"
    ui_msg = "" if r.ok else _user_message_from_result(r)

    data = {
        "action": r.action,
        "stage": r.stage,
        "code": r.code,
        "message": r.message,
        "details": r.details,
        "latency_ms": r.latency_ms,
        "folders": folders,
    }

    # логируем один раз: IMAP_CHECK (включая folders)
    log_mail_event(
        mailbox_id=mailbox_id,
        action="IMAP_CHECK",
        status=status,
        message=ui_msg,
        data=data,
    )

    return MailUiResult(status=status, user_message=ui_msg, data=data)


