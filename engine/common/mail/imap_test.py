# FILE: engine/common/mail/imap_test.py
# DATE: 2026-01-22
# PURPOSE: IMAP scenarios: run check/list, log, return UI-ready compact result.
# CHANGE: (new) IMAP_CHECK + IMAP_LIST_FOLDERS with check_and_log / list_and_log helpers.

from __future__ import annotations

from engine.common.mail.imap import imap_check, imap_list_folders
from engine.common.mail.logs import log_mail_event
from engine.common.mail.types import MailResult, MailUiResult


def imap_check_and_log(mailbox_id: int) -> MailUiResult:
    r: MailResult = imap_check(mailbox_id)

    status = "OK" if r.ok else "FAIL"
    ui_msg = "" if r.ok else _user_message_from_result(r)

    data = {
        "action": r.action,
        "stage": r.stage,
        "code": r.code,
        "message": r.message,
        "details": r.details,
        "latency_ms": r.latency_ms,
    }

    log_mail_event(
        mailbox_id=mailbox_id,
        action="IMAP_CHECK",
        status=status,
        message=ui_msg,
        data=data,
    )

    return MailUiResult(status=status, user_message=ui_msg, data=data)


def imap_list_folders_and_log(mailbox_id: int) -> MailUiResult:
    folders, r = imap_list_folders(mailbox_id)

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

    log_mail_event(
        mailbox_id=mailbox_id,
        action="IMAP_LIST_FOLDERS",
        status=status,
        message=ui_msg,
        data=data,
    )

    return MailUiResult(status=status, user_message=ui_msg, data=data)


def _user_message_from_result(r: MailResult) -> str:
    if r.stage == "timeout":
        return "IMAP: таймаут (сервер не ответил вовремя)."
    if r.stage == "connect":
        return "IMAP: не удалось подключиться (сеть/DNS/порт)."
    if r.stage == "auth":
        return "IMAP: ошибка авторизации (логин/пароль/токен)."
    if r.stage == "disconnect":
        return "IMAP: сервер разорвал соединение."
    if r.stage in ("input", "db", "cfg"):
        return "IMAP: некорректная настройка ящика."
    if r.message:
        return f"IMAP: {r.message}"
    if r.code:
        return f"IMAP: ошибка ({r.code})."
    return "IMAP: неизвестная ошибка."
