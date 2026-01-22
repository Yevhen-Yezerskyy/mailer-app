# FILE: engine/common/mail/smtp_test.py
# DATE: 2026-01-22
# PURPOSE: SMTP_CHECK scenario: run check, log, return UI-ready compact result.
# CHANGE: Return MailUiResult (OK/FAIL + user_message + data), keep raw MailResult in data.

from __future__ import annotations

from engine.common.mail.logs import log_mail_event
from engine.common.mail.smtp import smtp_check
from engine.common.mail.types import MailResult, MailUiResult


def smtp_check_and_log(mailbox_id: int) -> MailUiResult:
    r: MailResult = smtp_check(mailbox_id)

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
        action="SMTP_CHECK",
        status=status,
        message=ui_msg,
        data=data,
    )

    return MailUiResult(status=status, user_message=ui_msg, data=data)


def _user_message_from_result(r: MailResult) -> str:
    # коротко, печатабельно, без простыней
    if r.stage == "timeout":
        return "SMTP: таймаут (сервер не ответил вовремя)."
    if r.stage == "connect":
        return "SMTP: не удалось подключиться (сеть/DNS/порт)."
    if r.stage == "auth":
        return "SMTP: ошибка авторизации (логин/пароль/токен)."
    if r.stage == "disconnect":
        return "SMTP: сервер разорвал соединение."
    if r.stage in ("input", "db", "cfg"):
        return "SMTP: некорректная настройка ящика."
    # fallback
    if r.message:
        return f"SMTP: {r.message}"
    if r.code:
        return f"SMTP: ошибка ({r.code})."
    return "SMTP: неизвестная ошибка."
