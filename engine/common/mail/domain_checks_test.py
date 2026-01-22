# FILE: engine/common/mail/domain_checks_test.py
# DATE: 2026-01-22
# PURPOSE:
# - Add links for QUESTIONABLE reputation result.
# - Keep logging unchanged.
# - UI text exactly as specified.

from engine.common.mail.domain_checks import (
    mailbox_domain_tech_check,
    mailbox_domain_reputation_check,
)
from engine.common.mail.logs import log_mail_event
from engine.common.mail.types import MailUiResult

TRUSTED_MSG = "TRUSTED SERVICE PROVIDER - CHECK IS NOT NEEDED"
UNKNOWN_MSG = "UNKNOWN — TRY AGAIN LATER"

QUESTIONABLE_MSG = "Проверьте ваш домен здесь:"
QUESTIONABLE_LINKS = [
    "https://check.spamhaus.org/",
    "https://mxtoolbox.com/blacklists.aspx",
    "https://multirbl.valli.org/",
]


def domain_tech_check_and_log(mailbox_id: int) -> MailUiResult:
    status, data, trusted = mailbox_domain_tech_check(mailbox_id)

    if trusted:
        return MailUiResult(status="TRUSTED", user_message=TRUSTED_MSG, data=data)

    log_mail_event(
        mailbox_id=mailbox_id,
        action="DOMAIN_TECH_CHECK",
        status=status,
        message=status,
        data=data,
    )

    ui = UNKNOWN_MSG if status == "CHECK_FAILED" else status
    return MailUiResult(status=status, user_message=ui, data=data)


def domain_reputation_check_and_log(mailbox_id: int) -> MailUiResult:
    status, data, trusted = mailbox_domain_reputation_check(mailbox_id)

    if trusted:
        return MailUiResult(status="TRUSTED", user_message=TRUSTED_MSG, data=data)

    log_mail_event(
        mailbox_id=mailbox_id,
        action="DOMAIN_REPUTATION_CHECK",
        status=status,
        message=status,
        data=data,
    )

    if status == "CHECK_FAILED":
        return MailUiResult(status=status, user_message=UNKNOWN_MSG, data=data)

    if status == "QUESTIONABLE":
        data = dict(data)
        data["links"] = QUESTIONABLE_LINKS
        return MailUiResult(
            status=status,
            user_message=QUESTIONABLE_MSG,
            data=data,
        )

    return MailUiResult(status=status, user_message=status, data=data)
