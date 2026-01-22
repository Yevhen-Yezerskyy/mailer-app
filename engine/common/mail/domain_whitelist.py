# FILE: engine/common/mail/domain_whitelist.py
# DATE: 2026-01-22
# PURPOSE: Whitelist of domains/providers where reputation check is not needed (big providers).
# CHANGE: (new) Add global + German providers; extend later as needed.

from __future__ import annotations

from typing import Set


WHITELIST_DOMAINS: Set[str] = {
    # Google
    "gmail.com",
    "google.com",
    "googlemail.com",
    # Microsoft
    "outlook.com",
    "hotmail.com",
    "live.com",
    "msn.com",
    "office365.com",
    # Apple
    "icloud.com",
    "me.com",
    "mac.com",
    # Yahoo/AOL
    "yahoo.com",
    "ymail.com",
    "aol.com",
    # German providers
    "gmx.de",
    "gmx.net",
    "web.de",
    "t-online.de",
    "telekom.de",
    "1und1.de",
    "ionos.de",
    "strato.de",
    "vodafone.de",
    "arcor.de",
    "freenet.de",
    "mail.de",
    "posteo.de",
    "mailbox.org",
}


def is_domain_whitelisted(domain: str) -> bool:
    d = (domain or "").strip().lower().strip(".")
    return bool(d) and d in WHITELIST_DOMAINS
