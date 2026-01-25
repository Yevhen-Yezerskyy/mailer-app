# FILE: engine/common/mail/domain_checks.py
# DATE: 2026-01-24 (новое)
# PURPOSE:
# - Single file: domain tech + domain reputation checks.
# - Public API (2 funcs): domain_check_tech(), domain_check_reputation()
# - Return JSON: {"action": str, "status": str, "data": {...}}
# - If status == "CHECK_FAILED" -> DO NOT write to DB; else write mailbox_events.

from __future__ import annotations

import re
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from engine.common import db
from engine.common.mail.logs import log_mail_event
from .domain_whitelist import is_domain_whitelisted


SPAMHAUS_DQS_KEY = "l3lq722e7lftnbeqbd7oh4d4pi"
_DQS_ZONE = "dq.spamhaus.net"

_SPF_RE = re.compile(r'^\s*"?v=spf1\b', re.I)
_DMARC_RE = re.compile(r'^\s*"?v=DMARC1\b', re.I)

ACTION_TECH = "DOMAIN_CHECK_TECH"
ACTION_REPUTATION = "DOMAIN_CHECK_REPUTATION"


def domain_check_tech(mailbox_id: int) -> Dict[str, Any]:
    status, data = _domain_check_tech_impl(mailbox_id)
    out = {"action": ACTION_TECH, "status": status, "data": data}

    if status != "CHECK_FAILED":
        log_mail_event(
            mailbox_id=int(mailbox_id),
            action=ACTION_TECH,
            status=status,
            payload_json=out,
        )

    return out


def domain_check_reputation(mailbox_id: int) -> Dict[str, Any]:
    status, data = _domain_check_reputation_impl(mailbox_id)
    out = {"action": ACTION_REPUTATION, "status": status, "data": data}

    if status != "CHECK_FAILED":
        log_mail_event(
            mailbox_id=int(mailbox_id),
            action=ACTION_REPUTATION,
            status=status,
            payload_json=out,
        )

    return out


# =========================
# Impl
# =========================

def _mailbox_domain(mailbox_id: int) -> Optional[str]:
    r = db.fetch_one("SELECT domain FROM aap_settings_mailboxes WHERE id=%s", (int(mailbox_id),))
    d = (r[0] or "").strip().lower().strip(".") if r else ""
    return d or None


def _domain_check_tech_impl(mailbox_id: int) -> Tuple[str, Dict[str, Any]]:
    d = _mailbox_domain(mailbox_id)
    if not d:
        return "CHECK_FAILED", {"error": "domain_not_found"}

    if is_domain_whitelisted(d):
        return "TRUSTED", {"domain": d}

    spf_txt, spf_err = _dig_txt(d)
    dmarc_txt, dmarc_err = _dig_txt(f"_dmarc.{d}")

    if spf_err or dmarc_err:
        return "CHECK_FAILED", {
            "domain": d,
            "error": "dns_error",
            "spf_err": spf_err,
            "dmarc_err": dmarc_err,
        }

    spf_ok = _spf_ok(spf_txt)
    dmarc_ok = _dmarc_ok(dmarc_txt)

    # GOOD: оба ок
    # NORMAL: один ок
    # BAD: ни один не ок
    if spf_ok and dmarc_ok:
        status = "GOOD"
    elif spf_ok or dmarc_ok:
        status = "NORMAL"
    else:
        status = "BAD"

    return status, {
        "domain": d,
        "spf": {"ok": spf_ok, "records": spf_txt},
        "dmarc": {"ok": dmarc_ok, "records": dmarc_txt},
    }


def _domain_check_reputation_impl(mailbox_id: int) -> Tuple[str, Dict[str, Any]]:
    d = _mailbox_domain(mailbox_id)
    if not d:
        return "CHECK_FAILED", {"error": "domain_not_found"}

    if is_domain_whitelisted(d):
        return "TRUSTED", {"domain": d}

    q = f"{d}.{SPAMHAUS_DQS_KEY}.dbl.{_DQS_ZONE}"
    ips, err = _dig_a(q)

    if err:
        return "CHECK_FAILED", {
            "domain": d,
            "error": err,
            "query": q,
        }

    for ip in ips:
        if ip.startswith("127.255.255."):
            return "CHECK_FAILED", {
                "domain": d,
                "error": "spamhaus_acl",
                "query": q,
                "ips": ips,
                "links": [
                    "https://check.spamhaus.org/",
                    "https://mxtoolbox.com/blacklists.aspx",
                    "https://multirbl.valli.org/",
                ],
            }

    listed = any(ip.startswith("127.") for ip in ips)
    status = "QUESTIONABLE" if listed else "NORMAL"

    data: Dict[str, Any] = {
        "domain": d,
        "query": q,
        "ips": ips,
        "listed": listed,
    }

    if status == "QUESTIONABLE":
        data["links"] = [
            "https://check.spamhaus.org/",
            "https://mxtoolbox.com/blacklists.aspx",
            "https://multirbl.valli.org/",
        ]

    return status, data


# =========================
# DNS helpers
# =========================

def _dig_txt(name: str) -> Tuple[List[str], str]:
    try:
        p = subprocess.run(
            ["dig", "+short", "TXT", name],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return [], "dig_failed"
    if p.returncode != 0:
        return [], "dig_error"
    out = (p.stdout or "").strip()
    return ([x.strip() for x in out.splitlines() if x.strip()], "")


def _dig_a(name: str) -> Tuple[List[str], str]:
    try:
        p = subprocess.run(
            ["dig", "+short", "A", name],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return [], "dig_failed"
    if p.returncode != 0:
        return [], "dig_error"
    out = (p.stdout or "").strip()
    return ([x.strip() for x in out.splitlines() if x.strip()], "")


def _spf_ok(txt: List[str]) -> bool:
    spf = [x for x in txt if _SPF_RE.search(x)]
    return len(spf) == 1


def _dmarc_ok(txt: List[str]) -> bool:
    return any(_DMARC_RE.search(x) for x in txt)
