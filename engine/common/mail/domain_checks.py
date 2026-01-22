# FILE: engine/common/mail/domain_checks.py
# DATE: 2026-01-22
# PURPOSE:
# - DOMAIN_TECH_CHECK / DOMAIN_REPUTATION_CHECK
# - If domain is in whitelist → skip ALL checks, DO NOT write to DB.
# - UI response: "TRUSTED SERVICE PROVIDER - CHECK IS NOT NEEDED"

from __future__ import annotations
import re, subprocess
from typing import Any, Dict, List, Tuple, Optional
from engine.common import db
from .domain_whitelist import is_domain_whitelisted

SPAMHAUS_DQS_KEY = "l3lq722e7lftnbeqbd7oh4d4pi"
_DQS_ZONE = "dq.spamhaus.net"

_SPF_RE = re.compile(r'^\s*"?v=spf1\b', re.I)
_DMARC_RE = re.compile(r'^\s*"?v=DMARC1\b', re.I)


def mailbox_domain(mailbox_id: int) -> Optional[str]:
    r = db.fetch_one("SELECT domain FROM aap_settings_mailboxes WHERE id=%s", (int(mailbox_id),))
    return (r[0] or "").strip().lower().strip(".") if r else None


# ================= TECH =================

def mailbox_domain_tech_check(mailbox_id: int) -> Tuple[str, Dict[str, Any], bool]:
    d = mailbox_domain(mailbox_id)
    if not d:
        return "CHECK_FAILED", {"error": "domain_not_found"}, False

    if is_domain_whitelisted(d):
        return "TRUSTED", {"domain": d}, True

    spf_txt, spf_err = _dig_txt(d)
    dmarc_txt, dmarc_err = _dig_txt(f"_dmarc.{d}")

    if spf_err or dmarc_err:
        return "CHECK_FAILED", {
            "domain": d,
            "error": "dns_error",
            "spf_err": spf_err,
            "dmarc_err": dmarc_err,
        }, False

    spf_ok = _spf_ok(spf_txt)
    dmarc_ok = _dmarc_ok(dmarc_txt)

    status = "GOOD" if (spf_ok and dmarc_ok) else "BAD"
    return status, {
        "domain": d,
        "spf": {"ok": spf_ok, "records": spf_txt},
        "dmarc": {"ok": dmarc_ok, "records": dmarc_txt},
    }, False


# ================= REPUTATION =================

def mailbox_domain_reputation_check(mailbox_id: int) -> Tuple[str, Dict[str, Any], bool]:
    d = mailbox_domain(mailbox_id)
    if not d:
        return "CHECK_FAILED", {"error": "domain_not_found"}, False

    if is_domain_whitelisted(d):
        return "TRUSTED", {"domain": d}, True

    q = f"{d}.{SPAMHAUS_DQS_KEY}.dbl.{_DQS_ZONE}"
    ips, err = _dig_a(q)

    if err:
        return "CHECK_FAILED", {
            "domain": d,
            "error": err,
            "query": q,
        }, False

    for ip in ips:
        if ip.startswith("127.255.255."):
            return "CHECK_FAILED", {
                "domain": d,
                "error": "spamhaus_acl",
                "query": q,
                "ips": ips,
            }, False

    listed = any(ip.startswith("127.") for ip in ips)
    status = "QUESTIONABLE" if listed else "NORMAL"

    return status, {
        "domain": d,
        "query": q,
        "ips": ips,
        "listed": listed,
    }, False


# ================= DNS =================

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
