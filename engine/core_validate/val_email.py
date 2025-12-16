# FILE: engine/core_validate/val_email.py  (обновлено — 2025-12-16)
# Смысл: валидирует email в raw_contacts_gb (processed_email=false): trim+синтаксис+allowlist+MX,
# пишет status_email/processed_email, при успехе перезаписывает email (trim).

from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Optional, Set, Tuple

from engine.common.db import get_connection

BATCH_SIZE = 100

STATUS_EMPTY = "EMPTY"
STATUS_BAD_SYNTAX = "WRONG EMAIL SYNTAX"
STATUS_BAD_MX = "WRONG DOMAIN MX"
STATUS_OK = "OK"

_DOMAINS_JSON_PATH = os.path.join(os.path.dirname(__file__), "domains.json")

_RE_HAS_SPACE_OR_CTRL = re.compile(r"[\s\x00-\x1f\x7f]")
_RE_DOMAIN_ALLOWED = re.compile(r"^[a-z0-9.-]+$")  # domain lowercased


def _load_domains_allowlist() -> Set[str]:
    try:
        with open(_DOMAINS_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        out: Set[str] = set()
        for x in data or []:
            d = (x.get("domain") or "").strip().lower()
            if d:
                out.add(d)
        return out
    except Exception:
        return set()


def _trim(s: Optional[str]) -> str:
    return (s or "").strip()


def _split_email(email: str) -> Tuple[Optional[str], Optional[str]]:
    if email.count("@") != 1:
        return None, None
    local, domain = email.split("@", 1)
    if not local or not domain:
        return None, None
    return local, domain


def _is_bad_syntax(email: str) -> bool:
    # total length
    if len(email) > 254:
        return True

    local, domain = _split_email(email)
    if local is None or domain is None:
        return True

    # local length
    if len(local) > 64:
        return True

    # spaces / control chars anywhere
    if _RE_HAS_SPACE_OR_CTRL.search(email):
        return True

    # local-part dot rules
    if local.startswith(".") or local.endswith("."):
        return True
    if ".." in local:
        return True

    # domain checks
    d = domain.strip().lower()
    if "." not in d:
        return True
    if not _RE_DOMAIN_ALLOWED.match(d):
        return True

    labels = d.split(".")
    for lab in labels:
        if not lab:
            return True
        if len(lab) > 63:
            return True
        if lab.startswith("-") or lab.endswith("-"):
            return True

    return False


def _domain_from_email(email: str) -> Optional[str]:
    _local, domain = _split_email(email)
    if domain is None:
        return None
    return domain.strip().lower()


def _has_mx(domain: str) -> bool:
    # 1) dnspython, если есть
    try:
        import dns.resolver  # type: ignore

        ans = dns.resolver.resolve(domain, "MX", lifetime=3.0)
        return bool(list(ans))
    except Exception:
        pass

    # 2) dig
    try:
        r = subprocess.run(
            ["dig", "+short", "MX", domain],
            capture_output=True,
            text=True,
            timeout=4,
        )
        return r.returncode == 0 and bool(r.stdout.strip())
    except Exception:
        pass

    # 3) nslookup
    try:
        r = subprocess.run(
            ["nslookup", "-type=mx", domain],
            capture_output=True,
            text=True,
            timeout=4,
        )
        return r.returncode == 0 and ("mail exchanger" in (r.stdout + r.stderr).lower())
    except Exception:
        return False


def run_batch() -> None:
    allow = _load_domains_allowlist()

    sql_pick = """
        SELECT id, email
        FROM raw_contacts_gb
        WHERE processed_email = FALSE
        ORDER BY id
        LIMIT %s
        FOR UPDATE SKIP LOCKED
    """

    sql_set = """
        UPDATE raw_contacts_gb
        SET email = %s,
            status_email = %s,
            processed_email = TRUE,
            updated_at = now()
        WHERE id = %s
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_pick, (BATCH_SIZE,))
            rows = cur.fetchall()

            for rid, email in rows:
                trimmed = _trim(email)

                if not trimmed:
                    cur.execute(sql_set, ("", STATUS_EMPTY, rid))
                    continue

                if _is_bad_syntax(trimmed):
                    cur.execute(sql_set, (trimmed, STATUS_BAD_SYNTAX, rid))
                    continue

                domain = _domain_from_email(trimmed)
                if not domain:
                    cur.execute(sql_set, (trimmed, STATUS_BAD_SYNTAX, rid))
                    continue

                if domain in allow:
                    cur.execute(sql_set, (trimmed, STATUS_OK, rid))
                    continue

                if not _has_mx(domain):
                    cur.execute(sql_set, (trimmed, STATUS_BAD_MX, rid))
                    continue

                cur.execute(sql_set, (trimmed, STATUS_OK, rid))

        conn.commit()


def main() -> None:
    run_batch()


if __name__ == "__main__":
    main()
