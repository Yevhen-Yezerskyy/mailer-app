# FILE: engine/common/utils.py  (обновлено — 2026-03-31)
# PURPOSE: common utilities: stable text hash, JSON parsing, and reusable email helpers.

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from typing import Any, Optional, Set

_EMAIL_DOMAINS_JSON_PATH = os.path.join(os.path.dirname(__file__), "email_domains.json")

_RE_HAS_SPACE_OR_CTRL = re.compile(r"[\s\x00-\x1f\x7f]")
_RE_DOMAIN_ALLOWED = re.compile(r"^[a-z0-9.-]+$")


def h64_text(text: str) -> int:
    """
    64-bit хеш текста под Postgres BIGINT.
    Алгоритм:
    - UTF-8 bytes
    - blake2b digest_size=8
    - unsigned big-endian -> signed int64 (для BIGINT)
    """
    if not isinstance(text, str):
        raise TypeError(f"h64_text expects str, got {type(text).__name__}")

    digest8 = hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest()
    u = int.from_bytes(digest8, "big", signed=False)

    # signed int64 (Postgres BIGINT)
    return u - (1 << 64) if u >= (1 << 63) else u


def parse_json_response(text: str) -> Any | None:
    raw = str(text or "").strip()
    if not raw:
        return None

    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)

    try:
        return json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        try:
            return json.loads(raw[start : end + 1])
        except Exception:
            return None


def parse_json_object(value: Any, *, field_name: str = "json") -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value

    raw = ""
    if isinstance(value, str):
        raw = value.strip()
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value).decode("utf-8", errors="ignore").strip()
    else:
        raise TypeError(f"{field_name} expects dict/json string/bytes, got {type(value).__name__}")

    if not raw:
        return {}

    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise TypeError(f"{field_name} JSON must be object")
    return parsed


def safe_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def load_email_domains_allowlist() -> Set[str]:
    try:
        with open(_EMAIL_DOMAINS_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return set()

    out: Set[str] = set()
    for item in data or []:
        domain = ""
        if isinstance(item, dict):
            domain = str(item.get("domain") or "").strip().lower()
        elif isinstance(item, str):
            domain = str(item).strip().lower()
        if domain:
            out.add(domain)
    return out


def _split_email(email: str) -> tuple[Optional[str], Optional[str]]:
    if email.count("@") != 1:
        return None, None
    local, domain = email.split("@", 1)
    if not local or not domain:
        return None, None
    return local, domain


def email_is_bad_syntax(email: str) -> bool:
    if len(email) > 254:
        return True

    local, domain = _split_email(email)
    if local is None or domain is None:
        return True

    if len(local) > 64:
        return True

    if _RE_HAS_SPACE_OR_CTRL.search(email):
        return True

    if local.startswith(".") or local.endswith("."):
        return True
    if ".." in local:
        return True

    d = domain.strip().lower()
    if "." not in d:
        return True
    if not _RE_DOMAIN_ALLOWED.match(d):
        return True

    labels = d.split(".")
    for label in labels:
        if not label:
            return True
        if len(label) > 63:
            return True
        if label.startswith("-") or label.endswith("-"):
            return True

    return False


def email_domain_from_email(email: str) -> Optional[str]:
    _local, domain = _split_email(email)
    if domain is None:
        return None
    return domain.strip().lower()


def email_has_mx(domain: str) -> bool:
    try:
        import dns.resolver  # type: ignore

        ans = dns.resolver.resolve(domain, "MX", lifetime=3.0)
        return bool(list(ans))
    except Exception:
        pass

    try:
        result = subprocess.run(
            ["dig", "+short", "MX", domain],
            capture_output=True,
            text=True,
            timeout=4,
        )
        return result.returncode == 0 and bool(result.stdout.strip())
    except Exception:
        pass

    try:
        result = subprocess.run(
            ["nslookup", "-type=mx", domain],
            capture_output=True,
            text=True,
            timeout=4,
        )
        return result.returncode == 0 and ("mail exchanger" in (result.stdout + result.stderr).lower())
    except Exception:
        return False
