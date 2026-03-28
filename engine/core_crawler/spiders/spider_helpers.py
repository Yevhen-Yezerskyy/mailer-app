# FILE: engine/core_crawler/spiders/spider_helpers.py
# DATE: 2026-03-27
# PURPOSE: Common helpers for core_crawler catalog spiders.

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any, Optional


def clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = " ".join(s.split()).strip()
    return s or None


def dedup_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def clean_email(s: Optional[str]) -> Optional[str]:
    s = clean_text(s)
    if not s:
        return None
    if s.startswith("mailto:"):
        s = s[7:]
    s = s.split("?", 1)[0].strip()
    s = clean_text(s)
    if not s or "@" not in s:
        return None
    if "." not in s.split("@", 1)[-1]:
        return None
    if len(s) < 5:
        return None
    return s


def clean_tel(s: Optional[str]) -> Optional[str]:
    s = clean_text(s)
    if not s:
        return None
    if s.lower().startswith("tel:"):
        s = s[4:].split("?", 1)[0].strip()
    s = clean_text(s)
    if not s:
        return None
    digits = re.sub(r"\D+", "", s)
    if len(digits) < 6:
        return None
    return s


def clean_url(s: Optional[str]) -> Optional[str]:
    s = clean_text(s)
    if not s:
        return None
    if s.lower().startswith("javascript:"):
        return None
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if "." in s and " " not in s:
        return s
    return None


def extract_texts(selector) -> str | None:
    parts = selector.css("::text").getall()
    parts = [clean_text(p) for p in parts]
    parts = [p for p in parts if p]
    return clean_text(" ".join(parts))


def init_card_from_contract(contract: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, sample in (contract.get("required") or {}).items():
        if isinstance(sample, list):
            out[key] = []
        else:
            out[key] = deepcopy(sample)
    return out


def set_scalar(card: dict[str, Any], key: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, str):
        v = clean_text(value)
        if v is None:
            return
        card[key] = v
        return
    card[key] = value


def add_many(card: dict[str, Any], key: str, values: list[str]) -> None:
    if key not in card or not isinstance(card[key], list):
        card[key] = []
    cleaned = [clean_text(x) for x in values]
    cleaned = [x for x in cleaned if x]
    if not cleaned:
        return
    card[key] = dedup_keep_order(list(card[key]) + cleaned)
