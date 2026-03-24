# FILE: engine/common/utils.py  (обновлено — 2025-12-26)
# CHANGE: h64 теперь принимает ТОЛЬКО str (текст). Любой другой тип -> TypeError.
# PURPOSE: короткий стабильный 64-bit хеш (BIGINT) для текста (UTF-8), совпадающий с Postgres-функцией.

from __future__ import annotations

import hashlib
import json
import re
from typing import Any


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
