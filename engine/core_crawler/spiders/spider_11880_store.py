# FILE: engine/core_crawler/spiders/spider_11880_store.py
# DATE: 2026-03-28
# PURPOSE: 11880 spider payload logging into crawler/spider_11880.

from __future__ import annotations

import json
from typing import Any, Dict

from engine.common.logs import log


LOG_FILE = "spider_11880"
LOG_FOLDER = "crawler"


def save_11880_probe_run(payload: Dict[str, Any]) -> int:
    log(
        LOG_FILE,
        folder=LOG_FOLDER,
        message=json.dumps(payload, ensure_ascii=False, default=str, indent=2),
    )
    items = payload.get("items") or []
    return len(items) if isinstance(items, list) else 0
