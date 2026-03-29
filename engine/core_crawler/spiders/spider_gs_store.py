# FILE: engine/core_crawler/spiders/spider_gs_store.py
# DATE: 2026-03-27
# PURPOSE: GS spider payload logging into crawler/spider_gs.

from __future__ import annotations

import json
from typing import Any, Dict

from engine.common.logs import log


LOG_FILE = "spider_gs"
LOG_FOLDER = "crawler"


def save_gs_probe_run(payload: Dict[str, Any]) -> int:
    log(
        LOG_FILE,
        folder=LOG_FOLDER,
        message=json.dumps(payload, ensure_ascii=False, default=str, indent=2),
    )
    items = payload.get("items") or []
    return len(items) if isinstance(items, list) else 0
