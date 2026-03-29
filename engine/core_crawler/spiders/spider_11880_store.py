# FILE: engine/core_crawler/spiders/spider_11880_store.py
# DATE: 2026-03-29
# PURPOSE: Save core_crawler 11880 cards into public.raw_contacts_cb.

from __future__ import annotations

import json
from typing import Any, Dict

from engine.common.db import get_connection


def save_11880_probe_run(payload: Dict[str, Any]) -> int:
    cb_id = int(payload.get("cb_id") or 0)
    items = payload.get("items") or []
    if cb_id <= 0:
        raise RuntimeError("save_11880_probe_run requires cb_id")
    if not isinstance(items, list):
        raise RuntimeError("save_11880_probe_run requires items list")

    written = 0
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM public.raw_contacts_cb WHERE cb_id=%s", (cb_id,))
        for item in items:
            if not isinstance(item, dict):
                continue
            card = dict(item.get("card") or {})
            if not card:
                continue
            url = str(item.get("url") or "").strip() or None
            cur.execute(
                """
                INSERT INTO public.raw_contacts_cb (cb_id, card, url)
                VALUES (%s, %s::jsonb, %s)
                """,
                (cb_id, json.dumps(card, ensure_ascii=False, default=str), url),
            )
            written += 1
        conn.commit()
    return int(written)
