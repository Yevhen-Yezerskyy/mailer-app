# FILE: engine/work/classify_11880_is_name_json.py  (новое — 2026-02-04)
# PURPOSE: Batch GPT(nano) classification for crwl_slug_11880 using strict JSON I/O; write only if response is valid.

from __future__ import annotations

import json
import os
from typing import Any

from engine.common.db import get_connection
from engine.common.gpt import GPTClient

MODEL = os.getenv("GPT_MODEL", "mini")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_BATCHES = int(os.getenv("MAX_BATCHES", "1"))  # 1 = one pass; increase later


INSTRUCTIONS = (
    "You are given JSON with items to classify in German (Deutsch).\n"
    "Decide for each item whether it CAN be a proper name.\n"
    "Proper names include: surnames and given names, brands, company names, organizations, institutions,\n"
    "toponyms/place names, names of people or animals, and other unique proper names.\n"
    "If there is doubt, ambiguity, homonymy, or lack of context, prefer true.\n\n"
    "Return ONLY valid JSON (no markdown, no comments, no extra text).\n"
    "Output must be a JSON array of objects exactly like:\n"
    "[{\"id\": 123, \"is_name\": true}, ...]\n"
    "All ids from input must be present exactly once. is_name must be boolean."
)


def _validate_response(data: Any, expected_ids: list[int]) -> dict[int, bool] | None:
    if not isinstance(data, list):
        return None

    exp_set = set(expected_ids)
    got: dict[int, bool] = {}

    for item in data:
        if not isinstance(item, dict):
            return None
        if set(item.keys()) != {"id", "is_name"}:
            return None

        rid = item["id"]
        val = item["is_name"]

        if not isinstance(rid, int):
            return None
        if not isinstance(val, bool):
            return None
        if rid in got:
            return None
        got[rid] = val

    if set(got.keys()) != exp_set:
        return None

    return got


def _ask_gpt(gpt: GPTClient, rows: list[tuple[int, str]]) -> dict[int, bool] | None:
    payload = {"items": [{"id": rid, "label": label} for rid, label in rows]}
    resp = gpt.ask(
        model=MODEL,
        instructions=INSTRUCTIONS,
        input=json.dumps(payload, ensure_ascii=False),
        use_cache=False,
        user_id="engine.work.classify_11880_is_name_json",
        service_tier="flex",
    )

    try:
        parsed = json.loads(resp.content)
    except Exception:
        return None

    expected_ids = [rid for rid, _ in rows]
    return _validate_response(parsed, expected_ids)


def main() -> None:
    gpt = GPTClient(debug=False)

    batches_done = 0
    total_updated = 0
    total_selected = 0
    total_skipped = 0

    with get_connection() as conn:
        while batches_done < MAX_BATCHES:
            with conn.cursor() as cur:
                cur.execute(
                    "select id, label from crwl_slug_11880 where is_name is null order by id limit %s",
                    (BATCH_SIZE,),
                )
                rows = cur.fetchall()

            if not rows:
                print("done: nothing_to_do")
                break

            total_selected += len(rows)

            mapping = _ask_gpt(gpt, rows)
            if mapping is None:
                total_skipped += 1
                print(f"batch={batches_done+1} status=SKIP_BAD_RESPONSE selected={len(rows)}")
                batches_done += 1
                continue  # ничего не пишем в БД

            with conn.cursor() as cur:
                updated = 0
                for rid, _label in rows:
                    cur.execute(
                        "update crwl_slug_11880 set is_name=%s where id=%s and is_name is null",
                        (mapping[rid], rid),
                    )
                    updated += cur.rowcount

            conn.commit()
            total_updated += updated
            print(f"batch={batches_done+1} status=OK selected={len(rows)} updated={updated}")
            batches_done += 1

    print(
        f"summary: batches={batches_done} selected={total_selected} updated={total_updated} "
        f"skipped_bad_response={total_skipped} model={MODEL} batch_size={BATCH_SIZE}"
    )


if __name__ == "__main__":
    main()
