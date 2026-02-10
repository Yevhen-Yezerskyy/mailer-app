# FILE: engine/work/classify_11880_is_name_json.py  (обновлено — 2026-02-09)
# PURPOSE: Batch GPT classification for branches_raw_11880 via strict JSON I/O; update is_active:
#          if proper name => is_active=false, else is_active=true. Uses local last_id state to avoid reprocessing.

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from engine.common.db import get_connection
from engine.common.gpt import GPTClient

MODEL = os.getenv("GPT_MODEL", "mini")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "30"))
MAX_BATCHES = int(os.getenv("MAX_BATCHES", "1000"))

STATE_PATH = os.getenv("STATE_PATH", "cache/classify_11880_is_name.last_id")


INSTRUCTIONS = (
    "You are given JSON with items to classify in German (Deutsch).\n"
    "Decide for each item whether it CAN be a proper name.\n"
    "Proper names include: surnames and given names, brands, company names, organizations, institutions,\n"
    "toponyms/place names, names of people or animals, and other unique proper names.\n"
    "If there is doubt, ambiguity, homonymy, or lack of context, prefer false.\n\n"
    "Return ONLY valid JSON (no markdown, no comments, no extra text).\n"
    "Output must be a JSON array of objects exactly like:\n"
    "[{\"id\": 123, \"is_name\": true}, ...]\n"
    "All ids from input must be present exactly once. is_name must be boolean."
)


def _read_last_id() -> int:
    p = Path(STATE_PATH)
    try:
        txt = p.read_text(encoding="utf-8").strip()
        if not txt:
            return 0
        val = int(txt)
        return max(val, 0)
    except Exception:
        return 0


def _write_last_id(last_id: int) -> None:
    p = Path(STATE_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(str(int(last_id)), encoding="utf-8")


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
        user_id="engine.work.classify_11880_is_name_json.branches_raw_11880",
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

    last_id = _read_last_id()

    with get_connection() as conn:
        while batches_done < MAX_BATCHES:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select id, label
                    from branches_raw_11880
                    where id > %s
                    order by id
                    limit %s
                    """,
                    (last_id, BATCH_SIZE),
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
                # всё равно двигаем last_id, чтобы не зациклиться на одной и той же пачке
                last_id = rows[-1][0]
                _write_last_id(last_id)
                batches_done += 1
                continue

            with conn.cursor() as cur:
                updated = 0
                for rid, _label in rows:
                    is_name = mapping[rid]
                    is_active = (not is_name)  # proper name => inactive
                    cur.execute(
                        "update branches_raw_11880 set is_active=%s where id=%s",
                        (is_active, rid),
                    )
                    updated += cur.rowcount

            conn.commit()
            total_updated += updated

            last_id = rows[-1][0]
            _write_last_id(last_id)

            print(f"batch={batches_done+1} status=OK selected={len(rows)} updated={updated} last_id={last_id}")
            batches_done += 1

    print(
        f"summary: batches={batches_done} selected={total_selected} updated={total_updated} "
        f"skipped_bad_response={total_skipped} model={MODEL} batch_size={BATCH_SIZE} last_id={last_id}"
    )


if __name__ == "__main__":
    main()
