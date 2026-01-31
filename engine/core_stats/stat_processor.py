# FILE: engine/core_stats/stat_processor.py
# PATH: engine/core_stats/stat_processor.py
# DATE: 2026-01-31
# SUMMARY:
# - Parse smrel.log from mounted volume
# - Batch insert into mailbox_stats(letter_id, time)
# - Append-only processing with file offset
# - Run via engine.common.worker.Worker (run_forever)

from __future__ import annotations

import os
from datetime import datetime

from engine.common.db import execute
from engine.common.worker import Worker

LOG_PATH = "/var/www/serenity-stat/smrel.log"
OFFSET_PATH = "/var/www/serenity-stat/.smrel.offset"
BATCH_SIZE = 500


def _load_offset() -> int:
    try:
        with open(OFFSET_PATH, "r") as f:
            return int(f.read().strip())
    except Exception:
        return 0


def _save_offset(offset: int) -> None:
    with open(OFFSET_PATH, "w") as f:
        f.write(str(offset))


def _parse_line(line: str):
    line = line.strip()
    if not line:
        return None

    try:
        ts_raw, smrel = line.split("\t", 1)
        letter_id = int(smrel)
        ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        return letter_id, ts
    except Exception:
        return None


def _flush_batch(batch) -> int:
    if not batch:
        return 0

    values_sql = ",".join(["(%s,%s)"] * len(batch))
    params = []
    for letter_id, ts in batch:
        params.extend([letter_id, ts])

    execute(
        f"""
        INSERT INTO mailbox_stats (letter_id, time)
        VALUES {values_sql}
        """,
        params,
    )
    return len(batch)


def process_once() -> int:
    if not os.path.exists(LOG_PATH):
        print("[STAT] log file not found")
        return 0

    offset = _load_offset()
    processed = 0
    batch = []

    with open(LOG_PATH, "r") as f:
        f.seek(offset)

        for line in f:
            parsed = _parse_line(line)
            if not parsed:
                continue

            batch.append(parsed)

            if len(batch) >= BATCH_SIZE:
                n = _flush_batch(batch)
                processed += n
                print(f"[STAT] inserted batch: {n}")
                batch.clear()

        if batch:
            n = _flush_batch(batch)
            processed += n
            print(f"[STAT] inserted batch: {n}")
            batch.clear()

        new_offset = f.tell()
        _save_offset(new_offset)

    if processed:
        print(f"[STAT] done, rows={processed}, offset={new_offset}")
    else:
        print("[STAT] nothing new")

    return processed


def main() -> None:
    print("[STAT] worker started")

    w = Worker(name="stat_processor", tick_sec=0.5)

    w.register(
        "process_smrel_log",
        process_once,
        every_sec=10,
        timeout_sec=60,
        singleton=True,
        heavy=False,
        priority=50,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
