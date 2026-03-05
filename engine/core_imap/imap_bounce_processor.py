# FILE: engine/core_imap/imap_bounce_processor.py
# DATE: 2026-03-04
# PURPOSE: Background worker wrapper for IMAP bounce scan.

from __future__ import annotations

import os

from engine.common.worker import Worker
from engine.core_imap.imap_bounce import task_imap_bounce_scan_once


def _every_sec() -> int:
    raw = (os.environ.get("IMAP_BOUNCE_EVERY_SEC") or "").strip()
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    return 60


def _timeout_sec() -> int:
    raw = (os.environ.get("IMAP_BOUNCE_TIMEOUT_SEC") or "").strip()
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    return 900


def main() -> None:
    w = Worker(
        name="imap_bounce_processor",
        tick_sec=2,
        max_parallel=1,
    )
    w.register(
        name="imap_bounce_scan_once",
        fn=task_imap_bounce_scan_once,
        every_sec=_every_sec(),
        timeout_sec=_timeout_sec(),
        singleton=True,
        heavy=False,
        priority=50,
    )
    w.run_forever()


if __name__ == "__main__":
    main()
