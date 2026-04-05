# FILE: engine/core_rate_cities_expand_pairs/expand_pairs_processor.py
# DATE: 2026-04-05
# PURPOSE: Dedicated worker for CB expansion.

import time

from engine.common.worker import Worker
from engine.core_rate_cities_expand_pairs import expand_cb_pairs

EXPAND_TIMEOUT_SEC = 900


def main() -> None:
    w = Worker(
        name="expand_pairs_processor",
        tick_sec=1,
        max_parallel=1,
    )

    w.register(
        name="expand_cb_pairs_initial_once",
        fn=expand_cb_pairs.run_initial_once,
        every_sec=2,
        timeout_sec=EXPAND_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=10,
    )

    w.register(
        name="expand_cb_pairs_active_once",
        fn=expand_cb_pairs.run_active_once,
        every_sec=2,
        timeout_sec=EXPAND_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=20,
    )

    now = time.time()
    w._next_run_at["expand_cb_pairs_initial_once"] = now
    w._next_run_at["expand_cb_pairs_active_once"] = now + 1.0

    w.run_forever()


if __name__ == "__main__":
    main()
