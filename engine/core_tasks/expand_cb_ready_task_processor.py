# FILE: engine/core_tasks/expand_cb_ready_task_processor.py
# DATE: 2026-03-26
# PURPOSE: Dedicated worker for ready flag recomputation and CB expansion.

from engine.common.worker import Worker
from engine.core_tasks import expand_cb_pairs, ready_tasks

EXPAND_TIMEOUT_SEC = 900
READY_TIMEOUT_SEC = 120


def main() -> None:
    w = Worker(
        name="expand_cb_ready_task_processor",
        tick_sec=1,
        max_parallel=1,
    )

    w.register(
        name="ready_once",
        fn=ready_tasks.run_once,
        every_sec=10,
        timeout_sec=READY_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=10,
    )

    w.register(
        name="expand_cb_pairs_once",
        fn=expand_cb_pairs.run_once,
        every_sec=2,
        timeout_sec=EXPAND_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=30,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
