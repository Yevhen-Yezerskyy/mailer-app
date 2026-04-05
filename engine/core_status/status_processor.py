# FILE: engine/core_status/status_processor.py
# DATE: 2026-04-05
# PURPOSE: Dedicated worker for status-based audience task active recalculation.

from engine.common.worker import Worker
from engine.core_status import status

TASK_TIMEOUT_SEC = 120


def main() -> None:
    w = Worker(
        name="core_status_processor",
        tick_sec=1,
        max_parallel=1,
    )

    w.register(
        name="ready_run_once",
        fn=status.run_ready_once,
        every_sec=2,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=5,
    )

    w.register(
        name="status_run_once",
        fn=status.run_once,
        every_sec=5,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=10,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
