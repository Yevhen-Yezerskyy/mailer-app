# FILE: engine/core_tasks/tasks_processor.py
# DATE: 2026-03-25
# PURPOSE: Shared worker processor for the new core_tasks flow. Runs city rating and
# ready-flag recomputation on separate schedules.

from engine.common.worker import Worker
from engine.core_tasks import rate_cities, ready_tasks

RATE_TIMEOUT_SEC = 900
READY_TIMEOUT_SEC = 120


def main() -> None:
    w = Worker(
        name="tasks_processor",
        tick_sec=1,
        max_parallel=10,
    )

    w.register(
        name="rate_cities_once",
        fn=rate_cities.run_once,
        every_sec=1,
        timeout_sec=RATE_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=10,
    )

    w.register(
        name="ready_once",
        fn=ready_tasks.run_once,
        every_sec=3,
        timeout_sec=READY_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=30,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
