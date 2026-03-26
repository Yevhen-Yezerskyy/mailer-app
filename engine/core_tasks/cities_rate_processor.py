# FILE: engine/core_tasks/cities_rate_processor.py
# DATE: 2026-03-26
# PURPOSE: Dedicated worker for city rating tasks only.

from engine.common.worker import Worker
from engine.core_tasks import rate_cities

RATE_TIMEOUT_SEC = 900


def main() -> None:
    w = Worker(
        name="cities_rate_processor",
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

    w.run_forever()


if __name__ == "__main__":
    main()
