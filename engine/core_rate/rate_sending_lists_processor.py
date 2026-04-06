# FILE: engine/core_rate/rate_sending_lists_processor.py
# DATE: 2026-04-06
# PURPOSE: Dedicated worker for sending list contact rating.

from engine.common.worker import Worker
from engine.core_rate import rate_sending_lists

RATE_TIMEOUT_SEC = 900


def main() -> None:
    w = Worker(
        name="rate_sending_lists_processor",
        tick_sec=1,
        max_parallel=8,
    )

    w.register(
        name="rate_sending_lists_once",
        fn=rate_sending_lists.run_once,
        every_sec=1,
        timeout_sec=RATE_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=10,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
