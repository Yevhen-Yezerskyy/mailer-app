# FILE: engine/core_rate/rate_contacts_processor.py  (новое — 2025-12-29)


from engine.common.worker import Worker
from engine.core_rate.rate_contacts import (
    task_rate_contacts_done_scan,
    task_rate_contacts_once,
    task_rate_contacts_reset_cache,
)

TASK_TIMEOUT_SEC = 900  # 15 минут


def main() -> None:
    w = Worker(
        name="core_rate_contacts_processor",
        tick_sec=2,
        max_parallel=5,
    )

    w.register(
        name="rate_contacts_once",
        fn=task_rate_contacts_once,
        every_sec=2,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=40,
    )

    w.register(
        name="rate_contacts_done_scan",
        fn=task_rate_contacts_done_scan,
        every_sec=20,
        timeout_sec=60,
        singleton=True,
        heavy=False,
        priority=60,
    )

    w.register(
        name="rate_contacts_reset_cache",
        fn=task_rate_contacts_reset_cache,
        every_sec=600,
        timeout_sec=30,
        singleton=True,
        heavy=False,
        priority=80,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
