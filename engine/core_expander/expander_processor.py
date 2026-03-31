# FILE: engine/core_expander/expander_processor.py
# DATE: 2026-03-31
# PURPOSE: Worker loop for core_expander.

from __future__ import annotations

from engine.common.worker import Worker
from engine.core_expander import expander

TASK_TIMEOUT_SEC = 900


def main() -> None:
    worker = Worker(
        name="core_expander_processor",
        tick_sec=2,
        max_parallel=1,
    )

    worker.register(
        name="core_expander_run_batch",
        fn=expander.run_batch,
        every_sec=5,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=10,
    )

    worker.run_forever()


if __name__ == "__main__":
    main()
