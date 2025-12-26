# FILE: engine/core_prepare/prepare_cb_processor.py  (новое — 2025-12-26)
# Смысл: процессор подготовки (geo/branches/done) через Worker. Каждую секунду делает один шаг по каждой задаче.

from engine.common.worker import Worker
from engine.core_prepare.prepare_cb import (
    task_prepare_geo,
    task_prepare_branches,
    task_prepare_done,
)

TASK_TIMEOUT_SEC = 900  # 15 минут


def main() -> None:
    w = Worker(
        name="prepare_cb_processor",
        tick_sec=0.5,
        max_parallel=16,
    )

    # Каждую секунду. LIFO по created_at внутри задач.
    w.register(
        name="prepare_geo",
        fn=task_prepare_geo,
        every_sec=2,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=10,
    )

    w.register(
        name="prepare_branches",
        fn=task_prepare_branches,
        every_sec=3,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=10,
    )

    # done всегда последним
    w.register(
        name="prepare_done",
        fn=task_prepare_done,
        every_sec=20,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=30,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
