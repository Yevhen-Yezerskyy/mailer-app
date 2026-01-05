# FILE: engine/core_prepare/prepare_cb_processor.py  (обновлено — 2025-12-28)
# Смысл:
# - Worker НЕ ТРОГАЕМ
# - Раз в 2 минуты сбрасываем очереди задач (Q_tasks), чтобы “мертвые” и done сами вымывались.

from engine.common.worker import Worker
from engine.core_prepare import prepare_cb

TASK_TIMEOUT_SEC = 900


def main() -> None:
    w = Worker(
        name="prepare_cb_processor",
        tick_sec=3,
        max_parallel=16,
    )

    w.register(
        name="prepare_geo",
        fn=prepare_cb.task_prepare_geo,
        every_sec=2,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=10,
    )

    w.register(
        name="prepare_branches",
        fn=prepare_cb.task_prepare_branches,
        every_sec=3,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=10,
    )

    w.register(
        name="prepare_done",
        fn=prepare_cb.task_prepare_done,
        every_sec=15,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=30,
    )

    w.register(
        name="prepare_reset_queues",
        fn=prepare_cb.reset_prepare_queues,
        every_sec=60,
        timeout_sec=30,
        singleton=True,
        heavy=False,
        priority=90,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
