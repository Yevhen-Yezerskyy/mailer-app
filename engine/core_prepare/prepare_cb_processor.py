# FILE: engine/core_prepare/prepare_cb_processor.py
# (новое — 2025-12-27)
# - Shared-memory через multiprocessing.Manager
# - Глобальный mutex + TTL-локи для prepare_cb
# - Worker НЕ ТРОГАЕМ

from multiprocessing import Manager
import time

from engine.common.worker import Worker
from engine.core_prepare import prepare_cb

TASK_TIMEOUT_SEC = 900
LOCK_TTL_SEC = 900


def main() -> None:
    manager = Manager()

    # shared между всеми процессами Worker
    ipc_locks = manager.dict()     # (task_id, kind, entity_id) -> ts
    ipc_guard = manager.Lock()     # глобальный mutex

    # инициализируем IPC-контекст В prepare_cb
    prepare_cb.init_ipc(
        locks=ipc_locks,
        guard=ipc_guard,
        ttl_sec=LOCK_TTL_SEC,
    )

    w = Worker(
        name="prepare_cb_processor",
        tick_sec=3,
        max_parallel=3,
    )

    w.register(
        name="prepare_geo",
        fn=prepare_cb.task_prepare_geo,
        every_sec=3,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=10,
    )

    w.register(
        name="prepare_branches",
        fn=prepare_cb.task_prepare_branches,
        every_sec=5,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=10,
    )

    w.register(
        name="prepare_done",
        fn=prepare_cb.task_prepare_done,
        every_sec=30,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=30,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
