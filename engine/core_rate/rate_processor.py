# FILE: engine/core_rate/rate_processor.py  (новое) 2025-12-17
# Смысл: процессор ранжирования компаний. Крутит Worker:
# - раз в 10 минут синхронизирует __rate_priority (rt_needed из task.subscribers_limit, rt_done из rate_contacts)
# - часто запускает rate_contacts.run_batch(), который берет пачки (YES WEB / NOT YES WEB) и пишет rate_contacts.

from engine.common.worker import Worker
from engine.core_rate import rate_contacts

TASK_TIMEOUT_SEC = 900  # 15 минут


def main() -> None:
    w = Worker(
        name="rate_processor",
        tick_sec=0.5,
        max_parallel=1,  # GPT + БД: не параллелим
    )

    w.register(
        name="rate_priority_updater",
        fn=rate_contacts.run_priority_updater,
        every_sec=10,  # раз в 10 минут
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=True,
        priority=10,
    )

    w.register(
        name="rate_contacts",
        fn=rate_contacts.run_batch,
        every_sec=10,  # часто, но не бешено
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=20,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
