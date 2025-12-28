# FILE: engine/core_validate/val_processor.py  (обновлено) 2025-12-17
# Fix:
# - enrich_priority_updater переехал в engine/core_validate/val_enrich.py (val_enrich.run_priority_updater)
# - every_sec=600 это раз в 10 минут

from engine.common.worker import Worker
from engine.core_validate import val_email, val_prepare
#from engine.core_validate import val_enrich

TASK_TIMEOUT_SEC = 900  # 15 минут


def main() -> None:
    w = Worker(
        name="val_processor",
        tick_sec=2,
        max_parallel=1,
    )

    w.register(
        name="val_email",
        fn=val_email.run_batch,
        every_sec=5,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=10,
    )

    w.register(
        name="val_prepare",
        fn=val_prepare.run_batch,
        every_sec=10,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=20,
    )
# дорого и глупо пока что
#    w.register(
#        name="enrich_priority_updater",
#        fn=val_enrich.run_priority_updater,
#        every_sec=600,  # раз в 10 минут
#        timeout_sec=TASK_TIMEOUT_SEC,
#        singleton=True,
#        heavy=True,
#        priority=30,
#    )

#    w.register(
#        name="val_enrich",
#        fn=val_enrich.run_batch,
#        every_sec=10,  # каждые 10 секунд
#        timeout_sec=TASK_TIMEOUT_SEC,
#        singleton=True,
#        heavy=False,
#        priority=40,
#    )

    w.run_forever()


if __name__ == "__main__":
    main()
