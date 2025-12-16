# FILE: engine/core_validate/val_processor.py  (обновлено — 2025-12-16)
# Смысл: процессор валидации/подготовки. Крутит Worker и периодически запускает:
# - val_email: проверка email в raw_contacts_gb
# - val_prepare: перенос/агрегация OK-email из raw_contacts_gb -> raw_contacts_aggr (dedup по email)
# Параллельность=1.

from engine.common.worker import Worker
from engine.core_validate import val_email, val_prepare

TASK_TIMEOUT_SEC = 900  # 15 минут


def main() -> None:
    w = Worker(
        name="val_processor",
        tick_sec=0.5,
        max_parallel=1,  # DNS/MX + дедуп по email не параллелим
    )

    w.register(
        name="val_email",
        fn=val_email.run_batch,
        every_sec=2,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=10,
    )

    w.register(
        name="val_prepare",
        fn=val_prepare.run_batch,
        every_sec=3,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=20,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
