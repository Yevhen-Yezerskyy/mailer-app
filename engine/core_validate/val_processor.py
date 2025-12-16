# FILE: engine/core_validate/val_processor.py  (обновлено — 2025-12-16)
# Смысл: процессор валидации. Крутит Worker и периодически запускает валидаторы (сейчас: val_email). Параллельность=1.

from engine.common.worker import Worker
from engine.core_validate import val_email

TASK_TIMEOUT_SEC = 900  # 15 минут


def main() -> None:
    w = Worker(
        name="val_processor",
        tick_sec=0.5,
        max_parallel=1,  # DNS/MX не параллелим
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

    w.run_forever()


if __name__ == "__main__":
    main()
