# FILE: engine/worker/tick.py (новое) 2025-12-10
"""
Временный тикер-воркер.

Задача:
- Живёт в одном процессе.
- Каждые N секунд делает "цикл" и дергает зарегистрированные процессы.
- Каждый процесс сам знает, как часто ему тикать.

Через пару недель этот файл можно будет спокойно выбросить
и заменить нормальным оркестратором.
"""

import logging
import time
from typing import List, Optional


# --- Базовая настройка логирования для воркера ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


# --- Базовый класс процесса и менеджер ---


class BaseProcess:
    """
    Базовый класс для "вечноживущих" процессов в рамке этого тикера.

    Идея:
    - у процесса есть name (для логов) и interval_seconds;
    - тикер дергает .tick(), когда "пришло время";
    - сам процесс хранит в себе, когда ему в следующий раз надо отработать.

    Наследнику достаточно:
    - задать name (по желанию);
    - при необходимости переопределить interval_seconds;
    - реализовать tick().
    """

    #: Человеко-понятное имя процесса (для логов)
    name: str = "base"

    #: Как часто дергаем tick(), секунды
    interval_seconds: int = 60  # по умолчанию раз в минуту

    def __init__(self, interval_seconds: Optional[int] = None) -> None:
        if interval_seconds is not None:
            self.interval_seconds = interval_seconds

        # epoch-время следующего запуска
        self._next_run_ts: float = 0.0

    # --- Жизненный цикл ---

    def should_run(self, now_ts: float) -> bool:
        """
        Проверяем, пора ли запускать процесс.
        """
        return now_ts >= self._next_run_ts

    def schedule_next_run(self, now_ts: float) -> None:
        """
        Выставляем время следующего запуска.
        """
        self._next_run_ts = now_ts + self.interval_seconds

    def tick(self) -> None:
        """
        Основная работа процесса.

        ДОЛЖЕН быть переопределён в наследнике.
        """
        raise NotImplementedError("tick() must be implemented in subclass")


class ProcessManager:
    """
    Мини-менеджер процессов для текущего тикера.

    - хранит список процессов;
    - в одном цикле (run_cycle) пробегается по ним и дергает тех, кому "пора";
    - оборачивает вызовы tick() в try/except, чтобы один упавший процесс
      не валил весь воркер.

    Это временная штука, чисто под этот тикер; потом её заменит оркестратор.
    """

    def __init__(self) -> None:
        self._processes: List[BaseProcess] = []

    # --- Регистрация процессов ---

    def register(self, process: BaseProcess) -> None:
        name = getattr(process, "name", process.__class__.__name__)
        logger.info(
            "Registering process: %s (interval=%ss)",
            name,
            process.interval_seconds,
        )
        self._processes.append(process)

    # --- Основной цикл одного шага ---

    def run_cycle(self) -> None:
        """
        Один "шаг" менеджера:
        - смотрим текущее время;
        - для каждого процесса проверяем, пришло ли время его дернуть;
        - если да — дергаем tick() и планируем следующий запуск.
        """
        if not self._processes:
            # ничего не зарегистрировано — тикер просто будет спать
            return

        now_ts = time.time()

        for process in self._processes:
            try:
                if process.should_run(now_ts):
                    name = getattr(process, "name", process.__class__.__name__)
                    logger.debug("Running process tick: %s", name)
                    process.tick()
                    process.schedule_next_run(now_ts)
            except Exception:
                # важно: один упавший процесс не валит всё
                logger.exception("Error while running process tick: %r", process)


# --- Пример простого процесса (для проверки, что всё живо) ---


class HeartbeatProcess(BaseProcess):
    """
    Примитивный процесс "для проверки", что тикер жив.

    Раз в interval_seconds пишет сообщение в лог.
    Этот класс можно спокойно удалить, когда появятся реальные процессы.
    """

    name = "heartbeat"

    def __init__(self, interval_seconds: int = 60) -> None:
        super().__init__(interval_seconds=interval_seconds)

    def tick(self) -> None:
        logger.info("Heartbeat tick: воркер жив.")


# --- Инициализация процессов ---


def build_processes() -> ProcessManager:
    """
    Создаём ProcessManager и регистрируем в нём процессы.

    Сейчас тут только пример — HeartbeatProcess.
    Когда появится реальный процесс (например, PromptTranslatorProcess),
    его надо будет импортировать и зарегистрировать здесь же.
    """
    manager = ProcessManager()

    # Пример: "пульс" раз в 60 секунд.
    heartbeat = HeartbeatProcess(interval_seconds=60)
    manager.register(heartbeat)

    # TODO: сюда добавим, например:
    # from engine.prompts.translate import PromptTranslatorProcess
    # translator = PromptTranslatorProcess(interval_seconds=300)
    # manager.register(translator)

    return manager


# --- Основной цикл тикера ---


def main() -> None:
    logger.info("Starting worker tick loop")
    manager = build_processes()

    # Базовый интервал сна тикера (в секундах).
    # Он может быть меньше, чем у процессов, чтобы чаще проверять "кому пора".
    sleep_base_seconds = 5

    try:
        while True:
            manager.run_cycle()
            time.sleep(sleep_base_seconds)
    except KeyboardInterrupt:
        logger.info("Worker tick stopped by KeyboardInterrupt")
    except Exception:
        logger.exception("Fatal error in worker tick loop")


if __name__ == "__main__":
    main()
