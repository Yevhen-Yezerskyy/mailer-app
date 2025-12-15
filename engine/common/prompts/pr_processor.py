# FILE: engine/common/prompts/pr_processor.py  (новое) 2025-12-15
# Prompt processor:
# - крутит Worker
# - раз в 5 минут запускает process_once(verbose=False)
# - singleton=True (параллельно не нужно)
# - heavy=False

from engine.common.worker import Worker
from engine.common.prompts.process import process_once

TASK_TIMEOUT_SEC = 900  # 15 минут


def main() -> None:
    w = Worker(
        name="prompt_processor",
        tick_sec=0.5,
        max_parallel=5,
    )

    w.register(
        name="prompt_sync",
        fn=lambda: process_once(verbose=False),
        every_sec=5,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=50,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
