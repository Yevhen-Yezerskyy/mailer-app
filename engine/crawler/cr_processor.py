# FILE: engine/crawler/cr_processor.py  (новое) 2025-12-15
# CB processor:
# - крутит Worker
# - раз в секунду запускает fetch_gs_cb.main
# - до 3 scrapy-процессов параллельно
# - очередь синхронизирована через SQL (FOR UPDATE SKIP LOCKED)

from engine.common.worker import Worker
from engine.crawler.fetch_gs_cb import main as run_gs_cb_spider

TASK_TIMEOUT_SEC = 900  # 15 минут


def main() -> None:
    w = Worker(
        name="cb_processor",
        tick_sec=0.5,
        max_parallel=1,
    )

    w.register(
        name="gs_cb_spider",
        fn=run_gs_cb_spider,
        every_sec=1,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=40,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
