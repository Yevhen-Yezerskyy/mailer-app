# FILE: engine/crawler/cr_processor.py  (обновлено — 2026-01-03)
# PATH: engine/crawler/cr_processor.py
# Смысл:
# - gs_cb_spider: каждый тик берёт 1 item из очереди и запускает паука
# - cbq_reset_cache: каждые 10 минут обнуляет кеш-очередь (cbq:list)

from engine.common.worker import Worker
from engine.crawler.fetch_gs_cb import cbq_reset_cache, worker_run_once

TASK_TIMEOUT_SEC = 900  # 15 минут


def main() -> None:
    w = Worker(
        name="crawl_cr_processor",
        tick_sec=2,
        max_parallel=5,
    )

    w.register(
        name="gs_cb_spider",
        fn=worker_run_once,
        every_sec=2,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=40,
    )

    w.register(
        name="cbq_reset_cache",
        fn=cbq_reset_cache,
        every_sec=600,  # 10 минут
        timeout_sec=30,
        singleton=True,
        heavy=False,
        priority=90,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
