# FILE: engine/crawler/cr_processor.py  (обновлено — 2025-12-29)
# Смысл:
# - gs_cb_spider: как было (каждый тик берёт 1 item из очереди и запускает паука)
# - cbq_reset_cache: каждые 10 минут обнуляет кеш-очередь (cbq:list)
# - cb_mark_tasks_collected: раз в 2 часа выставляет aap_audience_audiencetask.collected=true

from engine.common.worker import Worker
from engine.crawler.fetch_gs_cb import cb_mark_tasks_collected, cbq_reset_cache, worker_run_once

TASK_TIMEOUT_SEC = 900  # 15 минут


def main() -> None:
    w = Worker(
        name="crawl_cr_processor",
        tick_sec=2,
        max_parallel=16,
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

    w.register(
        name="cb_mark_tasks_collected",
        fn=cb_mark_tasks_collected,
        every_sec=7200,  # 2 часа
        timeout_sec=600,
        singleton=True,
        heavy=False,
        priority=80,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
