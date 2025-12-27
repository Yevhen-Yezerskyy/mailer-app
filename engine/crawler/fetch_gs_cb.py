# FILE: engine/crawler/fetch_gs_cb.py
# (обновлено — 2025-12-27)
# Смысл:
# - Выбираем ОДНУ cb_crawler where collected=false
# - Передаём plz + branch_slug + cb_crawler_id в паука
# - Никаких queue_sys, task_id, rate и т.п.

from scrapy.crawler import CrawlerProcess

from engine.common.db import fetch_one
from engine.crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider


def main():
    row = fetch_one(
        """
        SELECT id, plz, branch_slug
        FROM cb_crawler
        WHERE collected = false
        ORDER BY id
        LIMIT 1
        """
    )

    if not row:
        print("DEBUG: no uncollected cb_crawler rows")
        return

    cb_crawler_id, plz, branch_slug = row

    print(f"DEBUG: picked cb_crawler_id={cb_crawler_id} plz={plz} branch={branch_slug}")

    process = CrawlerProcess(
        settings={
            "LOG_LEVEL": "ERROR",
            "TELNETCONSOLE_ENABLED": False,

            "DOWNLOAD_DELAY": 2.0,
            "RANDOMIZE_DOWNLOAD_DELAY": True,
            "CONCURRENT_REQUESTS": 1,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,

            "AUTOTHROTTLE_ENABLED": True,
            "AUTOTHROTTLE_START_DELAY": 2.0,
            "AUTOTHROTTLE_MAX_DELAY": 30.0,
            "AUTOTHROTTLE_TARGET_CONCURRENCY": 0.5,

            "COOKIES_ENABLED": True,

            "ITEM_PIPELINES": {
                "engine.crawler.pipelines.pipeline_gs_cb.GSCBPipeline": 300,
            },
        }
    )

    process.crawl(
        GelbeSeitenCBSpider,
        plz=plz,
        branch_slug=branch_slug,
        cb_crawler_id=cb_crawler_id,
    )
    process.start()


if __name__ == "__main__":
    main()
