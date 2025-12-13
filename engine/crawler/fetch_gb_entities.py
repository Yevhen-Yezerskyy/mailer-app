# FILE: engine/crawler/fetch_gb_entities.py  (новое) 2025-12-13

from engine.common.db import fetch_all
from scrapy.crawler import CrawlerProcess

from engine.crawler.spiders.spider_gelbeseiten_entities import GelbeSeitenEntitiesSpider


BRANCHES_PER_RUN = 500


def load_branches_without_entities(limit: int) -> list[tuple[int, str, str]]:
    """
    Возвращает список (id, slug, name) для тех, у кого entities NULL.
    """
    sql = """
        SELECT id, slug, name
        FROM gb_branches
        WHERE entities IS NULL
        ORDER by random()
        LIMIT %s;
    """
    return fetch_all(sql, (limit,))


def main():
    rows = load_branches_without_entities(BRANCHES_PER_RUN)
    print(f"Найдено бранчей без entities: {len(rows)} (лимит {BRANCHES_PER_RUN})")

    if not rows:
        return

    process = CrawlerProcess(
        settings={
            "LOG_LEVEL": "ERROR",
            "TELNETCONSOLE_ENABLED": False,

            # антибан (медленно и стабильно)
            "DOWNLOAD_DELAY": 3.0,
            "RANDOMIZE_DOWNLOAD_DELAY": True,
            "CONCURRENT_REQUESTS": 1,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
            "CONCURRENT_REQUESTS_PER_IP": 1,

            "AUTOTHROTTLE_ENABLED": True,
            "AUTOTHROTTLE_START_DELAY": 3.0,
            "AUTOTHROTTLE_MAX_DELAY": 60.0,
            "AUTOTHROTTLE_TARGET_CONCURRENCY": 0.5,

            "RETRY_ENABLED": True,
            "RETRY_TIMES": 8,
            "RETRY_HTTP_CODES": [429, 500, 502, 503, 504, 522, 524, 408],
            "DOWNLOAD_TIMEOUT": 30,

            "COOKIES_ENABLED": True,
            "USER_AGENT": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),

            "ITEM_PIPELINES": {
                "engine.crawler.pipelines.pipeline_gelbeseiten_entities.EntitiesPipeline": 100,
            },
        }
    )

    for branch_id, slug, name in rows:
        process.crawl(GelbeSeitenEntitiesSpider, branch_id=branch_id, slug=slug, name=name)

    process.start()


if __name__ == "__main__":
    main()
