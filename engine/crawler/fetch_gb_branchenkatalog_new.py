# FILE: engine/crawler/fetch_gb_branchenkatalog_new.py  (новое) 2025-12-13

from scrapy.crawler import CrawlerProcess

from engine.crawler.spiders.spider_gelbeseiten_branchenkatalog_new import (
    GelbeSeitenBranchenkatalogSpiderNew,
)


def main():
    process = CrawlerProcess(
        settings={
            "LOG_LEVEL": "ERROR",
            "TELNETCONSOLE_ENABLED": False,

            # антибан
            "DOWNLOAD_DELAY": 2.0,
            "RANDOMIZE_DOWNLOAD_DELAY": True,
            "CONCURRENT_REQUESTS": 1,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,

            "AUTOTHROTTLE_ENABLED": True,
            "AUTOTHROTTLE_START_DELAY": 2.0,
            "AUTOTHROTTLE_MAX_DELAY": 30.0,
            "AUTOTHROTTLE_TARGET_CONCURRENCY": 0.5,

            "COOKIES_ENABLED": True,
            "USER_AGENT": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),

            # ВАЖНО: НИКАКИХ PIPELINES ТУТ НЕТ
            "ITEM_PIPELINES": {},
        }
    )

    process.crawl(GelbeSeitenBranchenkatalogSpiderNew)
    process.start()


if __name__ == "__main__":
    main()
