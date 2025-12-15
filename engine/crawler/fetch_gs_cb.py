# FILE: engine/crawler/fetch_gs_cb.py  (новое) 2025-12-14
# Runner GelbeSeiten cb_crawler → raw_contacts_gb

from scrapy.crawler import CrawlerProcess
from engine.crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider


def main():
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

    process.crawl(GelbeSeitenCBSpider)
    process.start()


if __name__ == "__main__":
    main()
