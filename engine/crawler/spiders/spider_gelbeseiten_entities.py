# FILE: engine/crawler/spiders/spider_gelbeseiten_entities.py  (новое) 2025-12-13

from urllib.parse import quote, unquote

import scrapy
from scrapy import Request

from engine.crawler.parsers.parser_gelbeseiten_entities import parse_entities_count


class GelbeSeitenEntitiesSpider(scrapy.Spider):
    name = "gelbeseiten_entities"

    def __init__(
        self,
        branch_id: int | None = None,
        slug: str | None = None,
        name: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.branch_id = int(branch_id) if branch_id is not None else None
        self.slug = (slug or "").strip()
        self.branch_name = (name or "").strip()

        self.start_urls = []
        if self.slug:
            # slug в БД часто уже percent-encoded -> сначала декодируем, потом кодируем ровно 1 раз
            decoded = unquote(self.slug)
            encoded = quote(decoded, safe="")
            self.start_urls = [f"https://www.gelbeseiten.de/suche/{encoded}/bundesweit"]

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url, callback=self.parse, dont_filter=True)

    def parse(self, response: scrapy.http.Response):
        entities = parse_entities_count(response)

        yield {
            "branch_id": self.branch_id,
            "slug": self.slug,              # оставляем как в БД
            "name": self.branch_name,
            "entities": entities,
            "url": response.url,
        }
