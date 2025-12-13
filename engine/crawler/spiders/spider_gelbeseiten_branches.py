# FILE: engine/crawler/spiders/spider_gelbeseiten_branches.py  (новое) 2025-12-13

import scrapy
from scrapy import Request

from engine.crawler.parsers.parser_gelbeseiten_branches import parse_city_branches


def extract_letter_links(response: scrapy.http.Response) -> list[str]:
    """Ссылки на буквы A–Z с городской страницы."""
    hrefs = response.css("div.alphabetfilter a.alphabetfilter__btn::attr(href)").getall()
    return [response.urljoin(h) for h in hrefs]


class GelbeSeitenBranchesSpider(scrapy.Spider):
    name = "gelbeseiten_branches"

    def __init__(self, city_url: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.start_urls = [city_url] if city_url else []

    def parse(self, response: scrapy.http.Response):
        """
        Открыли город → берём ВСЕ буквы и идём по каждой.
        """
        letter_links = extract_letter_links(response)
        if not letter_links:
            self.logger.warning("НЕТ БУКВ на странице: %s", response.url)
            return

        self.logger.info("БУКВЫ ДЛЯ ГОРОДА %s: %d", response.url, len(letter_links))

        for url in letter_links:
            yield Request(url, callback=self.parse_letter)

    def parse_letter(self, response: scrapy.http.Response):
        """
        Страница буквы (/branchen/.../a, /b, /c, ...).
        Тут только yield item'ов, вывод делает пайплайн.
        """
        for item in parse_city_branches(response):
            yield item
