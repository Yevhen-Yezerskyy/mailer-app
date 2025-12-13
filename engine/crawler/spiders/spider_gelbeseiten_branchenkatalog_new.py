# FILE: engine/crawler/spiders/spider_gelbeseiten_branchenkatalog_new.py  (новое) 2025-12-13

from __future__ import annotations

import scrapy
from scrapy import Request
from urllib.parse import unquote

from engine.common.db import get_connection
from engine.crawler.parsers.parser_gelbeseiten_branchenkatalog_new import (
    extract_level1_category_links,
    extract_level2_rubriken_links,
    parse_branche_items,
)


class GelbeSeitenBranchenkatalogSpiderNew(scrapy.Spider):
    name = "gelbeseiten_branchenkatalog_new"
    allowed_domains = ["www.gelbeseiten.de"]
    start_urls = ["https://www.gelbeseiten.de/branchenbuch"]

    SQL_UPSERT = """
        INSERT INTO gb_branches (name, slug)
        VALUES (%s, %s)
        ON CONFLICT (slug) DO UPDATE
        SET name = EXCLUDED.name
        RETURNING id;
    """

    def save_branch(self, name: str, slug: str):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(self.SQL_UPSERT, (name, slug))
                row = cur.fetchone()
        print(f"[gb_branch] {slug} | {name}")

    def parse(self, response: scrapy.http.Response):
        for item in parse_branche_items(response):
            self.save_branch(
                item["branch_name_raw"],
                item["branch_slug"],
            )

        for url in extract_level1_category_links(response):
            yield Request(url, callback=self.parse_category)

    def parse_category(self, response: scrapy.http.Response):
        for item in parse_branche_items(response):
            self.save_branch(
                item["branch_name_raw"],
                item["branch_slug"],
            )

        for url in extract_level2_rubriken_links(response):
            yield Request(url, callback=self.parse_rubrik)

    def parse_rubrik(self, response: scrapy.http.Response):
        for item in parse_branche_items(response):
            self.save_branch(
                item["branch_name_raw"],
                item["branch_slug"],
            )

        for url in extract_level2_rubriken_links(response):
            yield Request(url, callback=self.parse_rubrik)
