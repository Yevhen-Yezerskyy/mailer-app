# FILE: engine/crawler/spiders/spider_11880_slugs.py  (обновлено — 2026-02-09)
# PURPOSE: Scrapy-спайдер: собирает все /suche/<slug> со страниц https://www.11880.com/branchen*,
#          берёт label из текста ссылки и пишет (upsert) в таблицу branches_raw_11880 (уникально по slug).

from __future__ import annotations

import re
from typing import Iterable, List, Tuple
from urllib.parse import urljoin, urlparse

import scrapy

from engine.common.db import get_connection


# было: ^/suche/([^/?#]+)/*$  (слишком строго)
SLUG_RE = re.compile(r"^/suche/([^/?#]+)")


class Slugs11880Spider(scrapy.Spider):
    name = "slugs_11880"

    custom_settings = {
        "LOG_ENABLED": False,
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_TIMEOUT": 30,
        "RETRY_TIMES": 2,
        "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    }

    start_urls = ["https://www.11880.com/branchen"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._seen_slugs: set[str] = set()
        self._pairs: List[Tuple[str, str]] = []
        self._seen_branchen_pages: set[str] = set()

    def parse(self, response):
        self._seen_branchen_pages.add(response.url)

        # 1) собрать /suche/<slug> (в любом виде: относит., полная ссылка, с хвостом /город и т.п.)
        for href, label in self._iter_suche_links(response):
            path = href
            if href.startswith("http://") or href.startswith("https://"):
                path = urlparse(href).path

            mm = SLUG_RE.match(path)
            if not mm:
                continue

            slug = (mm.group(1) or "").strip()
            if not slug or slug in self._seen_slugs:
                continue

            self._seen_slugs.add(slug)
            self._pairs.append((slug, (label or "").strip()))

        # 2) перейти по branchen/*
        for href in response.css('a::attr(href)').getall():
            if not href:
                continue
            href = href.strip()
            if not href.startswith("/branchen"):
                continue

            nxt = urljoin(response.url, href)
            if urlparse(nxt).netloc != "www.11880.com":
                continue
            if nxt in self._seen_branchen_pages:
                continue

            yield scrapy.Request(nxt, callback=self.parse, dont_filter=True)

    @staticmethod
    def _iter_suche_links(response) -> Iterable[Tuple[str, str]]:
        for a in response.css("a"):
            href = (a.attrib.get("href") or "").strip()
            if not href:
                continue

            # быстрее и точнее, чем "/suche/" in href
            if not (href.startswith("/suche/") or href.startswith("https://www.11880.com/suche/")):
                continue

            label = "".join(a.css("::text").getall()).strip()
            yield href, label

    def closed(self, reason):
        if not self._pairs:
            return

        # гарантия уникальности: (1) _seen_slugs в рантайме, (2) UNIQUE(slug) в БД
        sql = """
            INSERT INTO branches_raw_11880 (slug, label)
            VALUES (%s, %s)
            ON CONFLICT (slug)
            DO UPDATE SET
                label = EXCLUDED.label
        """

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, self._pairs)
            conn.commit()
        finally:
            try:
                conn.close()
            except Exception:
                pass
