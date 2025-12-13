# FILE: engine/crawler/parsers/parser_gelbeseiten_branchenkatalog_new.py  (новое) 2025-12-13

from __future__ import annotations

from urllib.parse import urlparse, unquote
from scrapy.http import Response


def extract_level1_category_links(response: Response) -> list[str]:
    hrefs = response.css("a.gc-iconbox__link::attr(href)").getall()
    return [response.urljoin(h) for h in hrefs if h and h.startswith("/branchenbuch/")]


def extract_level2_rubriken_links(response: Response) -> list[str]:
    # пример: <a href="rubriken/1053247">
    hrefs = response.css('a[href^="rubriken/"]::attr(href), a[href^="/branchenbuch/"][href*="rubriken/"]::attr(href)').getall()
    return [response.urljoin(h) for h in hrefs if h]


def parse_branche_items(response: Response):
    # пример: <a class="link" href="/branchenbuch/branche/dachdecker">Dachdecker</a>
    for a in response.css('a.link[href^="/branchenbuch/branche/"]'):
        href = (a.attrib.get("href") or "").strip()
        if not href:
            continue

        path = urlparse(href).path.strip("/")
        parts = path.split("/")
        if len(parts) < 3:
            continue

        slug = parts[2].strip()
        if not slug:
            continue

        name = " ".join(a.css("::text").getall()).strip()
        name = " ".join(name.split())
        if not name:
            name = unquote(slug)

        yield {
            "source": "gelbeseiten",
            "branch_name_raw": name,
            "branch_slug": unquote(slug),  # в БД храним декодированным
            "url": response.urljoin(href),
        }
