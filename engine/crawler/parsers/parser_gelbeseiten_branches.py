# FILE: engine/crawler/parsers/parser_gelbeseiten_branches.py  (новое) 2025-12-13

from urllib.parse import urlparse
from scrapy.http import Response


def extract_city_from_url(url: str) -> str | None:
    """
    https://www.gelbeseiten.de/branchenbuch/staedte/<land>/<kreis>/<stadt>
    → последний сегмент как city_slug.
    """
    try:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        if "staedte" not in parts:
            return None
        return parts[-1]
    except Exception:
        return None


def parse_city_branches(response: Response):
    """
    Парсит таблицы бранчей на странице буквы.
    Ничего сам не печатает — только yield dict'ы.
    """
    city_slug = extract_city_from_url(response.url)

    links = response.css("div.pagesection div.gs-box table.table a.link")

    for link in links:
        name = " ".join(link.css("::text").getall()).strip()
        if not name:
            continue

        name = name.lstrip("-").strip()

        href = link.attrib.get("href")
        if not href:
            continue

        full_url = response.urljoin(href)

        parts = href.strip("/").split("/")
        branch_slug = parts[1] if len(parts) > 1 else None

        yield {
            "source": "gelbeseiten",
            "city_slug": city_slug,
            "branch_name_raw": name,
            "branch_slug": branch_slug,
            "url": full_url,
        }
