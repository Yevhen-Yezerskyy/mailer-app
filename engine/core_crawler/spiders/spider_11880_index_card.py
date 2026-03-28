# FILE: engine/core_crawler/spiders/spider_11880_index_card.py
# DATE: 2026-03-27
# PURPOSE: Parse one 11880 company card from a search result page.

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

from engine.core_crawler.spiders.spider_helpers import clean_email, clean_tel, clean_text, clean_url


def _iter_json_nodes(value: Any) -> Iterable[Any]:
    stack = [value]
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, dict):
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


def _iter_json_scripts(response) -> Iterable[Any]:
    for raw in response.css('script[type="application/ld+json"]::text').getall():
        raw = (raw or "").strip()
        if not raw:
            continue
        try:
            yield json.loads(raw)
        except Exception:
            continue


def _build_index_card(item: Dict[str, Any], category_11880: str) -> Dict[str, str] | None:
    url = clean_url(item.get("url"))
    name = clean_text(item.get("name"))
    if not url or not name:
        return None

    address = item.get("address") or {}
    street = clean_text(address.get("streetAddress")) or ""
    plz = clean_text(address.get("postalCode")) or ""
    city = clean_text(address.get("addressLocality")) or ""
    full_address = clean_text(" ".join(x for x in [street + "," if street else "", plz, city] if x)) or ""

    email = clean_email(item.get("email")) or ""
    telephone = item.get("telephone")
    phone = ""
    if isinstance(telephone, str):
        phone = clean_tel(telephone) or ""
    elif isinstance(telephone, list):
        for value in telephone:
            phone = clean_tel(value) or ""
            if phone:
                break

    rating = ""
    review_count = ""
    aggregate = item.get("aggregateRating") or {}
    if isinstance(aggregate, dict):
        rating = clean_text(str(aggregate.get("ratingValue") or "")) or ""
        review_count = clean_text(str(aggregate.get("reviewCount") or "")) or ""

    return {
        "url": url,
        "company_name": name,
        "category_11880": clean_text(category_11880) or "",
        "street": street,
        "plz": plz,
        "city": city,
        "address": full_address,
        "phone": phone,
        "email": email,
        "rating_11880": rating,
        "review_count_11880": review_count,
    }


def parse_11880_index_cards(response, category_11880: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    for data in _iter_json_scripts(response):
        for node in _iter_json_nodes(data):
            if not isinstance(node, dict):
                continue
            item_list = node.get("itemListElement")
            if not isinstance(item_list, list):
                continue
            for row in item_list:
                if not isinstance(row, dict):
                    continue
                item = row.get("item")
                if not isinstance(item, dict):
                    continue
                card = _build_index_card(item, category_11880)
                if not card:
                    continue
                url = card["url"]
                if url in seen:
                    continue
                seen.add(url)
                out.append(card)

    return out


def extract_11880_next_page_url(response) -> Optional[str]:
    current_page = 1
    raw_current = clean_text(response.css(".pagination .numbertext--current::text").get())
    if raw_current and raw_current.isdigit():
        current_page = int(raw_current)

    candidates: list[tuple[int, str]] = []
    for href in response.css(".pagination .numbertext a[href]::attr(href), .pagination a.numbertext--counter[href]::attr(href)").getall():
        url = clean_url(urljoin(response.url, href))
        if not url:
            continue
        parsed = urlparse(url)
        page_vals = parse_qs(parsed.query).get("page") or []
        page_num = None
        if page_vals and str(page_vals[0]).isdigit():
            page_num = int(page_vals[0])
        if page_num is None:
            data_page = response.css(f'.pagination a[href="{href}"]::attr(data-page)').get()
            if data_page and str(data_page).isdigit():
                page_num = int(data_page)
        if page_num is None:
            continue
        candidates.append((page_num, url))

    candidates.sort(key=lambda item: item[0])
    for page_num, url in candidates:
        if page_num > current_page:
            return url
    return None
