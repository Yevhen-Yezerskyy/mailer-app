# FILE: engine/core_crawler/spiders/spider_gs_index_card.py
# DATE: 2026-03-27
# PURPOSE: Parse one GelbeSeiten company card from a list/search result page.

from __future__ import annotations

import re

from engine.core_crawler.spiders.spider_helpers import clean_tel, clean_text, clean_url


PLZ_RE = re.compile(r"\b(\d{5})\b")


def parse_gs_index_card(card_sel) -> dict[str, str] | None:
    url = clean_url(card_sel.css('a[href*="/gsbiz/"]::attr(href)').get())
    if not url:
        return None

    company_name = clean_text(card_sel.css("h2.mod-Treffer__name::text").get()) or ""
    category = clean_text(card_sel.css("p.mod-Treffer--besteBranche::text").get()) or ""

    street = clean_text(card_sel.css(".mod-AdresseKompakt__adress-text::text").get()) or ""
    street = street.rstrip(",").strip()

    ort = clean_text(card_sel.css("span.mod-AdresseKompakt__adress__ort::text").get()) or ""
    district = clean_text(card_sel.css(".mod-AdresseKompakt__adress-text::text").re_first(r"\(([^)]+)\)")) or ""

    plz = ""
    city = ""
    mm = PLZ_RE.search(ort)
    if mm:
        plz = mm.group(1)
        city = clean_text(ort.replace(plz, "", 1)) or ""

    address = ""
    if street and ort:
        address = clean_text(f"{street}, {ort}") or ""
    elif ort:
        address = ort
    elif street:
        address = street

    phone = clean_tel(card_sel.css(".mod-TelefonnummerKompakt__phoneNumber::text").get()) or ""
    website = clean_url(card_sel.css(".mod-WebseiteKompakt a::attr(href)").get()) or ""

    return {
        "url": url,
        "company_name": company_name,
        "category_gs": category,
        "street": street,
        "plz": plz,
        "city": city,
        "district": district,
        "address": address,
        "phone": phone,
        "website": website,
    }
