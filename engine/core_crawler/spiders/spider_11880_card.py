# FILE: engine/core_crawler/spiders/spider_11880_card.py
# DATE: 2026-03-27
# PURPOSE: 11880 detail-card parsing and flat card contract for core_crawler.

from __future__ import annotations

import json
import re
from typing import Any

from engine.core_crawler.spiders.spider_helpers import (
    add_many,
    clean_email,
    clean_tel,
    clean_text,
    clean_url,
    dedup_keep_order,
    extract_texts,
    init_card_from_contract,
    set_scalar,
)


CARD_11880_CONTRACT = {
    "required": {
        "company_name": "",
        "email": "",
        "categories_11880": [],
        "statuses_11880": [],
        "plz": "",
        "city": "",
        "district": "",
        "street": "",
        "phones": [""],
    },
    "optional": {
        "address": "",
        "emails": [],
        "fax": [],
        "website": "",
        "websites": [],
        "description": "",
        "keywords_11880": [],
        "json_11880": {},
        "json2_11880": {},
    },
}


def _extract_json_scripts(response) -> list[Any]:
    out: list[Any] = []
    for raw in response.css('script[type="application/ld+json"]::text').getall():
        raw = (raw or "").strip()
        if not raw:
            continue
        try:
            out.append(json.loads(raw))
        except Exception:
            continue
    return out


def _json_types(node: Any) -> list[str]:
    if not isinstance(node, dict):
        return []
    types = node.get("@type")
    if isinstance(types, str):
        return [clean_text(types) or ""]
    if isinstance(types, list):
        return [x for x in [clean_text(str(t)) for t in types] if x]
    return []


def _sanitize_json_11880(node: Any) -> Any:
    banned = {
        "review",
        "mediaGallery",
        "aggregateRating",
        "potentialAction",
        "openingHoursSpecification",
        "geo",
    }
    if isinstance(node, list):
        return [_sanitize_json_11880(x) for x in node]
    if not isinstance(node, dict):
        return node
    out: dict[str, Any] = {}
    for key, value in node.items():
        if key in banned:
            continue
        out[key] = _sanitize_json_11880(value)
    return out


def _extract_trade_feature_categories(response) -> list[str]:
    return dedup_keep_order(
        [
            clean_text(x)
            for x in response.xpath(
                "//section[@id='trade-service-features']"
                "//p[contains(normalize-space(), 'Dieses Unternehmen bietet Dienstleistungen in folgenden Branchen an:')]"
                "/following-sibling::*[contains(@class, 'term-box__panel-content')][1]"
                "//*[contains(@class, 'term-box__panel-col')]/text()"
            ).getall()
            if clean_text(x)
        ]
    )


def _extract_trade_feature_keywords(response) -> list[str]:
    return dedup_keep_order(
        [
            clean_text(x)
            for x in response.xpath(
                "//section[@id='trade-service-features']"
                "//p[contains(normalize-space(), 'Das Unternehmen wird unter folgenden Suchworten gefunden:')]"
                "/following-sibling::*[contains(@class, 'term-box__panel-content')][1]"
                "//*[contains(@class, 'term-box__panel-col')]/text()"
            ).getall()
            if clean_text(x)
        ]
    )


def _extract_top_media_keywords(response, city: str) -> list[str]:
    raw = extract_texts(response.css(".top-media-keywords"))
    raw = clean_text(raw)
    if not raw:
        return []
    raw = re.sub(r"\(\+\d+\s*km\)", "", raw)
    parts = [clean_text(x) for x in re.split(r"\s*,\s*|\s*&\s*", raw)]
    parts = [x for x in parts if x]
    if parts and city:
        parts[-1] = clean_text(re.sub(rf"\s+in\s+{re.escape(city)}\s*$", "", parts[-1], flags=re.I))
    return dedup_keep_order([x for x in parts if x])


def _clean_contact_text(value: str | None) -> str | None:
    value = (value or "").replace("&nbsp;", " ").replace("\xa0", " ")
    return clean_text(value)


def _decode_cf_email(value: str | None) -> str | None:
    value = clean_text(value)
    if not value:
        return None
    try:
        key = int(value[:2], 16)
        decoded = "".join(chr(int(value[i : i + 2], 16) ^ key) for i in range(2, len(value), 2))
    except Exception:
        return None
    return clean_email(decoded)


def _extract_text_emails(text: str | None) -> list[str]:
    if not text:
        return []
    return dedup_keep_order(
        [x for x in [clean_email(m) for m in re.findall(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", text)] if x]
    )


def _extract_text_urls(text: str | None) -> list[str]:
    if not text:
        return []
    matches = re.findall(r"(https?://[^\s,]+|www\.[^\s,]+)", text)
    return dedup_keep_order([x for x in [clean_url(m) for m in matches] if x])


def _extract_contact_number(text: str | None, *, is_fax: bool = False) -> str | None:
    text = _clean_contact_text(text)
    if not text:
        return None
    if is_fax:
        text = re.sub(r"^\s*fax\s*:?\s*", "", text, flags=re.I)
    else:
        text = re.sub(r"^\s*(telefon|tel\.?)\s*:?\s*", "", text, flags=re.I)
    return clean_tel(text)


def _extract_entry_detail_contacts(response) -> dict[str, list[str]]:
    out = {"phones": [], "emails": [], "fax": [], "websites": []}
    for item in response.css(
        ".item-detail-information .entry-detail-list__item, "
        "#kontakt .entry-detail-list__item"
    ):
        item_text = _clean_contact_text(extract_texts(item))
        icon_classes = " ".join(item.css(".entry-detail-list__icon::attr(class)").getall())
        icon_classes = _clean_contact_text(icon_classes) or ""
        hrefs = [_clean_contact_text(x) for x in item.css("a[href]::attr(href)").getall()]
        hrefs = [x for x in hrefs if x]
        label_text = _clean_contact_text(" ".join(item.css(".entry-detail-list__label ::text").getall())) or item_text

        if "entry-detail-list__icon--phone" in icon_classes or "entry-detail-list__icon--cellphone" in icon_classes:
            phone = next(
                (clean_tel(_clean_contact_text(h)) for h in hrefs if h.lower().startswith("tel:")),
                None,
            ) or _extract_contact_number(label_text)
            if phone:
                out["phones"].append(phone)
            continue

        if "entry-detail-list__icon--fax" in icon_classes:
            fax = next(
                (clean_tel(_clean_contact_text(h)) for h in hrefs if h.lower().startswith("tel:")),
                None,
            ) or _extract_contact_number(label_text, is_fax=True)
            if fax:
                out["fax"].append(fax)
            continue

        if "entry-detail-list__icon--email" in icon_classes:
            email_values = [clean_email(h) for h in hrefs if h.lower().startswith("mailto:")]
            email_values = [x for x in email_values if x]
            cf_email = _decode_cf_email(item.css(".__cf_email__::attr(data-cfemail)").get())
            if cf_email:
                email_values.append(cf_email)
            if not email_values and label_text and "[email" not in label_text.lower():
                email_values.extend(_extract_text_emails(label_text))
            if email_values:
                out["emails"].extend(email_values)
            continue

        if "entry-detail-list__icon--website" in icon_classes:
            web_values = [
                clean_url(h)
                for h in hrefs
                if not h.lower().startswith("tel:") and not h.lower().startswith("mailto:")
            ]
            web_values = [x for x in web_values if x]
            if not web_values and label_text:
                web_values.extend(_extract_text_urls(label_text))
            if web_values:
                out["websites"].extend(web_values)
            continue

    return {key: dedup_keep_order([x for x in values if x]) for key, values in out.items()}


def _extract_location_address_and_district(response) -> tuple[str, str]:
    for item in response.css(".item-detail-information .entry-detail-list__item, #kontakt .entry-detail-list__item"):
        icon_classes = " ".join(item.css(".entry-detail-list__icon::attr(class)").getall())
        icon_classes = _clean_contact_text(icon_classes) or ""
        if "entry-detail-list__icon--location" not in icon_classes:
            continue

        label_text = _clean_contact_text(" ".join(item.css(".entry-detail-list__label ::text").getall()))
        item_text = _clean_contact_text(extract_texts(item))
        raw_address = label_text or item_text or ""
        district = ""

        if raw_address:
            match = re.search(r"\(([^)]+)\)\s*$", raw_address)
            if match:
                district = clean_text(match.group(1)) or ""
        return raw_address, district

    return "", ""


def _extract_address_parts_from_entry_address(response) -> tuple[str, str, str, str]:
    for item in response.css(".item-detail-information .entry-detail-list__item, #kontakt .entry-detail-list__item"):
        icon_classes = " ".join(item.css(".entry-detail-list__icon::attr(class)").getall())
        icon_classes = _clean_contact_text(icon_classes) or ""
        if "entry-detail-list__icon--location" not in icon_classes:
            continue

        container = item.css(".entry-detail-list__label > div").xpath("(.)[1]")
        if not container:
            continue

        rows = container.xpath("./div")
        street = ""
        if rows:
            street = clean_text(" ".join(rows[0].xpath(".//text()").getall())) or ""

        address_row = rows[1] if len(rows) > 1 else container
        plz = clean_text(" ".join(address_row.css(".js-postal-code::text, .js-postal-code ::text").getall())) or ""
        city = clean_text(" ".join(address_row.css(".js-address-locality::text, .js-address-locality ::text").getall())) or ""

        district = ""
        row_text = clean_text(" ".join(address_row.xpath(".//text()").getall())) or ""
        if row_text:
            match = re.search(r"\(([^)]+)\)\s*$", row_text)
            if match:
                district = clean_text(match.group(1)) or ""

        return street, plz, city, district

    return "", "", "", ""


def parse_11880_card(response):
    company_name = clean_text(" ".join(response.css("h1 ::text").getall())) or clean_text(response.css("h1::text").get())
    if not company_name:
        return None

    card = init_card_from_contract(CARD_11880_CONTRACT)
    set_scalar(card, "company_name", company_name)
    json_scripts = _extract_json_scripts(response)
    if len(json_scripts) >= 1:
        set_scalar(card, "json_11880", _sanitize_json_11880(json_scripts[0]))
    if len(json_scripts) >= 2:
        second = json_scripts[1]
        second_types = _json_types(second)
        if "FAQPage" not in second_types:
            set_scalar(card, "json2_11880", second)
    entry_contacts = _extract_entry_detail_contacts(response)

    street, plz, city, district_from_entry = _extract_address_parts_from_entry_address(response)

    set_scalar(card, "street", street)
    set_scalar(card, "plz", plz)
    set_scalar(card, "city", city)
    raw_address, district = _extract_location_address_and_district(response)
    set_scalar(card, "district", district or district_from_entry)
    set_scalar(card, "address", raw_address)

    emails = dedup_keep_order(
        list(entry_contacts.get("emails") or [])
        + [x for x in [clean_email(response.css("[itemprop='email']::attr(content)").get())] if x]
    )
    if emails:
        set_scalar(card, "email", emails[0])
        if len(emails) > 1:
            add_many(card, "emails", emails)

    phones = dedup_keep_order(
        list(entry_contacts.get("phones") or [])
    )
    add_many(card, "phones", phones)

    faxes = dedup_keep_order(
        list(entry_contacts.get("fax") or [])
    )
    if faxes:
        add_many(card, "fax", faxes)

    websites = dedup_keep_order(
        list(entry_contacts.get("websites") or [])
        + [x for x in [clean_url(response.css(".tracking--entry-detail-website-link::attr(href)").get())] if x]
        + [x for x in [clean_url(response.css("[itemprop='url']::attr(content)").get())] if x]
    )
    if websites:
        set_scalar(card, "website", websites[0])
        if len(websites) > 1:
            add_many(card, "websites", websites)

    categories = dedup_keep_order(
        [clean_text(x) for x in response.css(".trades-list ::text").getall() if clean_text(x)]
        + _extract_trade_feature_categories(response)
        + [
            clean_text(x)
            for x in response.css(".features li::text, .entry-detail-feature-list li::text, [itemprop='serviceType']::text").getall()
            if clean_text(x)
        ]
    )
    if categories:
        add_many(card, "categories_11880", categories)

    keywords = dedup_keep_order(
        _extract_top_media_keywords(response, city)
        + _extract_trade_feature_keywords(response)
    )
    if keywords:
        add_many(card, "keywords_11880", keywords)

    statuses = dedup_keep_order(
        [
            clean_text(x)
            for x in response.css("#additional-features .additional-feature-label::text").getall()
            if clean_text(x)
        ]
    )
    if statuses:
        add_many(card, "statuses_11880", statuses)

    description = None
    for sel in [
        ".box-entry-detail--about",
        "#ueber-uns .content",
        "[id='about-us'] .content",
        ".entry-detail-about-us .content",
    ]:
        block = response.css(sel)
        if block.get():
            description = extract_texts(block)
            if description:
                break
    set_scalar(card, "description", description)

    return card
