# FILE: engine/core_crawler/spiders/spider_gs_card.py
# DATE: 2026-03-27
# PURPOSE: GelbeSeiten detail-card parsing and flat card contract for core_crawler.

from __future__ import annotations

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

GS_CARD_CONTRACT = {
    "required": {
        "company_name": "",
        "email": "",
        "categories_gs": [],
        "plz": "",
        "city": "",
        "street": "",
        "phones": [""],
    },
    "optional": {
        "address": "",
        "emails": [],
        "fax": [],
        "website": "",
        "websites": [],
        "socials": [],
        "description": "",
        "children": ["https://www.gelbeseiten.de/gsbiz/..."],
    },
}


def _extract_address_parts(response) -> dict[str, str]:
    header = response.css("address.mod-TeilnehmerKopf__adresse")
    if not header.get():
        return {}

    parts = [
        clean_text(x)
        for x in header.css(".mod-TeilnehmerKopf__adresse-daten::text, .mod-TeilnehmerKopf__adresse-daten--noborder::text").getall()
    ]
    parts = [x for x in parts if x]
    out: dict[str, str] = {}

    plz_idx = None
    for idx, part in enumerate(parts):
        if any(ch.isdigit() for ch in part) and len("".join(ch for ch in part if ch.isdigit())) == 5:
            out["plz"] = part.strip()
            plz_idx = idx
            break

    if plz_idx is not None:
        city_idx = plz_idx + 1
        if city_idx < len(parts):
            out["city"] = parts[city_idx].strip()
        street_parts = [parts[idx].rstrip(",").strip() for idx in range(plz_idx) if parts[idx].strip()]
        if street_parts:
            out["street"] = ", ".join(street_parts)
    elif parts:
        out["street"] = parts[0].rstrip(",").strip()
        if len(parts) > 1:
            out["city"] = parts[1].strip()

    return out

def _extract_children(response) -> list[str]:
    urls = response.css("a.mod-WeitereStandorte__list-item::attr(href)").getall()
    urls = [clean_text(x) for x in urls]
    return dedup_keep_order([x for x in urls if x])


def _norm_same_scalar(value: str) -> str:
    value = clean_text(value) or ""
    return " ".join(value.lower().split())


def _pick_same_scalar(*values: str) -> str:
    cleaned = [clean_text(x) for x in values]
    cleaned = [x for x in cleaned if x]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    first_norm = _norm_same_scalar(cleaned[0])
    for value in cleaned[1:]:
        if _norm_same_scalar(value) == first_norm:
            if len(value) > len(cleaned[0]):
                return value
            return cleaned[0]
    return cleaned[0]


def _pick_href_or_text(anchor_sel, value_cleaner):
    href = value_cleaner(anchor_sel.xpath("./@href").get())
    if href:
        return href
    for text in anchor_sel.css("*::text, ::text").getall():
        value = value_cleaner(text)
        if value:
            return value
    return ""


def _pick_href_or_text_many(anchor_sels, value_cleaner) -> list[str]:
    out: list[str] = []
    for anchor in anchor_sels:
        value = _pick_href_or_text(anchor, value_cleaner)
        if value:
            out.append(value)
    return dedup_keep_order(out)


def _extract_header_source(response) -> dict[str, object]:
    source: dict[str, object] = {
        "company_name": "",
        "categories_gs": [],
        "street": "",
        "plz": "",
        "city": "",
        "emails": [],
        "website": "",
    }

    company_name = clean_text(response.css("h1.mod-TeilnehmerKopf__name::text").get())
    if company_name:
        source["company_name"] = company_name

    categories = [
        clean_text(x)
        for x in response.css(
            '.mod-TeilnehmerKopf__branchen span[data-selenium="teilnehmerkopf__branche"]::text'
        ).getall()
        if clean_text(x)
    ]
    source["categories_gs"] = dedup_keep_order(categories)

    addr_parts = _extract_address_parts(response)
    source["street"] = str(addr_parts.get("street") or "")
    source["plz"] = str(addr_parts.get("plz") or "")
    source["city"] = str(addr_parts.get("city") or "")

    email_link = clean_text(response.css('#email_versenden::attr(data-link)').get())
    emails: list[str] = []
    e = clean_email(email_link)
    if e:
        emails.append(e)
    for anchor in response.css('div.aktionsleiste a[href^="mailto:"]'):
        e = _pick_href_or_text(anchor, clean_email)
        if e:
            emails.append(e)
    source["emails"] = dedup_keep_order(emails)

    website = ""
    ws = response.css("div.aktionsleiste a:has(i.icon-homepage)")
    if ws.get():
        website = _pick_href_or_text(ws[0], clean_url)
    if not website:
        for anchor in response.css("div.aktionsleiste a[title]"):
            website = _pick_href_or_text(anchor, clean_url)
            if website:
                break
    source["website"] = website or ""
    return source


def _extract_kd_source(response) -> dict[str, object]:
    source: dict[str, object] = {
        "company_name": "",
        "street": "",
        "plz": "",
        "city": "",
        "address": "",
        "emails": [],
        "phones": [],
        "fax": [],
        "website": "",
        "websites": [],
        "socials": [],
    }

    kd = response.css("div.mod.mod-Kontaktdaten")
    if not kd.get():
        return source

    company_name = clean_text(kd.css(".gc-text--h2::text").get())
    if company_name:
        source["company_name"] = company_name

    kd_addr_sel = kd.css(".mod-Kontaktdaten__address-container .adresse-text")
    kd_address = extract_texts(kd_addr_sel) if kd_addr_sel.get() else None
    if kd_address:
        source["address"] = kd_address

    phones: list[str] = []
    for block in kd.css(".mod-Kontaktdaten__list-item.contains-icon-big-tel"):
        p = ""
        for anchor in block.css("a"):
            p = _pick_href_or_text(anchor, clean_tel)
            if p:
                break
        if not p:
            suffix = block.css('[data-role="telefonnummer"]::attr(data-suffix)').get()
            p = clean_tel(suffix)
        if not p:
            p = clean_tel(extract_texts(block))
        if p:
            phones.append(p)
    source["phones"] = dedup_keep_order(phones)

    faxes: list[str] = []
    for block in kd.css(".mod-Kontaktdaten__list-item.contains-icon-big-fax"):
        fax_clean = ""
        for anchor in block.css("a"):
            fax_clean = _pick_href_or_text(anchor, clean_tel)
            if fax_clean:
                break
        if not fax_clean:
            fax_clean = clean_tel(extract_texts(block)) or clean_text(extract_texts(block))
        if fax_clean:
            faxes.append(fax_clean)
    source["fax"] = dedup_keep_order(faxes)

    websites = _pick_href_or_text_many(
        kd.css(".contains-icon-big-homepage a"),
        clean_url,
    )
    source["websites"] = dedup_keep_order(websites)
    source["website"] = str(source["websites"][0] if source["websites"] else "")

    emails = _pick_href_or_text_many(
        kd.css('a[href^="mailto:"], .contains-icon-big-email a'),
        clean_email,
    )
    source["emails"] = dedup_keep_order(emails)

    socials = _pick_href_or_text_many(
        kd.css(".mod-Kontaktdaten__social-media-iconlist a"),
        clean_url,
    )
    source["socials"] = dedup_keep_order(socials)
    return source


def parse_gs_card(response):
    header = _extract_header_source(response)
    kd = _extract_kd_source(response)

    company_name = _pick_same_scalar(
        str(header.get("company_name") or ""),
        str(kd.get("company_name") or ""),
    )
    if not company_name:
        return None

    card = init_card_from_contract(GS_CARD_CONTRACT)
    set_scalar(card, "company_name", company_name)

    add_many(card, "categories_gs", list(header.get("categories_gs") or []))
    set_scalar(card, "street", str(header.get("street") or ""))
    set_scalar(card, "plz", str(header.get("plz") or ""))
    set_scalar(card, "city", str(header.get("city") or ""))

    description = None
    desc_sel = response.css("section#beschreibung .mod-Beschreibung")
    if desc_sel.get():
        description = extract_texts(desc_sel)

    emails = dedup_keep_order(list(header.get("emails") or []) + list(kd.get("emails") or []))
    phones = dedup_keep_order(list(kd.get("phones") or []))
    faxes = dedup_keep_order(list(kd.get("fax") or []))
    socials = dedup_keep_order(list(kd.get("socials") or []))
    children = _extract_children(response)
    websites = dedup_keep_order(
        [x for x in [str(header.get("website") or "")] if x]
        + list(kd.get("websites") or [])
    )
    website = _pick_same_scalar(
        str(header.get("website") or ""),
        str(kd.get("website") or ""),
    )
    address = clean_text(str(kd.get("address") or ""))

    if emails:
        set_scalar(card, "email", emails[0])
        if len(emails) > 1:
            add_many(card, "emails", emails)
    add_many(card, "phones", phones)
    if faxes:
        add_many(card, "fax", faxes)
    if len(websites) > 1:
        add_many(card, "websites", websites)
    if socials:
        add_many(card, "socials", socials)
    if children:
        add_many(card, "children", children)
    set_scalar(card, "address", address)
    set_scalar(card, "website", website)
    set_scalar(card, "description", description)

    return card
