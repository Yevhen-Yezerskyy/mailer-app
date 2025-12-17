# FILE: engine/crawler/parsers/parser_gs_cb.py  (обновлено — 2025-12-17)
# Fix: добавлен city в company_data — берём всё после 5-значного PLZ до конца address (trim по краям).

from __future__ import annotations

import re
from typing import Optional


def _clean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = " ".join(s.split()).strip()
    return s or None


def _dedup_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _clean_email(s: Optional[str]) -> Optional[str]:
    s = _clean(s)
    if not s:
        return None
    if s.startswith("mailto:"):
        s = s[7:]
    s = s.split("?", 1)[0].strip()
    s = _clean(s)
    if not s:
        return None
    # минимальный sanity filter
    if "@" not in s:
        return None
    if "." not in s.split("@", 1)[-1]:
        return None
    if len(s) < 5:
        return None
    return s


def _clean_tel(s: Optional[str]) -> Optional[str]:
    s = _clean(s)
    if not s:
        return None
    if s.lower().startswith("tel:"):
        s = s[4:].split("?", 1)[0].strip()
    s = _clean(s)
    if not s:
        return None
    # минимальный sanity filter: достаточно цифр
    digits = re.sub(r"\D+", "", s)
    if len(digits) < 6:
        return None
    return s


def _clean_url(s: Optional[str]) -> Optional[str]:
    s = _clean(s)
    if not s:
        return None
    if s.lower().startswith("javascript:"):
        return None
    # допускаем http(s) и "похожее на домен"
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if "." in s and " " not in s:
        return s
    return None


def _extract_texts(selector) -> str | None:
    parts = selector.css("::text").getall()
    parts = [_clean(p) for p in parts]
    parts = [p for p in parts if p]
    return _clean(" ".join(parts))


def parse_gs_cb_detail(response):
    # ---------- company_name (required) ----------
    company_name = _clean(response.css("h1.mod-TeilnehmerKopf__name::text").get())
    if not company_name:
        company_name = _clean(response.css("div.mod-Kontaktdaten .gc-text--h2::text").get())
    if not company_name:
        return None

    # ---------- branches ----------
    branches = [
        _clean(x)
        for x in response.css(
            '.mod-TeilnehmerKopf__branchen span[data-selenium="teilnehmerkopf__branche"]::text'
        ).getall()
        if _clean(x)
    ]
    branches = _dedup_keep_order(branches)

    # ---------- address: header first ----------
    header_addr_sel = response.css("address.mod-TeilnehmerKopf__adresse")
    address = None
    if header_addr_sel.get():
        address = _extract_texts(header_addr_sel)

    # ---------- aktionsleiste (email/website recall) ----------
    emails: list[str] = []

    email_link = _clean(response.css('#email_versenden::attr(data-link)').get())
    e = _clean_email(email_link)
    if e:
        emails.append(e)

    for href in response.css('div.aktionsleiste a[href^="mailto:"]::attr(href)').getall():
        e = _clean_email(href)
        if e:
            emails.append(e)

    website = None
    # пытаемся вытащить "Website" из aktionsleiste
    ws = response.css("div.aktionsleiste a i.icon-homepage")
    if ws.get():
        href = ws.xpath("./ancestor::a[1]/@href").get()
        website = _clean_url(href)
    if not website:
        href = response.css('div.aktionsleiste a[title]::attr(href)').get()
        website = _clean_url(href)

    # ---------- description ----------
    desc_sel = response.css("section#beschreibung .mod-Beschreibung")
    description = _extract_texts(desc_sel) if desc_sel.get() else None

    # ---------- parent flag ----------
    parent = "yes" if response.css("div.mod.mod-WeitereStandorte").get() else "no"

    # ---------- Kontaktdaten (main source, overwrite address / website, collect phones/fax/socials/emails) ----------
    kd = response.css("div.mod.mod-Kontaktdaten")
    if kd.get():
        # overwrite address if found
        kd_addr_sel = kd.css(".mod-Kontaktdaten__address-container .adresse-text")
        kd_address = _extract_texts(kd_addr_sel) if kd_addr_sel.get() else None
        if kd_address:
            address = kd_address

        # phones (0..N)
        phones: list[str] = []
        for block in kd.css(".mod-Kontaktdaten__list-item.contains-icon-big-tel"):
            # href tel:
            href = block.css('a[href^="tel:"]::attr(href)').get()
            p = _clean_tel(href)
            if p:
                phones.append(p)

            # text inside link (span/text)
            for t in block.css("a *::text, a::text").getall():
                p = _clean_tel(t)
                if p:
                    phones.append(p)

            # data-suffix
            suffix = block.css('[data-role="telefonnummer"]::attr(data-suffix)').get()
            p = _clean_tel(suffix)
            if p:
                phones.append(p)

        phones = _dedup_keep_order([p for p in phones if p])

        # fax (string or null)
        fax = None
        fax_sel = kd.css(".mod-Kontaktdaten__list-item.contains-icon-big-fax")
        if fax_sel.get():
            fax = _extract_texts(fax_sel)

        # website (overwrite)
        kd_website = _clean_url(kd.css(".contains-icon-big-homepage a::attr(href)").get())
        if kd_website:
            website = kd_website

        # emails inside kd
        for href in kd.css('a[href^="mailto:"]::attr(href)').getall():
            e = _clean_email(href)
            if e:
                emails.append(e)

        # socials
        socials: list[str] = []
        for u in kd.css(".mod-Kontaktdaten__social-media-iconlist a::attr(href)").getall():
            u = _clean_url(u)
            if u:
                socials.append(u)
        socials = _dedup_keep_order(socials)

    else:
        phones = []
        fax = None
        socials = []

    # ---------- final plz + city ----------
    plz = None
    city = None
    if address:
        m = re.search(r"\b(\d{5})\b", address)
        if m:
            plz = m.group(1)
            tail = address[m.end():]
            tail = tail.lstrip(" ,")
            city = _clean(tail)

    emails = _dedup_keep_order([e for e in emails if e])

    return {
        "company_name": company_name,
        "emails": emails,  # spider развернёт yield'ы и тип company_data.email по правилу
        "company_data": {
            "source_url": response.url,
            "branches": branches,
            "address": address,
            "plz": plz,
            "city": city,
            "phone": phones,
            "email": None,  # заполнит spider по правилу 0/1/2+
            "fax": fax,
            "website": website,
            "socials": socials,
            "description": description,
            "parent": parent,
        },
    }
