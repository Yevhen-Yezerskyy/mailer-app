# FILE: engine/crawler/parsers/parser_gs_cb.py  (обновление) 2025-12-14
# Парсер карточки GelbeSeiten: контакты берём из блока mod-Kontaktdaten (единственный),
# вытаскиваем phone/fax/website/socials/address/name/email + branches/description, без HTML.

from __future__ import annotations

import re
from typing import Optional


def _clean(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = " ".join(s.split()).strip()
    return s or None


def _clean_email_mailto(s: Optional[str]) -> Optional[str]:
    s = _clean(s)
    if not s:
        return None
    if s.startswith("mailto:"):
        s = s[len("mailto:") :]
    s = s.split("?", 1)[0].strip()
    return s or None


def _clean_tel(s: Optional[str]) -> Optional[str]:
    s = _clean(s)
    if not s:
        return None
    if s.startswith("tel:"):
        s = s[len("tel:") :].strip()
    return s or None


def parse_gs_cb_detail(response):
    # ---- header (name, branches, email, website) ----
    company_name = _clean(response.css("h1.mod-TeilnehmerKopf__name::text").get())
    if not company_name:
        company_name = _clean(response.css("div.mod-Kontaktdaten .gc-text--h2::text").get())
    if not company_name:
        return None

    branches = [
        _clean(x)
        for x in response.css(
            '.mod-TeilnehmerKopf__branchen span[data-selenium="teilnehmerkopf__branche"]::text'
        ).getall()
        if _clean(x)
    ]

    email = _clean_email_mailto(response.css('#email_versenden::attr(data-link)').get())
    if not email:
        email = _clean_email_mailto(response.css('a[href^="mailto:"]::attr(href)').get())

    description = _clean(
        response.css("section#beschreibung .mod-Beschreibung > div::text").get()
    )

    # ---- Kontaktdaten (THE source of truth) ----
    kd = response.css("div.mod.mod-Kontaktdaten")
    if not kd:
        # fallback: вернём хоть что-то
        return {
            "company_name": company_name,
            "email": email,
            "company_data": {
                "source_url": response.url,
                "branches": branches,
                "description": description,
            },
        }

    # address text: собираем все span в adresse-text
    address_parts = kd.css(".mod-Kontaktdaten__address-container .adresse-text span::text").getall()
    address_text = _clean(" ".join([p.strip() for p in address_parts if p and p.strip()]))

    # phone: сначала явный текст, потом tel:+49..., потом data-suffix
    phone = _clean(kd.css('.contains-icon-big-tel a[href^="tel:"] span::text').get())
    if not phone:
        phone = _clean_tel(kd.css('.contains-icon-big-tel a[href^="tel:"]::attr(href)').get())
    if not phone:
        phone = _clean(kd.css('.contains-icon-big-tel [data-role="telefonnummer"]::attr(data-suffix)').get())

    # fax (если есть)
    fax = _clean(kd.css(".contains-icon-big-fax span::text").get())

    # website (если есть) — именно из homepage блока
    website = _clean(kd.css(".contains-icon-big-homepage a::attr(href)").get())

    # social links (если есть) — любой <a> внутри social-media-iconlist
    socials = []
    for u in kd.css(".mod-Kontaktdaten__social-media-iconlist a::attr(href)").getall():
        u = _clean(u)
        if u:
            socials.append(u)

    # extras: всё что похоже на полезные штуки в data-* (например bahndata)
    bahndata = kd.css(".contains-icon-big-bahnurl button::attr(data-bahndata)").get()
    bahndata = _clean(bahndata)

    # PLZ отдельным полем (для удобства), но address_text тоже оставляем
    plz = None
    if address_text:
        m = re.search(r"\b(\d{5})\b", address_text)
        if m:
            plz = m.group(1)

    return {
        "company_name": company_name,
        "email": email,
        "company_data": {
            "source_url": response.url,
            "branches": branches,
            "address": address_text,
            "plz": plz,
            "phone": phone,
            "fax": fax,
            "website": website,
            "socials": socials,
            "bahndata": bahndata,
            "description": description,
        },
    }
