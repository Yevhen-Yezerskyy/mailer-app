# FILE: engine/crawler/spiders/spider_gs_cb.py  (новое) 2025-12-15
# CB spider:
# - task_id берём через ___crawler_priority_pick_task_id()
# - из queue_sys атомарно забираем 1 pending для этого task_id
# - FOR UPDATE SKIP LOCKED
# - остальная логика без изменений

from __future__ import annotations

import json
import re
from urllib.parse import quote, urljoin

import scrapy

from engine.common.db import fetch_one, execute


def _clean(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def _pick_phone(candidates: list[str]) -> str | None:
    for raw in candidates:
        s = _clean(raw)
        if not s:
            continue

        if s.lower().startswith("tel:"):
            s = s[4:].split("?", 1)[0].strip()
            s = _clean(s)
            if not s:
                continue

        if re.search(r"\d", s):
            return s
    return None


class GelbeSeitenCBSpider(scrapy.Spider):
    name = "gs_cb"

    custom_settings = {
        "LOG_ENABLED": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.task_id: int | None = None
        self.queue_id: int | None = None
        self.cb_crawler_id: int | None = None
        self.collected_num: int = 0

        self._abort_plz_mismatch: bool = False

    # ------------------------------------------------------------

    def start_requests(self):
        # 1) берём task_id по приоритету
        row = fetch_one("SELECT ___crawler_priority_pick_task_id();")
        if not row or not row[0]:
            print("DEBUG: no task_id from priority picker")
            return

        self.task_id = row[0]
        print(f"DEBUG: picked task_id = {self.task_id}")

        # 2) атомарно забираем 1 queue_sys для этого task_id
        row = fetch_one(
            """
            UPDATE queue_sys q
            SET status = 'processing',
                time   = NOW()
            FROM cb_crawler c
            WHERE q.id = (
                SELECT q2.id
                FROM queue_sys q2
                JOIN cb_crawler c2 ON c2.id = q2.cb_crawler_id
                WHERE q2.status = 'pending'
                  AND q2.task_id = %s
                  AND c2.collected = false
                ORDER BY q2.rate, q2.id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
              AND q.status = 'pending'
              AND c.id = q.cb_crawler_id
              AND c.collected = false
            RETURNING
                q.id,
                q.cb_crawler_id,
                c.plz,
                c.city_name,
                c.branch_slug
            """,
            (self.task_id,),
        )

        if not row:
            print("DEBUG: no pending queue item for task_id")
            return

        self.queue_id, self.cb_crawler_id, plz, city, branch = row

        branch_q = quote(branch)
        start_url = f"https://www.gelbeseiten.de/suche/{branch_q}/{plz}"

        print(f"DEBUG: queue_id = {self.queue_id}")
        print(f"DEBUG: cb_crawler_id = {self.cb_crawler_id}")
        print(f"DEBUG: plz = {plz}")
        print(f"DEBUG: city = {city}")
        print(f"DEBUG: branch_slug = {branch}")
        print(f"DEBUG: start_url = {start_url}")

        yield scrapy.Request(
            start_url,
            callback=self.parse_list,
            meta={"plz": plz},
        )

    # ------------------------------------------------------------

    def parse_list(self, response):
        if self._abort_plz_mismatch:
            return

        plz = response.meta["plz"]

        gsbiz_links = response.css('a[href*="/gsbiz/"]::attr(href)').getall()
        gsbiz_links = list(dict.fromkeys(gsbiz_links))

        for href in gsbiz_links:
            if self._abort_plz_mismatch:
                return

            yield scrapy.Request(
                urljoin(response.url, href),
                callback=self.parse_detail,
                meta={"plz": plz},
            )

        next_href = response.css('a.pagination__next::attr(href)').get()
        if next_href and not self._abort_plz_mismatch:
            yield scrapy.Request(
                urljoin(response.url, next_href),
                callback=self.parse_list,
                meta={"plz": plz},
            )

    # ------------------------------------------------------------

    def parse_detail(self, response):
        if self._abort_plz_mismatch:
            return

        req_plz = response.meta["plz"]

        name = _clean(
            response.css(".mod-TeilnehmerKopf__name::text, .gc-text--h2::text").get()
        )

        branches = [
            _clean(b)
            for b in response.css(".mod-TeilnehmerKopf__branchen span::text").getall()
            if _clean(b)
        ]

        address_parts = [
            _clean(p)
            for p in response.css(
                ".mod-Kontaktdaten__address-container .adresse-text span::text"
            ).getall()
            if _clean(p)
        ]
        address_text = _clean(" ".join(address_parts)) or ""

        parsed_plz = None
        for part in address_parts:
            m = re.search(r"\b(\d{5})\b", part)
            if m:
                parsed_plz = m.group(1)
                break

        phone = _pick_phone(
            response.css(
                '[data-role="telefonnummer"] a::attr(href), '
                '[data-role="telefonnummer"] a::text, '
                '[data-role="telefonnummer"] span::text'
            ).getall()
        )

        email = _clean(response.css('#email_versenden::attr(data-link)').get())
        if email and email.startswith("mailto:"):
            email = _clean(email.replace("mailto:", "", 1).split("?")[0])

        website = _clean(
            response.css('.contains-icon-big-homepage a::attr(href)').get()
        )

        description = _clean(
            response.css("#beschreibung .mod-Beschreibung div::text").get()
        )

        if not parsed_plz:
            return

        if parsed_plz != req_plz:
            if self.collected_num == 0:
                self._abort_plz_mismatch = True
                self.crawler.engine.close_spider(
                    self, reason="plz_mismatch_first_detail"
                )
            return

        self.collected_num += 1

        yield {
            "company_name": name,
            "email": email,
            "cb_crawler_id": self.cb_crawler_id,
            "company_data": {
                "source_url": response.url,
                "branches": branches,
                "address_text": address_text,
                "plz": parsed_plz,
                "phone": phone,
                "website": website,
                "description": description,
            },
        }

    # ------------------------------------------------------------

    def closed(self, reason):
        if not self.queue_id or not self.cb_crawler_id:
            return

        execute(
            "UPDATE queue_sys SET status='collected', time=NOW() WHERE id=%s",
            (self.queue_id,),
        )

        execute(
            """
            UPDATE cb_crawler
            SET collected=true,
                collected_num=%s,
                updated_at=NOW()
            WHERE id=%s
            """,
            (self.collected_num, self.cb_crawler_id),
        )

        print(
            f"DEBUG: FINISH OK task_id={self.task_id} cb_crawler_id={self.cb_crawler_id} collected_num={self.collected_num} reason={reason}"
        )
