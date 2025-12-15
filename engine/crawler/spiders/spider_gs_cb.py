# FILE: engine/crawler/spiders/spider_gs_cb.py  (обновлено) 2025-12-15
# Fix: (1) защита от двойного захвата queue_sys (UPDATE только если status='pending' + cb_crawler.collected=false)
# Fix: (2) телефон берется и из href="tel:..."
# Fix: (3) если первый detail дал PLZ mismatch (collected_num==0) — останавливаем паука (дальше 100% тоже мимо)

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

        self.queue_id: int | None = None
        self.cb_crawler_id: int | None = None
        self.collected_num: int = 0

        # если первый detail не совпал по PLZ — дальше смысла нет
        self._abort_plz_mismatch: bool = False

    # ------------------------------------------------------------

    def start_requests(self):
        row = fetch_one(
            """
            WITH last AS (
                SELECT task_id
                FROM queue_sys
                WHERE status = 'collected' AND time IS NOT NULL
                ORDER BY time DESC
                LIMIT 1
            ),
            pick AS (
                SELECT q.id
                FROM queue_sys q
                JOIN cb_crawler c ON c.id = q.cb_crawler_id
                WHERE q.status = 'pending'
                  AND c.collected = false
                  AND (q.task_id > COALESCE((SELECT task_id FROM last), -1))
                ORDER BY q.task_id, q.rate, q.id
                LIMIT 1
            ),
            pick2 AS (
                SELECT q.id
                FROM queue_sys q
                JOIN cb_crawler c ON c.id = q.cb_crawler_id
                WHERE q.status = 'pending'
                  AND c.collected = false
                ORDER BY q.task_id, q.rate, q.id
                LIMIT 1
            ),
            chosen AS (
                SELECT id FROM pick
                UNION ALL
                SELECT id FROM pick2
                LIMIT 1
            )
            UPDATE queue_sys q
            SET status = 'processing',
                time   = NOW()
            FROM cb_crawler c
            WHERE q.id = (SELECT id FROM chosen)
              AND q.status = 'pending'      -- FIX #1
              AND c.id = q.cb_crawler_id
              AND c.collected = false       -- FIX #1
            RETURNING
                q.id,
                q.cb_crawler_id,
                c.plz,
                c.city_name,
                c.branch_slug
            """
        )

        if not row:
            print("DEBUG: queue empty or already taken")
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
            print("DEBUG: aborted by PLZ mismatch — skip list parsing")
            return

        plz = response.meta["plz"]

        print(
            f"DEBUG: LIST URL = {response.url} status={response.status} len={len(response.text)}"
        )

        gsbiz_links = response.css('a[href*="/gsbiz/"]::attr(href)').getall()
        gsbiz_links = list(dict.fromkeys(gsbiz_links))

        print(f"DEBUG: found gsbiz = {len(gsbiz_links)}")

        for href in gsbiz_links:
            if self._abort_plz_mismatch:
                print("DEBUG: aborted by PLZ mismatch — stop scheduling details")
                return

            url = urljoin(response.url, href)
            print(f"DEBUG: -> detail {url}")

            yield scrapy.Request(
                url,
                callback=self.parse_detail,
                meta={"plz": plz},
            )

        if self._abort_plz_mismatch:
            print("DEBUG: aborted by PLZ mismatch — no next page")
            return

        next_href = response.css('a.pagination__next::attr(href)').get()
        if next_href:
            next_url = urljoin(response.url, next_href)
            print(f"DEBUG: NEXT PAGE {next_url}")
            yield scrapy.Request(
                next_url,
                callback=self.parse_list,
                meta={"plz": plz},
            )
        else:
            print("DEBUG: no next page")

    # ------------------------------------------------------------

    def parse_detail(self, response):
        if self._abort_plz_mismatch:
            print("DEBUG: aborted by PLZ mismatch — skip detail")
            return

        req_plz = response.meta["plz"]

        # ---------- name ----------
        name = response.css(
            ".mod-TeilnehmerKopf__name::text, .gc-text--h2::text"
        ).get()
        name = _clean(name)

        # ---------- branches ----------
        branches = response.css(".mod-TeilnehmerKopf__branchen span::text").getall()
        branches = [_clean(b) for b in branches]
        branches = [b for b in branches if b]

        # ---------- address + plz ----------
        address_parts = response.css(
            ".mod-Kontaktdaten__address-container .adresse-text span::text"
        ).getall()
        address_parts = [_clean(p) for p in address_parts]
        address_parts = [p for p in address_parts if p]
        address_text = _clean(" ".join(address_parts)) or ""

        parsed_plz = None
        for part in address_parts:
            m = re.search(r"\b(\d{5})\b", part)
            if m:
                parsed_plz = m.group(1)
                break

        # ---------- phone (FIX #2) ----------
        phone_candidates = response.css(
            '[data-role="telefonnummer"] a::attr(href), '
            '[data-role="telefonnummer"] a::text, '
            '[data-role="telefonnummer"] span::text'
        ).getall()
        phone = _pick_phone(phone_candidates)

        # ---------- email ----------
        email = response.css('#email_versenden::attr(data-link)').get()
        email = _clean(email)
        if email and email.startswith("mailto:"):
            email = email.replace("mailto:", "", 1).split("?")[0]
            email = _clean(email)

        # ---------- website ----------
        website = response.css('.contains-icon-big-homepage a::attr(href)').get()
        website = _clean(website)

        # ---------- description ----------
        description = response.css("#beschreibung .mod-Beschreibung div::text").get()
        description = _clean(description)

        data = {
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

        print("DEBUG: PARSED DATA:")
        print(json.dumps(data, ensure_ascii=False, indent=2))

        if not parsed_plz:
            print("DEBUG: skip — no PLZ in address")
            return

        if parsed_plz != req_plz:
            print(f"DEBUG: skip — PLZ mismatch parsed={parsed_plz} req={req_plz}")

            # FIX #3: если первый же detail не совпал — дальше смысла нет
            if self.collected_num == 0:
                self._abort_plz_mismatch = True
                print("DEBUG: abort spider — first detail PLZ mismatch => all next will mismatch")
                self.crawler.engine.close_spider(self, reason="plz_mismatch_first_detail")

            return

        self.collected_num += 1
        yield data

    # ------------------------------------------------------------

    def closed(self, reason):
        if not self.queue_id or not self.cb_crawler_id:
            print("DEBUG: spider closed with no queue item")
            return

        execute(
            """
            UPDATE queue_sys
            SET status='collected', time=NOW()
            WHERE id=%s
            """,
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
            f"DEBUG: FINISH OK cb_crawler_id={self.cb_crawler_id} collected_num={self.collected_num} reason={reason}"
        )
