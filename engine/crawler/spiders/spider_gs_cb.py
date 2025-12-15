# FILE: engine/crawler/spiders/spider_gs_cb.py  (новое — 2025-12-15)
# Смысл: GelbeSeiten spider: атомарно забирает 1 queue_sys (pending→processing), краулит выдачу по /suche/<branch>/<plz>,
# ходит на карточки /gsbiz/*, парсит контактные данные (в т.ч. телефон), пишет item в pipeline,
# в конце ставит queue_sys.status='collected' и обновляет cb_crawler(collected, collected_num, updated_at).

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
    # берём первый “похожий на телефон” кусок
    for raw in candidates:
        s = _clean(raw)
        if not s:
            continue
        # должны быть цифры
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

    # ------------------------------------------------------------

    def start_requests(self):
        # АТОМАРНО:
        # - берём “следующий task_id после последнего collected” (round-robin)
        # - если таких нет — берём минимальный task_id из pending
        # - сразу ставим processing + time=NOW()
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
                  AND (
                        q.task_id > COALESCE((SELECT task_id FROM last), -1)
                      )
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
              AND c.id = q.cb_crawler_id
            RETURNING
                q.id,
                q.cb_crawler_id,
                c.plz,
                c.city_name,
                c.branch_slug
            """
        )

        if not row:
            print("DEBUG: queue empty")
            return

        self.queue_id, self.cb_crawler_id, plz, city, branch = row

        # ВАЖНО: branch в PATH -> quote (НЕ quote_plus)
        branch_q = quote(branch)

        # ЗАРУБИЛИ: работает /suche/<branch>/<plz> (город НЕ нужен)
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
        plz = response.meta["plz"]

        print(
            f"DEBUG: LIST URL = {response.url} status={response.status} len={len(response.text)}"
        )

        # В выдаче ссылки бывают абсолютные, поэтому ловим по contains
        gsbiz_links = response.css('a[href*="/gsbiz/"]::attr(href)').getall()
        gsbiz_links = list(dict.fromkeys(gsbiz_links))

        print(f"DEBUG: found gsbiz = {len(gsbiz_links)}")

        for href in gsbiz_links:
            url = urljoin(response.url, href)
            print(f"DEBUG: -> detail {url}")

            yield scrapy.Request(
                url,
                callback=self.parse_detail,
                meta={"plz": plz},
            )

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
        req_plz = response.meta["plz"]

        # ---------- name ----------
        name = response.css(
            ".mod-TeilnehmerKopf__name::text, .gc-text--h2::text"
        ).get()
        name = _clean(name)

        # ---------- branches ----------
        branches = response.css(
            ".mod-TeilnehmerKopf__branchen span::text"
        ).getall()
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
        # часто второй span выглядит как "50739 Köln" или "70563 Stuttgart-Vaihingen"
        for part in address_parts:
            m = re.search(r"\b(\d{5})\b", part)
            if m:
                parsed_plz = m.group(1)
                break

        # ---------- phone (ВАЖНО) ----------
        # На GS телефон бывает:
        # - span[data-role=telefonnummer] a span::text
        # - span[data-role=telefonnummer] a::text
        # - просто текст внутри a
        phone_candidates = response.css(
            '[data-role="telefonnummer"] a::text, [data-role="telefonnummer"] span::text'
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
