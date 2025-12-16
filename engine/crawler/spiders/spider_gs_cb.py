# FILE: engine/crawler/spiders/spider_gs_cb.py  (обновлено) 2025-12-16
# Fix: spider использует parser_gs_cb.parse_gs_cb_detail только для карточек компаний;
# - парсинг списка/пагинации остаётся в пауке
# - yield-логика по email (0/1/2+) реализована в пауке (N yield’ов при 2+)
# - company_data формат фиксированный (address, phone=list, email=string|list|null, fax/socials/parent)
# - plz mismatch abort логика сохранена; collected_num увеличивается 1 раз на карточку (не на yield)

from __future__ import annotations

import re
from urllib.parse import quote, urljoin

import scrapy

from engine.common.db import execute, fetch_one
from engine.crawler.parsers.parser_gs_cb import parse_gs_cb_detail


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
        row = fetch_one("SELECT ___crawler_priority_pick_task_id();")
        if not row or not row[0]:
            print("DEBUG: no task_id from priority picker")
            return

        self.task_id = row[0]
        print(f"DEBUG: picked task_id = {self.task_id}")

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

        yield scrapy.Request(start_url, callback=self.parse_list, meta={"plz": plz})

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

    def _expand_yields_by_email(self, parsed: dict):
        """
        Правило:
        - 0 emails: 1 yield (email=None, company_data.email=None)
        - 1 email : 1 yield (email=str, company_data.email=str)
        - 2+      : N yield (email=str конкретный, company_data.email=[...])
        """
        emails: list[str] = parsed.get("emails") or []
        company_name = parsed["company_name"]
        base_cd: dict = parsed["company_data"]

        # готовим базу company_data (не мутируем исходник)
        if len(emails) == 0:
            cd = dict(base_cd)
            cd["email"] = None
            yield {
                "company_name": company_name,
                "email": None,
                "cb_crawler_id": self.cb_crawler_id,
                "company_data": cd,
            }
            return

        if len(emails) == 1:
            cd = dict(base_cd)
            cd["email"] = emails[0]
            yield {
                "company_name": company_name,
                "email": emails[0],
                "cb_crawler_id": self.cb_crawler_id,
                "company_data": cd,
            }
            return

        # 2+
        cd_list = dict(base_cd)
        cd_list["email"] = list(emails)
        for e in emails:
            yield {
                "company_name": company_name,
                "email": e,
                "cb_crawler_id": self.cb_crawler_id,
                "company_data": cd_list,
            }

    def parse_detail(self, response):
        if self._abort_plz_mismatch:
            return

        req_plz = response.meta["plz"]

        parsed = parse_gs_cb_detail(response)
        if not parsed:
            return

        parsed_plz = parsed.get("company_data", {}).get("plz")

        # если плз не извлечён — не можем валидировать; пропускаем
        if not parsed_plz:
            return

        if parsed_plz != req_plz:
            if self.collected_num == 0:
                self._abort_plz_mismatch = True
                self.crawler.engine.close_spider(self, reason="plz_mismatch_first_detail")
            return

        # плз совпал: считаем 1 карточку (не yield)
        self.collected_num += 1

        # yield'им по email-правилу
        for item in self._expand_yields_by_email(parsed):
            yield item

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
