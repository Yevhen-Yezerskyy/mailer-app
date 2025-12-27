# FILE: engine/crawler/spiders/spider_gs_cb.py  (обновлено — 2025-12-27)
# Смысл (правки):
# - Если ЛЮБОЙ request (в т.ч. detail-карточка) упал в errback → abort + close_spider → БД НЕ трогаем.
# - Если на list-странице есть карточки и среди их PLZ НЕТ запрошенного → это "completed пусто":
#   сразу помечаем cb_crawler.collected=true, collected_num=0 отдельной короткой транзакцией и закрываем паука.
# - Основной commit (raw_contacts_gb + cb_crawler.collected/collected_num) остаётся ТОЛЬКО при reason=="finished" и без abort.

from __future__ import annotations

import json
import re
from urllib.parse import quote, urljoin

import scrapy

from engine.common.db import get_connection
from engine.crawler.parsers.parser_gs_cb import parse_gs_cb_detail


DEBUG = True  # <-- переключай на False

PLZ_RE = re.compile(r"\b(\d{5})\b")


class GelbeSeitenCBSpider(scrapy.Spider):
    name = "gs_cb"

    custom_settings = {
        "LOG_ENABLED": False,
    }

    def __init__(self, plz: str, branch_slug: str, cb_crawler_id: int, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.plz = str(plz)
        self.branch_slug = str(branch_slug)
        self.cb_crawler_id = int(cb_crawler_id)

        self.collected_num = 0  # карточек detail с совпавшим PLZ
        self.items: list[dict] = []  # items для UPSERT в конце

        self._abort = False
        self._abort_reason: str | None = None

        # если хоть один HTTP-запрос упал — БД не трогаем вообще
        self._any_request_failed = False

        # summary для DEBUG=False (ровно 3 строки)
        self._start_url: str | None = None
        self._db_written = False
        self._db_written_rows = 0

        # доп. счётчики
        self._companies_ok = 0
        self._companies_skip = 0

    # ------------------------------------------------------------

    def _p(self, msg: str):
        if DEBUG:
            print(f"GS_CB[{self.cb_crawler_id}] {msg}")

    def _p3(self):
        # ровно 3 строки
        print(f"GS_CB[{self.cb_crawler_id}] GO {self._start_url}")
        print(
            f"GS_CB[{self.cb_crawler_id}] FOUND companies_ok={self._companies_ok} items={len(self.items)} "
            f"abort={self._abort} reason={self._abort_reason or '-'} any_fail={self._any_request_failed}"
        )
        if self._db_written:
            print(
                f"GS_CB[{self.cb_crawler_id}] DB COMMIT rows={self._db_written_rows} collected_num={self.collected_num}"
            )
        else:
            print(
                f"GS_CB[{self.cb_crawler_id}] DB SKIP (no commit) collected_num={self.collected_num}"
            )

    # ------------------------------------------------------------

    def start_requests(self):
        self._start_url = f"https://www.gelbeseiten.de/suche/{quote(self.branch_slug)}/{self.plz}"

        if DEBUG:
            self._p("START")
            self._p(f"plz={self.plz} branch_slug={self.branch_slug}")
            self._p(f"start_url={self._start_url}")

        yield scrapy.Request(
            self._start_url,
            callback=self.parse_list,
            errback=self._errback,
            dont_filter=True,
        )

    def _errback(self, failure):
        if self._abort:
            return

        self._any_request_failed = True
        self._abort = True
        self._abort_reason = "request_failed"

        req = getattr(failure, "request", None)
        url = req.url if req else "<?>"
        if DEBUG:
            self._p(f"ABORT {self._abort_reason} url={url} err={failure!r}")

        self.crawler.engine.close_spider(self, reason=self._abort_reason)

    # ------------------------------------------------------------

    def _extract_list_plz_set(self, response) -> set[str]:
        texts = response.css("span.mod-AdresseKompakt__adress__ort::text").getall()
        out: set[str] = set()
        for t in texts:
            m = PLZ_RE.search(t or "")
            if m:
                out.add(m.group(1))
        return out

    def _mark_cb_crawler_collected_zero(self) -> bool:
        """
        Отдельная короткая транзакция:
        used-case: list-page содержит карточки, но PLZ не совпадает (значит для этой связки реально пусто).
        """
        conn = get_connection()
        try:
            try:
                conn.autocommit = False
            except Exception:
                pass

            cur = conn.cursor()
            cur.execute(
                """
                UPDATE cb_crawler
                SET collected=true,
                    collected_num=0,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (self.cb_crawler_id,),
            )
            conn.commit()

            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

            if DEBUG:
                self._p("DB MARK collected=true collected_num=0 (plz_mismatch_list_page)")

            return True

        except Exception as e:
            if DEBUG:
                self._p(f"DB MARK ERROR -> ROLLBACK err={e!r}")
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            return False

    def parse_list(self, response):
        if self._abort:
            return

        if DEBUG:
            self._p(f"LIST OK status={response.status} bytes={len(response.body)} url={response.url}")

        # ранний guard: если карточки есть и PLZ среди них нет — это “пусто”, помечаем collected и выходим
        list_plz_set = self._extract_list_plz_set(response)
        if list_plz_set and self.plz not in list_plz_set:
            self._abort = True
            self._abort_reason = "plz_mismatch_list_page"
            if DEBUG:
                self._p(f"ABORT {self._abort_reason} req_plz={self.plz} list_plz={sorted(list_plz_set)}")

            # ВАЖНО: это считаем "completed пусто" → ставим collected=true сразу
            self._mark_cb_crawler_collected_zero()

            self.crawler.engine.close_spider(self, reason=self._abort_reason)
            return

        gsbiz_links = response.css('a[href*="/gsbiz/"]::attr(href)').getall()
        gsbiz_links = list(dict.fromkeys(gsbiz_links))

        if DEBUG:
            self._p(f"LIST gsbiz_links={len(gsbiz_links)}")

        for href in gsbiz_links:
            if self._abort:
                return
            url = urljoin(response.url, href)
            if DEBUG:
                self._p(f"GET {url}")
            yield scrapy.Request(
                url,
                callback=self.parse_detail,
                errback=self._errback,
                dont_filter=True,
            )

        next_href = response.css('a.pagination__next::attr(href)').get()
        if next_href and not self._abort:
            next_url = urljoin(response.url, next_href)
            if DEBUG:
                self._p(f"LIST next -> {next_url}")
            yield scrapy.Request(
                next_url,
                callback=self.parse_list,
                errback=self._errback,
                dont_filter=True,
            )
        else:
            if DEBUG:
                self._p("LIST no next page")

    # ------------------------------------------------------------

    def _buffer_item(self, company_name: str, email, company_data: dict):
        self.items.append(
            {
                "cb_crawler_id": self.cb_crawler_id,
                "company_name": company_name,
                "email": email,
                "company_data": company_data,
            }
        )

    def parse_detail(self, response):
        if self._abort:
            return

        if DEBUG:
            self._p(f"DETAIL OK status={response.status} bytes={len(response.body)} url={response.url}")

        parsed = parse_gs_cb_detail(response)
        if not parsed:
            self._companies_skip += 1
            if DEBUG:
                self._p("DETAIL skip parsed=None")
            return

        company_name = parsed.get("company_name") or "<?>"
        company_data = parsed.get("company_data") or {}
        parsed_plz = company_data.get("plz")

        if not parsed_plz:
            self._companies_skip += 1
            if DEBUG:
                self._p(f"DETAIL skip no_plz company={company_name}")
            return

        if parsed_plz != self.plz:
            self._companies_skip += 1
            if DEBUG:
                self._p(f"DETAIL skip plz_mismatch req={self.plz} got={parsed_plz} company={company_name}")
            return

        # валидная карточка
        self.collected_num += 1
        self._companies_ok += 1

        emails = parsed.get("emails") or []

        if not emails:
            cd = dict(company_data)
            cd["email"] = None
            self._buffer_item(company_name, None, cd)
            if DEBUG:
                self._p(f"COMPANY OK name={company_name} emails=0 -> items+1 total_items={len(self.items)}")
            return

        if len(emails) == 1:
            cd = dict(company_data)
            cd["email"] = emails[0]
            self._buffer_item(company_name, emails[0], cd)
            if DEBUG:
                self._p(f"COMPANY OK name={company_name} emails=1 -> items+1 total_items={len(self.items)}")
            return

        cd_list = dict(company_data)
        cd_list["email"] = list(emails)
        for e in emails:
            self._buffer_item(company_name, e, cd_list)

        if DEBUG:
            self._p(
                f"COMPANY OK name={company_name} emails={len(emails)} -> items+{len(emails)} total_items={len(self.items)}"
            )

    # ------------------------------------------------------------

    def _db_flush_commit(self) -> tuple[bool, int]:
        conn = get_connection()
        written = 0

        try:
            try:
                conn.autocommit = False
            except Exception:
                pass

            cur = conn.cursor()

            upsert_sql = """
                INSERT INTO raw_contacts_gb
                    (cb_crawler_id, company_name, email, company_data, created_at, updated_at)
                VALUES
                    (%s, %s, %s, %s::jsonb, now(), now())
                ON CONFLICT (cb_crawler_id, company_name)
                DO UPDATE SET
                    email = EXCLUDED.email,
                    company_data = EXCLUDED.company_data,
                    updated_at = now()
            """

            total = len(self.items)
            if DEBUG:
                self._p(f"DB BEGIN total_items={total}")

            for item in self.items:
                cur.execute(
                    upsert_sql,
                    (
                        item["cb_crawler_id"],
                        item["company_name"],
                        item.get("email"),
                        json.dumps(item.get("company_data", {}), ensure_ascii=False),
                    ),
                )
                written += 1

            cur.execute(
                """
                UPDATE cb_crawler
                SET collected=true,
                    collected_num=%s,
                    updated_at=NOW()
                WHERE id=%s
                """,
                (self.collected_num, self.cb_crawler_id),
            )

            conn.commit()

            self._db_written = True
            self._db_written_rows = written

            if DEBUG:
                self._p(f"DB COMMIT OK rows={written} collected_num={self.collected_num}")

            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

            return True, written

        except Exception as e:
            if DEBUG:
                self._p(f"DB ERROR -> ROLLBACK err={e!r}")
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            self._db_written = False
            self._db_written_rows = 0
            return False, 0

    def closed(self, reason):
        # если был хоть один request fail — гарантированно ничего не пишем (кроме спец-кейса plz_mismatch, который уже помечен отдельно)
        if self._any_request_failed:
            if DEBUG:
                self._p(
                    f"FINISH reason={reason} any_fail=true -> DB SKIP "
                    f"companies_ok={self._companies_ok} items={len(self.items)}"
                )
            if not DEBUG:
                self._p3()
            return

        # COMMIT только при нормальном завершении и без abort
        if reason != "finished" or self._abort:
            if DEBUG:
                self._p(
                    f"FINISH reason={reason} abort={self._abort} abort_reason={self._abort_reason} "
                    f"companies_ok={self._companies_ok} items={len(self.items)} -> DB SKIP"
                )
            if not DEBUG:
                self._p3()
            return

        ok, written = self._db_flush_commit()
        if DEBUG:
            self._p(
                f"FINISH reason={reason} companies_ok={self._companies_ok} items={len(self.items)} "
                f"-> DB {'OK' if ok else 'FAIL'} rows={written}"
            )

        if not DEBUG:
            self._p3()
