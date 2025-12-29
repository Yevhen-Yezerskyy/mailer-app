# FILE: engine/crawler/spiders/spider_gs_cb.py  (обновлено — 2025-12-28)
# PATH: engine/crawler/spiders/spider_gs_cb.py
# Смысл (КОНТРАКТ, НЕ МЕНЯТЬ):
# 1) Если любая "беда" / невалидная list-страница / любой request упал → В БД НИЧЕГО НЕ ПИШЕМ.
# 2) PLZ mismatch — НЕ беда: считаем обработанным → cb_crawler.collected=true, collected_num=0.
# 3) Если всё в порядке и собрали → БД ОДИН РАЗ: UPSERT items + cb_crawler.collected=true + collected_num, COMMIT.
# 4) URL: /suche/ lowercase. НИКАКОГО quote() для branch_slug (umlaut'ы должны ходить UTF-8 как в браузере).
# 5) Мини-лог всегда печатается (3 строки) на закрытии паука: видно DB SKIP/COMMIT и причину.

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import scrapy

from engine.common.db import get_connection
from engine.crawler.parsers.parser_gs_cb import parse_gs_cb_detail

PLZ_RE = re.compile(r"\b(\d{5})\b")
TREFFER_RE = re.compile(r"\b(\d+)\s*Treffer\b", re.IGNORECASE)


@dataclass(frozen=True)
class _Abort:
    reason: str
    is_bad: bool  # True => "беда" => DB SKIP; False => "нормальный ноль" => mark collected=true,0


class GelbeSeitenCBSpider(scrapy.Spider):
    name = "gs_cb"

    custom_settings = {
        # Scrapy-лог нам не нужен, мы печатаем свой мини-лог (3 строки) в closed()
        "LOG_ENABLED": False,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, plz: str, branch_slug: str, cb_crawler_id: int, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.plz = str(plz or "").strip()
        self.branch_slug = str(branch_slug or "").strip()
        self.cb_crawler_id = int(cb_crawler_id)

        self.collected_num = 0
        self.items: List[Dict[str, Any]] = []

        self._abort: Optional[_Abort] = None
        self._any_request_failed = False

        # list-page sanity
        self._list_seen = False
        self._list_valid = False
        self._list_treffer_num: Optional[int] = None
        self._list_had_results = False
        self._list_plz_set: set[str] = set()

        # detail sanity
        self._detail_seen = 0
        self._detail_parsed_any = 0

        self._start_url: Optional[str] = None

        # DB result
        self._db_action = "skip"  # skip|mark0|commit
        self._db_rows = 0
        self._db_error: Optional[str] = None

    # -------------------- requests --------------------

    def start_requests(self):
        # CONTRACT: no quote() here
        self._start_url = f"https://www.gelbeseiten.de/suche/{self.branch_slug}/{self.plz}"

        yield scrapy.Request(
            self._start_url,
            callback=self.parse_list,
            errback=self._errback,
            dont_filter=True,
        )

    def _errback(self, failure):
        # CONTRACT: любой request упал => беда => DB SKIP
        self._any_request_failed = True
        if self._abort is None:
            req = getattr(failure, "request", None)
            url = req.url if req else "<?>"
            self._abort = _Abort(reason=f"request_failed:{url}", is_bad=True)
        self.crawler.engine.close_spider(self, reason="request_failed")

    # -------------------- list helpers --------------------

    def _mark_list_validity(self, response) -> None:
        self._list_seen = True

        # Treffer => валидная страница поиска
        m = TREFFER_RE.search(response.text or "")
        if m:
            self._list_valid = True
            try:
                self._list_treffer_num = int(m.group(1))
            except Exception:
                self._list_treffer_num = None

        # gsbiz links => тоже валидность
        gsbiz_links = response.css('a[href*="/gsbiz/"]::attr(href)').getall()
        if gsbiz_links:
            self._list_valid = True

        # явный плохой редирект
        u = (response.url or "").lower()
        if "<keine eingabe gemacht>" in u or "keine%20eingabe%20gemacht" in u:
            self._list_valid = False

    def _extract_list_plz_set(self, response) -> set[str]:
        texts = response.css("span.mod-AdresseKompakt__adress__ort::text").getall()
        out: set[str] = set()
        for t in texts:
            mm = PLZ_RE.search(t or "")
            if mm:
                out.add(mm.group(1))
        return out

    # -------------------- parse list --------------------

    def parse_list(self, response):
        if self._abort is not None:
            return

        self._mark_list_validity(response)

        # links
        links = response.css('a[href*="/gsbiz/"]::attr(href)').getall()
        seen = set()
        gsbiz_links: List[str] = []
        for x in links:
            if x in seen:
                continue
            seen.add(x)
            gsbiz_links.append(x)

        if gsbiz_links:
            self._list_had_results = True

        # Case: валидный 0 Treffer (и карточек нет) => НЕ беда => mark0
        if self._list_valid and self._list_treffer_num == 0 and not gsbiz_links:
            self._abort = _Abort(reason="zero_treffer", is_bad=False)
            self.crawler.engine.close_spider(self, reason="zero_treffer")
            return

        # Case: карточки есть, но на list нет нужного PLZ => НЕ беда => mark0
        if self._list_valid and gsbiz_links:
            self._list_plz_set |= self._extract_list_plz_set(response)
            if self._list_plz_set and (self.plz not in self._list_plz_set):
                self._abort = _Abort(reason="plz_mismatch_list_page", is_bad=False)
                self.crawler.engine.close_spider(self, reason="plz_mismatch_list_page")
                return

        # Если list невалидная и карточек нет и next нет => беда
        next_href = response.css('a.pagination__next::attr(href)').get()
        if (not self._list_valid) and (not gsbiz_links) and (not next_href):
            self._abort = _Abort(reason="list_invalid", is_bad=True)
            self.crawler.engine.close_spider(self, reason="list_invalid")
            return

        # детали
        for href in gsbiz_links:
            if self._abort is not None:
                return
            url = urljoin(response.url, href)
            yield scrapy.Request(
                url,
                callback=self.parse_detail,
                errback=self._errback,
                dont_filter=True,
            )

        # next
        if next_href and self._abort is None:
            next_url = urljoin(response.url, next_href)
            yield scrapy.Request(
                next_url,
                callback=self.parse_list,
                errback=self._errback,
                dont_filter=True,
            )

    # -------------------- parse detail --------------------

    def parse_detail(self, response):
        if self._abort is not None:
            return

        self._detail_seen += 1

        parsed = parse_gs_cb_detail(response)
        if not parsed:
            return

        self._detail_parsed_any += 1

        company_name = parsed.get("company_name") or "<?>"
        company_data = parsed.get("company_data") or {}
        parsed_plz = company_data.get("plz")

        if not parsed_plz:
            return

        # PLZ mismatch на detail — НЕ беда
        if str(parsed_plz) != self.plz:
            return

        # OK
        self.collected_num += 1

        emails = parsed.get("emails") or []

        if not emails:
            cd = dict(company_data)
            cd["email"] = None
            self.items.append(
                {
                    "cb_crawler_id": self.cb_crawler_id,
                    "company_name": company_name,
                    "email": None,
                    "company_data": cd,
                }
            )
            return

        if len(emails) == 1:
            cd = dict(company_data)
            cd["email"] = emails[0]
            self.items.append(
                {
                    "cb_crawler_id": self.cb_crawler_id,
                    "company_name": company_name,
                    "email": emails[0],
                    "company_data": cd,
                }
            )
            return

        cd_list = dict(company_data)
        cd_list["email"] = list(emails)
        for e in emails:
            self.items.append(
                {
                    "cb_crawler_id": self.cb_crawler_id,
                    "company_name": company_name,
                    "email": e,
                    "company_data": cd_list,
                }
            )

    # -------------------- close + DB --------------------

    def closed(self, reason):
        # 3 строки всегда
        print(f"GS_CB[{self.cb_crawler_id}] GO {self._start_url}")
        print(
            f"GS_CB[{self.cb_crawler_id}] END reason={reason} abort={self._abort.reason if self._abort else '-'} "
            f"any_fail={self._any_request_failed} list_seen={self._list_seen} list_valid={self._list_valid} "
            f"treffer={self._list_treffer_num} list_results={self._list_had_results} "
            f"detail_seen={self._detail_seen} detail_parsed_any={self._detail_parsed_any} "
            f"collected_num={self.collected_num} items={len(self.items)}"
        )

        # CONTRACT: любой request упал => DB SKIP
        if self._any_request_failed:
            print(f"GS_CB[{self.cb_crawler_id}] DB SKIP (request_failed)")
            return

        # CONTRACT: если list не видели/невалидная => DB SKIP
        if not self._list_seen:
            print(f"GS_CB[{self.cb_crawler_id}] DB SKIP (no_list)")
            return
        if not self._list_valid:
            print(f"GS_CB[{self.cb_crawler_id}] DB SKIP (list_invalid)")
            return

        # CONTRACT: если abort bad => DB SKIP
        if self._abort is not None and self._abort.is_bad:
            print(f"GS_CB[{self.cb_crawler_id}] DB SKIP (abort_bad={self._abort.reason})")
            return

        # CONTRACT: если list имел результаты, но мы не распарсили ни одной detail вообще => беда => DB SKIP
        if self._list_had_results and self._detail_seen > 0 and self._detail_parsed_any == 0:
            print(f"GS_CB[{self.cb_crawler_id}] DB SKIP (details_unparsed)")
            return

        # Нормальные завершения:
        if self.collected_num == 0:
            ok = self._db_mark_collected_zero()
            if ok:
                print(f"GS_CB[{self.cb_crawler_id}] DB MARK0 OK collected_num=0")
            else:
                print(f"GS_CB[{self.cb_crawler_id}] DB MARK0 ERROR {self._db_error or ''}".rstrip())
            return

        ok = self._db_flush_commit()
        if ok:
            print(f"GS_CB[{self.cb_crawler_id}] DB COMMIT OK rows={self._db_rows} collected_num={self.collected_num}")
        else:
            print(f"GS_CB[{self.cb_crawler_id}] DB COMMIT ERROR {self._db_error or ''}".rstrip())

    def _db_mark_collected_zero(self) -> bool:
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

            self._db_action = "mark0"
            self._db_rows = 0
            return True

        except Exception as e:
            self._db_error = repr(e)
            try:
                conn.rollback()
            except Exception:
                pass
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _db_flush_commit(self) -> bool:
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

            try:
                cur.close()
            except Exception:
                pass

            self._db_action = "commit"
            self._db_rows = written
            return True

        except Exception as e:
            self._db_error = repr(e)
            try:
                conn.rollback()
            except Exception:
                pass
            return False
        finally:
            try:
                conn.close()
            except Exception:
                pass
