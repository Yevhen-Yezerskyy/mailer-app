# FILE: engine/crawler/spiders/spider_gs_cb.py  (обновлено — 2025-12-30)
# PATH: engine/crawler/spiders/spider_gs_cb.py
# Смысл:
# - gs_cb: без _Abort; БД пишем ТОЛЬКО в closed().
# - Любой request-fail => close_spider("REQUEST FAILED <url>").
# - parse_list: FAILED TO PARSE / FAILED TO LOCATE GSBIS / PLZ MISMATCH; далее yield gsbiz + paging.
# - parse_detail: если парсер не понял страницу => close_spider("FAILED TO PARSE <current_url>").
# - closed():
#   1) REQUEST FAILED / FAILED TO PARSE / FAILED TO LOCATE GSBIS => cb_crawler.collected=true, collected_num=0, reason=<reason>
#   2) PLZ MISMATCH => cb_crawler.collected=true, collected_num=0, reason=NULL
#   3) иначе: пишем raw_contacts_gb + cb_crawler.collected=true, collected_num=<n>, reason=NULL
# - Дебаг: только в closed(), по веткам (без json-dump всей пачки).

from __future__ import annotations

import json
import pickle
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import scrapy

from engine.common.db import get_connection
from engine.common.cache.client import CLIENT
from engine.crawler.parsers.parser_gs_cb import parse_gs_cb_detail

PLZ_RE = re.compile(r"\b(\d{5})\b")
TREFFER_RE = re.compile(r"\b(\d+)\s*Treffer\b", re.IGNORECASE)


class GelbeSeitenCBSpider(scrapy.Spider):
    name = "gs_cb"

    custom_settings = {
        "LOG_ENABLED": False,
        "ROBOTSTXT_OBEY": False,
    }

    def __init__(self, plz: str, branch_slug: str, cb_crawler_id: int, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.plz = str(plz or "").strip()
        self.branch_slug = str(branch_slug or "").strip()
        self.cb_crawler_id = int(cb_crawler_id)
        # side-channel: task_id приходит не аргументом, а из кеша
        self.task_id = 0
        payload = CLIENT.get(f"cbq:cb2task:{self.cb_crawler_id}", ttl_sec=3600)
        if payload:
            try:
                v = pickle.loads(payload)
                if isinstance(v, int):
                    self.task_id = int(v)
            except Exception:
                pass

        self._start_url: Optional[str] = None

        # counters / state
        self._list_seen = 0
        self._detail_seen = 0
        self._detail_parsed = 0
        self._paging_seen = 0

        # parsed payloads
        self.items: List[Dict[str, Any]] = []

        # DB debug
        self._db_action: str = "skip"  # skip|mark_fail|mark_mismatch|commit
        self._db_rows: int = 0

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
        req = getattr(failure, "request", None)
        url = req.url if req else "<?>"
        self.crawler.engine.close_spider(self, reason=f"REQUEST FAILED {url}")

    # -------------------- helpers --------------------

    def _extract_list_plz_set(self, response) -> set[str]:
        texts = response.css("span.mod-AdresseKompakt__adress__ort::text").getall()
        out: set[str] = set()
        for t in texts:
            mm = PLZ_RE.search(t or "")
            if mm:
                out.add(mm.group(1))
        return out

    def _looks_unparseable(self, response, gsbiz_links: List[str], next_href: Optional[str]) -> bool:
        """
        "Ничего не видим" => FAILED TO PARSE.
        Считаем страницу мусорной, если нет ни Treffer, ни gsbiz, ни next, ни адресных строк.
        """
        has_treffer = bool(TREFFER_RE.search(response.text or ""))
        has_addr = bool(response.css("span.mod-AdresseKompakt__adress__ort::text").get())
        has_any_signal = has_treffer or bool(gsbiz_links) or bool(next_href) or has_addr
        return not has_any_signal

    @staticmethod
    def _reason_is_fail(reason: str) -> bool:
        r = (reason or "").strip()
        return (
            r.startswith("REQUEST FAILED")
            or r.startswith("FAILED TO PARSE")
            or r.startswith("FAILED TO LOCATE GSBIS")
        )

    # -------------------- parse list --------------------

    def parse_list(self, response):
        self._list_seen += 1

        links = response.css('a[href*="/gsbiz/"]::attr(href)').getall()
        seen = set()
        gsbiz_links: List[str] = []
        for x in links:
            if not x or x in seen:
                continue
            seen.add(x)
            gsbiz_links.append(x)

        next_href = response.css('a.pagination__next::attr(href)').get()

        # 1) FAILED TO PARSE
        if self._looks_unparseable(response, gsbiz_links, next_href):
            self.crawler.engine.close_spider(self, reason=f"FAILED TO PARSE {response.url}")
            return

        # 2) FAILED TO LOCATE GSBIS + current url
        if not gsbiz_links:
            self.crawler.engine.close_spider(self, reason=f"FAILED TO LOCATE GSBIS {response.url}")
            return

        # 3) PLZ MISMATCH (прогнозируемое поведение)
        plz_set = self._extract_list_plz_set(response)
        if plz_set and (self.plz not in plz_set):
            self.crawler.engine.close_spider(self, reason="PLZ MISMATCH")
            return

        # details
        for href in gsbiz_links:
            url = urljoin(response.url, href)
            yield scrapy.Request(
                url,
                callback=self.parse_detail,
                errback=self._errback,
                dont_filter=True,
            )

        # paging
        if next_href:
            self._paging_seen += 1
            next_url = urljoin(response.url, next_href)
            yield scrapy.Request(
                next_url,
                callback=self.parse_list,
                errback=self._errback,
                dont_filter=True,
            )

    # -------------------- parse detail --------------------

    def parse_detail(self, response):
        self._detail_seen += 1

        parsed = parse_gs_cb_detail(response)
        if not parsed:
            self.crawler.engine.close_spider(self, reason=f"FAILED TO PARSE {response.url}")
            return
        print(f"PARSING GBIS {response.url}")
        self._detail_parsed += 1
        self.items.append(
            {
                "cb_crawler_id": self.cb_crawler_id,
                "url": response.url,
                "parsed": parsed,
            }
        )

    # -------------------- DB helpers --------------------

    def _db_flush_items_and_mark(self) -> bool:
        """
        Пишем raw_contacts_gb (upsert по (cb_crawler_id, company_name)),
        потом отмечаем cb_crawler collected=true, collected_num=<n>, reason=NULL.
        """
        conn = get_connection()
        written = 0
        collected_num = 0
        try:
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

            with conn.cursor() as cur:
                for it in self.items:
                    parsed = it.get("parsed") or {}
                    company_name = parsed.get("company_name") or "<?>"
                    company_data = parsed.get("company_data") or {}

                    emails = parsed.get("emails") or []
                    collected_num += 1  # один gsbiz-результат = 1 collected

                    if not emails:
                        cd = dict(company_data)
                        cd["email"] = None
                        cur.execute(
                            upsert_sql,
                            (
                                self.cb_crawler_id,
                                company_name,
                                None,
                                json.dumps(cd, ensure_ascii=False),
                            ),
                        )
                        written += 1
                        continue

                    if len(emails) == 1:
                        cd = dict(company_data)
                        cd["email"] = emails[0]
                        cur.execute(
                            upsert_sql,
                            (
                                self.cb_crawler_id,
                                company_name,
                                emails[0],
                                json.dumps(cd, ensure_ascii=False),
                            ),
                        )
                        written += 1
                        continue

                    cd_list = dict(company_data)
                    cd_list["email"] = list(emails)
                    for e in emails:
                        cur.execute(
                            upsert_sql,
                            (
                                self.cb_crawler_id,
                                company_name,
                                e,
                                json.dumps(cd_list, ensure_ascii=False),
                            ),
                        )
                        written += 1

                cur.execute(
                    """
                    UPDATE cb_crawler
                    SET collected=true,
                        collected_num=%s,
                        reason=NULL,
                        updated_at=NOW()
                    WHERE id=%s
                    """,
                    (int(collected_num), self.cb_crawler_id),
                )

            conn.commit()
            self._db_rows = written
            return True
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # -------------------- close --------------------

    def closed(self, reason):
        r = (reason or "").strip()

        print(
            f"_____________________task_id={self.task_id} _____________________________________ "
            f"GS_CB[{self.cb_crawler_id}] END reason='{r}' start_url='{self._start_url}' "
            f"plz='{self.plz}' branch='{self.branch_slug}' "
            f"list_seen={self._list_seen} paging_seen={self._paging_seen} "
            f"detail_seen={self._detail_seen} detail_parsed={self._detail_parsed} items={len(self.items)}"
        )

        # 1) FAIL reasons => collected=true, collected_num=0, reason=<reason>
        if self._reason_is_fail(r):
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cb_crawler
                    SET collected=true,
                        collected_num=0,
                        reason=%s,
                        updated_at=NOW()
                    WHERE id=%s
                    """,
                    (r, self.cb_crawler_id),
                )
            conn.commit()
            conn.close()

            self._db_action = "mark_fail"
            print(f"GS_CB[{self.cb_crawler_id}] DB MARK_FAIL collected=true collected_num=0 reason='{r}'")
            return

        # 2) PLZ MISMATCH => collected=true, collected_num=0, reason=NULL
        if r == "PLZ MISMATCH":
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cb_crawler
                    SET collected=true,
                        collected_num=0,
                        reason=NULL,
                        updated_at=NOW()
                    WHERE id=%s
                    """,
                    (self.cb_crawler_id,),
                )
            conn.commit()
            conn.close()

            self._db_action = "mark_mismatch"
            print(f"GS_CB[{self.cb_crawler_id}] DB MARK_MISMATCH collected=true collected_num=0 reason=NULL")
            return

        # 3) Success path => write items + mark collected
        ok = self._db_flush_items_and_mark()
        if ok:
            self._db_action = "commit"
            print(
                f"GS_CB[{self.cb_crawler_id}] DB COMMIT OK rows_written={self._db_rows} "
                f"collected_num={len(self.items)} reason=NULL"
            )
        else:
            # сюда не должны попадать: _db_flush_items_and_mark пусть падает исключением
            print(f"GS_CB[{self.cb_crawler_id}] DB COMMIT FAILED (unexpected false)")

        if self.items:
            sample = self.items[0].get("parsed") or {}
            cname = sample.get("company_name") or "<?>"
            emails = sample.get("emails") or []
            print(f"GS_CB[{self.cb_crawler_id}] SAMPLE company='{cname}' emails={len(emails)}")
