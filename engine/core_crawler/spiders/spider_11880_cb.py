# FILE: engine/core_crawler/spiders/spider_11880_cb.py
# DATE: 2026-03-29
# PURPOSE: 11880 single-pair runner using the shared browser fetch layer without Scrapy runtime.

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from engine.common.db import fetch_one, get_connection
from engine.common.logs import log
from engine.core_crawler.browser.fetcher import fetch_html, to_text_response
from engine.core_crawler.spiders.spider_11880_card import parse_11880_card
from engine.core_crawler.spiders.spider_11880_index_card import (
    extract_11880_next_page_url,
    parse_11880_index_cards,
)
from engine.core_crawler.spiders.spider_11880_store import save_11880_probe_run


class OneOneEightZeroCBSpider:
    name = "core_11880_cb"

    def __init__(self, task_id: int, cb_id: int, plz: str, branch_slug: str, branch_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_id = int(task_id)
        self.cb_id = int(cb_id)
        self.plz = str(plz or "").strip()
        self.branch_slug = str(branch_slug or "").strip()
        self.branch_name = str(branch_name or "").strip()
        self._start_url: Optional[str] = None
        self._list_seen = 0
        self._detail_seen = 0
        self._detail_parsed = 0
        self._paging_seen = 0
        self.items: List[Dict[str, Any]] = []
        self.index_cards: List[Dict[str, Any]] = []
        self.selected_urls: List[str] = []
        self.failed_urls: List[Dict[str, str]] = []
        self._final_reason = "INIT"
        self._last_tunnel: Dict[str, Any] = {}
        self._detail_referers: Dict[str, str] = {}
        self._db_action: str = "skip"
        self._db_rows: int = 0

    def _remember_index_cards(
        self,
        cards: List[Dict[str, Any]],
        *,
        page_url: str,
        seen_urls: set[str],
        selected_urls: List[str],
    ) -> None:
        for card in cards:
            row = dict(card)
            url = urljoin(page_url, str(row.get("url") or ""))
            plz = str(row.get("plz") or "").strip()
            selected = bool(plz) and plz == self.plz
            row["page_url"] = page_url
            row["selected"] = selected
            row["skip_reason"] = "" if selected else ("PLZ MISMATCH" if plz else "NO PLZ")
            self.index_cards.append(row)
            if not selected or not url or url in seen_urls:
                continue
            seen_urls.add(url)
            selected_urls.append(url)
            self._detail_referers[url] = page_url

    def _run_fetch(self) -> None:
        # self._start_url = f"https://www.11880.com/suche/{self.branch_slug}/{self.plz}"
        self._start_url = f"https://serenity-mail.de/suche/{self.branch_slug}/{self.plz}"
        current_search_url = self._start_url
        current_referer = ""
        selected_urls: List[str] = []
        seen_urls: set[str] = set()

        seen_search_urls: set[str] = set()
        while current_search_url and current_search_url not in seen_search_urls:
            seen_search_urls.add(current_search_url)
            search_result = fetch_html(
                site="11880",
                url=current_search_url,
                kind="search",
                task_id=self.task_id,
                cb_id=self.cb_id,
                referer=current_referer,
                mode="http_only",
            )
            self._last_tunnel = dict(search_result.tunnel)
            if search_result.status not in {200, 404}:
                self._final_reason = f"SEARCH HTTP {search_result.status}"
                return

            self._list_seen += 1
            search_response = to_text_response(search_result)
            parsed_index_cards = parse_11880_index_cards(search_response, self.branch_name)
            if not parsed_index_cards:
                self._final_reason = "NO DETAIL ITEMS"
                return
            self._remember_index_cards(
                parsed_index_cards,
                page_url=search_result.final_url,
                seen_urls=seen_urls,
                selected_urls=selected_urls,
            )

            next_search_url = extract_11880_next_page_url(search_response)
            if not next_search_url or next_search_url in seen_search_urls:
                break
            self._paging_seen += 1
            current_referer = search_result.final_url
            current_search_url = urljoin(search_result.final_url, next_search_url)

        self.selected_urls = selected_urls
        if not self.index_cards:
            self._final_reason = f"FAILED TO PARSE {self._start_url}"
            return

        for detail_url in self.selected_urls:
            self._detail_seen += 1
            detail_result = fetch_html(
                site="11880",
                url=detail_url,
                kind="detail",
                task_id=self.task_id,
                cb_id=self.cb_id,
                referer=self._detail_referers.get(detail_url, self._start_url),
                mode="http_only",
            )
            self._last_tunnel = dict(detail_result.tunnel)
            if detail_result.status != 200:
                self.failed_urls.append({"kind": "detail", "url": detail_url, "reason": f"DETAIL HTTP {detail_result.status}"})
                continue
            detail_response = to_text_response(detail_result)
            card = parse_11880_card(detail_response)
            if not card:
                self.failed_urls.append({"kind": "detail", "url": detail_url, "reason": "FAILED TO PARSE"})
                continue
            self._detail_parsed += 1
            self.items.append(
                {
                    "cb_id": self.cb_id,
                    "url": detail_result.final_url,
                    "card": card,
                }
            )

        self._final_reason = "OK" if self.items else "NO DETAIL ITEMS"

    def run(self) -> None:
        row = fetch_one("SELECT collected FROM cb_crawl_pairs WHERE id=%s", (self.cb_id,))
        if bool(row and row[0] is True):
            self._final_reason = "ALREADY COLLECTED"
        else:
            try:
                self._run_fetch()
            except Exception as exc:
                self._final_reason = f"FETCH EXCEPTION {type(exc).__name__}: {exc}"
                self.failed_urls.append({"kind": "run", "url": self._start_url or "", "reason": self._final_reason})
        self.closed(self._final_reason or "run")

    def _db_flush_items_and_mark(self) -> bool:
        payload = {
            "task_id": self.task_id,
            "cb_id": self.cb_id,
            "plz": self.plz,
            "branch_slug": self.branch_slug,
            "branch_name": self.branch_name,
            "tunnel": self._last_tunnel,
            "start_url": self._start_url,
            "final_reason": self._final_reason,
            "failed_urls": self.failed_urls,
            "index_cards": self.index_cards,
            "selected_urls": self.selected_urls,
            "items": self.items,
        }
        self._db_rows = save_11880_probe_run(payload)
        return True

    @staticmethod
    def _reason_marks_collected(reason: str) -> bool:
        reason_s = str(reason or "").strip()
        if not reason_s:
            return False
        if reason_s in {"OK", "NO DETAIL ITEMS"}:
            return True
        if reason_s.startswith("SEARCH HTTP"):
            return True
        if reason_s.startswith("DETAIL HTTP"):
            return True
        return False

    def _mark_pair_result(self, reason: str) -> None:
        error_value = None if str(reason or "").strip() == "OK" else str(reason or "").strip() or None
        collected_num = int(self._db_rows if self._db_rows >= 0 else len(self.items))
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.cb_crawl_pairs
                SET collected = true,
                    collected_num = %s,
                    error = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (collected_num, error_value, self.cb_id),
            )
            conn.commit()

    def _index_log_line(self, reason: str) -> str:
        if str(reason or "") == "ALREADY COLLECTED":
            return f"cb_id={self.cb_id} index skip already_collected"
        selected = int(len(self.selected_urls))
        indexed = int(len(self.index_cards))
        if indexed <= 0:
            return f"cb_id={self.cb_id} index fail reason={reason}"
        if selected <= 0:
            return f"cb_id={self.cb_id} index mismatch indexed={indexed} selected=0"
        return f"cb_id={self.cb_id} index ok indexed={indexed} selected={selected} paging={int(self._paging_seen)}"

    def _detail_log_line(self) -> str:
        selected = int(len(self.selected_urls))
        detail_seen = int(self._detail_seen)
        detail_parsed = int(self._detail_parsed)
        all_parsed = detail_seen > 0 and detail_seen == detail_parsed
        return (
            f"cb_id={self.cb_id} detail selected={selected} seen={detail_seen} "
            f"parsed={detail_parsed} all_parsed={'yes' if all_parsed else 'no'}"
        )

    def _result_log_line(self, reason: str) -> str:
        ok = str(reason or "") == "OK"
        return f"cb_id={self.cb_id} result {'ok' if ok else 'fail'} reason={reason} items={int(len(self.items))}"

    def _log_status(self, reason: str) -> None:
        log("spider_11880", folder="crawler", message=self._index_log_line(reason))
        log("spider_11880", folder="crawler", message=self._detail_log_line())
        log("spider_11880", folder="crawler", message=self._result_log_line(reason))

    def closed(self, reason):
        r = (self._final_reason or reason or "").strip() or "UNKNOWN"
        self._log_status(r)

        if r == "ALREADY COLLECTED":
            self._db_action = "skip_already"
            print(f"CORE_11880_CB cb_id={self.cb_id} result=skip reason={r}")
            return

        # if not self._reason_marks_collected(r):
        #     self._db_action = "leave_pending"
        #     print(
        #         f"CORE_11880_CB cb_id={self.cb_id} result=pending reason={r} "
        #         f"indexed={len(self.index_cards)} selected={len(self.selected_urls)} parsed={self._detail_parsed}"
        #     )
        #     return
        if not self._start_url:
            self._db_action = "leave_pending"
            print(
                f"CORE_11880_CB cb_id={self.cb_id} result=pending reason={r} "
                f"indexed={len(self.index_cards)} selected={len(self.selected_urls)} parsed={self._detail_parsed}"
            )
            return

        self._db_flush_items_and_mark()
        self._mark_pair_result(r)
        self._db_action = "commit"
        print(
            f"CORE_11880_CB cb_id={self.cb_id} result=commit reason={r} "
            f"stored={self._db_rows} indexed={len(self.index_cards)} selected={len(self.selected_urls)} "
            f"parsed={self._detail_parsed}"
        )
