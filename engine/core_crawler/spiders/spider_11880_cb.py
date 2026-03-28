# FILE: engine/core_crawler/spiders/spider_11880_cb.py
# DATE: 2026-03-27
# PURPOSE: 11880 spider using the shared browser fetch layer.

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import scrapy

from engine.common.db import fetch_one
from engine.common.logs import sys_log
from engine.core_crawler.browser.fetcher import fetch_html, to_text_response
from engine.core_crawler.spiders.spider_11880_card import parse_11880_card
from engine.core_crawler.spiders.spider_11880_index_card import (
    extract_11880_next_page_url,
    parse_11880_index_cards,
)
from engine.core_crawler.spiders.spider_11880_store import save_11880_probe_run


class OneOneEightZeroCBSpider(scrapy.Spider):
    name = "core_11880_cb"

    custom_settings = {
        "LOG_ENABLED": False,
        "ROBOTSTXT_OBEY": False,
    }

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

    def _already_collected(self) -> bool:
        row = fetch_one("SELECT collected FROM cb_crawl_pairs WHERE id=%s", (self.cb_id,))
        return bool(row and row[0] is True)

    @staticmethod
    def _reason_is_fail(reason: str) -> bool:
        r = (reason or "").strip()
        return (
            r.startswith("SEARCH HTTP")
            or r.startswith("DETAIL HTTP")
            or r.startswith("FAILED TO PARSE")
            or r.startswith("FETCH EXCEPTION")
        )

    def _search_url(self) -> str:
        return f"https://www.11880.com/suche/{self.branch_slug}/{self.plz}?query={self.branch_slug}"

    def _run_fetch(self) -> None:
        self._start_url = self._search_url()
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
                mode="index_browser",
            )
            self._last_tunnel = dict(search_result.tunnel)
            if search_result.status != 200:
                self._final_reason = f"SEARCH HTTP {search_result.status}"
                return

            self._list_seen += 1
            search_response = to_text_response(search_result)
            parsed_index_cards = parse_11880_index_cards(search_response, self.branch_name)
            if not parsed_index_cards:
                self._final_reason = f"FAILED TO PARSE {search_result.final_url}"
                return

            for card in parsed_index_cards:
                row = dict(card)
                url = urljoin(search_result.final_url, str(row.get("url") or ""))
                plz = str(row.get("plz") or "").strip()
                selected = bool(plz) and plz == self.plz
                row["page_url"] = search_result.final_url
                row["selected"] = selected
                row["skip_reason"] = "" if selected else ("PLZ MISMATCH" if plz else "NO PLZ")
                self.index_cards.append(row)
                if not selected or not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                selected_urls.append(url)
                self._detail_referers[url] = search_result.final_url

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

    def start_requests(self):
        if self._already_collected():
            self._final_reason = "ALREADY COLLECTED"
            return
        try:
            self._run_fetch()
        except Exception as exc:
            self._final_reason = f"FETCH EXCEPTION {type(exc).__name__}: {exc}"
            self.failed_urls.append({"kind": "run", "url": self._start_url or "", "reason": self._final_reason})
        return
        yield  # pragma: no cover

    def _db_flush_items_and_mark(self) -> bool:
        payload = {
            "event": "11880_payload",
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

    def _log_status(self, reason: str) -> None:
        payload = {
            "event": "11880_status",
            "task_id": self.task_id,
            "cb_id": self.cb_id,
            "plz": self.plz,
            "branch_slug": self.branch_slug,
            "branch_name": self.branch_name,
            "reason": str(reason or ""),
            "list_seen": int(self._list_seen),
            "paging_seen": int(self._paging_seen),
            "detail_seen": int(self._detail_seen),
            "detail_parsed": int(self._detail_parsed),
            "index_cards": int(len(self.index_cards)),
            "selected_urls": int(len(self.selected_urls)),
            "items": int(len(self.items)),
            "failed_urls": self.failed_urls,
            "tunnel": self._last_tunnel,
            "start_url": self._start_url,
        }
        sys_log(
            "spider_11880",
            folder="crawler",
            message=json.dumps(payload, ensure_ascii=False, default=str, indent=2),
        )

    def closed(self, reason):
        r = (self._final_reason or reason or "").strip() or "UNKNOWN"
        self._log_status(r)
        print(
            f"CORE_11880_CB task_id={self.task_id} cb_id={self.cb_id} reason='{r}' "
            f"plz='{self.plz}' branch='{self.branch_slug}' list_seen={self._list_seen} "
            f"paging_seen={self._paging_seen} detail_seen={self._detail_seen} "
            f"detail_parsed={self._detail_parsed} index_cards={len(self.index_cards)} "
            f"selected={len(self.selected_urls)} items={len(self.items)} failed={len(self.failed_urls)}"
        )

        if r == "ALREADY COLLECTED":
            self._db_action = "skip_already"
            return

        if self._reason_is_fail(r):
            self._db_action = "mark_fail"
            return

        ok = self._db_flush_items_and_mark()
        if ok:
            self._db_action = "commit"
