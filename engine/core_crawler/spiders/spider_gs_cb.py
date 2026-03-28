# FILE: engine/core_crawler/spiders/spider_gs_cb.py
# DATE: 2026-03-27
# PURPOSE: GelbeSeiten spider using the shared browser fetch layer.

from __future__ import annotations

import re
import json
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
import scrapy
from scrapy.http import TextResponse

from engine.common.db import fetch_one
from engine.common.logs import sys_log
from engine.core_crawler.browser.fetcher import fetch_html, to_text_response
from engine.core_crawler.spiders.spider_gs_card import parse_gs_card
from engine.core_crawler.spiders.spider_helpers import clean_text
from engine.core_crawler.spiders.spider_gs_index_card import parse_gs_index_card
from engine.core_crawler.spiders.spider_gs_store import save_gs_probe_run

TREFFER_RE = re.compile(r"\b(\d+)\s*Treffer\b", re.IGNORECASE)
SPELL_SUGGEST_RE = re.compile(r"rechtschreibvorschl(?:a|ä)ge", re.IGNORECASE)
LOG_FILE = "spider_gs"
LOG_FOLDER = "crawler"


class GelbeSeitenCBSpider(scrapy.Spider):
    name = "core_gs_cb"

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
        self._db_action: str = "skip"
        self._db_rows: int = 0
        self._final_reason = "INIT"
        self._last_tunnel: Dict[str, Any] = {}
        self._detail_referers: Dict[str, str] = {}
        self.items: List[Dict[str, Any]] = []
        self.index_cards: List[Dict[str, Any]] = []
        self.selected_urls: List[str] = []
        self.failed_urls: List[Dict[str, str]] = []

    def _already_collected(self) -> bool:
        row = fetch_one("SELECT collected FROM cb_crawl_pairs WHERE id=%s", (self.cb_id,))
        return bool(row and row[0] is True)

    def _looks_unparseable(self, response, index_cards: List[Dict[str, str]], next_href: str) -> bool:
        has_treffer = bool(TREFFER_RE.search(response.text or ""))
        has_addr = bool(response.css("span.mod-AdresseKompakt__adress__ort::text").get())
        has_any_signal = has_treffer or bool(index_cards) or bool(next_href) or has_addr
        return not has_any_signal

    def _has_spell_suggestion(self, response) -> bool:
        return bool(SPELL_SUGGEST_RE.search(response.text or ""))

    @staticmethod
    def _reason_is_fail(reason: str) -> bool:
        r = (reason or "").strip()
        return (
            r.startswith("SEARCH HTTP")
            or r.startswith("DETAIL HTTP")
            or r.startswith("FAILED TO PARSE")
            or r.startswith("FAILED TO LOCATE GSBIS")
            or r.startswith("SPELL SUGGESTION")
            or r.startswith("FETCH EXCEPTION")
        )

    def _search_url(self) -> str:
        return f"https://www.gelbeseiten.de/suche/{self.branch_slug}/{self.plz}"

    @staticmethod
    def _extract_load_more_form(response) -> dict[str, Any]:
        form = response.css("#mod-LoadMore")
        if not form.get():
            return {}

        action = str(form.attrib.get("action") or "").strip()
        if not action:
            return {}

        fields: dict[str, str] = {}
        for node in form.css("input[name], select[name], textarea[name]"):
            name = str(node.attrib.get("name") or "").strip()
            if not name:
                continue
            value = str(node.attrib.get("value") or "").strip()
            fields[name] = value

        shown = int(clean_text(response.css("#loadMoreGezeigteAnzahl::text").get()) or "0")
        total = int(clean_text(response.css("#loadMoreGesamtzahl::text").get()) or "0")
        amount = int(str(fields.get("anzahl") or "0") or "0")

        if shown <= 0 or total <= shown or amount <= 0:
            return {}

        return {
            "action": action,
            "fields": fields,
            "shown": shown,
            "total": total,
            "amount": amount,
        }

    @staticmethod
    def _selector_from_ajax_html(url: str, html: str) -> TextResponse:
        response = TextResponse(
            url=str(url or ""),
            body=str(html or "").encode("utf-8", errors="ignore"),
            encoding="utf-8",
        )
        response.status = 200
        return response

    @staticmethod
    def _post_ajaxsuche(
        *,
        action_url: str,
        fields: dict[str, str],
        referer: str,
        tunnel: dict[str, Any],
    ) -> dict[str, Any]:
        session = requests.Session()
        session.trust_env = False
        proxy_server = str((tunnel or {}).get("proxy_server") or "")
        if proxy_server:
            session.proxies.update({"http": proxy_server, "https": proxy_server})
        response = session.post(
            action_url,
            data=fields,
            headers={
                "User-Agent": "Mozilla/5.0",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": str(referer or ""),
                "Accept": "application/json, text/javascript, */*; q=0.01",
            },
            timeout=30,
        )
        response.raise_for_status()
        return dict(response.json() or {})

    def _run_fetch(self) -> None:
        self._start_url = self._search_url()
        page_url = self._start_url
        page_referer = ""
        page_guard = 0
        seen_page_urls: set[str] = set()
        seen_detail_urls: set[str] = set()

        while page_url and page_url not in seen_page_urls:
            seen_page_urls.add(page_url)
            page_guard += 1
            search_result = fetch_html(
                site="gs",
                url=page_url,
                kind="search",
                task_id=self.task_id,
                cb_id=self.cb_id,
                referer=page_referer,
                mode="index_browser",
            )
            self._last_tunnel = dict(search_result.tunnel)
            if search_result.status != 200:
                self._final_reason = f"SEARCH HTTP {search_result.status}"
                return

            self._list_seen += 1
            response = to_text_response(search_result)
            seen_index_urls: set[str] = set()

            parsed_index_cards: List[Dict[str, str]] = []
            for card_sel in response.css("article.mod.mod-Treffer"):
                card = parse_gs_index_card(card_sel)
                if not card:
                    continue
                url = str(card.get("url") or "")
                if not url or url in seen_index_urls:
                    continue
                seen_index_urls.add(url)
                parsed_index_cards.append(card)

            next_href = str(response.css('a.pagination__next::attr(href)').get() or "").strip()
            load_more = self._extract_load_more_form(response)

            if self._has_spell_suggestion(response):
                self._final_reason = "SPELL SUGGESTION"
                return

            if self._looks_unparseable(response, parsed_index_cards, next_href or str(load_more.get("action") or "")):
                self._final_reason = f"FAILED TO PARSE {search_result.final_url}"
                return

            if not parsed_index_cards:
                self._final_reason = f"FAILED TO LOCATE GSBIS {search_result.final_url}"
                return

            for index_card in parsed_index_cards:
                row = dict(index_card)
                url = urljoin(search_result.final_url, str(row.get("url") or ""))
                index_plz = str(row.get("plz") or "").strip()
                selected = bool(index_plz) and index_plz == self.plz
                row["page_url"] = search_result.final_url
                row["selected"] = selected
                row["skip_reason"] = "" if selected else ("PLZ MISMATCH" if index_plz else "NO PLZ")
                self.index_cards.append(row)
                if not selected or not url or url in seen_detail_urls:
                    continue
                seen_detail_urls.add(url)
                self.selected_urls.append(url)
                self._detail_referers[url] = search_result.final_url

            while load_more:
                action_url = urljoin(search_result.final_url, str(load_more.get("action") or ""))
                fields = dict(load_more.get("fields") or {})
                if not action_url or not fields:
                    break

                ajax_payload = self._post_ajaxsuche(
                    action_url=action_url,
                    fields=fields,
                    referer=search_result.final_url,
                    tunnel=search_result.tunnel,
                )
                ajax_html = str(ajax_payload.get("html") or "")
                if not ajax_html:
                    break

                ajax_response = self._selector_from_ajax_html(search_result.final_url, ajax_html)
                ajax_cards: List[Dict[str, str]] = []
                seen_ajax_urls: set[str] = set()
                for card_sel in ajax_response.css("article.mod.mod-Treffer"):
                    card = parse_gs_index_card(card_sel)
                    if not card:
                        continue
                    url = str(card.get("url") or "")
                    if not url or url in seen_ajax_urls:
                        continue
                    seen_ajax_urls.add(url)
                    ajax_cards.append(card)

                if not ajax_cards:
                    break

                self._paging_seen += 1
                self._list_seen += 1

                for index_card in ajax_cards:
                    row = dict(index_card)
                    url = urljoin(search_result.final_url, str(row.get("url") or ""))
                    index_plz = str(row.get("plz") or "").strip()
                    selected = bool(index_plz) and index_plz == self.plz
                    row["page_url"] = search_result.final_url
                    row["selected"] = selected
                    row["skip_reason"] = "" if selected else ("PLZ MISMATCH" if index_plz else "NO PLZ")
                    self.index_cards.append(row)
                    if not selected or not url or url in seen_detail_urls:
                        continue
                    seen_detail_urls.add(url)
                    self.selected_urls.append(url)
                    self._detail_referers[url] = search_result.final_url

                shown = int(load_more.get("shown") or 0) + int(ajax_payload.get("anzahlTreffer") or 0)
                total = int(ajax_payload.get("gesamtanzahlTreffer") or load_more.get("total") or 0)
                amount = int(load_more.get("amount") or 0)
                if shown >= total or amount <= 0:
                    break

                next_fields = dict(fields)
                next_fields["position"] = str(shown + 1)
                load_more = {
                    "action": action_url,
                    "fields": next_fields,
                    "shown": shown,
                    "total": total,
                    "amount": amount,
                }

            if not next_href:
                break
            self._paging_seen += 1
            page_referer = search_result.final_url
            page_url = urljoin(search_result.final_url, next_href)
            if page_guard >= 25:
                break

        for detail_url in self.selected_urls:
            self._detail_seen += 1
            detail_result = fetch_html(
                site="gs",
                url=detail_url,
                kind="detail",
                task_id=self.task_id,
                cb_id=self.cb_id,
                referer=self._detail_referers.get(detail_url, self._start_url or ""),
                mode="http_only",
            )
            self._last_tunnel = dict(detail_result.tunnel)
            if detail_result.status != 200:
                self.failed_urls.append({"kind": "detail", "url": detail_url, "reason": f"DETAIL HTTP {detail_result.status}"})
                continue

            detail_response = to_text_response(detail_result)
            card = parse_gs_card(detail_response)
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
            "event": "gs_payload",
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
        self._db_rows = save_gs_probe_run(payload)
        return True

    def _log_status(self, reason: str) -> None:
        payload = {
            "event": "gs_status",
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
            LOG_FILE,
            folder=LOG_FOLDER,
            message=json.dumps(payload, ensure_ascii=False, default=str, indent=2),
        )

    def closed(self, reason):
        r = (self._final_reason or reason or "").strip() or "UNKNOWN"
        self._log_status(r)
        print(
            f"CORE_GS_CB task_id={self.task_id} cb_id={self.cb_id} reason='{r}' "
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
