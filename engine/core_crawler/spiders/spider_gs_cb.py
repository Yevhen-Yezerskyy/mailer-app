# FILE: engine/core_crawler/spiders/spider_gs_cb.py
# DATE: 2026-03-29
# PURPOSE: GelbeSeiten single-pair runner using the shared browser fetch layer without Scrapy runtime.

from __future__ import annotations

import concurrent.futures
import re
import json
import random
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from engine.common.db import fetch_one, get_connection
from engine.common.logs import log
from engine.core_crawler.browser.fetcher import build_text_response, fetch_html, to_text_response
from engine.core_crawler.browser.session_config import SITE_CONFIGS
from engine.core_crawler.spiders.spider_gs_card import parse_gs_card
from engine.core_crawler.spiders.spider_helpers import clean_text
from engine.core_crawler.spiders.spider_gs_index_card import parse_gs_index_card
from engine.core_crawler.spiders.spider_gs_store import save_gs_probe_run

TREFFER_RE = re.compile(r"\b(\d+)\s*Treffer\b", re.IGNORECASE)
SPELL_SUGGEST_RE = re.compile(r"rechtschreibvorschl(?:a|ä)ge", re.IGNORECASE)
LOG_FILE = "spider_gs"
LOG_FOLDER = "crawler"


class GelbeSeitenCBSpider:
    name = "core_gs_cb"

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
        self._detail_routes: Dict[str, Dict[str, Any]] = {}
        self.items: List[Dict[str, Any]] = []
        self.index_cards: List[Dict[str, Any]] = []
        self.selected_urls: List[str] = []
        self.failed_urls: List[Dict[str, str]] = []

    def _run_detail_fetches(self) -> None:
        if not self.selected_urls:
            self._detail_seen = 0
            return
        cfg = SITE_CONFIGS["gs"]
        self._detail_seen = int(len(self.selected_urls))
        max_workers = max(1, min(int(cfg.concurrent_pages_per_session), int(len(self.selected_urls))))

        def _fetch_one(detail_url: str) -> dict[str, Any]:
            pause_sec = random.uniform(float(cfg.pause_min_sec), float(cfg.pause_max_sec))
            if pause_sec > 0:
                time.sleep(pause_sec)
            route = dict(self._detail_routes.get(detail_url) or {})
            detail_result = fetch_html(
                site="gs",
                url=detail_url,
                kind="detail",
                task_id=self.task_id,
                cb_id=self.cb_id,
                referer=str(route.get("referer") or self._start_url or ""),
                mode="http_only",
                preferred_slot_name=str(route.get("slot_name") or ""),
                preferred_slot_idx=-1 if route.get("slot_idx") in (None, "") else int(route.get("slot_idx")),
            )
            reason = ""
            card = None
            if detail_result.status != 200:
                reason = f"DETAIL HTTP {detail_result.status}"
            else:
                detail_response = to_text_response(detail_result)
                card = parse_gs_card(detail_response)
                if not card:
                    reason = "FAILED TO PARSE"
            return {
                "url": detail_url,
                "result": detail_result,
                "reason": reason,
                "card": card,
            }

        first_exc: Exception | None = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="gs_detail") as executor:
            futures = [executor.submit(_fetch_one, detail_url) for detail_url in self.selected_urls]
            for future in concurrent.futures.as_completed(futures):
                try:
                    row = future.result()
                except Exception as exc:
                    if first_exc is None:
                        first_exc = exc
                    continue
                detail_result = row["result"]
                detail_url = str(row["url"] or "")
                self._last_tunnel = dict(detail_result.tunnel)
                reason = str(row["reason"] or "")
                if reason:
                    self.failed_urls.append({"kind": "detail", "url": detail_url, "reason": reason})
                    continue
                self._detail_parsed += 1
                self.items.append(
                    {
                        "cb_id": self.cb_id,
                        "url": detail_result.final_url,
                        "card": row["card"],
                    }
                )
        if first_exc is not None:
            raise first_exc

    @staticmethod
    def _parse_index_cards(response) -> List[Dict[str, str]]:
        parsed_index_cards: List[Dict[str, str]] = []
        seen_index_urls: set[str] = set()
        for card_sel in response.css("article.mod.mod-Treffer"):
            card = parse_gs_index_card(card_sel)
            if not card:
                continue
            url = str(card.get("url") or "")
            if not url or url in seen_index_urls:
                continue
            seen_index_urls.add(url)
            parsed_index_cards.append(card)
        return parsed_index_cards

    def _remember_index_cards(
        self,
        cards: List[Dict[str, str]],
        *,
        page_url: str,
        slot_name: str,
        slot_idx: int,
        seen_detail_urls: set[str],
    ) -> None:
        for index_card in cards:
            row = dict(index_card)
            url = urljoin(page_url, str(row.get("url") or ""))
            index_plz = str(row.get("plz") or "").strip()
            selected = bool(index_plz) and index_plz == self.plz
            row["page_url"] = page_url
            row["selected"] = selected
            row["skip_reason"] = "" if selected else ("PLZ MISMATCH" if index_plz else "NO PLZ")
            self.index_cards.append(row)
            if not selected or not url or url in seen_detail_urls:
                continue
            seen_detail_urls.add(url)
            self.selected_urls.append(url)
            self._detail_routes[url] = {
                "referer": page_url,
                "slot_name": str(slot_name or ""),
                "slot_idx": int(slot_idx),
            }

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
    def _post_ajaxsuche(
        *,
        task_id: int,
        cb_id: int,
        action_url: str,
        fields: dict[str, str],
        referer: str,
        preferred_slot_name: str,
        preferred_slot_idx: int,
    ) -> dict[str, Any]:
        ajax_result = fetch_html(
            site="gs",
            url=action_url,
            kind="search_ajax",
            task_id=int(task_id),
            cb_id=int(cb_id),
            referer=str(referer or ""),
            mode="http_only",
            method="POST",
            form=dict(fields or {}),
            extra_headers={
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            },
            preferred_slot_name=str(preferred_slot_name or ""),
            preferred_slot_idx=int(preferred_slot_idx),
        )
        if ajax_result.status != 200:
            raise RuntimeError(f"AJAX HTTP {ajax_result.status}")
        try:
            return dict(json.loads(ajax_result.html or "{}") or {})
        except Exception as exc:
            raise RuntimeError(f"AJAX JSON {type(exc).__name__}: {exc}") from exc

    def _run_fetch(self) -> None:
        self._start_url = f"https://www.gelbeseiten.de/suche/{self.branch_slug}/{self.plz}"
        page_url = self._start_url
        page_referer = ""
        page_slot_name = ""
        page_slot_idx = -1
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
                mode="http_only",
                preferred_slot_name=page_slot_name,
                preferred_slot_idx=page_slot_idx,
            )
            self._last_tunnel = dict(search_result.tunnel)
            page_slot_name = str(search_result.tunnel.get("name") or "")
            page_slot_idx = int(search_result.session_slot)
            if search_result.status not in {200, 404}:
                self._final_reason = f"SEARCH HTTP {search_result.status}"
                return

            self._list_seen += 1
            response = to_text_response(search_result)
            parsed_index_cards = self._parse_index_cards(response)

            next_href = str(response.css('a.pagination__next::attr(href)').get() or "").strip()
            load_more = self._extract_load_more_form(response)

            if SPELL_SUGGEST_RE.search(response.text or ""):
                self._final_reason = "SPELL SUGGESTION"
                return

            has_treffer = bool(TREFFER_RE.search(response.text or ""))
            has_addr = bool(response.css("span.mod-AdresseKompakt__adress__ort::text").get())
            has_any_signal = has_treffer or bool(parsed_index_cards) or bool(next_href) or bool(load_more.get("action")) or has_addr
            if not has_any_signal:
                self._final_reason = f"FAILED TO PARSE {search_result.final_url}"
                return

            if not parsed_index_cards:
                self._final_reason = "NO DETAIL ITEMS"
                return

            self._remember_index_cards(
                parsed_index_cards,
                page_url=search_result.final_url,
                slot_name=page_slot_name,
                slot_idx=page_slot_idx,
                seen_detail_urls=seen_detail_urls,
            )

            while load_more:
                action_url = urljoin(search_result.final_url, str(load_more.get("action") or ""))
                fields = dict(load_more.get("fields") or {})
                if not action_url or not fields:
                    break

                ajax_payload = self._post_ajaxsuche(
                    task_id=self.task_id,
                    cb_id=self.cb_id,
                    action_url=action_url,
                    fields=fields,
                    referer=search_result.final_url,
                    preferred_slot_name=page_slot_name,
                    preferred_slot_idx=page_slot_idx,
                )
                ajax_html = str(ajax_payload.get("html") or "")
                if not ajax_html:
                    break

                ajax_response = build_text_response(
                    url=str(search_result.final_url or ""),
                    html=ajax_html,
                    status=200,
                )
                ajax_cards = self._parse_index_cards(ajax_response)

                if not ajax_cards:
                    break

                self._paging_seen += 1
                self._list_seen += 1

                self._remember_index_cards(
                    ajax_cards,
                    page_url=search_result.final_url,
                    slot_name=page_slot_name,
                    slot_idx=page_slot_idx,
                    seen_detail_urls=seen_detail_urls,
                )

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

        self._run_detail_fetches()

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
        self._db_rows = save_gs_probe_run(payload)
        return True

    @staticmethod
    def _reason_marks_collected(reason: str) -> bool:
        reason_s = str(reason or "").strip()
        if not reason_s:
            return False
        if reason_s in {"OK", "NO DETAIL ITEMS", "SPELL SUGGESTION"}:
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
        log(LOG_FILE, folder=LOG_FOLDER, message=self._index_log_line(reason))
        log(LOG_FILE, folder=LOG_FOLDER, message=self._detail_log_line())
        log(LOG_FILE, folder=LOG_FOLDER, message=self._result_log_line(reason))

    def closed(self, reason):
        r = (self._final_reason or reason or "").strip() or "UNKNOWN"
        self._log_status(r)

        if r == "ALREADY COLLECTED":
            self._db_action = "skip_already"
            print(f"CORE_GS_CB cb_id={self.cb_id} result=skip reason={r}")
            return

        if not self._reason_marks_collected(r):
            self._db_action = "leave_pending"
            print(
                f"CORE_GS_CB cb_id={self.cb_id} result=pending reason={r} "
                f"indexed={len(self.index_cards)} selected={len(self.selected_urls)} parsed={self._detail_parsed}"
            )
            return

        self._db_flush_items_and_mark()
        self._mark_pair_result(r)
        self._db_action = "commit"
        print(
            f"CORE_GS_CB cb_id={self.cb_id} result=commit reason={r} "
            f"stored={self._db_rows} indexed={len(self.index_cards)} selected={len(self.selected_urls)} "
            f"parsed={self._detail_parsed}"
        )
