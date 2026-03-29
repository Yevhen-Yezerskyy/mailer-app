# FILE: engine/core_crawler/browser/fetcher.py
# DATE: 2026-03-29
# PURPOSE: Small public wrapper around the shared browser session router and lightweight HTML response helpers.

from __future__ import annotations

from parsel import Selector

from engine.core_crawler.browser.broker_client import fetch_html_via_broker
from engine.core_crawler.browser.session_router import FetchResult


class HtmlTextResponse:
    def __init__(self, url: str, html: str, status: int = 200):
        self.url = str(url or "")
        self.text = str(html or "")
        self.status = int(status)
        self._selector = Selector(text=self.text)

    def css(self, query: str):
        query = str(query or "")
        selector = self._selector
        result = selector.css(query)
        return result

    def xpath(self, query: str):
        query = str(query or "")
        selector = self._selector
        result = selector.xpath(query)
        return result


def fetch_html(
    site: str,
    url: str,
    kind: str,
    task_id: int,
    cb_id: int,
    referer: str = "",
    mode: str = "",
    method: str = "GET",
    form: dict[str, str] | None = None,
    extra_headers: dict[str, str] | None = None,
    preferred_slot_name: str = "",
    preferred_slot_idx: int = -1,
) -> FetchResult:
    return fetch_html_via_broker(
        site=site,
        url=url,
        kind=kind,
        task_id=int(task_id),
        cb_id=int(cb_id),
        referer=str(referer or ""),
        mode=str(mode or ""),
        method=str(method or "GET"),
        form=dict(form or {}) or None,
        extra_headers=dict(extra_headers or {}) or None,
        preferred_slot_name=str(preferred_slot_name or ""),
        preferred_slot_idx=int(preferred_slot_idx),
    )


def build_text_response(url: str, html: str, status: int = 200) -> HtmlTextResponse:
    response = HtmlTextResponse(
        url=str(url or ""),
        html=str(html or ""),
        status=int(status),
    )
    return response


def to_text_response(result: FetchResult) -> HtmlTextResponse:
    response = build_text_response(
        url=result.final_url or result.url,
        html=result.html or "",
        status=int(result.status or 0),
    )
    return response
