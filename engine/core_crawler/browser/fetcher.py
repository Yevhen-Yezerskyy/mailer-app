# FILE: engine/core_crawler/browser/fetcher.py
# DATE: 2026-03-27
# PURPOSE: Small public wrapper around the shared browser session router.

from __future__ import annotations

from scrapy.http import TextResponse

from engine.core_crawler.browser.broker_client import fetch_html_via_broker
from engine.core_crawler.browser.session_router import FetchResult


def fetch_html(
    site: str,
    url: str,
    kind: str,
    task_id: int,
    cb_id: int,
    referer: str = "",
    mode: str = "",
) -> FetchResult:
    return fetch_html_via_broker(
        site=site,
        url=url,
        kind=kind,
        task_id=int(task_id),
        cb_id=int(cb_id),
        referer=str(referer or ""),
        mode=str(mode or ""),
    )


def to_text_response(result: FetchResult) -> TextResponse:
    response = TextResponse(
        url=result.final_url or result.url,
        body=(result.html or "").encode("utf-8", errors="ignore"),
        encoding="utf-8",
    )
    response.status = int(result.status or 0)
    return response
