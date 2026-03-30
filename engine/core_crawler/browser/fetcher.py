# FILE: engine/core_crawler/browser/fetcher.py
# DATE: 2026-03-29
# PURPOSE: Small public wrapper around the shared browser session router and lightweight HTML response helpers.

from __future__ import annotations

import threading
from dataclasses import dataclass

from parsel import Selector

from engine.core_crawler.browser.session_router import BrowserSessionRouter, FetchResult

_ROUTER_LOCAL = threading.local()
_ROUTER_REGISTRY_MU = threading.Lock()
_ROUTER_REGISTRY: list[BrowserSessionRouter] = []
_ROUTE_CONTEXT_MU = threading.Lock()


@dataclass(frozen=True)
class FetchRouteContext:
    site: str
    slot_name: str
    slot_idx: int = 0


_ROUTE_CONTEXT: FetchRouteContext | None = None


def _get_router() -> BrowserSessionRouter:
    router = getattr(_ROUTER_LOCAL, "router", None)
    if router is not None:
        return router
    router = BrowserSessionRouter(register_atexit=False)
    _ROUTER_LOCAL.router = router
    with _ROUTER_REGISTRY_MU:
        _ROUTER_REGISTRY.append(router)
    return router


def close_all_fetch_routers() -> None:
    with _ROUTER_REGISTRY_MU:
        routers = list(_ROUTER_REGISTRY)
        _ROUTER_REGISTRY.clear()
    try:
        _ROUTER_LOCAL.router = None
    except Exception:
        pass
    for router in routers:
        try:
            router.close_all()
        except Exception:
            pass


def close_current_fetch_router() -> None:
    router = getattr(_ROUTER_LOCAL, "router", None)
    if router is None:
        return
    with _ROUTER_REGISTRY_MU:
        try:
            _ROUTER_REGISTRY.remove(router)
        except ValueError:
            pass
    try:
        _ROUTER_LOCAL.router = None
    except Exception:
        pass
    try:
        router.close_all()
    except Exception:
        pass


def set_fetch_route_context(site: str, slot_name: str, slot_idx: int = 0) -> None:
    global _ROUTE_CONTEXT
    with _ROUTE_CONTEXT_MU:
        _ROUTE_CONTEXT = FetchRouteContext(
            site=str(site or "").strip(),
            slot_name=str(slot_name or "").strip(),
            slot_idx=int(slot_idx),
        )


def clear_fetch_route_context() -> None:
    global _ROUTE_CONTEXT
    with _ROUTE_CONTEXT_MU:
        _ROUTE_CONTEXT = None


def get_fetch_route_context() -> FetchRouteContext | None:
    with _ROUTE_CONTEXT_MU:
        return _ROUTE_CONTEXT


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
    allowed_slot_names: list[str] | None = None,
) -> FetchResult:
    route_ctx = get_fetch_route_context()
    route_site = str(site or "").strip()
    effective_slot_name = str(preferred_slot_name or "").strip()
    effective_slot_idx = int(preferred_slot_idx)
    effective_allowed_slot_names = [str(name or "").strip() for name in list(allowed_slot_names or []) if str(name or "").strip()]
    if route_ctx is not None and route_site == route_ctx.site:
        if not effective_slot_name:
            effective_slot_name = str(route_ctx.slot_name or "")
        if effective_slot_idx < 0:
            effective_slot_idx = int(route_ctx.slot_idx)
        if not effective_allowed_slot_names and effective_slot_name:
            effective_allowed_slot_names = [effective_slot_name]
    return _get_router().fetch(
        site=route_site,
        url=str(url),
        kind=str(kind),
        task_id=int(task_id),
        cb_id=int(cb_id),
        referer=str(referer or ""),
        mode=str(mode or ""),
        method=str(method or "GET"),
        form=dict(form or {}) or None,
        extra_headers=dict(extra_headers or {}) or None,
        preferred_slot_name=effective_slot_name,
        preferred_slot_idx=effective_slot_idx,
        allowed_slot_names=effective_allowed_slot_names or None,
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
