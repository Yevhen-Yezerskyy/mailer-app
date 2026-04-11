# FILE: engine/core_crawler/browser/http_fetch.py
# DATE: 2026-03-27
# PURPOSE: Lightweight HTTP fetch layer with curl_cffi browser impersonation for warmed crawler sessions.

from __future__ import annotations

from typing import Any
from urllib.parse import urljoin, urlsplit

from curl_cffi import requests as curl_requests

from engine.core_crawler.browser.browser_signature import (
    build_navigation_headers,
    http_impersonate,
)
from engine.core_crawler.browser.session_config import BrowserProfile


class SkippedFetchError(RuntimeError):
    pass


def _cookie_list_from_storage_state(storage_state: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(storage_state, dict):
        return []
    cookies = storage_state.get("cookies") or []
    if not isinstance(cookies, list):
        return []
    out: list[dict[str, Any]] = []
    for row in cookies:
        if isinstance(row, dict) and row.get("name"):
            out.append(dict(row))
    return out


def storage_state_has_cookies(storage_state: dict[str, Any] | None) -> bool:
    return bool(_cookie_list_from_storage_state(storage_state))


def _normalize_same_site(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    if raw == "strict":
        return "Strict"
    if raw == "lax":
        return "Lax"
    if raw == "none":
        return "None"
    return None


def _storage_origins(storage_state: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(storage_state, dict):
        return []
    origins = storage_state.get("origins") or []
    if not isinstance(origins, list):
        return []
    return [dict(row) for row in origins if isinstance(row, dict)]


def build_http_headers(profile: BrowserProfile, url: str, referer: str = "") -> dict[str, str]:
    return build_navigation_headers(profile, url, referer)


def _load_cookies(session: Any, storage_state: dict[str, Any] | None) -> None:
    for row in _cookie_list_from_storage_state(storage_state):
        try:
            session.cookies.set(
                name=str(row.get("name") or ""),
                value=str(row.get("value") or ""),
                domain=str(row.get("domain") or ""),
                path=str(row.get("path") or "/"),
                secure=bool(row.get("secure") is True),
            )
        except Exception:
            continue


def _http_proxy_server(proxy_server: str) -> str:
    raw = str(proxy_server or "").strip()
    if raw.startswith("socks5://"):
        return "socks5h://" + raw[len("socks5://") :]
    return raw


def build_http_session(profile: BrowserProfile, tunnel: dict[str, Any], storage_state: dict[str, Any] | None) -> Any:
    session = curl_requests.Session()
    try:
        session.trust_env = False
    except Exception:
        pass
    session.headers.update(build_http_headers(profile, "", ""))
    proxy_server = str(tunnel.get("proxy_server") or "")
    if not proxy_server:
        tunnel_name = str(tunnel.get("name") or "").strip()
        raise RuntimeError(f"MISSING SLOT PROXY {tunnel_name or 'unknown'}")
    http_proxy_server = _http_proxy_server(proxy_server)
    session.proxies = {"http": http_proxy_server, "https": http_proxy_server}
    _load_cookies(session, storage_state)
    return session


def export_storage_state(session: Any, previous_state: dict[str, Any] | None) -> dict[str, Any]:
    cookies: list[dict[str, Any]] = []
    jar = getattr(session.cookies, "jar", None)
    source = jar if jar is not None else []
    for cookie in source:
        row = {
            "name": cookie.name,
            "value": cookie.value,
            "domain": cookie.domain or "",
            "path": cookie.path or "/",
            "expires": int(cookie.expires) if cookie.expires else -1,
            "httpOnly": False,
            "secure": bool(cookie.secure),
        }
        same_site = _normalize_same_site(getattr(cookie, "_rest", {}).get("SameSite"))
        if same_site:
            row["sameSite"] = same_site
        cookies.append(row)
    return {
        "cookies": cookies,
        "origins": _storage_origins(previous_state),
    }


def cookie_snapshot(session: Any) -> list[dict[str, Any]]:
    return export_storage_state(session, {}).get("cookies") or []


def _redirect_location(response: Any) -> str:
    headers = getattr(response, "headers", {}) or {}
    location = ""
    try:
        location = str(headers.get("location") or headers.get("Location") or "").strip()
    except Exception:
        location = ""
    return location


def _follow_redirect_request(
    method: str,
    form: dict[str, Any] | None,
    status_code: int,
) -> tuple[str, dict[str, Any] | None]:
    method_s = str(method or "GET").upper()
    code = int(status_code or 0)
    if code == 303:
        return "GET", None
    if code in {301, 302} and method_s == "POST":
        return "GET", None
    return method_s, dict(form or {}) or None


def fetch_html(
    session: Any,
    profile: BrowserProfile,
    url: str,
    *,
    referer: str = "",
    timeout_ms: int = 90_000,
    method: str = "GET",
    form: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    current_url = str(url or "")
    current_referer = str(referer or "")
    current_method = str(method or "GET").upper()
    current_form = dict(form or {}) or None
    redirect_count = 0
    timeout_sec = max(1.0, float(timeout_ms) / 1000.0)

    while True:
        headers = build_http_headers(profile, current_url, current_referer)
        if current_method == "POST" and current_referer:
            referer_parts = urlsplit(current_referer)
            if referer_parts.scheme and referer_parts.netloc:
                headers["Origin"] = f"{referer_parts.scheme}://{referer_parts.netloc}"
        for key, value in dict(extra_headers or {}).items():
            k = str(key or "").strip()
            if not k:
                continue
            headers[k] = str(value or "")
        response = session.request(
            current_method,
            current_url,
            headers=headers,
            data=current_form,
            timeout=timeout_sec,
            allow_redirects=False,
            stream=False,
            impersonate=http_impersonate(profile),
        )
        status_code = int(response.status_code or 0)
        location = _redirect_location(response)
        if status_code not in {301, 302, 303, 307, 308} or not location:
            break
        redirect_count += 1
        if redirect_count > 1:
            raise SkippedFetchError("SKIPPED REDIRECT LIMIT")
        next_url = urljoin(current_url, location)
        current_referer = current_url
        current_method, current_form = _follow_redirect_request(current_method, current_form, status_code)
        current_url = str(next_url or current_url)

    return {
        "status": status_code,
        "url": str(url),
        "final_url": str(response.url),
        "html": str(response.text or ""),
        "request_headers": dict(headers),
        "response_headers": dict(response.headers),
    }
