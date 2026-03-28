# FILE: engine/core_crawler/browser/http_fetch.py
# DATE: 2026-03-27
# PURPOSE: Lightweight HTTP fetch layer with requests+SOCKS for warmed crawler sessions.

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

import requests

from engine.core_crawler.browser.session_config import BrowserProfile


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


def _same_site(url: str, referer: str) -> bool:
    if not referer:
        return False
    try:
        return urlsplit(url).netloc == urlsplit(referer).netloc
    except Exception:
        return False


def _sec_ch_ua(profile: BrowserProfile) -> str:
    brands = profile.user_agent_metadata.get("brands") or []
    out: list[str] = []
    for row in brands:
        brand = str((row or {}).get("brand") or "").replace('"', "")
        version = str((row or {}).get("version") or "").replace('"', "")
        if brand and version:
            out.append(f'"{brand}";v="{version}"')
    return ", ".join(out)


def build_http_headers(profile: BrowserProfile, url: str, referer: str = "") -> dict[str, str]:
    same_site = _same_site(url, referer)
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": profile.accept_language,
        "Cache-Control": "max-age=0",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": profile.user_agent,
        "Sec-CH-UA": _sec_ch_ua(profile),
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": f"\"{profile.platform}\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin" if same_site else "none",
        "Sec-Fetch-User": "?1",
    }
    if referer:
        headers["Referer"] = referer
        if not same_site:
            headers["Sec-Fetch-Site"] = "cross-site"
    return headers


def _load_cookies(session: requests.Session, storage_state: dict[str, Any] | None) -> None:
    for row in _cookie_list_from_storage_state(storage_state):
        try:
            rest = {"HttpOnly": bool(row.get("httpOnly") is True)}
            same_site = _normalize_same_site(row.get("sameSite"))
            if same_site:
                rest["SameSite"] = same_site
            session.cookies.set(
                name=str(row.get("name") or ""),
                value=str(row.get("value") or ""),
                domain=str(row.get("domain") or ""),
                path=str(row.get("path") or "/"),
                secure=bool(row.get("secure") is True),
                expires=int(row.get("expires")) if row.get("expires") not in (None, "", -1) else None,
                rest=rest,
            )
        except Exception:
            continue


def build_http_session(profile: BrowserProfile, tunnel: dict[str, Any], storage_state: dict[str, Any] | None) -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(build_http_headers(profile, "", ""))
    proxy_server = str(tunnel.get("proxy_server") or "")
    if proxy_server:
        session.proxies.update({"http": proxy_server, "https": proxy_server})
    _load_cookies(session, storage_state)
    return session


def export_storage_state(session: requests.Session, previous_state: dict[str, Any] | None) -> dict[str, Any]:
    cookies: list[dict[str, Any]] = []
    for cookie in session.cookies:
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


def cookie_snapshot(session: requests.Session) -> list[dict[str, Any]]:
    return export_storage_state(session, {}).get("cookies") or []


def fetch_html(
    session: requests.Session,
    profile: BrowserProfile,
    url: str,
    *,
    referer: str = "",
    timeout_ms: int = 90_000,
) -> dict[str, Any]:
    headers = build_http_headers(profile, url, referer)
    request = requests.Request("GET", url, headers=headers)
    prepared = session.prepare_request(request)
    response = session.send(
        prepared,
        timeout=max(1.0, float(timeout_ms) / 1000.0),
        allow_redirects=True,
        stream=False,
    )
    return {
        "status": int(response.status_code),
        "url": str(url),
        "final_url": str(response.url),
        "html": str(response.text or ""),
        "request_headers": dict(prepared.headers),
        "response_headers": dict(response.headers),
    }
