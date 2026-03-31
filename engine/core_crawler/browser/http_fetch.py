# FILE: engine/core_crawler/browser/http_fetch.py
# DATE: 2026-03-27
# PURPOSE: Lightweight HTTP fetch layer with curl_cffi browser impersonation for warmed crawler sessions.

from __future__ import annotations

from typing import Any
from urllib.parse import urljoin, urlsplit

from curl_cffi import requests as curl_requests

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


def _sec_ch_ua_full_version_list(profile: BrowserProfile) -> str:
    brands = profile.user_agent_metadata.get("fullVersionList") or []
    out: list[str] = []
    for row in brands:
        brand = str((row or {}).get("brand") or "").replace('"', "")
        version = str((row or {}).get("version") or "").replace('"', "")
        if brand and version:
            out.append(f'"{brand}";v="{version}"')
    return ", ".join(out)


def _quoted_client_hint(value: Any) -> str:
    raw = str(value or "").replace('"', "")
    return f"\"{raw}\""


def _http_impersonate(profile: BrowserProfile) -> str:
    major_version = str((profile.user_agent_metadata or {}).get("fullVersion") or "").split(".", 1)[0]
    if major_version in {"116", "119", "120", "123", "124"}:
        return f"chrome{major_version}"
    raise RuntimeError(f"Unsupported browser profile for http impersonation: chrome{major_version}")


def build_http_headers(profile: BrowserProfile, url: str, referer: str = "") -> dict[str, str]:
    same_site = _same_site(url, referer)
    ua_meta = dict(profile.user_agent_metadata or {})
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": profile.accept_language,
        "Cache-Control": "max-age=0",
        "Pragma": "no-cache",
        "Priority": "u=0, i",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": profile.user_agent,
        "Sec-CH-UA": _sec_ch_ua(profile),
        "Sec-CH-UA-Arch": _quoted_client_hint(ua_meta.get("architecture") or "x86"),
        "Sec-CH-UA-Bitness": _quoted_client_hint(ua_meta.get("bitness") or "64"),
        "Sec-CH-UA-Full-Version-List": _sec_ch_ua_full_version_list(profile),
        "Sec-CH-UA-Model": _quoted_client_hint(ua_meta.get("model") or ""),
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": f"\"{profile.platform}\"",
        "Sec-CH-UA-Platform-Version": _quoted_client_hint(ua_meta.get("platformVersion") or "10.0.0"),
        "Sec-CH-UA-WoW64": "?1" if bool(ua_meta.get("wow64")) else "?0",
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


def build_http_session(profile: BrowserProfile, tunnel: dict[str, Any], storage_state: dict[str, Any] | None) -> Any:
    session = curl_requests.Session()
    try:
        session.trust_env = False
    except Exception:
        pass
    session.headers.update(build_http_headers(profile, "", ""))
    proxy_server = str(tunnel.get("proxy_server") or "")
    if proxy_server:
        session.proxies = {"http": proxy_server, "https": proxy_server}
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
            impersonate=_http_impersonate(profile),
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
