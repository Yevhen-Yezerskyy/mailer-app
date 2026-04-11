# FILE: engine/core_crawler/browser/browser_signature.py
# DATE: 2026-04-11
# PURPOSE: Single source of truth for per-session browser/light signature fields.

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from engine.core_crawler.browser.session_config import BrowserProfile

NAVIGATION_ACCEPT = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,image/apng,*/*;q=0.8,"
    "application/signed-exchange;v=b3;q=0.7"
)


def _same_site(url: str, referer: str) -> bool:
    if not referer:
        return False
    try:
        return urlsplit(url).netloc == urlsplit(referer).netloc
    except Exception:
        return False


def _quoted_client_hint(value: Any) -> str:
    raw = str(value or "").replace('"', "")
    return f"\"{raw}\""


def sec_ch_ua(profile: BrowserProfile) -> str:
    brands = profile.user_agent_metadata.get("brands") or []
    out: list[str] = []
    for row in brands:
        brand = str((row or {}).get("brand") or "").replace('"', "")
        version = str((row or {}).get("version") or "").replace('"', "")
        if brand and version:
            out.append(f'"{brand}";v="{version}"')
    return ", ".join(out)


def http_impersonate(profile: BrowserProfile) -> str:
    major_version = str((profile.user_agent_metadata or {}).get("fullVersion") or "").split(".", 1)[0]
    if major_version in {"116", "119", "120", "123", "124"}:
        return f"chrome{major_version}"
    raise RuntimeError(f"Unsupported browser profile for http impersonation: chrome{major_version}")


def navigation_accept_language(profile: BrowserProfile) -> str:
    return str(profile.locale or "de-DE")


def build_navigation_headers(profile: BrowserProfile, url: str, referer: str = "") -> dict[str, str]:
    same_site = _same_site(url, referer)
    headers = {
        "Accept": NAVIGATION_ACCEPT,
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": navigation_accept_language(profile),
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": profile.user_agent,
        "Sec-CH-UA": sec_ch_ua(profile),
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": _quoted_client_hint(profile.platform),
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


def build_browser_context_kwargs(profile: BrowserProfile, storage_state: dict[str, Any] | None) -> dict[str, Any]:
    context_kwargs: dict[str, Any] = {
        "user_agent": profile.user_agent,
        "locale": profile.locale,
        "timezone_id": profile.timezone_id,
        "viewport": {"width": profile.viewport_width, "height": profile.viewport_height},
        "screen": {"width": profile.screen_width, "height": profile.screen_height},
        "color_scheme": "light",
        "device_scale_factor": profile.device_scale_factor,
        "has_touch": bool(profile.max_touch_points > 0),
        "ignore_https_errors": True,
        "reduced_motion": "reduce",
        "service_workers": "block",
    }
    if isinstance(storage_state, dict) and storage_state:
        context_kwargs["storage_state"] = storage_state
    return context_kwargs


def build_browser_extra_http_headers(profile: BrowserProfile) -> dict[str, str]:
    return {"Accept-Language": navigation_accept_language(profile)}


def build_browser_ua_override(profile: BrowserProfile) -> dict[str, Any]:
    return {
        "userAgent": profile.user_agent,
        "platform": profile.platform,
        "userAgentMetadata": profile.user_agent_metadata,
    }
