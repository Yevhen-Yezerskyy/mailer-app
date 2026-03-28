# FILE: engine/core_crawler/browser/session_config.py
# DATE: 2026-03-27
# PURPOSE: Shared browser session profiles and per-site settings for core_crawler.

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BrowserProfile:
    name: str
    user_agent: str
    accept_language: str
    locale: str
    timezone_id: str
    platform: str
    navigator_platform: str
    navigator_vendor: str
    languages: tuple[str, ...]
    hardware_concurrency: int
    device_memory: int
    viewport_width: int
    viewport_height: int
    screen_width: int
    screen_height: int
    user_agent_metadata: dict


@dataclass(frozen=True)
class SiteSessionConfig:
    site: str
    home_url: str
    http_log_file: str
    egress_slots: tuple[str, ...]
    active_slot_count: int
    slot_quarantine_sec: int
    sessions_per_egress: int
    concurrent_pages_per_session: int
    max_requests_per_session: int
    max_session_age_sec: int
    runtime_recycle_min_sec: int
    runtime_recycle_max_sec: int
    pause_min_sec: float
    pause_max_sec: float
    browser_timeout_ms: int


LOG_FOLDER = "crawler"
ROUTER_HTTP_LOG_FILE = "http_router"
BROKER_WORKERS = 3
BROKER_QUEUE_MAX = 256


def _chrome_124_windows(name: str, width: int, height: int, concurrency: int, memory: int) -> BrowserProfile:
    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.6367.91 Safari/537.36"
    )
    return BrowserProfile(
        name=name,
        user_agent=ua,
        accept_language="de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        locale="de-DE",
        timezone_id="Europe/Berlin",
        platform="Windows",
        navigator_platform="Win32",
        navigator_vendor="Google Inc.",
        languages=("de-DE", "de", "en-US", "en"),
        hardware_concurrency=concurrency,
        device_memory=memory,
        viewport_width=width,
        viewport_height=height,
        screen_width=width,
        screen_height=height,
        user_agent_metadata={
            "brands": [
                {"brand": "Google Chrome", "version": "124"},
                {"brand": "Chromium", "version": "124"},
                {"brand": "Not.A/Brand", "version": "24"},
            ],
            "fullVersionList": [
                {"brand": "Google Chrome", "version": "124.0.6367.91"},
                {"brand": "Chromium", "version": "124.0.6367.91"},
                {"brand": "Not.A/Brand", "version": "24.0.0.0"},
            ],
            "fullVersion": "124.0.6367.91",
            "platform": "Windows",
            "platformVersion": "10.0.0",
            "architecture": "x86",
            "model": "",
            "mobile": False,
            "bitness": "64",
            "wow64": False,
        },
    )


BROWSER_PROFILES = (
    _chrome_124_windows("win_chrome_124_a", 1440, 900, 8, 8),
    _chrome_124_windows("win_chrome_124_b", 1366, 768, 4, 8),
    _chrome_124_windows("win_chrome_124_c", 1536, 864, 8, 4),
)


SITE_CONFIGS: dict[str, SiteSessionConfig] = {
    "probe11880": SiteSessionConfig(
        site="probe11880",
        home_url="https://dev.serenity-mail.de/__probe__/11880-http/?source=router_probe&step=0",
        http_log_file="http_probe11880",
        egress_slots=("fenster_ukraine", "nowedel", "zenosolar"),
        active_slot_count=3,
        slot_quarantine_sec=3 * 60 * 60,
        sessions_per_egress=2,
        concurrent_pages_per_session=1,
        max_requests_per_session=500,
        max_session_age_sec=60 * 60,
        runtime_recycle_min_sec=5 * 60,
        runtime_recycle_max_sec=10 * 60,
        pause_min_sec=0.1,
        pause_max_sec=2.0,
        browser_timeout_ms=90_000,
    ),
    "11880": SiteSessionConfig(
        site="11880",
        home_url="https://www.11880.com/",
        http_log_file="http_11880",
        egress_slots=("fenster_ukraine", "nowedel", "zenosolar", "direct"),
        active_slot_count=3,
        slot_quarantine_sec=10_800,
        sessions_per_egress=1,
        concurrent_pages_per_session=1,
        max_requests_per_session=8,
        max_session_age_sec=1200,
        runtime_recycle_min_sec=300,
        runtime_recycle_max_sec=600,
        pause_min_sec=2.5,
        pause_max_sec=6.0,
        browser_timeout_ms=90_000,
    ),
    "gs": SiteSessionConfig(
        site="gs",
        home_url="https://www.gelbeseiten.de/",
        http_log_file="http_gs",
        egress_slots=("fenster_ukraine", "nowedel", "zenosolar", "direct"),
        active_slot_count=3,
        slot_quarantine_sec=10_800,
        sessions_per_egress=1,
        concurrent_pages_per_session=1,
        max_requests_per_session=8,
        max_session_age_sec=1200,
        runtime_recycle_min_sec=300,
        runtime_recycle_max_sec=600,
        pause_min_sec=2.5,
        pause_max_sec=6.0,
        browser_timeout_ms=90_000,
    ),
}
