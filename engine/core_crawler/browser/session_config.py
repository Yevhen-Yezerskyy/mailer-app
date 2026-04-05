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
    device_scale_factor: float
    max_touch_points: int
    viewport_width: int
    viewport_height: int
    screen_width: int
    screen_height: int
    avail_width: int
    avail_height: int
    outer_width: int
    outer_height: int
    color_depth: int
    pixel_depth: int
    connection_downlink: float
    connection_rtt: int
    connection_effective_type: str
    user_agent_metadata: dict


@dataclass(frozen=True)
class SiteSessionConfig:
    site: str
    home_url: str
    egress_slots: tuple[str, ...]
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
BROKER_QUEUE_MAX = 256
CRAWLER_ACTIVE_TUNNEL_CAP = 2
CRAWLER_SLOT_HOLD_MIN_SEC = 10 * 60
CRAWLER_SLOT_HOLD_MAX_SEC = 15 * 60
ONE_ONE_EIGHTY_WINDOW_MIN_SEC = 7 * 60
ONE_ONE_EIGHTY_WINDOW_MAX_SEC = 12 * 60
ONE_ONE_EIGHTY_WINDOW_COOLDOWN_MIN_SEC = 70 * 60
ONE_ONE_EIGHTY_WINDOW_COOLDOWN_MAX_SEC = 90 * 60
ONE_ONE_EIGHTY_WINDOW_MAIN_REQUEST_LIMIT = 300
ONE_ONE_EIGHTY_ACTIVE_TUNNEL_RATIO = 0.50
ONE_ONE_EIGHTY_ACTIVE_TUNNEL_MAX = CRAWLER_ACTIVE_TUNNEL_CAP
GS_ACTIVE_TUNNEL_MAX = CRAWLER_ACTIVE_TUNNEL_CAP
ONE_ONE_EIGHTY_MISMATCH_VISIT_MAX = 2
ONE_ONE_EIGHTY_MISMATCH_VISIT_PROBABILITY = 0.35


def _chrome_windows(
    name: str,
    major_version: str,
    full_version: str,
    width: int,
    height: int,
    concurrency: int,
    memory: int,
    scale_factor: float,
    max_touch_points: int,
    avail_height_delta: int,
    outer_width_delta: int,
    outer_height_delta: int,
    connection_effective_type: str,
    connection_downlink: float,
    connection_rtt: int,
) -> BrowserProfile:
    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{full_version} Safari/537.36"
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
        device_scale_factor=scale_factor,
        max_touch_points=max_touch_points,
        viewport_width=width,
        viewport_height=height,
        screen_width=width,
        screen_height=height,
        avail_width=width,
        avail_height=max(640, height - avail_height_delta),
        outer_width=width + outer_width_delta,
        outer_height=height + outer_height_delta,
        color_depth=24,
        pixel_depth=24,
        connection_downlink=connection_downlink,
        connection_rtt=connection_rtt,
        connection_effective_type=connection_effective_type,
        user_agent_metadata={
            "brands": [
                {"brand": "Google Chrome", "version": major_version},
                {"brand": "Chromium", "version": major_version},
                {"brand": "Not.A/Brand", "version": "24"},
            ],
            "fullVersionList": [
                {"brand": "Google Chrome", "version": full_version},
                {"brand": "Chromium", "version": full_version},
                {"brand": "Not.A/Brand", "version": "24.0.0.0"},
            ],
            "fullVersion": full_version,
            "platform": "Windows",
            "platformVersion": "10.0.0",
            "architecture": "x86",
            "model": "",
            "mobile": False,
            "bitness": "64",
            "wow64": False,
        },
    )

CHROME_FULL_VERSIONS = (
    ("116", "116.0.5845.180"),
    ("119", "119.0.6045.200"),
    ("120", "120.0.6099.225"),
    ("123", "123.0.6312.123"),
    ("124", "124.0.6367.91"),
)

WINDOWS_DESKTOP_SHAPES = (
    ("a", 1366, 768, 4, 8, 1.0, 0, 40, 16, 88, "4g", 8.9, 60),
    ("b", 1440, 900, 8, 8, 1.0, 0, 40, 16, 88, "4g", 10.4, 55),
    ("c", 1536, 864, 8, 4, 1.25, 0, 40, 16, 88, "4g", 12.2, 45),
    ("d", 1600, 900, 12, 8, 1.25, 5, 40, 16, 90, "4g", 9.7, 50),
    ("e", 1920, 1080, 8, 8, 1.0, 0, 40, 16, 96, "4g", 13.6, 40),
    ("f", 1280, 720, 4, 4, 1.0, 0, 40, 14, 84, "4g", 7.8, 70),
)


BROWSER_PROFILES = (
    tuple(
        _chrome_windows(
            f"win_chrome_{major_version}_{suffix}",
            major_version,
            full_version,
            width,
            height,
            concurrency,
            memory,
            scale_factor,
            max_touch_points,
            avail_height_delta,
            outer_width_delta,
            outer_height_delta,
            connection_effective_type,
            connection_downlink,
            connection_rtt,
        )
        for major_version, full_version in CHROME_FULL_VERSIONS
        for (
            suffix,
            width,
            height,
            concurrency,
            memory,
            scale_factor,
            max_touch_points,
            avail_height_delta,
            outer_width_delta,
            outer_height_delta,
            connection_effective_type,
            connection_downlink,
            connection_rtt,
        ) in WINDOWS_DESKTOP_SHAPES
    )
)


SITE_CONFIGS: dict[str, SiteSessionConfig] = {
    "11880": SiteSessionConfig(
        site="11880",
        home_url="https://www.11880.com/",
        # home_url="https://serenity-mail.de",
        egress_slots=(
            "fenster_ukraine",
            "nowedel",
            "zenosolar",
            "aws_54_93_240_19",
            "aws_18_196_116_251",
            "aws_18_196_79_5",
            "aws_18_195_154_38",
            "aws_35_159_92_68",
            "aws_3_79_102_24",
            "aws_52_28_12_242",
            "aws_18_157_158_129",
            "aws_3_76_124_90",
            "aws_3_66_88_254",
            "aws_18_153_80_121",
            "aws_3_120_39_180",
            "aws_3_68_216_17",
            "aws_3_70_100_238",
            "aws_18_192_129_122",
            "aws_63_177_248_144",
            "aws_3_76_209_105",
            "aws_63_183_216_59",
            "direct",
        ),
        slot_quarantine_sec=26 * 60 * 60,
        sessions_per_egress=1,
        concurrent_pages_per_session=2,
        max_requests_per_session=500,
        max_session_age_sec=1200,
        runtime_recycle_min_sec=300,
        runtime_recycle_max_sec=600,
        pause_min_sec=1.5,
        pause_max_sec=3.0,
        browser_timeout_ms=90_000,
    ),
    "gs": SiteSessionConfig(
        site="gs",
        home_url="https://www.gelbeseiten.de/",
        egress_slots=(
            "fenster_ukraine",
            "nowedel",
            "zenosolar",
            "aws_54_93_240_19",
            "aws_18_196_116_251",
            "aws_18_196_79_5",
            "aws_18_195_154_38",
            "aws_35_159_92_68",
            "aws_3_79_102_24",
            "aws_52_28_12_242",
            "aws_18_157_158_129",
            "aws_3_76_124_90",
            "aws_3_66_88_254",
            "aws_18_153_80_121",
            "aws_3_120_39_180",
            "aws_3_68_216_17",
            "aws_3_70_100_238",
            "aws_18_192_129_122",
            "aws_63_177_248_144",
            "aws_3_76_209_105",
            "aws_63_183_216_59",
            "direct",
        ),
        slot_quarantine_sec=1 * 60 * 60,
        sessions_per_egress=1,
        concurrent_pages_per_session=2,
        max_requests_per_session=500,
        max_session_age_sec=1200,
        runtime_recycle_min_sec=300,
        runtime_recycle_max_sec=600,
        pause_min_sec=1.0,
        pause_max_sec=2.0,
        browser_timeout_ms=90_000,
    ),
}
