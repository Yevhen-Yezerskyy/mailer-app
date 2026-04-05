# FILE: engine/core_crawler/browser/session_router.py
# DATE: 2026-03-27
# PURPOSE: Shared browser fetch router with Redis-backed slot/session state and concurrent pages per logical session.

from __future__ import annotations

import atexit
import json
import pickle
import random
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from engine.common.cache.client import CLIENT
from engine.common.logs import log
from engine.core_crawler.browser.http_fetch import (
    SkippedFetchError,
    build_http_session,
    cookie_snapshot,
    export_storage_state,
    fetch_html,
    storage_state_has_cookies,
)
from engine.core_crawler.browser.session_config import (
    BROWSER_PROFILES,
    LOG_FOLDER,
    SITE_CONFIGS,
    BrowserProfile,
    SiteSessionConfig,
)
from engine.core_crawler.tunnels_11880 import list_tunnels, load_tunnel_statuses

ROUTER_BOOT_ID = uuid4().hex
SESSION_STATE_TTL_SEC = 3 * 60
SESSION_STATE_CLEAR_TTL_SEC = 1
QUARANTINE_BACKOFF_TTL_SEC = 7 * 24 * 60 * 60
SESSION_STATE_IDLE_MAX_SEC = 3 * 60
WAIT_TIMEOUT_SEC = 60.0
RUNTIME_IDLE_REAP_SEC = 90.0
SESSION_GATE_TTL_SEC = 5.0
SESSION_GATE_WAIT_SEC = 5.0
SESSION_LEASE_TTL_SEC = 5 * 60.0
HTTP_CHROMIUM_LOG_FILE = "http_chromium.log"
HTTP_LIGHT_LOG_FILE = "http_light.log"
ROUTER_STATE_LOG_FILE = "router_state.log"
QUARANTINE_STEP_LADDER_SEC = (
    1 * 60 * 60,
    4 * 60 * 60,
    12 * 60 * 60,
    24 * 60 * 60,
    48 * 60 * 60,
)


@dataclass
class FetchResult:
    status: int
    url: str
    final_url: str
    html: str
    title: str
    ms: int
    site: str
    session_id: str
    session_slot: int
    tunnel: dict[str, Any]


@dataclass
class BrowserSession:
    session_id: str
    site: str
    profile: BrowserProfile
    tunnel: dict[str, Any]
    slot_idx: int
    browser: Any
    context: Any
    storage_state: dict[str, Any]
    http_session: Any
    created_at: float
    last_used_at: float
    requests_total: int = 0
    warmed: bool = False
    current_url: str = ""
    active_pages: int = 0
    recycle_after_ts: float = 0.0
    next_dispatch_ts: float = 0.0
    http_mu: Any = field(default_factory=threading.Lock)


@dataclass
class BrowserRuntime:
    browser_key: str
    site: str
    tunnel: dict[str, Any]
    browser: Any
    created_at: float
    last_used_at: float
    recycle_after_ts: float = 0.0


@dataclass
class SessionLease:
    session_key: str
    slot_name: str
    slot_idx: int
    session_id: str
    state: dict[str, Any]
    needs_warm: bool
    base_requests_total: int
    page_lock_key: str
    page_lock_token: str
    warm_lock_key: str = ""
    warm_lock_token: str = ""


class BrowserSessionRouter:
    def __init__(self, register_atexit: bool = True) -> None:
        self._pw_mu = threading.Lock()
        self._playwright = None
        self._runtime_cv = threading.Condition()
        self._runtimes: dict[str, BrowserSession] = {}
        self._browsers: dict[str, BrowserRuntime] = {}
        if register_atexit:
            atexit.register(self.close_all)

    def close_all(self) -> None:
        with self._runtime_cv:
            runtimes = list(self._runtimes.values())
            browsers = list(self._browsers.values())
            self._runtimes = {}
            self._browsers = {}
            self._runtime_cv.notify_all()
        for runtime in runtimes:
            try:
                cfg = SITE_CONFIGS[runtime.site]
                if not self._slot_is_quarantined(cfg, runtime.tunnel["name"]):
                    self._persist_session_state(cfg, runtime)
            except Exception:
                pass
            self._close_session(runtime)
        for browser_runtime in browsers:
            self._close_browser_runtime(browser_runtime)
        with self._pw_mu:
            if self._playwright is not None:
                try:
                    self._playwright.stop()
                except Exception:
                    pass
                self._playwright = None

    @staticmethod
    def _port_open(port: int) -> bool:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.5)
        try:
            sock.connect(("127.0.0.1", int(port)))
            return True
        except OSError:
            return False
        finally:
            sock.close()

    def _ensure_playwright(self):
        with self._pw_mu:
            if self._playwright is None:
                self._playwright = sync_playwright().start()
            return self._playwright

    def _log_fetch_start(
        self,
        *,
        log_file: str,
        site: str,
        has_cookies: bool,
        tunnel: dict[str, Any],
        url: str,
    ) -> None:
        log(
            log_file,
            folder=LOG_FOLDER,
            message=self._request_log_line(
                stage="start",
                site=site,
                has_cookies=has_cookies,
                tunnel=tunnel,
                url=url,
            ),
        )

    def _log_fetch_done(
        self,
        *,
        log_file: str,
        site: str,
        has_cookies: bool,
        tunnel: dict[str, Any],
        final_url: str,
        status: int,
        ms: int,
    ) -> None:
        log(
            log_file,
            folder=LOG_FOLDER,
            message=self._request_log_line(
                stage="end",
                site=site,
                has_cookies=has_cookies,
                tunnel=tunnel,
                url=final_url,
                status=status,
                ms=ms,
            ),
        )

    def _log_fetch_error(
        self,
        *,
        log_file: str,
        site: str,
        has_cookies: bool,
        tunnel: dict[str, Any],
        url: str,
        status: int,
        ms: int,
        error: str,
    ) -> None:
        log(
            log_file,
            folder=LOG_FOLDER,
            message=self._request_log_line(
                stage="end",
                site=site,
                has_cookies=has_cookies,
                tunnel=tunnel,
                url=url,
                status=status,
                ms=ms,
                error=error,
            ),
        )

    @staticmethod
    def _format_error_message(exc: Exception) -> str:
        exc_type = type(exc).__name__
        detail = str(exc or "").strip()
        if not detail:
            return exc_type
        return f"{exc_type}: {detail}"

    @staticmethod
    def _request_log_line(
        *,
        stage: str,
        site: str,
        has_cookies: bool,
        tunnel: dict[str, Any],
        url: str,
        status: int | None = None,
        ms: int | None = None,
        error: str = "",
    ) -> str:
        site_name = str(site or "").strip().lower()
        if site_name == "gs":
            site_name = "gelbeseiten"
        parts = [
            stage,
            f"site={site_name}",
            f"cookies={'yes' if bool(has_cookies) else 'no'}",
            f"tunnel={str((tunnel or {}).get('name') or '')}",
            f"url={url}",
        ]
        if status is not None:
            parts.append(f"status={int(status)}")
        if ms is not None:
            parts.append(f"ms={int(ms)}")
        if error:
            parts.append(f"error={error}")
        return " ".join(parts)

    @staticmethod
    def _cache_get_obj(key: str) -> Any:
        payload = CLIENT.get(key, ttl_sec=SESSION_STATE_TTL_SEC)
        if not payload:
            return None
        try:
            return pickle.loads(payload)
        except Exception as exc:
            raise RuntimeError(f"BAD CACHE PAYLOAD {key}: {type(exc).__name__}: {exc}") from exc

    @staticmethod
    def _cache_set_obj(key: str, value: Any, ttl_sec: int = SESSION_STATE_TTL_SEC) -> None:
        try:
            payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as exc:
            raise RuntimeError(f"CACHE ENCODE FAILED {key}: {type(exc).__name__}: {exc}") from exc
        CLIENT.set(key, payload, ttl_sec=ttl_sec)

    @classmethod
    def _clear_cache_obj(cls, key: str) -> None:
        cls._cache_set_obj(key, {}, ttl_sec=SESSION_STATE_CLEAR_TTL_SEC)

    @staticmethod
    def _quarantine_ttl_sec(state: dict[str, float]) -> int:
        now = time.time()
        max_left = 0.0
        for until in state.values():
            try:
                until_f = float(until or 0.0)
            except Exception:
                continue
            max_left = max(max_left, until_f - now)
        return max(60, int(max_left) if max_left > 0.0 else 60)

    @staticmethod
    def _quarantine_backoff_ttl_sec(backoff: dict[str, dict[str, float | int]], grace_sec: float) -> int:
        now = time.time()
        max_left = 0.0
        for row in backoff.values():
            if not isinstance(row, dict):
                continue
            try:
                until_f = float(row.get("until") or 0.0)
            except Exception:
                continue
            max_left = max(max_left, (until_f + float(grace_sec)) - now)
        if max_left <= 0.0:
            return 60
        return max(60, min(int(max_left), QUARANTINE_BACKOFF_TTL_SEC))

    @staticmethod
    def _runtime_key(site: str, slot_name: str, slot_idx: int) -> str:
        site_name = str(site or "").strip()
        tunnel_name = str(slot_name or "").strip()
        if not site_name or not tunnel_name:
            raise ValueError("runtime key requires site and slot_name")
        return f"{site_name}:{tunnel_name}:{int(slot_idx)}"

    @staticmethod
    def _browser_key(site: str, slot_name: str) -> str:
        site_name = str(site or "").strip()
        tunnel_name = str(slot_name or "").strip()
        if not site_name or not tunnel_name:
            raise ValueError("browser key requires site and slot_name")
        return f"{site_name}:{tunnel_name}"

    @staticmethod
    def _quarantine_key(site: str) -> str:
        site_name = str(site or "").strip()
        if not site_name:
            raise ValueError("quarantine key requires site")
        return f"core_crawler:slot_quarantine:{site_name}"

    @staticmethod
    def _quarantine_backoff_key(site: str) -> str:
        site_name = str(site or "").strip()
        if not site_name:
            raise ValueError("quarantine backoff key requires site")
        return f"core_crawler:slot_quarantine_backoff:{site_name}"

    @staticmethod
    def _session_key(site: str, slot_name: str, slot_idx: int) -> str:
        site_name = str(site or "").strip()
        tunnel_name = str(slot_name or "").strip()
        if not site_name or not tunnel_name:
            raise ValueError("session key requires site and slot_name")
        return f"core_crawler:browser_session:{site_name}:{tunnel_name}:{int(slot_idx)}"

    @staticmethod
    def _session_gate_key(site: str, slot_name: str, slot_idx: int) -> str:
        site_name = str(site or "").strip()
        tunnel_name = str(slot_name or "").strip()
        if not site_name or not tunnel_name:
            raise ValueError("session gate key requires site and slot_name")
        return f"core_crawler:browser_session_gate:{site_name}:{tunnel_name}:{int(slot_idx)}"

    @staticmethod
    def _session_warm_key(site: str, slot_name: str, slot_idx: int) -> str:
        site_name = str(site or "").strip()
        tunnel_name = str(slot_name or "").strip()
        if not site_name or not tunnel_name:
            raise ValueError("session warm key requires site and slot_name")
        return f"core_crawler:browser_session_warm:{site_name}:{tunnel_name}:{int(slot_idx)}"

    @staticmethod
    def _session_page_key(site: str, slot_name: str, slot_idx: int, page_idx: int) -> str:
        site_name = str(site or "").strip()
        tunnel_name = str(slot_name or "").strip()
        if not site_name or not tunnel_name:
            raise ValueError("session page key requires site and slot_name")
        return f"core_crawler:browser_session_page:{site_name}:{tunnel_name}:{int(slot_idx)}:{int(page_idx)}"

    @staticmethod
    def _slot_launch_id(slot: dict[str, Any] | None) -> str:
        return str((slot or {}).get("launch_id") or "").strip()

    @classmethod
    def _runtime_matches_slot(cls, runtime: BrowserSession | BrowserRuntime, slot: dict[str, Any]) -> bool:
        current_launch_id = cls._slot_launch_id(slot)
        runtime_launch_id = cls._slot_launch_id(getattr(runtime, "tunnel", None))
        if not current_launch_id:
            return True
        return runtime_launch_id == current_launch_id

    def _try_lock(self, key: str, ttl_sec: float, owner: str) -> str:
        info = CLIENT.lock_try(key, ttl_sec=ttl_sec, owner=owner)
        if not info or not bool(info.get("acquired")):
            return ""
        return str(info.get("token") or "")

    def _lock_until(self, key: str, ttl_sec: float, owner: str, wait_sec: float) -> str:
        deadline = time.time() + max(0.1, float(wait_sec))
        while time.time() < deadline:
            token = self._try_lock(key, ttl_sec, owner)
            if token:
                return token
            time.sleep(0.05)
        return ""

    def _release_lock(self, key: str, token: str) -> None:
        if not key or not token:
            return
        try:
            CLIENT.lock_release(key, token=token)
        except Exception:
            pass

    def _load_slots(self, cfg: SiteSessionConfig, allowed_slot_names: list[str] | None = None) -> list[dict[str, Any]]:
        by_name = {
            str(row.get("name") or ""): row
            for row in list_tunnels()
            if str(row.get("name") or "")
        }
        resolved: list[dict[str, Any]] = []
        slot_errors: list[str] = []
        quarantined = self._load_quarantine(cfg)
        tunnel_statuses = load_tunnel_statuses(cfg.egress_slots)
        if allowed_slot_names is None:
            slot_names = list(cfg.egress_slots)
        else:
            allowed = {str(name or "").strip() for name in allowed_slot_names if str(name or "").strip()}
            slot_names = [name for name in cfg.egress_slots if name in allowed]
        for name in slot_names:
            if name in quarantined:
                slot_errors.append(f"{name}: quarantined")
                continue
            if name == "direct":
                resolved.append(
                    {
                        "name": "direct",
                        "host": "direct",
                        "local_port": 0,
                        "proxy_server": "",
                        "launch_id": "direct",
                    }
                )
                continue
            row = by_name.get(name)
            if not row:
                slot_errors.append(f"{name}: not configured")
                continue
            status = dict(tunnel_statuses.get(name) or {})
            if not bool(status.get("alive")):
                slot_errors.append(
                    f"{name}: down port_open={bool(status.get('port_open'))} "
                    f"control_ok={bool(status.get('control_ok'))}"
                )
                continue
            port = int(status.get("local_port") or row.get("local_port") or 0)
            if not port:
                slot_errors.append(f"{name}: missing local_port")
                continue
            launch_id = str(status.get("launch_id") or "").strip()
            if not launch_id:
                slot_errors.append(f"{name}: missing launch_id")
                continue
            resolved.append(
                {
                    "name": name,
                    "host": str(row.get("host") or ""),
                    "local_port": port,
                    "proxy_server": f"socks5://127.0.0.1:{port}",
                    "launch_id": launch_id,
                }
            )
        if not resolved:
            detail = "; ".join(slot_errors) if slot_errors else "no live tunnels"
            raise RuntimeError(f"NO LIVE TUNNELS FOR {cfg.site}: {detail}")
        return resolved

    def _load_quarantine(self, cfg: SiteSessionConfig) -> dict[str, float]:
        raw = self._cache_get_obj(self._quarantine_key(cfg.site)) or {}
        if not isinstance(raw, dict):
            return {}
        now = time.time()
        out: dict[str, float] = {}
        for name, until in raw.items():
            try:
                until_f = float(until or 0.0)
            except Exception as exc:
                raise RuntimeError(
                    f"BAD QUARANTINE STATE {cfg.site} {name}: {type(exc).__name__}: {exc}"
                ) from exc
            if until_f > now:
                out[str(name)] = until_f
        if out != raw:
            self._cache_set_obj(
                self._quarantine_key(cfg.site),
                out,
                ttl_sec=self._quarantine_ttl_sec(out),
            )
        return out

    def _slot_is_quarantined(self, cfg: SiteSessionConfig, slot_name: str) -> bool:
        quarantined = self._load_quarantine(cfg)
        return slot_name in quarantined

    def _load_quarantine_backoff(self, cfg: SiteSessionConfig) -> dict[str, dict[str, float | int]]:
        raw = self._cache_get_obj(self._quarantine_backoff_key(cfg.site)) or {}
        if not isinstance(raw, dict):
            return {}
        now = time.time()
        grace_sec = max(60.0, float(cfg.slot_quarantine_sec))
        out: dict[str, dict[str, float | int]] = {}
        for name, row in raw.items():
            if not isinstance(row, dict):
                continue
            try:
                until = float(row.get("until") or 0.0)
                level = int(row.get("level") or 0)
            except Exception:
                continue
            if until <= 0.0:
                continue
            if now > (until + grace_sec):
                continue
            out[str(name)] = {"until": until, "level": max(0, int(level))}
        if out != raw:
            self._cache_set_obj(
                self._quarantine_backoff_key(cfg.site),
                out,
                ttl_sec=self._quarantine_backoff_ttl_sec(out, grace_sec),
            )
        return out

    def _next_quarantine_level(self, cfg: SiteSessionConfig, slot_name: str) -> tuple[int, int]:
        history = self._load_quarantine_backoff(cfg)
        current = dict(history.get(str(slot_name)) or {})
        now = time.time()
        prev_until = float(current.get("until") or 0.0)
        prev_level = max(0, int(current.get("level") or 0))
        grace_sec = max(60.0, float(cfg.slot_quarantine_sec))
        if prev_until > 0.0 and now <= (prev_until + grace_sec):
            next_level = min(prev_level + 1, len(QUARANTINE_STEP_LADDER_SEC) - 1)
        else:
            next_level = 0
        duration_sec = int(QUARANTINE_STEP_LADDER_SEC[next_level])
        return next_level, duration_sec

    def _mute_slot(self, cfg: SiteSessionConfig, slot_name: str, reason: str) -> None:
        if not slot_name:
            return
        state = self._load_quarantine(cfg)
        backoff = self._load_quarantine_backoff(cfg)
        if cfg.site == "11880":
            level = 0
            duration_sec = int(cfg.slot_quarantine_sec)
        else:
            level, duration_sec = self._next_quarantine_level(cfg, slot_name)
        until = time.time() + float(duration_sec)
        state[slot_name] = until
        self._cache_set_obj(
            self._quarantine_key(cfg.site),
            state,
            ttl_sec=self._quarantine_ttl_sec(state),
        )
        backoff[str(slot_name)] = {"level": int(level), "until": float(until)}
        self._cache_set_obj(
            self._quarantine_backoff_key(cfg.site),
            backoff,
            ttl_sec=self._quarantine_backoff_ttl_sec(backoff, max(60.0, float(cfg.slot_quarantine_sec))),
        )
        self._drop_egress_session_state(cfg, slot_name, clear_state=True)
        log(
            ROUTER_STATE_LOG_FILE,
            folder=LOG_FOLDER,
            message=(
                f"slot_quarantine site={cfg.site} tunnel={slot_name} reason={reason} "
                f"level={int(level) + 1} duration_sec={int(duration_sec)} until_ts={int(until)}"
            ),
        )

    @staticmethod
    def _profile_script(profile: BrowserProfile) -> str:
        langs = json.dumps(list(profile.languages), ensure_ascii=False)
        ua_meta = json.dumps(profile.user_agent_metadata, ensure_ascii=False)
        mime_types = json.dumps(
            [
                {
                    "type": "application/pdf",
                    "suffixes": "pdf",
                    "description": "Portable Document Format",
                },
                {
                    "type": "text/pdf",
                    "suffixes": "pdf",
                    "description": "Portable Document Format",
                },
            ],
            ensure_ascii=False,
        )
        plugins = json.dumps(
            [
                {
                    "name": "Chrome PDF Viewer",
                    "filename": "internal-pdf-viewer",
                    "description": "Portable Document Format",
                },
                {
                    "name": "Chromium PDF Viewer",
                    "filename": "internal-pdf-viewer",
                    "description": "Portable Document Format",
                },
                {
                    "name": "Microsoft Edge PDF Viewer",
                    "filename": "internal-pdf-viewer",
                    "description": "Portable Document Format",
                },
            ],
            ensure_ascii=False,
        )
        connection = json.dumps(
            {
                "downlink": profile.connection_downlink,
                "effectiveType": profile.connection_effective_type,
                "rtt": profile.connection_rtt,
                "saveData": False,
            },
            ensure_ascii=False,
        )
        return f"""
(() => {{
  const patch = (obj, key, value) => {{
    try {{
      Object.defineProperty(obj, key, {{ get: () => value, configurable: true }});
    }} catch (_) {{}}
  }};
  const defineValue = (obj, key, value) => {{
    try {{
      Object.defineProperty(obj, key, {{ value, configurable: true }});
    }} catch (_) {{}}
  }};
  const buildCollection = (rows, nameKey) => {{
    const items = rows.map((row, index) => {{
      const item = Object.assign({{}}, row);
      defineValue(item, 'index', index);
      return item;
    }});
    defineValue(items, 'item', (index) => items[index] || null);
    defineValue(items, 'namedItem', (name) => items.find((row) => row[nameKey] === name) || null);
    return items;
  }};
  const uaMeta = {ua_meta};
  const highEntropyValues = {{
    architecture: uaMeta.architecture,
    bitness: uaMeta.bitness,
    brands: uaMeta.brands,
    fullVersionList: uaMeta.fullVersionList,
    mobile: uaMeta.mobile,
    model: uaMeta.model,
    platform: uaMeta.platform,
    platformVersion: uaMeta.platformVersion,
    uaFullVersion: uaMeta.fullVersion,
    wow64: uaMeta.wow64,
  }};
  const mimeTypes = buildCollection({mime_types}, 'type');
  const plugins = buildCollection({plugins}, 'name');
  for (const plugin of plugins) {{
    defineValue(plugin, 'item', (index) => mimeTypes[index] || null);
    defineValue(plugin, 'namedItem', (name) => mimeTypes.find((entry) => entry.type === name) || null);
    patch(plugin, 'length', mimeTypes.length);
  }}
  for (const mimeType of mimeTypes) {{
    defineValue(mimeType, 'enabledPlugin', plugins[0] || null);
  }}
  const connection = Object.assign({{}}, {connection});
  defineValue(connection, 'addEventListener', () => undefined);
  defineValue(connection, 'removeEventListener', () => undefined);
  defineValue(connection, 'dispatchEvent', () => true);
  patch(navigator, 'webdriver', undefined);
  patch(navigator, 'platform', {json.dumps(profile.navigator_platform)});
  patch(navigator, 'vendor', {json.dumps(profile.navigator_vendor)});
  patch(navigator, 'language', {json.dumps(profile.languages[0])});
  patch(navigator, 'languages', {langs});
  patch(navigator, 'hardwareConcurrency', {int(profile.hardware_concurrency)});
  patch(navigator, 'deviceMemory', {int(profile.device_memory)});
  patch(navigator, 'maxTouchPoints', {int(profile.max_touch_points)});
  patch(navigator, 'vendorSub', '');
  patch(navigator, 'productSub', '20030107');
  patch(navigator, 'cookieEnabled', true);
  patch(navigator, 'onLine', true);
  patch(navigator, 'pdfViewerEnabled', true);
  patch(navigator, 'plugins', plugins);
  patch(navigator, 'mimeTypes', mimeTypes);
  patch(navigator, 'connection', connection);
  patch(navigator, 'userAgentData', {{
    brands: uaMeta.brands,
    mobile: false,
    platform: uaMeta.platform,
    getHighEntropyValues: async (hints) => {{
      const out = {{}};
      for (const hint of Array.isArray(hints) ? hints : []) {{
        if (Object.prototype.hasOwnProperty.call(highEntropyValues, hint)) {{
          out[hint] = highEntropyValues[hint];
        }}
      }}
      return out;
    }},
    toJSON: () => ({{ brands: uaMeta.brands, mobile: false, platform: uaMeta.platform }}),
  }});
  patch(screen, 'availWidth', {int(profile.avail_width)});
  patch(screen, 'availHeight', {int(profile.avail_height)});
  patch(screen, 'colorDepth', {int(profile.color_depth)});
  patch(screen, 'pixelDepth', {int(profile.pixel_depth)});
  patch(window, 'devicePixelRatio', {float(profile.device_scale_factor)});
  patch(window, 'outerWidth', {int(profile.outer_width)});
  patch(window, 'outerHeight', {int(profile.outer_height)});
  patch(window, 'screenX', 0);
  patch(window, 'screenY', 0);
  patch(window, 'screenLeft', 0);
  patch(window, 'screenTop', 0);
  try {{
    window.chrome = window.chrome || {{}};
    window.chrome.runtime = window.chrome.runtime || {{}};
    window.chrome.app = window.chrome.app || {{ isInstalled: false }};
    window.chrome.webstore = window.chrome.webstore || {{}};
    window.chrome.csi = window.chrome.csi || (() => ({{
      onloadT: Date.now(),
      startE: Date.now(),
      pageT: Math.max(1, Math.round(performance.now())),
      tran: 15,
    }}));
    window.chrome.loadTimes = window.chrome.loadTimes || (() => ({{
      requestTime: Date.now() / 1000,
      startLoadTime: Date.now() / 1000,
      commitLoadTime: Date.now() / 1000,
      finishDocumentLoadTime: Date.now() / 1000,
      finishLoadTime: Date.now() / 1000,
      firstPaintTime: Date.now() / 1000,
      firstPaintAfterLoadTime: 0,
      navigationType: 'Other',
      wasFetchedViaSpdy: true,
      wasNpnNegotiated: true,
      npnNegotiatedProtocol: 'h2',
      wasAlternateProtocolAvailable: false,
      connectionInfo: 'h2',
    }}));
  }} catch (_) {{}}
  try {{
    const orig = navigator.permissions && navigator.permissions.query;
    if (orig) {{
      navigator.permissions.query = (params) => (
        params && params.name === 'notifications'
          ? Promise.resolve({{ state: Notification.permission }})
          : orig.call(navigator.permissions, params)
      );
    }}
  }} catch (_) {{}}
}})();
"""

    def _pick_profile(self, site: str) -> BrowserProfile:
        site_name = str(site or "").strip()
        if not site_name:
            raise RuntimeError("profile selection requires site")
        if not BROWSER_PROFILES:
            raise RuntimeError("NO BROWSER PROFILES CONFIGURED")
        return random.choice(BROWSER_PROFILES)

    def _load_session_state(
        self,
        cfg: SiteSessionConfig,
        slot_name: str,
        slot_idx: int,
        slot_launch_id: str = "",
    ) -> dict[str, Any] | None:
        session_key = self._session_key(cfg.site, slot_name, slot_idx)
        state = self._cache_get_obj(session_key)
        if not isinstance(state, dict):
            return None
        if not state:
            return None
        current_launch_id = str(slot_launch_id or "").strip()
        if current_launch_id and str(state.get("slot_launch_id") or "").strip() != current_launch_id:
            self._clear_cache_obj(session_key)
            return None
        last_used_at = float(state.get("last_used_at") or 0.0)
        created_at = float(state.get("created_at") or 0.0)
        requests_total = int(state.get("requests_total") or 0)
        if requests_total >= cfg.max_requests_per_session:
            self._clear_cache_obj(session_key)
            return None
        if created_at and (time.time() - created_at) >= cfg.max_session_age_sec:
            self._clear_cache_obj(session_key)
            return None
        if last_used_at and (time.time() - last_used_at) >= SESSION_STATE_IDLE_MAX_SEC:
            self._clear_cache_obj(session_key)
            return None
        self._cache_set_obj(session_key, state, ttl_sec=SESSION_STATE_TTL_SEC)
        return state

    def _new_session_state(
        self,
        cfg: SiteSessionConfig,
        slot_idx: int,
        slot_launch_id: str = "",
        previous_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        previous = dict(previous_state or {}) if isinstance(previous_state, dict) else {}
        profile_name = str(previous.get("profile_name") or "").strip()
        profile = None
        if profile_name:
            for candidate in BROWSER_PROFILES:
                if candidate.name == profile_name:
                    profile = candidate
                    break
        if profile is None:
            profile = self._pick_profile(cfg.site)
        return {
            "session_id": uuid4().hex[:12],
            "profile_name": profile.name,
            "slot_idx": int(slot_idx),
            "created_at": now,
            "last_used_at": now,
            "requests_total": 0,
            "warmed": False,
            "current_url": "",
            "next_dispatch_ts": 0.0,
            "storage_state": {},
            "router_boot_id": ROUTER_BOOT_ID,
            "slot_launch_id": str(slot_launch_id or ""),
        }

    def _apply_state_to_runtime(self, session: BrowserSession, state: dict[str, Any]) -> None:
        if not isinstance(state, dict):
            return
        storage_state = dict(state.get("storage_state") or {})
        should_sync_http = storage_state != dict(session.storage_state or {})
        session.created_at = float(state.get("created_at") or session.created_at or time.time())
        session.last_used_at = float(state.get("last_used_at") or session.last_used_at or time.time())
        session.requests_total = int(state.get("requests_total") or 0)
        session.warmed = bool(state.get("warmed") is True)
        session.current_url = str(state.get("current_url") or "")
        session.next_dispatch_ts = float(state.get("next_dispatch_ts") or 0.0)
        session.storage_state = storage_state
        if should_sync_http:
            self._sync_http_session(session)

    def _merge_session_state(
        self,
        session: BrowserSession,
        current_state: dict[str, Any] | None,
        requests_delta: int,
    ) -> dict[str, Any]:
        merged = dict(current_state or {})
        merged["session_id"] = str(session.session_id)
        merged["profile_name"] = str(session.profile.name)
        merged["slot_idx"] = int(session.slot_idx)
        merged["created_at"] = float(merged.get("created_at") or session.created_at or time.time())
        merged["last_used_at"] = max(
            float(merged.get("last_used_at") or 0.0),
            float(session.last_used_at or 0.0),
            time.time(),
        )
        merged["requests_total"] = int(merged.get("requests_total") or 0) + max(0, int(requests_delta))
        merged["warmed"] = bool(bool(merged.get("warmed") is True) or session.warmed)
        merged["current_url"] = str(session.current_url or merged.get("current_url") or "")
        merged["next_dispatch_ts"] = 0.0
        merged["router_boot_id"] = ROUTER_BOOT_ID
        merged["slot_launch_id"] = self._slot_launch_id(session.tunnel)
        exported_state = export_storage_state(session.http_session, session.storage_state)
        merged["storage_state"] = exported_state if exported_state else dict(merged.get("storage_state") or {})
        return merged

    def _checkout_session_lease(
        self,
        cfg: SiteSessionConfig,
        slot: dict[str, Any],
        slot_idx: int,
    ) -> SessionLease | None:
        slot_name = str(slot["name"])
        gate_key = self._session_gate_key(cfg.site, slot_name, slot_idx)
        owner = f"{cfg.site}:{slot_name}:{int(slot_idx)}:{uuid4().hex}"
        gate_token = self._try_lock(gate_key, SESSION_GATE_TTL_SEC, owner)
        if not gate_token:
            return None
        try:
            session_key = self._session_key(cfg.site, slot_name, slot_idx)
            raw_state = self._cache_get_obj(session_key)
            current_launch_id = self._slot_launch_id(slot)
            state = self._load_session_state(cfg, slot_name, slot_idx, current_launch_id)
            if state is None:
                state = self._new_session_state(
                    cfg,
                    slot_idx,
                    slot_launch_id=current_launch_id,
                    previous_state=raw_state if isinstance(raw_state, dict) else None,
                )
            needs_warm = not bool(state.get("warmed") is True)
            warm_lock_key = ""
            warm_lock_token = ""
            if needs_warm:
                warm_lock_key = self._session_warm_key(cfg.site, slot_name, slot_idx)
                warm_lock_token = self._try_lock(warm_lock_key, SESSION_LEASE_TTL_SEC, owner)
                if not warm_lock_token:
                    return None
            page_lock_key = ""
            page_lock_token = ""
            for current_page_idx in range(int(cfg.concurrent_pages_per_session)):
                candidate_key = self._session_page_key(cfg.site, slot_name, slot_idx, current_page_idx)
                candidate_token = self._try_lock(candidate_key, SESSION_LEASE_TTL_SEC, owner)
                if not candidate_token:
                    continue
                page_lock_key = candidate_key
                page_lock_token = candidate_token
                break
            if not page_lock_token:
                self._release_lock(warm_lock_key, warm_lock_token)
                return None
            state = dict(state)
            state["slot_idx"] = int(slot_idx)
            state["last_used_at"] = max(float(state.get("last_used_at") or 0.0), time.time())
            state["next_dispatch_ts"] = 0.0
            self._cache_set_obj(session_key, state)
            return SessionLease(
                session_key=session_key,
                slot_name=slot_name,
                slot_idx=int(slot_idx),
                session_id=str(state.get("session_id") or ""),
                state=state,
                needs_warm=needs_warm,
                base_requests_total=int(state.get("requests_total") or 0),
                page_lock_key=page_lock_key,
                page_lock_token=page_lock_token,
                warm_lock_key=warm_lock_key,
                warm_lock_token=warm_lock_token,
            )
        finally:
            self._release_lock(gate_key, gate_token)

    def _drop_egress_session_state(self, cfg: SiteSessionConfig, slot_name: str, *, clear_state: bool = True) -> None:
        doomed: list[BrowserSession] = []
        doomed_browser: BrowserRuntime | None = None
        for slot_idx in range(cfg.sessions_per_egress):
            if clear_state:
                self._clear_cache_obj(self._session_key(cfg.site, slot_name, slot_idx))
        with self._runtime_cv:
            for runtime_key, runtime in list(self._runtimes.items()):
                if runtime.site != cfg.site or str(runtime.tunnel.get("name") or "") != str(slot_name):
                    continue
                if runtime.active_pages > 0:
                    continue
                self._runtimes.pop(runtime_key, None)
                doomed.append(runtime)
            browser_key = self._browser_key(cfg.site, slot_name)
            if not any(
                row.site == cfg.site and str(row.tunnel.get("name") or "") == str(slot_name)
                for row in self._runtimes.values()
            ):
                doomed_browser = self._browsers.pop(browser_key, None)
            self._runtime_cv.notify_all()
        for runtime in doomed:
            if not clear_state:
                try:
                    self._persist_session_state(cfg, runtime)
                except Exception:
                    pass
            self._close_session(runtime)
        if doomed_browser is not None:
            self._close_browser_runtime(doomed_browser)

    @staticmethod
    def _runtime_expired(cfg: SiteSessionConfig, runtime: BrowserSession) -> bool:
        now = time.time()
        if runtime.requests_total >= cfg.max_requests_per_session:
            return True
        if (now - float(runtime.created_at or 0.0)) >= float(cfg.max_session_age_sec):
            return True
        recycle_after_ts = float(runtime.recycle_after_ts or 0.0)
        return bool(recycle_after_ts and now >= recycle_after_ts)

    def reap_idle_runtimes(self) -> None:
        doomed: list[tuple[SiteSessionConfig, BrowserSession, bool]] = []
        doomed_browsers: list[BrowserRuntime] = []
        now = time.time()
        with self._runtime_cv:
            for runtime_key, runtime in list(self._runtimes.items()):
                if runtime.active_pages > 0:
                    continue
                cfg = SITE_CONFIGS[runtime.site]
                drop_runtime = self._slot_is_quarantined(cfg, runtime.tunnel["name"])
                clear_state = False
                if not drop_runtime:
                    clear_state = self._runtime_expired(cfg, runtime)
                if not drop_runtime and not clear_state and (now - float(runtime.last_used_at or 0.0)) < RUNTIME_IDLE_REAP_SEC:
                    continue
                self._runtimes.pop(runtime_key, None)
                doomed.append((cfg, runtime, clear_state))
            for browser_key, browser_runtime in list(self._browsers.items()):
                if any(self._browser_key(row.site, row.tunnel["name"]) == browser_key for row in self._runtimes.values()):
                    continue
                recycle_after_ts = float(browser_runtime.recycle_after_ts or 0.0)
                browser_expired = bool(recycle_after_ts and now >= recycle_after_ts)
                if (now - float(browser_runtime.last_used_at or 0.0)) < RUNTIME_IDLE_REAP_SEC and not browser_expired:
                    continue
                self._browsers.pop(browser_key, None)
                doomed_browsers.append(browser_runtime)
            self._runtime_cv.notify_all()

        for cfg, runtime, clear_state in doomed:
            if clear_state:
                self._clear_cache_obj(self._session_key(cfg.site, runtime.tunnel["name"], runtime.slot_idx))
            else:
                self._persist_session_state(cfg, runtime)
            self._close_session(runtime)
        for browser_runtime in doomed_browsers:
            self._close_browser_runtime(browser_runtime)

    def _session_candidates(
        self,
        cfg: SiteSessionConfig,
        excluded: set[tuple[str, int]],
        preferred_slot_name: str = "",
        preferred_slot_idx: int = -1,
        allowed_slot_names: list[str] | None = None,
    ) -> list[tuple[dict[str, Any], int]]:
        active = [row for row in self._load_slots(cfg, allowed_slot_names) if not self._slot_is_quarantined(cfg, row["name"])]
        if not active:
            raise RuntimeError(f"NO ACTIVE SLOTS FOR {cfg.site}")

        weighted: list[tuple[int, int, int, float, dict[str, Any], int, dict[str, Any] | None]] = []
        with self._runtime_cv:
            for slot in active:
                for slot_idx in range(cfg.sessions_per_egress):
                    key = (slot["name"], slot_idx)
                    if key in excluded:
                        continue
                    preferred_rank = 0 if (str(slot["name"]) == str(preferred_slot_name) and int(slot_idx) == int(preferred_slot_idx)) else 1
                    runtime = self._runtimes.get(self._runtime_key(cfg.site, slot["name"], slot_idx))
                    state = self._load_session_state(cfg, slot["name"], slot_idx, self._slot_launch_id(slot))
                    if runtime is not None and not self._runtime_matches_slot(runtime, slot):
                        runtime = None
                    if runtime is not None and self._runtime_expired(cfg, runtime):
                        runtime = None
                    has_local_runtime = 0 if runtime is not None else 1
                    local_uses = int((runtime.active_pages if runtime is not None else 0) or 0)
                    last_used_at = float(
                        (state or {}).get("last_used_at")
                        or (runtime.last_used_at if runtime is not None else 0.0)
                        or 0.0
                    )
                    weighted.append((preferred_rank, has_local_runtime, local_uses, last_used_at, slot, slot_idx))

        weighted.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
        return [(slot, slot_idx) for _, _, _, _, slot, slot_idx in weighted]

    def _create_session(
        self,
        cfg: SiteSessionConfig,
        tunnel: dict[str, Any],
        slot_idx: int,
        state: dict[str, Any] | None,
    ) -> BrowserSession:
        profile_name = str((state or {}).get("profile_name") or "")
        profile = None
        for candidate in BROWSER_PROFILES:
            if candidate.name == profile_name:
                profile = candidate
                break
        if profile is None:
            profile = self._pick_profile(cfg.site)
        storage_state = dict((state or {}).get("storage_state") or {})
        http_session = build_http_session(profile, tunnel, storage_state)

        return BrowserSession(
            session_id=str((state or {}).get("session_id") or uuid4().hex[:12]),
            site=cfg.site,
            profile=profile,
            tunnel=tunnel,
            slot_idx=int(slot_idx),
            browser=None,
            context=None,
            storage_state=storage_state,
            http_session=http_session,
            created_at=float((state or {}).get("created_at") or time.time()),
            last_used_at=float((state or {}).get("last_used_at") or time.time()),
            requests_total=int((state or {}).get("requests_total") or 0),
            warmed=bool((state or {}).get("warmed") is True),
            current_url=str((state or {}).get("current_url") or ""),
            active_pages=0,
            recycle_after_ts=time.time() + random.uniform(float(cfg.runtime_recycle_min_sec), float(cfg.runtime_recycle_max_sec)),
            next_dispatch_ts=float((state or {}).get("next_dispatch_ts") or 0.0),
        )

    def _launch_browser_runtime(self, cfg: SiteSessionConfig, tunnel: dict[str, Any]) -> BrowserRuntime:
        pw = self._ensure_playwright()
        launch_kwargs: dict[str, Any] = {
            "headless": True,
            "ignore_default_args": ["--enable-automation"],
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--lang=de-DE",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
                "--disable-default-apps",
                "--disable-features=AudioServiceOutOfProcess,BackForwardCache,MediaRouter,Translate",
                "--no-default-browser-check",
                "--no-first-run",
                "--renderer-process-limit=2",
            ],
        }
        if tunnel.get("proxy_server"):
            launch_kwargs["proxy"] = {"server": tunnel["proxy_server"]}
        browser = pw.chromium.launch(**launch_kwargs)
        now = time.time()
        return BrowserRuntime(
            browser_key=self._browser_key(cfg.site, tunnel["name"]),
            site=cfg.site,
            tunnel=tunnel,
            browser=browser,
            created_at=now,
            last_used_at=now,
            recycle_after_ts=now + random.uniform(float(cfg.runtime_recycle_min_sec), float(cfg.runtime_recycle_max_sec)),
        )

    def _checkout_browser_runtime(self, cfg: SiteSessionConfig, tunnel: dict[str, Any]) -> BrowserRuntime:
        browser_key = self._browser_key(cfg.site, tunnel["name"])
        doomed: BrowserRuntime | None = None
        with self._runtime_cv:
            existing = self._browsers.get(browser_key)
            if existing is not None:
                if not self._runtime_matches_slot(existing, tunnel):
                    self._browsers.pop(browser_key, None)
                    doomed = existing
                    existing = None
                else:
                    existing.last_used_at = time.time()
                    return existing
        if doomed is not None:
            self._close_browser_runtime(doomed)
        created = self._launch_browser_runtime(cfg, tunnel)
        doomed = None
        with self._runtime_cv:
            existing = self._browsers.get(browser_key)
            if existing is not None:
                if not self._runtime_matches_slot(existing, tunnel):
                    self._browsers.pop(browser_key, None)
                    doomed = existing
                    existing = None
                else:
                    existing.last_used_at = time.time()
                    self._close_browser_runtime(created)
                    return existing
            if doomed is not None:
                self._close_browser_runtime(doomed)
            self._browsers[browser_key] = created
            self._runtime_cv.notify_all()
            return created

    def _checkout_runtime(
        self,
        cfg: SiteSessionConfig,
        slot: dict[str, Any],
        slot_idx: int,
    ) -> tuple[BrowserSession, SessionLease] | None:
        lease = self._checkout_session_lease(cfg, slot, int(slot_idx))
        if lease is None:
            return None
        runtime_key = self._runtime_key(cfg.site, slot["name"], slot_idx)
        doomed: BrowserSession | None = None
        with self._runtime_cv:
            runtime = self._runtimes.get(runtime_key)
            if runtime is not None:
                if (
                    str(runtime.session_id) != str(lease.session_id)
                    or not self._runtime_matches_slot(runtime, slot)
                ):
                    if runtime.active_pages > 0:
                        self._release_lock(lease.page_lock_key, lease.page_lock_token)
                        self._release_lock(lease.warm_lock_key, lease.warm_lock_token)
                        return None
                    self._runtimes.pop(runtime_key, None)
                    doomed = runtime
                    runtime = None
                if runtime is not None:
                    self._apply_state_to_runtime(runtime, lease.state)
                    runtime.next_dispatch_ts = float(lease.state.get("next_dispatch_ts") or 0.0)
                    runtime.last_used_at = time.time()
                    runtime.active_pages += 1
                    self._runtime_cv.notify_all()
                    return runtime, lease

        if doomed is not None:
            self._close_session(doomed)

        runtime = None
        try:
            runtime = self._create_session(cfg, slot, slot_idx, lease.state)
            runtime.next_dispatch_ts = float(lease.state.get("next_dispatch_ts") or 0.0)
            runtime.active_pages = 1
            with self._runtime_cv:
                existing = self._runtimes.get(runtime_key)
                if existing is not None:
                    if (
                        str(existing.session_id) != str(lease.session_id)
                        or not self._runtime_matches_slot(existing, slot)
                    ):
                        if existing.active_pages > 0:
                            self._release_lock(lease.page_lock_key, lease.page_lock_token)
                            self._release_lock(lease.warm_lock_key, lease.warm_lock_token)
                            self._close_session(runtime)
                            return None
                        self._runtimes.pop(runtime_key, None)
                        doomed = existing
                        existing = None
                    if existing is not None:
                        self._apply_state_to_runtime(existing, lease.state)
                        existing.next_dispatch_ts = float(lease.state.get("next_dispatch_ts") or 0.0)
                        existing.last_used_at = time.time()
                        existing.active_pages += 1
                        self._runtime_cv.notify_all()
                        self._close_session(runtime)
                        return existing, lease
                self._runtimes[runtime_key] = runtime
                self._runtime_cv.notify_all()
            if doomed is not None:
                self._close_session(doomed)
            return runtime, lease
        except Exception:
            self._release_lock(lease.page_lock_key, lease.page_lock_token)
            self._release_lock(lease.warm_lock_key, lease.warm_lock_token)
            if runtime is not None:
                self._close_session(runtime)
            raise

    def _close_session(self, session: BrowserSession) -> None:
        try:
            if session.context is not None:
                session.context.close()
        except Exception:
            pass
        try:
            if session.http_session is not None:
                session.http_session.close()
        except Exception:
            pass

    def _close_browser_runtime(self, browser_runtime: BrowserRuntime) -> None:
        try:
            browser_runtime.browser.close()
        except Exception:
            pass

    def _persist_session_state(self, cfg: SiteSessionConfig, session: BrowserSession) -> None:
        slot_name = str(session.tunnel["name"])
        slot_idx = int(session.slot_idx)
        gate_key = self._session_gate_key(cfg.site, slot_name, slot_idx)
        owner = f"persist:{cfg.site}:{slot_name}:{slot_idx}:{uuid4().hex}"
        gate_token = self._lock_until(gate_key, SESSION_GATE_TTL_SEC, owner, SESSION_GATE_WAIT_SEC)
        if not gate_token:
            raise RuntimeError(f"SESSION GATE BUSY {cfg.site} {slot_name}:{slot_idx}")
        try:
            current_state = self._load_session_state(
                cfg,
                slot_name,
                slot_idx,
                self._slot_launch_id(session.tunnel),
            ) or {}
            requests_delta = max(
                0,
                int(session.requests_total) - int(current_state.get("requests_total") or 0),
            )
            merged = self._merge_session_state(session, current_state, requests_delta=requests_delta)
            session.requests_total = int(merged.get("requests_total") or session.requests_total)
            session.next_dispatch_ts = float(merged.get("next_dispatch_ts") or session.next_dispatch_ts)
            self._cache_set_obj(self._session_key(cfg.site, slot_name, slot_idx), merged)
        finally:
            self._release_lock(gate_key, gate_token)

    def _sync_http_session(self, session: BrowserSession) -> None:
        storage_state = dict(session.storage_state or {})
        old_http = session.http_session
        session.http_session = build_http_session(session.profile, session.tunnel, storage_state)
        if old_http is not None:
            try:
                old_http.close()
            except Exception:
                pass

    def _capture_browser_state(self, session: BrowserSession, page: Any) -> None:
        try:
            session.storage_state = dict(page.context.storage_state() or {})
        except Exception:
            session.storage_state = dict(session.storage_state or {})
        self._sync_http_session(session)

    def _session_has_live_cookies(self, session: BrowserSession) -> bool:
        try:
            has_http_cookies = bool(cookie_snapshot(session.http_session))
        except Exception:
            has_http_cookies = False
        return storage_state_has_cookies(session.storage_state) or has_http_cookies

    def _release_runtime(
        self,
        cfg: SiteSessionConfig,
        session: BrowserSession,
        lease: SessionLease,
        *,
        clear_state: bool = False,
        drop_runtime: bool = False,
    ) -> None:
        runtime_key = self._runtime_key(cfg.site, session.tunnel["name"], session.slot_idx)
        browser_key = self._browser_key(cfg.site, session.tunnel["name"])
        should_close = False
        close_browser: BrowserRuntime | None = None
        merged_state: dict[str, Any] | None = None
        slot_name = str(session.tunnel["name"])
        slot_idx = int(session.slot_idx)
        gate_key = self._session_gate_key(cfg.site, slot_name, slot_idx)
        owner = f"release:{cfg.site}:{slot_name}:{slot_idx}:{uuid4().hex}"

        gate_token = self._lock_until(gate_key, SESSION_GATE_TTL_SEC, owner, SESSION_GATE_WAIT_SEC)
        if not gate_token:
            raise RuntimeError(f"SESSION GATE BUSY {cfg.site} {slot_name}:{slot_idx}")

        try:
            current_state = self._load_session_state(
                cfg,
                slot_name,
                slot_idx,
                self._slot_launch_id(session.tunnel),
            ) or {}
            if str(current_state.get("session_id") or session.session_id) != str(session.session_id):
                current_state = {}
            requests_delta = max(0, int(session.requests_total) - int(lease.base_requests_total))
            if not clear_state:
                merged_state = self._merge_session_state(session, current_state, requests_delta)
                session.requests_total = int(merged_state.get("requests_total") or session.requests_total)
                session.next_dispatch_ts = float(merged_state.get("next_dispatch_ts") or session.next_dispatch_ts)
            with self._runtime_cv:
                session.active_pages = max(0, int(session.active_pages) - 1)
                session.last_used_at = time.time()
                browser_runtime = self._browsers.get(browser_key)
                if browser_runtime is not None:
                    browser_runtime.last_used_at = session.last_used_at
                runtime_is_expired = False
                if merged_state is not None:
                    session.created_at = float(merged_state.get("created_at") or session.created_at)
                    session.warmed = bool(merged_state.get("warmed") is True)
                    runtime_is_expired = (
                        int(merged_state.get("requests_total") or 0) >= cfg.max_requests_per_session
                        or (session.last_used_at - float(merged_state.get("created_at") or session.created_at or 0.0)) >= float(cfg.max_session_age_sec)
                    )
                if session.active_pages <= 0 and (drop_runtime or clear_state or self._slot_is_quarantined(cfg, slot_name) or runtime_is_expired or self._runtime_expired(cfg, session)):
                    self._runtimes.pop(runtime_key, None)
                    should_close = True
                    if not any(self._browser_key(row.site, row.tunnel["name"]) == browser_key for row in self._runtimes.values()):
                        browser_runtime = self._browsers.get(browser_key)
                        if browser_runtime is not None:
                            recycle_after_ts = float(browser_runtime.recycle_after_ts or 0.0)
                            browser_expired = bool(recycle_after_ts and time.time() >= recycle_after_ts)
                        else:
                            browser_expired = False
                        if browser_runtime is not None and (
                            clear_state
                            or self._slot_is_quarantined(cfg, slot_name)
                            or browser_expired
                        ):
                            close_browser = self._browsers.pop(browser_key, None)
                self._runtime_cv.notify_all()

            if clear_state:
                self._clear_cache_obj(self._session_key(cfg.site, slot_name, slot_idx))
            elif merged_state is not None:
                self._cache_set_obj(self._session_key(cfg.site, slot_name, slot_idx), merged_state)
        finally:
            self._release_lock(lease.page_lock_key, lease.page_lock_token)
            self._release_lock(lease.warm_lock_key, lease.warm_lock_token)
            self._release_lock(gate_key, gate_token)

        if not should_close:
            return

        self._close_session(session)
        if close_browser is not None:
            self._close_browser_runtime(close_browser)

    def _all_headers_or_empty(self, source: Any, source_name: str) -> dict[str, Any]:
        if source is None:
            return {}
        try:
            return dict(source.all_headers())
        except Exception as exc:
            return {"_error": f"{source_name}_headers_unavailable: {type(exc).__name__}: {exc}"}

    @staticmethod
    def _blocked_resource_types(cfg: SiteSessionConfig) -> set[str]:
        if cfg.site in {"11880", "gs"}:
            return {"image", "font", "media", "texttrack", "object", "manifest"}
        return set()

    def _install_page_resource_filter(self, context: Any, cfg: SiteSessionConfig) -> dict[str, str]:
        blocked_types = self._blocked_resource_types(cfg)
        state = {"skipped_reason": ""}

        def _handle_route(route: Any) -> None:
            try:
                request = route.request
                resource_type = str(getattr(request, "resource_type", "") or "").strip().lower()
                if resource_type == "document":
                    response = route.fetch(max_redirects=1, timeout=float(cfg.browser_timeout_ms))
                    route.fulfill(response=response)
                    return
                if resource_type in blocked_types:
                    route.abort()
                    return
                route.continue_()
                return
            except Exception as exc:
                msg = str(exc or "").strip()
                if "redirect" in msg.lower():
                    state["skipped_reason"] = "SKIPPED REDIRECT LIMIT"
                    try:
                        route.abort(error_code="blockedbyclient")
                    except Exception:
                        pass
                    return
                try:
                    route.continue_()
                except Exception:
                    pass
                return

        context.route("**/*", _handle_route)
        return state

    def _new_page(self, session: BrowserSession, cfg: SiteSessionConfig) -> tuple[Any, Any, dict[str, str]]:
        browser_runtime = self._checkout_browser_runtime(cfg, session.tunnel)
        context_kwargs: dict[str, Any] = {
            "user_agent": session.profile.user_agent,
            "locale": session.profile.locale,
            "timezone_id": session.profile.timezone_id,
            "viewport": {"width": session.profile.viewport_width, "height": session.profile.viewport_height},
            "screen": {"width": session.profile.screen_width, "height": session.profile.screen_height},
            "color_scheme": "light",
            "device_scale_factor": session.profile.device_scale_factor,
            "has_touch": bool(session.profile.max_touch_points > 0),
            "ignore_https_errors": True,
            "reduced_motion": "reduce",
            "service_workers": "block",
        }
        if isinstance(session.storage_state, dict) and session.storage_state:
            context_kwargs["storage_state"] = session.storage_state
        context = browser_runtime.browser.new_context(**context_kwargs)
        context.add_init_script(self._profile_script(session.profile))
        route_state = self._install_page_resource_filter(context, cfg)
        page = context.new_page()
        cdp = context.new_cdp_session(page)
        cdp.send(
            "Network.setUserAgentOverride",
            {
                "userAgent": session.profile.user_agent,
                "acceptLanguage": session.profile.accept_language,
                "platform": session.profile.platform,
                "userAgentMetadata": session.profile.user_agent_metadata,
            },
        )
        return context, page, route_state

    @staticmethod
    def _close_page_context(page: Any, context: Any) -> None:
        if page is not None:
            try:
                page.close()
            except Exception:
                pass
        if context is not None:
            try:
                context.close()
            except Exception:
                pass

    def _humanize(self, page: Any, profile: BrowserProfile) -> None:
        first_x = max(32, int(profile.viewport_width * random.uniform(0.18, 0.36)))
        first_y = max(32, int(profile.viewport_height * random.uniform(0.16, 0.34)))
        second_x = max(48, int(profile.viewport_width * random.uniform(0.44, 0.72)))
        second_y = max(48, int(profile.viewport_height * random.uniform(0.38, 0.68)))
        try:
            page.bring_to_front()
        except Exception:
            pass
        try:
            page.wait_for_timeout(random.randint(90, 180))
            page.mouse.move(first_x, first_y, steps=random.randint(7, 12))
            page.wait_for_timeout(random.randint(70, 140))
            page.mouse.move(second_x, second_y, steps=random.randint(6, 10))
            if profile.viewport_height >= 760:
                page.wait_for_timeout(random.randint(60, 120))
                page.mouse.wheel(0, random.randint(90, 220))
                page.wait_for_timeout(random.randint(80, 160))
                page.mouse.wheel(0, -random.randint(40, 120))
            page.wait_for_timeout(random.randint(120, 240))
        except Exception:
            pass

    @staticmethod
    def _cookie_names(page: Any, url: str) -> set[str]:
        try:
            return {str(row.get("name") or "") for row in (page.context.cookies([url]) or []) if str(row.get("name") or "")}
        except Exception:
            return set()

    def _home_cookies_ready(self, page: Any, cfg: SiteSessionConfig, url: str) -> bool:
        cookie_names = self._cookie_names(page, url)
        if cfg.site == "11880":
            return bool(cookie_names & {"PHPSESSID", "randomSeed", "__cf_bm", "referrer"})
        if cfg.site == "gs":
            return any(name.startswith("__cmp") for name in cookie_names) or "utag_main" in cookie_names
        return bool(cookie_names)

    def _wait_home_cookies(self, page: Any, cfg: SiteSessionConfig, url: str) -> None:
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if self._home_cookies_ready(page, cfg, url):
                try:
                    page.wait_for_timeout(180)
                except Exception:
                    pass
                return
            try:
                page.wait_for_timeout(120)
            except Exception:
                time.sleep(0.12)

    def _minimal_page_ready(self, page: Any, cfg: SiteSessionConfig, kind: str, url: str) -> bool:
        kind_s = str(kind or "")
        if kind_s == "home":
            return self._home_cookies_ready(page, cfg, url)
        cookie_names = self._cookie_names(page, url)
        if kind_s == "search":
            try:
                ready = page.evaluate(
                    """
                    ({ site }) => {
                      if (site === 'gs') {
                        return !!(
                          document.querySelector('article.mod.mod-Treffer a[href*="/gsbiz/"]') ||
                          document.querySelector('#mod-LoadMore')
                        );
                      }
                      if (site === '11880') {
                        const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
                        if (scripts.some((el) => (el.textContent || '').includes('itemListElement'))) return true;
                        return !!document.querySelector('a[href*="/branchenbuch/"]');
                      }
                      return true;
                    }
                    """,
                    {"site": cfg.site},
                )
                if bool(ready):
                    return True
            except Exception:
                pass
            return bool(cookie_names)
        return True

    def _wait_minimal_ready(self, page: Any, cfg: SiteSessionConfig, kind: str, url: str) -> None:
        timeout_ms = 1200 if str(kind or "") == "home" else 1500
        deadline = time.time() + (timeout_ms / 1000.0)
        while time.time() < deadline:
            if self._minimal_page_ready(page, cfg, kind, url):
                try:
                    page.wait_for_timeout(180)
                except Exception:
                    pass
                return
            time.sleep(0.05)

    def _looks_blocked(self, status: int, title: str, html: str) -> bool:
        title = str(title or "")
        html = str(html or "")
        if int(status or 0) in {403, 429}:
            return True
        if not title and not html:
            return False
        return "Nur einen Moment" in title or "challenge-platform" in html or "error code: 1015" in html

    def _should_quarantine_response(self, cfg: SiteSessionConfig, status: int, title: str, html: str) -> bool:
        status_code = int(status or 0)
        if cfg.site == "11880" and status_code in {403, 429}:
            return True
        return self._looks_blocked(status_code, title, html)

    @staticmethod
    def _resolve_request_mode(
        cfg: SiteSessionConfig,
        kind: str,
        requested_mode: str,
        requests_total_before_fetch: int,
    ) -> str:
        requested = str(requested_mode or "")
        if requested == "browser_click":
            requested = "index_browser"
        if cfg.site != "11880":
            return requested
        browser_mode = "index_browser"
        if random.random() < 0.99:
            return "http_only"
        return browser_mode

    def _should_try_click(self, session: BrowserSession, url: str, referer: str) -> bool:
        if not referer:
            return False
        url_parts = urlsplit(str(url or ""))
        referer_parts = urlsplit(str(referer or ""))
        same_origin = bool(
            url_parts.scheme
            and url_parts.netloc
            and url_parts.scheme == referer_parts.scheme
            and url_parts.netloc == referer_parts.netloc
        )
        if not same_origin:
            return False
        if session.site == "11880":
            return True
        if random.random() > 0.35:
            return False
        return True

    def _click_to_target(self, page: Any, target_url: str, timeout_ms: int) -> bool:
        parsed = urlsplit(str(target_url or ""))
        candidate_hrefs: list[str] = []
        full_url = str(target_url or "")
        if full_url:
            candidate_hrefs.append(full_url)
        path_with_query = parsed.path or "/"
        if parsed.query:
            path_with_query = f"{path_with_query}?{parsed.query}"
        candidate_hrefs.append(path_with_query)
        if parsed.path:
            candidate_hrefs.append(parsed.path)
        selectors = []
        for href in candidate_hrefs:
            if not href or href in selectors:
                continue
            href_escaped = href.replace('"', '\\"')
            selectors.append(f'a[href="{href_escaped}"]')
        for selector in selectors:
            try:
                loc = page.locator(selector).first
                if loc.count() < 1:
                    continue
                with page.expect_navigation(wait_until="domcontentloaded", timeout=timeout_ms):
                    loc.click(timeout=min(3000, timeout_ms))
                return True
            except Exception:
                continue
        return False

    def _simulate_index_clicks(self, page: Any, cfg: SiteSessionConfig) -> int:
        if cfg.site not in {"gs", "11880"}:
            return 0
        try:
            clicked = page.evaluate(
                """
                ({ site }) => {
                  const selector = site === '11880'
                    ? 'a[href*="/branchenbuch/"]'
                    : 'article.mod.mod-Treffer a[href*="/gsbiz/"]';
                  const roots = Array.from(document.querySelectorAll(selector));
                  const seen = new Set();
                  const links = [];
                  for (const a of roots) {
                    const href = (a.getAttribute('href') || '').trim();
                    if (!href || seen.has(href)) continue;
                    seen.add(href);
                    links.push(a);
                  }
                  if (!links.length) return 0;
                  const stop = (ev) => {
                    ev.preventDefault();
                    ev.stopPropagation();
                    ev.stopImmediatePropagation();
                  };
                  document.addEventListener('click', stop, true);
                  document.addEventListener('auxclick', stop, true);
                  let total = 0;
                  for (const a of links) {
                    const rect = a.getBoundingClientRect();
                    const common = {
                      bubbles: true,
                      cancelable: true,
                      composed: true,
                      clientX: Math.max(1, rect.left + Math.min(rect.width / 2, 24)),
                      clientY: Math.max(1, rect.top + Math.min(rect.height / 2, 12)),
                      button: 0,
                    };
                    for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup', 'click']) {
                      a.dispatchEvent(new MouseEvent(type, common));
                    }
                    total += 1;
                  }
                  document.removeEventListener('click', stop, true);
                  document.removeEventListener('auxclick', stop, true);
                  return total;
                }
                """,
                {"site": cfg.site},
            )
            try:
                page.wait_for_timeout(100)
            except Exception:
                pass
            return int(clicked or 0)
        except Exception:
            return 0

    def _fetch_index_browser_once(
        self,
        session: BrowserSession,
        cfg: SiteSessionConfig,
        url: str,
        kind: str,
        task_id: int,
        cb_id: int,
        referer: str = "",
    ) -> FetchResult:
        context = None
        page = None
        route_state: dict[str, str] | None = None
        try:
            context, page, route_state = self._new_page(session, cfg)
            result = self._fetch_once(session, page, cfg, url, kind, task_id, cb_id, referer, route_state=route_state)
            if result.status == 200 and not self._looks_blocked(result.status, result.title, result.html):
                self._capture_browser_state(session, page)
            return result
        finally:
            if page is not None:
                try:
                    page.close()
                except Exception:
                    pass
            if context is not None:
                try:
                    context.close()
                except Exception:
                    pass

    def _fetch_once(
        self,
        session: BrowserSession,
        page: Any,
        cfg: SiteSessionConfig,
        url: str,
        kind: str,
        task_id: int,
        cb_id: int,
        referer: str = "",
        route_state: dict[str, str] | None = None,
    ) -> FetchResult:
        session.current_url = url
        cookies_before = []
        try:
            cookies_before = page.context.cookies([url])
        except Exception:
            cookies_before = []
        nav_started = time.time()
        self._log_fetch_start(
            log_file=HTTP_CHROMIUM_LOG_FILE,
            site=cfg.site,
            has_cookies=bool(cookies_before),
            tunnel=session.tunnel,
            url=url,
        )
        try:
            response = page.goto(
                url,
                referer=referer or None,
                wait_until="domcontentloaded",
                timeout=cfg.browser_timeout_ms,
            )
        except PlaywrightError as exc:
            nav_elapsed_ms = int((time.time() - nav_started) * 1000)
            skipped_reason = str((route_state or {}).get("skipped_reason") or "").strip()
            if skipped_reason or "ERR_TOO_MANY_REDIRECTS" in str(exc or ""):
                final_url = str(getattr(page, "url", "") or url)
                self._log_fetch_done(
                    log_file=HTTP_CHROMIUM_LOG_FILE,
                    site=cfg.site,
                    has_cookies=bool(cookies_before),
                    tunnel=session.tunnel,
                    final_url=final_url or url,
                    status=0,
                    ms=nav_elapsed_ms,
                )
                raise SkippedFetchError(skipped_reason or "SKIPPED REDIRECT LIMIT") from exc
            raise
        final_url = str(page.url)
        status = int(response.status) if response else 0
        nav_elapsed_ms = int((time.time() - nav_started) * 1000)
        self._log_fetch_done(
            log_file=HTTP_CHROMIUM_LOG_FILE,
            site=cfg.site,
            has_cookies=bool(cookies_before),
            tunnel=session.tunnel,
            final_url=final_url or url,
            status=status,
            ms=nav_elapsed_ms,
        )
        self._wait_minimal_ready(page, cfg, kind, url)
        if str(kind or "") == "home":
            self._humanize(page, session.profile)
            self._wait_home_cookies(page, cfg, url)
        html = page.content()
        title = page.title() or ""
        session.last_used_at = time.time()
        session.current_url = final_url or url
        session.requests_total += 1
        self._capture_browser_state(session, page)
        return FetchResult(
            status=status,
            url=url,
            final_url=final_url,
            html=html,
            title=title,
            ms=nav_elapsed_ms,
            site=cfg.site,
            session_id=session.session_id,
            session_slot=int(session.slot_idx),
            tunnel=dict(session.tunnel),
        )

    def _fetch_http_once(
        self,
        session: BrowserSession,
        cfg: SiteSessionConfig,
        url: str,
        kind: str,
        task_id: int,
        cb_id: int,
        referer: str = "",
        method: str = "GET",
        form: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> FetchResult:
        session.current_url = url
        method_s = str(method or "GET").upper()
        started = time.time()
        had_cookies = False
        payload: dict[str, Any] | None = None
        with session.http_mu:
            cookies_before = cookie_snapshot(session.http_session)
            had_cookies = bool(cookies_before)
        self._log_fetch_start(
            log_file=HTTP_LIGHT_LOG_FILE,
            site=cfg.site,
            has_cookies=had_cookies,
            tunnel=session.tunnel,
            url=url,
        )
        try:
            with session.http_mu:
                try:
                    payload = fetch_html(
                        session.http_session,
                        session.profile,
                        url,
                        referer=referer or "",
                        timeout_ms=cfg.browser_timeout_ms,
                        method=method_s,
                        form=dict(form or {}) or None,
                        extra_headers=dict(extra_headers or {}) or None,
                    )
                finally:
                    session.storage_state = export_storage_state(session.http_session, session.storage_state)
        except SkippedFetchError:
            elapsed_ms = int((time.time() - started) * 1000)
            self._log_fetch_done(
                log_file=HTTP_LIGHT_LOG_FILE,
                site=cfg.site,
                has_cookies=had_cookies,
                tunnel=session.tunnel,
                final_url=url,
                status=0,
                ms=elapsed_ms,
            )
            raise
        final_url = str(payload.get("final_url") or url)
        status = int(payload.get("status") or 0)
        html = str(payload.get("html") or "")
        title = ""
        low = html.lower()
        p1 = low.find("<title>")
        p2 = low.find("</title>")
        if p1 >= 0 and p2 > p1:
            title = html[p1 + 7 : p2].strip()
        finished = time.time()
        elapsed_ms = int((finished - started) * 1000)
        self._log_fetch_done(
            log_file=HTTP_LIGHT_LOG_FILE,
            site=cfg.site,
            has_cookies=had_cookies,
            tunnel=session.tunnel,
            final_url=final_url or url,
            status=status,
            ms=elapsed_ms,
        )
        session.last_used_at = time.time()
        session.current_url = final_url or url
        session.requests_total += 1
        return FetchResult(
            status=status,
            url=url,
            final_url=final_url,
            html=html,
            title=title,
            ms=elapsed_ms,
            site=cfg.site,
            session_id=session.session_id,
            session_slot=int(session.slot_idx),
            tunnel=dict(session.tunnel),
        )

    def _fetch_browser_click_once(
        self,
        session: BrowserSession,
        cfg: SiteSessionConfig,
        url: str,
        kind: str,
        task_id: int,
        cb_id: int,
        referer: str = "",
    ) -> FetchResult | None:
        if not self._should_try_click(session, url, referer):
            return None
        session.current_url = url
        context = None
        page = None
        route_state: dict[str, str] | None = None
        try:
            context, page, route_state = self._new_page(session, cfg)
            warm_ref = self._fetch_once(session, page, cfg, referer, "referer", task_id, cb_id, "", route_state=route_state)
            if warm_ref.status != 200 or self._looks_blocked(warm_ref.status, warm_ref.title, warm_ref.html):
                return None
            self._humanize(page, session.profile)
            cookies_before = False
            try:
                cookies_before = bool(page.context.cookies([url]))
            except Exception:
                cookies_before = False
            click_started = time.time()
            self._log_fetch_start(
                log_file=HTTP_CHROMIUM_LOG_FILE,
                site=cfg.site,
                has_cookies=cookies_before,
                tunnel=session.tunnel,
                url=url,
            )
            clicked = self._click_to_target(page, url, cfg.browser_timeout_ms)
            if not clicked:
                return None
            final_url = str(page.url)
            status = 200
            elapsed_ms = int((time.time() - click_started) * 1000)
            self._log_fetch_done(
                log_file=HTTP_CHROMIUM_LOG_FILE,
                site=cfg.site,
                has_cookies=cookies_before,
                tunnel=session.tunnel,
                final_url=final_url or url,
                status=status,
                ms=elapsed_ms,
            )
            html = page.content()
            title = page.title() or ""
            if final_url != url and not final_url.startswith(url):
                return None
            session.last_used_at = time.time()
            session.current_url = final_url or url
            session.requests_total += 1
            self._capture_browser_state(session, page)
            return FetchResult(
                status=status,
                url=url,
                final_url=final_url,
                html=html,
                title=title,
                ms=elapsed_ms,
                site=cfg.site,
                session_id=session.session_id,
                session_slot=int(session.slot_idx),
                tunnel=dict(session.tunnel),
            )
        except PlaywrightTimeoutError:
            return None
        finally:
            if page is not None:
                try:
                    page.close()
                except Exception:
                    pass
            if context is not None:
                try:
                    context.close()
                except Exception:
                    pass

    def _warm_session(self, session: BrowserSession, cfg: SiteSessionConfig, task_id: int, cb_id: int) -> FetchResult:
        context = None
        page = None
        route_state: dict[str, str] | None = None
        try:
            context, page, route_state = self._new_page(session, cfg)
            warm = self._fetch_once(session, page, cfg, cfg.home_url, "home", task_id, cb_id, "", route_state=route_state)
            if warm.status != 200 or self._looks_blocked(warm.status, warm.title, warm.html):
                return warm
            session.warmed = True
            self._persist_session_state(cfg, session)
            return warm
        finally:
            self._close_page_context(page, context)

    def _run_mode_fetch(
        self,
        session: BrowserSession,
        cfg: SiteSessionConfig,
        url: str,
        kind: str,
        task_id: int,
        cb_id: int,
        referer: str = "",
        mode: str = "",
        method: str = "GET",
        form: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[FetchResult, str]:
        if mode == "index_browser":
            result = self._fetch_index_browser_once(
                session,
                cfg,
                url,
                kind,
                task_id,
                cb_id,
                referer or session.current_url,
            )
            self._persist_session_state(cfg, session)
            return result, HTTP_CHROMIUM_LOG_FILE

        if mode == "browser_click":
            result = self._fetch_index_browser_once(
                session,
                cfg,
                url,
                kind,
                task_id,
                cb_id,
                referer or session.current_url,
            )
            self._persist_session_state(cfg, session)
            return result, HTTP_CHROMIUM_LOG_FILE

        if mode == "http_only":
            if not self._session_has_live_cookies(session):
                result = self._fetch_index_browser_once(
                    session,
                    cfg,
                    url,
                    kind,
                    task_id,
                    cb_id,
                    referer or session.current_url,
                )
                self._persist_session_state(cfg, session)
                return result, HTTP_CHROMIUM_LOG_FILE
            return (
                self._fetch_http_once(
                    session,
                    cfg,
                    url,
                    kind,
                    task_id,
                    cb_id,
                    referer or session.current_url,
                    method=method,
                    form=form,
                    extra_headers=extra_headers,
                ),
                HTTP_LIGHT_LOG_FILE,
            )

        if not self._session_has_live_cookies(session):
            result = self._fetch_index_browser_once(
                session,
                cfg,
                url,
                kind,
                task_id,
                cb_id,
                referer or session.current_url,
            )
            self._persist_session_state(cfg, session)
            return result, HTTP_CHROMIUM_LOG_FILE

        return (
            self._fetch_http_once(
                session,
                cfg,
                url,
                kind,
                task_id,
                cb_id,
                referer or session.current_url,
                method=method,
                form=form,
                extra_headers=extra_headers,
            ),
            HTTP_LIGHT_LOG_FILE,
        )

    def _execute_fetch_for_session(
        self,
        session: BrowserSession,
        cfg: SiteSessionConfig,
        slot: dict[str, Any],
        needs_warm: bool,
        url: str,
        kind: str,
        task_id: int,
        cb_id: int,
        referer: str = "",
        mode: str = "",
        method: str = "GET",
        form: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[FetchResult, str]:
        if needs_warm:
            warm = self._warm_session(session, cfg, task_id, cb_id)
            if warm.status != 200:
                if self._should_quarantine_response(cfg, warm.status, warm.title, warm.html):
                    self._mute_slot(cfg, slot["name"], f"home:{warm.status}")
                raise RuntimeError(f"WARM BLOCKED {warm.status}")
            if self._should_quarantine_response(cfg, warm.status, warm.title, warm.html):
                self._mute_slot(cfg, slot["name"], f"home:{warm.status}")
                raise RuntimeError(f"WARM BLOCKED {warm.status}")
            pause_sec = random.uniform(float(cfg.pause_min_sec), float(cfg.pause_max_sec))
            if pause_sec > 0:
                time.sleep(float(pause_sec))

        result, current_log_file = self._run_mode_fetch(
            session,
            cfg,
            url,
            kind,
            task_id,
            cb_id,
            referer=referer,
            mode=mode,
            method=method,
            form=form,
            extra_headers=extra_headers,
        )
        if self._should_quarantine_response(cfg, result.status, result.title, result.html):
            if self._should_quarantine_response(cfg, result.status, result.title, result.html):
                self._mute_slot(cfg, slot["name"], f"{kind}:{result.status}")
            raise RuntimeError(f"BLOCKED {result.status}")
        return result, current_log_file

    def fetch(
        self,
        site: str,
        url: str,
        kind: str,
        task_id: int,
        cb_id: int,
        referer: str = "",
        mode: str = "",
        method: str = "GET",
        form: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        preferred_slot_name: str = "",
        preferred_slot_idx: int = -1,
        allowed_slot_names: list[str] | None = None,
    ) -> FetchResult:
        cfg = SITE_CONFIGS[site]
        requested_mode = str(mode or "")
        self.reap_idle_runtimes()
        last_error = None
        current_log_file = HTTP_LIGHT_LOG_FILE
        tried: set[tuple[str, int]] = set()
        deadline = time.time() + WAIT_TIMEOUT_SEC

        while time.time() < deadline:
            try:
                candidates = self._session_candidates(
                    cfg,
                    tried,
                    preferred_slot_name,
                    int(preferred_slot_idx),
                    allowed_slot_names,
                )
            except RuntimeError as exc:
                last_error = exc
                time.sleep(0.2)
                continue
            if not candidates:
                tried.clear()
                with self._runtime_cv:
                    self._runtime_cv.wait(timeout=0.2)
                continue

            slot, slot_idx = candidates[0]
            tried.add((slot["name"], int(slot_idx)))
            checked = self._checkout_runtime(cfg, slot, int(slot_idx))
            if checked is None:
                if len(tried) >= len(candidates):
                    tried.clear()
                    with self._runtime_cv:
                        self._runtime_cv.wait(timeout=0.1)
                continue

            session, lease = checked
            needs_warm = bool(lease.needs_warm)
            effective_mode = self._resolve_request_mode(
                cfg,
                kind,
                requested_mode,
                int(session.requests_total) + (1 if needs_warm else 0),
            )
            clear_state = False
            drop_runtime = False
            try:
                current_log_file = (
                    HTTP_CHROMIUM_LOG_FILE
                    if needs_warm or effective_mode in {"index_browser", "browser_click"}
                    else HTTP_LIGHT_LOG_FILE
                )
                result, current_log_file = self._execute_fetch_for_session(
                    session,
                    cfg,
                    slot,
                    needs_warm,
                    url,
                    kind,
                    task_id,
                    cb_id,
                    referer=referer,
                    mode=effective_mode,
                    method=method,
                    form=form,
                    extra_headers=extra_headers,
                )
                return result
            except SkippedFetchError:
                raise
            except RuntimeError as exc:
                last_error = exc
                drop_runtime = True
                if not str(exc).startswith("BLOCKED ") and not str(exc).startswith("WARM BLOCKED "):
                    self._log_fetch_error(
                        log_file=current_log_file,
                        site=site,
                        has_cookies=bool(session.http_session.cookies) if current_log_file == HTTP_LIGHT_LOG_FILE else bool(session.storage_state.get("cookies") if isinstance(session.storage_state, dict) else []),
                        tunnel=slot,
                        url=str(session.current_url or (cfg.home_url if needs_warm else url) or url),
                        status=0,
                        ms=0,
                        error=self._format_error_message(exc),
                    )
            except PlaywrightTimeoutError as exc:
                drop_runtime = True
                last_error = exc
                self._log_fetch_error(
                    log_file=current_log_file,
                    site=site,
                    has_cookies=bool(session.http_session.cookies) if current_log_file == HTTP_LIGHT_LOG_FILE else bool(session.storage_state.get("cookies") if isinstance(session.storage_state, dict) else []),
                    tunnel=slot,
                    url=str(session.current_url or (cfg.home_url if needs_warm else url) or url),
                    status=0,
                    ms=0,
                    error=self._format_error_message(exc),
                )
            except Exception as exc:
                drop_runtime = True
                last_error = exc
                self._log_fetch_error(
                    log_file=current_log_file,
                    site=site,
                    has_cookies=bool(session.http_session.cookies) if current_log_file == HTTP_LIGHT_LOG_FILE else bool(session.storage_state.get("cookies") if isinstance(session.storage_state, dict) else []),
                    tunnel=slot,
                    url=str(session.current_url or (cfg.home_url if needs_warm else url) or url),
                    status=0,
                    ms=0,
                    error=self._format_error_message(exc),
                )
            finally:
                self._release_runtime(cfg, session, lease, clear_state=clear_state, drop_runtime=drop_runtime)

        raise RuntimeError(str(last_error or f"FETCH FAILED {site} {url}"))


ROUTER = BrowserSessionRouter()
