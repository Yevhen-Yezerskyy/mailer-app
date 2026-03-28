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
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from engine.common.cache.client import CLIENT
from engine.common.logs import sys_log
from engine.core_crawler.browser.http_fetch import build_http_session, cookie_snapshot, export_storage_state, fetch_html, storage_state_has_cookies
from engine.core_crawler.browser.session_config import BROWSER_PROFILES, LOG_FOLDER, ROUTER_HTTP_LOG_FILE, SITE_CONFIGS, BrowserProfile, SiteSessionConfig
from engine.core_crawler.tunnels_11880 import ensure_tunnel_up, list_tunnels, stop_tunnel_by_name

STATE_TTL_SEC = 7 * 24 * 60 * 60
LOCK_TTL_SEC = 15 * 60
WAIT_TIMEOUT_SEC = 60.0
RUNTIME_IDLE_REAP_SEC = 90.0


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
    warming: bool = False
    recycle_after_ts: float = 0.0
    next_dispatch_ts: float = 0.0
    dispatch_mu: Any = field(default_factory=threading.Lock)
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


class BrowserSessionRouter:
    def __init__(self) -> None:
        self._pw_mu = threading.Lock()
        self._playwright = None
        self._profile_pos: dict[str, int] = {}
        self._runtime_cv = threading.Condition()
        self._runtimes: dict[str, BrowserSession] = {}
        self._browsers: dict[str, BrowserRuntime] = {}
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

    def _sys_log(self, log_file: str, payload: dict[str, Any]) -> None:
        sys_log(
            log_file,
            folder=LOG_FOLDER,
            message=json.dumps(payload, ensure_ascii=False, default=str, indent=2),
        )

    @staticmethod
    def _iso_ts(ts: float) -> str:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()

    @staticmethod
    def _cache_get_obj(key: str) -> Any:
        payload = CLIENT.get(key, ttl_sec=STATE_TTL_SEC)
        if not payload:
            return None
        try:
            return pickle.loads(payload)
        except Exception:
            return None

    @staticmethod
    def _cache_set_obj(key: str, value: Any, ttl_sec: int = STATE_TTL_SEC) -> None:
        try:
            payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            return
        CLIENT.set(key, payload, ttl_sec=ttl_sec)

    @staticmethod
    def _runtime_key(site: str, slot_name: str, slot_idx: int) -> str:
        return f"{site}:{slot_name}:{slot_idx}"

    @staticmethod
    def _browser_key(site: str, slot_name: str) -> str:
        return f"{site}:{slot_name}"

    @staticmethod
    def _schedule_key(site: str) -> str:
        return f"core_crawler:slot_schedule:{site}"

    @staticmethod
    def _rr_key(site: str) -> str:
        return f"core_crawler:slot_rr:{site}"

    @staticmethod
    def _quarantine_key(site: str) -> str:
        return f"core_crawler:slot_quarantine:{site}"

    @staticmethod
    def _session_key(site: str, slot_name: str, slot_idx: int) -> str:
        return f"core_crawler:browser_session:{site}:{slot_name}:{slot_idx}"

    def _load_slots(self, cfg: SiteSessionConfig) -> list[dict[str, Any]]:
        by_name = {
            str(row.get("name") or ""): row
            for row in list_tunnels()
            if str(row.get("name") or "")
        }
        resolved: list[dict[str, Any]] = []
        muted_name = self._scheduled_muted_slot(cfg)
        quarantined = self._load_quarantine(cfg)
        for name in cfg.egress_slots:
            if name == "direct":
                if name == muted_name or name in quarantined:
                    continue
                resolved.append(
                    {
                        "name": "direct",
                        "host": "direct",
                        "local_port": 0,
                        "proxy_server": "",
                    }
                )
                continue
            row = by_name.get(name)
            if not row:
                continue
            if name == muted_name or name in quarantined:
                try:
                    stop_tunnel_by_name(name)
                except Exception:
                    pass
                continue
            try:
                ensure_tunnel_up(name)
            except Exception:
                pass
            port = int(row.get("local_port") or 0)
            if not self._port_open(port):
                continue
            resolved.append(
                {
                    "name": name,
                    "host": str(row.get("host") or ""),
                    "local_port": port,
                    "proxy_server": f"socks5://127.0.0.1:{port}",
                }
            )
        if not resolved:
            raise RuntimeError(f"NO LIVE TUNNELS FOR {cfg.site}")
        return resolved

    def _next_rr_slot(self, cfg: SiteSessionConfig) -> str:
        state = self._cache_get_obj(self._rr_key(cfg.site)) or {"pos": 0}
        pos = int(state.get("pos") or 0)
        name = cfg.egress_slots[pos % len(cfg.egress_slots)]
        self._cache_set_obj(self._rr_key(cfg.site), {"pos": pos + 1})
        return name

    def _scheduled_muted_slot(self, cfg: SiteSessionConfig) -> str:
        if cfg.active_slot_count >= len(cfg.egress_slots):
            return ""
        now = time.time()
        state = self._cache_get_obj(self._schedule_key(cfg.site)) or {}
        name = str(state.get("name") or "")
        until = float(state.get("until") or 0.0)
        if name and until > now:
            return name
        name = self._next_rr_slot(cfg)
        state = {"name": name, "until": now + float(cfg.slot_quarantine_sec)}
        self._cache_set_obj(self._schedule_key(cfg.site), state)
        return name

    def _load_quarantine(self, cfg: SiteSessionConfig) -> dict[str, float]:
        raw = self._cache_get_obj(self._quarantine_key(cfg.site)) or {}
        if not isinstance(raw, dict):
            return {}
        now = time.time()
        out: dict[str, float] = {}
        dirty = False
        for name, until in raw.items():
            try:
                until_f = float(until or 0.0)
            except Exception:
                dirty = True
                continue
            if until_f > now:
                out[str(name)] = until_f
            else:
                dirty = True
        if dirty:
            self._cache_set_obj(self._quarantine_key(cfg.site), out)
        return out

    def _slot_is_quarantined(self, cfg: SiteSessionConfig, slot_name: str) -> bool:
        if slot_name == self._scheduled_muted_slot(cfg):
            return True
        return slot_name in self._load_quarantine(cfg)

    def _mute_slot(self, cfg: SiteSessionConfig, slot_name: str, reason: str) -> None:
        if not slot_name:
            return
        state = self._load_quarantine(cfg)
        until = time.time() + float(cfg.slot_quarantine_sec)
        state[slot_name] = until
        self._cache_set_obj(self._quarantine_key(cfg.site), state)
        self._drop_egress_session_state(cfg, slot_name)
        if slot_name != "direct":
            try:
                stop_tunnel_by_name(slot_name)
            except Exception:
                pass
        self._sys_log(
            ROUTER_HTTP_LOG_FILE,
            {
                "event": "slot_quarantine",
                "site": cfg.site,
                "slot": slot_name,
                "reason": reason,
                "until_ts": until,
            },
        )

    def _active_egresses(self, cfg: SiteSessionConfig) -> list[dict[str, Any]]:
        return [row for row in self._load_slots(cfg) if not self._slot_is_quarantined(cfg, row["name"])]

    @staticmethod
    def _profile_script(profile: BrowserProfile) -> str:
        langs = json.dumps(list(profile.languages), ensure_ascii=False)
        return f"""
(() => {{
  const patch = (obj, key, value) => {{
    try {{
      Object.defineProperty(obj, key, {{ get: () => value, configurable: true }});
    }} catch (_) {{}}
  }};
  patch(navigator, 'webdriver', undefined);
  patch(navigator, 'platform', {json.dumps(profile.navigator_platform)});
  patch(navigator, 'vendor', {json.dumps(profile.navigator_vendor)});
  patch(navigator, 'language', {json.dumps(profile.languages[0])});
  patch(navigator, 'languages', {langs});
  patch(navigator, 'hardwareConcurrency', {int(profile.hardware_concurrency)});
  patch(navigator, 'deviceMemory', {int(profile.device_memory)});
  patch(navigator, 'pdfViewerEnabled', true);
  patch(screen, 'colorDepth', 24);
  patch(screen, 'pixelDepth', 24);
  try {{
    Object.defineProperty(navigator, 'plugins', {{
      get: () => [
        {{ name: 'Chrome PDF Plugin' }},
        {{ name: 'Chrome PDF Viewer' }},
        {{ name: 'Native Client' }},
      ],
      configurable: true,
    }});
  }} catch (_) {{}}
  try {{
    window.chrome = window.chrome || {{}};
    window.chrome.runtime = window.chrome.runtime || {{}};
    window.chrome.app = window.chrome.app || {{ isInstalled: false }};
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
        pos = self._profile_pos.get(site, 0)
        profile = BROWSER_PROFILES[pos % len(BROWSER_PROFILES)]
        self._profile_pos[site] = pos + 1
        return profile

    @staticmethod
    def _profile_by_name(name: str) -> BrowserProfile | None:
        for profile in BROWSER_PROFILES:
            if profile.name == name:
                return profile
        return None

    def _load_session_state(self, cfg: SiteSessionConfig, slot_name: str, slot_idx: int) -> dict[str, Any] | None:
        state = self._cache_get_obj(self._session_key(cfg.site, slot_name, slot_idx))
        if not isinstance(state, dict):
            return None
        created_at = float(state.get("created_at") or 0.0)
        requests_total = int(state.get("requests_total") or 0)
        if requests_total >= cfg.max_requests_per_session:
            return None
        if created_at and (time.time() - created_at) >= cfg.max_session_age_sec:
            return None
        if bool(state.get("warmed") is True) and not storage_state_has_cookies((state or {}).get("storage_state")):
            return None
        return state

    @staticmethod
    def _dispatch_ready(next_dispatch_ts: float) -> bool:
        return time.time() >= float(next_dispatch_ts or 0.0)

    def _reserve_dispatch_window(self, session: BrowserSession, cfg: SiteSessionConfig) -> bool:
        now = time.time()
        with session.dispatch_mu:
            current_next = float(session.next_dispatch_ts or 0.0)
            if not self._dispatch_ready(current_next):
                return False
            min_pause = max(0.1, float(cfg.pause_min_sec))
            max_pause = max(min_pause, float(cfg.pause_max_sec))
            session.next_dispatch_ts = max(now, current_next) + round(random.uniform(min_pause, max_pause), 2)
            return True

    def _drop_egress_session_state(self, cfg: SiteSessionConfig, slot_name: str) -> None:
        doomed: list[BrowserSession] = []
        doomed_browser: BrowserRuntime | None = None
        for slot_idx in range(cfg.sessions_per_egress):
            self._cache_set_obj(self._session_key(cfg.site, slot_name, slot_idx), {})
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

    @staticmethod
    def _browser_runtime_expired(browser_runtime: BrowserRuntime) -> bool:
        now = time.time()
        recycle_after_ts = float(browser_runtime.recycle_after_ts or 0.0)
        return bool(recycle_after_ts and now >= recycle_after_ts)

    def reap_idle_runtimes(self) -> None:
        doomed: list[tuple[SiteSessionConfig, BrowserSession, bool]] = []
        doomed_browsers: list[BrowserRuntime] = []
        now = time.time()
        with self._runtime_cv:
            for runtime_key, runtime in list(self._runtimes.items()):
                if runtime.active_pages > 0 or runtime.warming:
                    continue
                cfg = SITE_CONFIGS[runtime.site]
                clear_state = self._slot_is_quarantined(cfg, runtime.tunnel["name"])
                if not clear_state:
                    clear_state = self._runtime_expired(cfg, runtime)
                if not clear_state and (now - float(runtime.last_used_at or 0.0)) < RUNTIME_IDLE_REAP_SEC:
                    continue
                self._runtimes.pop(runtime_key, None)
                doomed.append((cfg, runtime, clear_state))
            for browser_key, browser_runtime in list(self._browsers.items()):
                if any(self._browser_key(row.site, row.tunnel["name"]) == browser_key for row in self._runtimes.values()):
                    continue
                if (now - float(browser_runtime.last_used_at or 0.0)) < RUNTIME_IDLE_REAP_SEC and not self._browser_runtime_expired(browser_runtime):
                    continue
                self._browsers.pop(browser_key, None)
                doomed_browsers.append(browser_runtime)
            self._runtime_cv.notify_all()

        for cfg, runtime, clear_state in doomed:
            if clear_state:
                self._cache_set_obj(self._session_key(cfg.site, runtime.tunnel["name"], runtime.slot_idx), {})
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
    ) -> list[tuple[dict[str, Any], int, dict[str, Any] | None]]:
        active = self._active_egresses(cfg)
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
                    if runtime is not None:
                        if self._runtime_expired(cfg, runtime):
                            continue
                        if runtime.warming:
                            continue
                        if runtime.active_pages >= cfg.concurrent_pages_per_session:
                            continue
                        if not self._dispatch_ready(float(runtime.next_dispatch_ts or 0.0)):
                            continue
                        weighted.append((preferred_rank, 0, int(runtime.active_pages), float(runtime.last_used_at), slot, slot_idx, None))
                        continue
                    state = self._load_session_state(cfg, slot["name"], slot_idx)
                    if state and not self._dispatch_ready(float(state.get("next_dispatch_ts") or 0.0)):
                        continue
                    last_used_at = float((state or {}).get("last_used_at") or 0.0)
                    weighted.append((preferred_rank, 1, 0, last_used_at, slot, slot_idx, state))

        weighted.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
        return [(slot, slot_idx, state) for _, _, _, _, slot, slot_idx, state in weighted]

    def _create_session(
        self,
        cfg: SiteSessionConfig,
        tunnel: dict[str, Any],
        slot_idx: int,
        state: dict[str, Any] | None,
    ) -> BrowserSession:
        profile = self._profile_by_name(str((state or {}).get("profile_name") or "")) or self._pick_profile(cfg.site)
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
            warming=not bool((state or {}).get("warmed") is True),
            recycle_after_ts=time.time() + random.uniform(float(cfg.runtime_recycle_min_sec), float(cfg.runtime_recycle_max_sec)),
            next_dispatch_ts=float((state or {}).get("next_dispatch_ts") or 0.0),
        )

    def _launch_browser_runtime(self, cfg: SiteSessionConfig, tunnel: dict[str, Any]) -> BrowserRuntime:
        pw = self._ensure_playwright()
        launch_kwargs: dict[str, Any] = {
            "headless": True,
            "slow_mo": 150,
            "ignore_default_args": ["--enable-automation"],
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--lang=de-DE",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--disable-background-networking",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
                "--disable-breakpad",
                "--disable-component-update",
                "--disable-default-apps",
                "--disable-extensions",
                "--disable-features=AudioServiceOutOfProcess,AutofillServerCommunication,CertificateTransparencyComponentUpdater,MediaRouter,OptimizationHints,Translate",
                "--disable-sync",
                "--metrics-recording-only",
                "--mute-audio",
                "--no-default-browser-check",
                "--no-first-run",
                "--renderer-process-limit=3",
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
        with self._runtime_cv:
            existing = self._browsers.get(browser_key)
            if existing is not None:
                existing.last_used_at = time.time()
                return existing
        created = self._launch_browser_runtime(cfg, tunnel)
        with self._runtime_cv:
            existing = self._browsers.get(browser_key)
            if existing is not None:
                existing.last_used_at = time.time()
                self._close_browser_runtime(created)
                return existing
            self._browsers[browser_key] = created
            self._runtime_cv.notify_all()
            return created

    def _checkout_runtime(
        self,
        cfg: SiteSessionConfig,
        slot: dict[str, Any],
        slot_idx: int,
        state: dict[str, Any] | None,
    ) -> tuple[BrowserSession, bool] | None:
        runtime_key = self._runtime_key(cfg.site, slot["name"], slot_idx)
        with self._runtime_cv:
            runtime = self._runtimes.get(runtime_key)
            if runtime is not None:
                if runtime.warming:
                    return None
                if runtime.active_pages >= cfg.concurrent_pages_per_session:
                    return None
                if not self._reserve_dispatch_window(runtime, cfg):
                    return None
                runtime.active_pages += 1
                runtime.last_used_at = time.time()
                return runtime, False

        runtime = None
        try:
            runtime = self._create_session(cfg, slot, slot_idx, state)
            if not self._reserve_dispatch_window(runtime, cfg):
                self._close_session(runtime)
                return None
            runtime.active_pages = 1
            with self._runtime_cv:
                existing = self._runtimes.get(runtime_key)
                if existing is not None:
                    self._close_session(runtime)
                    if existing.warming or existing.active_pages >= cfg.concurrent_pages_per_session:
                        return None
                    if not self._reserve_dispatch_window(existing, cfg):
                        return None
                    existing.active_pages += 1
                    existing.last_used_at = time.time()
                    return existing, False
                self._runtimes[runtime_key] = runtime
                self._runtime_cv.notify_all()
            return runtime, runtime.warming
        except Exception:
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
        exported_state = export_storage_state(session.http_session, session.storage_state)
        state = {
            "session_id": session.session_id,
            "profile_name": session.profile.name,
            "slot_idx": int(session.slot_idx),
            "created_at": float(session.created_at),
            "last_used_at": float(session.last_used_at),
            "requests_total": int(session.requests_total),
            "warmed": bool(session.warmed),
            "current_url": str(session.current_url or ""),
            "next_dispatch_ts": float(session.next_dispatch_ts or 0.0),
            "storage_state": exported_state,
        }
        self._cache_set_obj(self._session_key(cfg.site, session.tunnel["name"], session.slot_idx), state)
        self._sys_log(
            ROUTER_HTTP_LOG_FILE,
            {
                "event": "cookie_persist_redis",
                "site": cfg.site,
                "session_id": session.session_id,
                "session_slot": int(session.slot_idx),
                "tunnel": session.tunnel,
                "cookies": [str(row.get("name") or "") for row in ((exported_state or {}).get("cookies") or [])],
                "cookie_count": len(((exported_state or {}).get("cookies") or [])),
                "warmed": bool(session.warmed),
            },
        )

    def _sync_http_session(self, session: BrowserSession) -> None:
        storage_state = dict(session.storage_state or {})
        old_http = session.http_session
        session.http_session = build_http_session(session.profile, session.tunnel, storage_state)
        if old_http is not None:
            try:
                old_http.close()
            except Exception:
                pass
        self._sys_log(
            ROUTER_HTTP_LOG_FILE,
            {
                "event": "cookie_sync_http",
                "site": session.site,
                "session_id": session.session_id,
                "session_slot": int(session.slot_idx),
                "tunnel": session.tunnel,
                "storage_cookie_count": len((storage_state or {}).get("cookies") or []),
                "http_cookie_count": len(cookie_snapshot(session.http_session)),
                "cookies": [str(row.get("name") or "") for row in cookie_snapshot(session.http_session)],
            },
        )

    def _session_has_live_cookies(self, session: BrowserSession) -> bool:
        if not storage_state_has_cookies(session.storage_state):
            return False
        try:
            return bool(cookie_snapshot(session.http_session))
        except Exception:
            return False

    def _persist_and_verify_ready(self, cfg: SiteSessionConfig, session: BrowserSession, *, stage: str) -> bool:
        if not self._session_has_live_cookies(session):
            self._sys_log(
                ROUTER_HTTP_LOG_FILE,
                {
                    "event": "session_gate_failed",
                    "site": cfg.site,
                    "stage": stage,
                    "session_id": session.session_id,
                    "session_slot": int(session.slot_idx),
                    "tunnel": session.tunnel,
                    "reason": "NO_COOKIES_IN_MEMORY",
                },
            )
            return False
        self._persist_session_state(cfg, session)
        reloaded = self._load_session_state(cfg, session.tunnel["name"], session.slot_idx) or {}
        self._sys_log(
            ROUTER_HTTP_LOG_FILE,
            {
                "event": "cookie_reload_redis",
                "site": cfg.site,
                "stage": stage,
                "session_id": session.session_id,
                "session_slot": int(session.slot_idx),
                "tunnel": session.tunnel,
                "cookie_count": len(((reloaded or {}).get("storage_state") or {}).get("cookies") or []),
                "cookies": [
                    str(row.get("name") or "")
                    for row in ((((reloaded or {}).get("storage_state") or {}).get("cookies") or []))
                ],
            },
        )
        ok = storage_state_has_cookies((reloaded or {}).get("storage_state"))
        if not ok:
            self._sys_log(
                ROUTER_HTTP_LOG_FILE,
                {
                    "event": "session_gate_failed",
                    "site": cfg.site,
                    "stage": stage,
                    "session_id": session.session_id,
                    "session_slot": int(session.slot_idx),
                    "tunnel": session.tunnel,
                    "reason": "NO_COOKIES_IN_REDIS",
                },
            )
        return ok

    def _release_runtime(self, cfg: SiteSessionConfig, session: BrowserSession, *, clear_state: bool = False) -> None:
        runtime_key = self._runtime_key(cfg.site, session.tunnel["name"], session.slot_idx)
        browser_key = self._browser_key(cfg.site, session.tunnel["name"])
        should_close = False
        close_browser: BrowserRuntime | None = None
        with self._runtime_cv:
            session.active_pages = max(0, int(session.active_pages) - 1)
            session.last_used_at = time.time()
            browser_runtime = self._browsers.get(browser_key)
            if browser_runtime is not None:
                browser_runtime.last_used_at = session.last_used_at
            if session.active_pages <= 0 and (clear_state or self._slot_is_quarantined(cfg, session.tunnel["name"]) or self._runtime_expired(cfg, session)):
                self._runtimes.pop(runtime_key, None)
                should_close = True
                if not any(self._browser_key(row.site, row.tunnel["name"]) == browser_key for row in self._runtimes.values()):
                    browser_runtime = self._browsers.get(browser_key)
                    if browser_runtime is not None and (clear_state or self._slot_is_quarantined(cfg, session.tunnel["name"]) or self._browser_runtime_expired(browser_runtime)):
                        close_browser = self._browsers.pop(browser_key, None)
            self._runtime_cv.notify_all()

        if not should_close:
            try:
                self._persist_session_state(cfg, session)
            except Exception:
                pass
            return

        if clear_state or self._slot_is_quarantined(cfg, session.tunnel["name"]):
            self._cache_set_obj(self._session_key(cfg.site, session.tunnel["name"], session.slot_idx), {})
        else:
            self._persist_session_state(cfg, session)
        self._close_session(session)
        if close_browser is not None:
            self._close_browser_runtime(close_browser)

    def _safe_request_headers(self, req) -> dict[str, Any]:
        try:
            return dict(req.all_headers())
        except Exception:
            return {}

    def _safe_response_headers(self, resp) -> dict[str, Any]:
        try:
            return dict(resp.all_headers())
        except Exception:
            return {}

    def _attach_http_logging(self, page: Any, session: BrowserSession, cfg: SiteSessionConfig) -> dict[str, dict[str, Any]]:
        subrequests: dict[str, dict[str, Any]] = {}

        def on_request(req) -> None:
            req_id = uuid4().hex
            try:
                setattr(req, "_serenity_req_id", req_id)
            except Exception:
                pass
            started = time.time()
            subrequests[req_id] = {"started": started}
            self._sys_log(
                ROUTER_HTTP_LOG_FILE,
                {
                    "event": "subrequest",
                    "site": cfg.site,
                    "session_id": session.session_id,
                    "session_slot": int(session.slot_idx),
                    "tunnel": session.tunnel,
                    "request_id": req_id,
                    "url": str(req.url),
                    "method": str(req.method),
                    "resource_type": str(req.resource_type),
                    "is_navigation_request": bool(req.is_navigation_request()),
                    "request_headers": self._safe_request_headers(req),
                },
            )

        def on_response(resp) -> None:
            req = resp.request
            req_id = getattr(req, "_serenity_req_id", "")
            started = float((subrequests or {}).get(req_id, {}).get("started") or time.time())
            self._sys_log(
                ROUTER_HTTP_LOG_FILE,
                {
                    "event": "subresponse",
                    "site": cfg.site,
                    "session_id": session.session_id,
                    "session_slot": int(session.slot_idx),
                    "tunnel": session.tunnel,
                    "request_id": req_id,
                    "url": str(resp.url),
                    "method": str(req.method),
                    "resource_type": str(req.resource_type),
                    "status": int(resp.status),
                    "ms": int((time.time() - started) * 1000),
                    "response_headers": self._safe_response_headers(resp),
                },
            )

        def on_request_failed(req) -> None:
            req_id = getattr(req, "_serenity_req_id", "")
            started = float((subrequests or {}).get(req_id, {}).get("started") or time.time())
            failure = ""
            try:
                failure = str(req.failure)
            except Exception:
                failure = ""
            self._sys_log(
                ROUTER_HTTP_LOG_FILE,
                {
                    "event": "subresponse_failed",
                    "site": cfg.site,
                    "session_id": session.session_id,
                    "session_slot": int(session.slot_idx),
                    "tunnel": session.tunnel,
                    "request_id": req_id,
                    "url": str(req.url),
                    "method": str(req.method),
                    "resource_type": str(req.resource_type),
                    "ms": int((time.time() - started) * 1000),
                    "error": failure,
                },
            )

        page.on("request", on_request)
        page.on("response", on_response)
        page.on("requestfailed", on_request_failed)
        return subrequests

    def _new_page(self, session: BrowserSession, cfg: SiteSessionConfig) -> tuple[Any, Any]:
        browser_runtime = self._checkout_browser_runtime(cfg, session.tunnel)
        context_kwargs: dict[str, Any] = {
            "user_agent": session.profile.user_agent,
            "locale": session.profile.locale,
            "timezone_id": session.profile.timezone_id,
            "viewport": {"width": session.profile.viewport_width, "height": session.profile.viewport_height},
            "screen": {"width": session.profile.screen_width, "height": session.profile.screen_height},
            "color_scheme": "light",
            "ignore_https_errors": True,
            "service_workers": "block",
        }
        if isinstance(session.storage_state, dict) and session.storage_state:
            context_kwargs["storage_state"] = session.storage_state
        context = browser_runtime.browser.new_context(**context_kwargs)
        context.add_init_script(self._profile_script(session.profile))
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
        self._attach_http_logging(page, session, cfg)
        return context, page

    def _humanize(self, page: Any) -> None:
        try:
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        try:
            page.mouse.move(260, 220, steps=12)
            page.wait_for_timeout(300)
            page.mouse.wheel(0, 420)
            page.wait_for_timeout(450)
            page.mouse.move(520, 360, steps=10)
            page.wait_for_timeout(250)
            page.mouse.wheel(0, -180)
            page.wait_for_timeout(300)
        except Exception:
            pass

    def _looks_blocked(self, status: int, title: str, html: str) -> bool:
        if int(status or 0) in (403, 429):
            return True
        title = str(title or "")
        html = str(html or "")
        return "Nur einen Moment" in title or "challenge-platform" in html or "error code: 1015" in html

    def _should_quarantine_for_block(self, status: int, title: str, html: str) -> bool:
        return self._looks_blocked(int(status or 0), str(title or ""), str(html or ""))

    @staticmethod
    def _same_origin(url_a: str, url_b: str) -> bool:
        try:
            aa = urlsplit(str(url_a or ""))
            bb = urlsplit(str(url_b or ""))
            return bool(aa.scheme and aa.netloc and aa.scheme == bb.scheme and aa.netloc == bb.netloc)
        except Exception:
            return False

    def _should_try_click(self, session: BrowserSession, url: str, referer: str) -> bool:
        if not referer:
            return False
        if not self._same_origin(url, referer):
            return False
        if random.random() > 0.35:
            return False
        return True

    @staticmethod
    def _candidate_hrefs(url: str) -> list[str]:
        parsed = urlsplit(str(url or ""))
        out: list[str] = []
        full = str(url or "")
        if full:
            out.append(full)
        path_q = parsed.path or "/"
        if parsed.query:
            path_q = f"{path_q}?{parsed.query}"
        out.append(path_q)
        if parsed.path:
            out.append(parsed.path)
        dedup: list[str] = []
        for row in out:
            if row and row not in dedup:
                dedup.append(row)
        return dedup

    def _click_to_target(self, page: Any, target_url: str, timeout_ms: int) -> bool:
        selectors = []
        for href in self._candidate_hrefs(target_url):
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
        if cfg.site != "gs":
            return 0
        try:
            clicked = page.evaluate(
                """
                () => {
                  const roots = Array.from(document.querySelectorAll('article.mod.mod-Treffer a[href*="/gsbiz/"]'));
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
                """
            )
            try:
                page.wait_for_timeout(350)
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
        try:
            context, page = self._new_page(session, cfg)
            result = self._fetch_once(session, page, cfg, url, kind, task_id, cb_id, referer)
            if result.status == 200 and not self._looks_blocked(result.status, result.title, result.html):
                click_count = self._simulate_index_clicks(page, cfg)
                try:
                    session.storage_state = dict(page.context.storage_state() or {})
                except Exception:
                    pass
                self._sys_log(
                    ROUTER_HTTP_LOG_FILE,
                    {
                        "event": "cookie_capture_browser",
                        "site": cfg.site,
                        "stage": "index_browser",
                        "kind": kind,
                        "task_id": task_id,
                        "cb_id": cb_id,
                        "session_id": session.session_id,
                        "session_slot": int(session.slot_idx),
                        "tunnel": session.tunnel,
                        "cookie_count": len((session.storage_state or {}).get("cookies") or []),
                        "cookies": [str(row.get("name") or "") for row in ((session.storage_state or {}).get("cookies") or [])],
                    },
                )
                self._sync_http_session(session)
                self._sys_log(
                    ROUTER_HTTP_LOG_FILE,
                    {
                        "event": "index_click_simulation",
                        "site": cfg.site,
                        "kind": kind,
                        "task_id": task_id,
                        "cb_id": cb_id,
                        "session_id": session.session_id,
                        "session_slot": int(session.slot_idx),
                        "tunnel": session.tunnel,
                        "url": url,
                        "clicked": int(click_count),
                    },
                )
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
    ) -> FetchResult:
        started = time.time()
        started_iso = self._iso_ts(started)
        print(
            f"[browser-router] -> site={cfg.site} kind={kind} task_id={task_id} cb_id={cb_id} "
            f"tunnel={session.tunnel.get('name')} slot={session.slot_idx} "
            f"session={session.session_id} url={url}",
            flush=True,
        )
        cookies_before = []
        try:
            cookies_before = page.context.cookies([url])
        except Exception:
            cookies_before = []
        response = page.goto(
            url,
            referer=referer or None,
            wait_until="domcontentloaded",
            timeout=cfg.browser_timeout_ms,
        )
        html = page.content()
        final_url = str(page.url)
        status = int(response.status) if response else 0
        title = page.title() or ""
        body_text = " ".join((html or "")[:500].split())
        finished = time.time()
        finished_iso = self._iso_ts(finished)
        elapsed_ms = int((finished - started) * 1000)
        req = response.request if response else None
        self._sys_log(
            ROUTER_HTTP_LOG_FILE,
            {
                "event": "top_fetch",
                "site": cfg.site,
                "kind": kind,
                "task_id": task_id,
                "cb_id": cb_id,
                "session_id": session.session_id,
                "session_slot": int(session.slot_idx),
                "tunnel": session.tunnel,
                "request_started_at": started_iso,
                "response_received_at": finished_iso,
                "request": {
                    "url": url,
                    "referer": referer,
                    "headers": self._safe_request_headers(req) if req else {},
                    "cookies": cookies_before,
                },
                "response": {
                    "final_url": final_url,
                    "status": status,
                    "ms": elapsed_ms,
                    "headers": self._safe_response_headers(response) if response else {},
                    "cookies": page.context.cookies([final_url or url]),
                    "title": title,
                    "body_head": body_text[:300],
                },
            },
        )
        print(
            f"[browser-router] <- site={cfg.site} kind={kind} cb_id={cb_id} "
            f"status={status} ms={elapsed_ms} "
            f"tunnel={session.tunnel.get('name')} slot={session.slot_idx} "
            f"session={session.session_id} final_url={final_url}",
            flush=True,
        )
        session.last_used_at = time.time()
        session.current_url = final_url or url
        session.requests_total += 1
        try:
            session.storage_state = dict(page.context.storage_state() or {})
        except Exception:
            pass
        self._sys_log(
            ROUTER_HTTP_LOG_FILE,
            {
                "event": "cookie_capture_browser",
                "site": cfg.site,
                "stage": kind,
                "task_id": task_id,
                "cb_id": cb_id,
                "session_id": session.session_id,
                "session_slot": int(session.slot_idx),
                "tunnel": session.tunnel,
                "cookie_count": len((session.storage_state or {}).get("cookies") or []),
                "cookies": [str(row.get("name") or "") for row in ((session.storage_state or {}).get("cookies") or [])],
            },
        )
        self._sync_http_session(session)
        return FetchResult(
            status=status,
            url=url,
            final_url=final_url,
            html=html,
            title=title,
            ms=elapsed_ms,
            site=cfg.site,
            session_id=session.session_id,
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
    ) -> FetchResult:
        if not self._session_has_live_cookies(session):
            raise RuntimeError(f"HTTP SESSION NOT WARMED {cfg.site} {session.tunnel.get('name')}:{session.slot_idx}")
        started = time.time()
        started_iso = self._iso_ts(started)
        print(
            f"[browser-router] -> site={cfg.site} kind={kind} task_id={task_id} cb_id={cb_id} "
            f"tunnel={session.tunnel.get('name')} slot={session.slot_idx} "
            f"session={session.session_id} url={url}",
            flush=True,
        )
        with session.http_mu:
            cookies_before = cookie_snapshot(session.http_session)
            payload = fetch_html(
                session.http_session,
                session.profile,
                url,
                referer=referer or "",
                timeout_ms=cfg.browser_timeout_ms,
            )
            session.storage_state = export_storage_state(session.http_session, session.storage_state)
        final_url = str(payload.get("final_url") or url)
        status = int(payload.get("status") or 0)
        html = str(payload.get("html") or "")
        title = ""
        low = html.lower()
        p1 = low.find("<title>")
        p2 = low.find("</title>")
        if p1 >= 0 and p2 > p1:
            title = html[p1 + 7 : p2].strip()
        body_text = " ".join(html[:500].split())
        finished = time.time()
        finished_iso = self._iso_ts(finished)
        elapsed_ms = int((finished - started) * 1000)
        self._sys_log(
            ROUTER_HTTP_LOG_FILE,
            {
                "event": "top_fetch",
                "site": cfg.site,
                "kind": kind,
                "task_id": task_id,
                "cb_id": cb_id,
                "session_id": session.session_id,
                "session_slot": int(session.slot_idx),
                "tunnel": session.tunnel,
                "request_started_at": started_iso,
                "response_received_at": finished_iso,
                "request": {
                    "url": url,
                    "referer": referer,
                    "headers": dict(payload.get("request_headers") or {}),
                    "cookies": cookies_before,
                },
                "response": {
                    "final_url": final_url,
                    "status": status,
                    "ms": elapsed_ms,
                    "headers": dict(payload.get("response_headers") or {}),
                    "cookies": cookie_snapshot(session.http_session),
                    "title": title,
                    "body_head": body_text[:300],
                },
            },
        )
        print(
            f"[browser-router] <- site={cfg.site} kind={kind} cb_id={cb_id} "
            f"status={status} ms={elapsed_ms} "
            f"tunnel={session.tunnel.get('name')} slot={session.slot_idx} "
            f"session={session.session_id} final_url={final_url}",
            flush=True,
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
        context = None
        page = None
        try:
            context, page = self._new_page(session, cfg)
            warm_ref = self._fetch_once(session, page, cfg, referer, "referer", task_id, cb_id, "")
            if warm_ref.status != 200 or self._looks_blocked(warm_ref.status, warm_ref.title, warm_ref.html):
                return None
            self._humanize(page)
            clicked = self._click_to_target(page, url, cfg.browser_timeout_ms)
            if not clicked:
                return None
            html = page.content()
            final_url = str(page.url)
            title = page.title() or ""
            status = 200
            finished = time.time()
            elapsed_ms = int((finished - started) * 1000)
            if final_url != url and not final_url.startswith(url):
                return None
            session.last_used_at = time.time()
            session.current_url = final_url or url
            session.requests_total += 1
            try:
                session.storage_state = dict(page.context.storage_state() or {})
            except Exception:
                pass
            self._sync_http_session(session)
            self._sys_log(
                ROUTER_HTTP_LOG_FILE,
                {
                    "event": "top_fetch_click",
                    "site": cfg.site,
                    "kind": kind,
                    "task_id": task_id,
                    "cb_id": cb_id,
                    "session_id": session.session_id,
                    "session_slot": int(session.slot_idx),
                    "tunnel": session.tunnel,
                    "request_started_at": self._iso_ts(started),
                    "response_received_at": self._iso_ts(finished),
                    "request": {
                        "url": url,
                        "referer": referer,
                        "mode": "browser_click",
                    },
                    "response": {
                        "final_url": final_url,
                        "status": status,
                        "ms": elapsed_ms,
                        "title": title,
                        "cookies": page.context.cookies([final_url or url]),
                        "body_head": " ".join((html or "")[:300].split()),
                    },
                },
            )
            return FetchResult(
                status=status,
                url=url,
                final_url=final_url,
                html=html,
                title=title,
                ms=elapsed_ms,
                site=cfg.site,
                session_id=session.session_id,
                tunnel=dict(session.tunnel),
            )
        except Exception:
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

    def fetch(
        self,
        site: str,
        url: str,
        kind: str,
        task_id: int,
        cb_id: int,
        referer: str = "",
        mode: str = "",
        preferred_slot_name: str = "",
        preferred_slot_idx: int = -1,
    ) -> FetchResult:
        cfg = SITE_CONFIGS[site]
        self.reap_idle_runtimes()
        last_error = None
        tried: set[tuple[str, int]] = set()
        deadline = time.time() + WAIT_TIMEOUT_SEC

        while time.time() < deadline:
            try:
                candidates = self._session_candidates(cfg, tried, preferred_slot_name, int(preferred_slot_idx))
            except RuntimeError as exc:
                last_error = exc
                time.sleep(0.2)
                continue
            if not candidates:
                tried.clear()
                with self._runtime_cv:
                    self._runtime_cv.wait(timeout=0.2)
                continue

            slot, slot_idx, state = candidates[0]
            tried.add((slot["name"], int(slot_idx)))
            checked = self._checkout_runtime(cfg, slot, int(slot_idx), state)
            if checked is None:
                if len(tried) >= len(candidates):
                    tried.clear()
                    with self._runtime_cv:
                        self._runtime_cv.wait(timeout=0.1)
                continue

            session, needs_warm = checked
            page = None
            context = None
            clear_state = False
            try:
                if needs_warm:
                    context, page = self._new_page(session, cfg)
                    warm = self._fetch_once(session, page, cfg, cfg.home_url, "home", task_id, cb_id, "")
                    if warm.status != 200 or self._looks_blocked(warm.status, warm.title, warm.html):
                        clear_state = True
                        if self._should_quarantine_for_block(warm.status, warm.title, warm.html):
                            self._mute_slot(cfg, slot["name"], f"home:{warm.status}")
                        last_error = RuntimeError(f"WARM BLOCKED {warm.status}")
                        continue
                    session.warmed = True
                    session.warming = False
                    if not self._persist_and_verify_ready(cfg, session, stage="warmup"):
                        clear_state = True
                        last_error = RuntimeError(f"WARM NO COOKIES {cfg.site} {slot['name']}:{slot_idx}")
                        continue

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
                    if not self._persist_and_verify_ready(cfg, session, stage="index_browser"):
                        clear_state = True
                        last_error = RuntimeError(f"INDEX NO COOKIES {cfg.site} {slot['name']}:{slot_idx}")
                        continue
                elif mode == "http_only":
                    result = self._fetch_http_once(
                        session,
                        cfg,
                        url,
                        kind,
                        task_id,
                        cb_id,
                        referer or session.current_url,
                    )
                else:
                    result = self._fetch_browser_click_once(
                        session,
                        cfg,
                        url,
                        kind,
                        task_id,
                        cb_id,
                        referer or session.current_url,
                    )
                    if result is None:
                        result = self._fetch_http_once(
                            session,
                            cfg,
                            url,
                            kind,
                            task_id,
                            cb_id,
                            referer or session.current_url,
                        )
                if result.status != 200 or self._looks_blocked(result.status, result.title, result.html):
                    clear_state = True
                    if self._should_quarantine_for_block(result.status, result.title, result.html):
                        self._mute_slot(cfg, slot["name"], f"{kind}:{result.status}")
                    last_error = RuntimeError(f"BLOCKED {result.status}")
                    continue
                return result
            except PlaywrightTimeoutError as exc:
                clear_state = True
                last_error = exc
                self._sys_log(
                    ROUTER_HTTP_LOG_FILE,
                    {
                        "event": "response_error",
                        "site": site,
                        "kind": kind,
                        "task_id": task_id,
                        "cb_id": cb_id,
                        "session_slot": int(slot_idx),
                        "tunnel": slot,
                        "url": url,
                        "error": "TIMEOUT",
                        "detail": str(exc),
                    },
                )
            except Exception as exc:
                clear_state = True
                last_error = exc
                self._sys_log(
                    ROUTER_HTTP_LOG_FILE,
                    {
                        "event": "response_error",
                        "site": site,
                        "kind": kind,
                        "task_id": task_id,
                        "cb_id": cb_id,
                        "session_slot": int(slot_idx),
                        "tunnel": slot,
                        "url": url,
                        "error": type(exc).__name__,
                        "detail": str(exc),
                    },
                )
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
                if needs_warm:
                    with self._runtime_cv:
                        session.warming = False
                        self._runtime_cv.notify_all()
                self._release_runtime(cfg, session, clear_state=clear_state)

        raise RuntimeError(str(last_error or f"FETCH FAILED {site} {url}"))


ROUTER = BrowserSessionRouter()
