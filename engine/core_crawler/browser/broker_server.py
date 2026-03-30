# FILE: engine/core_crawler/browser/broker_server.py
# DATE: 2026-03-27
# PURPOSE: Local unix-socket browser broker process for core_crawler fetch requests.

from __future__ import annotations

import concurrent.futures
import json
import multiprocessing
import os
import pickle
import queue
import random
import socketserver
import struct
import subprocess
import threading
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from engine.common.cache.client import CLIENT
from engine.core_crawler.browser.session_config import (
    BROKER_QUEUE_MAX,
    BROKER_WORKERS,
    ONE_ONE_EIGHTY_ACTIVE_TUNNEL_MAX,
    ONE_ONE_EIGHTY_ACTIVE_TUNNEL_RATIO,
    ONE_ONE_EIGHTY_WINDOW_COOLDOWN_SEC,
    ONE_ONE_EIGHTY_WINDOW_MAIN_REQUEST_LIMIT,
    ONE_ONE_EIGHTY_WINDOW_MAX_SEC,
    ONE_ONE_EIGHTY_WINDOW_MIN_SEC,
    SITE_CONFIGS,
)
from engine.core_crawler.browser.session_router import BrowserSessionRouter
from engine.core_crawler.tunnels_11880 import (
    ensure_tunnel_watchdog,
    load_tunnel_statuses,
    refresh_tunnel_statuses,
    stop_tunnel_watchdog,
)

BROKER_SOCKET_PATH = "/tmp/core_crawler_browser.sock"
STATE_TTL_SEC = 7 * 24 * 60 * 60
SCHEDULE_ROTATE_MIN_SEC = 600.0
SCHEDULE_ROTATE_MAX_SEC = 1200.0
ROUTE_SITES = ("11880", "gs")
ROUTE_STATE_LOCK_TTL_SEC = 3.0
ROUTE_STATE_WAIT_SEC = 2.0
ROUTE_PLAN_CACHE_SEC = 60.0


def _broker_worker_parallelism() -> int:
    limit = 1
    for cfg in SITE_CONFIGS.values():
        try:
            limit = max(limit, int(cfg.concurrent_pages_per_session))
        except Exception:
            continue
    return max(1, limit)


def _cleanup_browser_processes() -> None:
    patterns = [
        "chrome-headless-shell",
        "playwright/driver/package/cli.js run-driver",
    ]
    for pattern in patterns:
        try:
            subprocess.run(
                ["pkill", "-f", pattern],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            pass


def _recv_exact(sock, size: int) -> bytes:
    out = bytearray()
    need = int(size)
    while len(out) < need:
        chunk = sock.recv(need - len(out))
        if not chunk:
            raise ConnectionError("socket_closed")
        out.extend(chunk)
    return bytes(out)


def _recv_json(sock) -> dict[str, Any]:
    raw_size = _recv_exact(sock, 4)
    size = struct.unpack("!I", raw_size)[0]
    payload = _recv_exact(sock, size)
    return json.loads(payload.decode("utf-8"))


def _send_json(sock, payload: dict[str, Any]) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    sock.sendall(struct.pack("!I", len(raw)))
    sock.sendall(raw)


def _int_value(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"expected int-compatible value, got {value!r}") from exc


def _normalize_fetch_payload(payload: dict[str, Any]) -> dict[str, Any]:
    site = str(payload.get("site") or "").strip()
    url = str(payload.get("url") or "").strip()
    kind = str(payload.get("kind") or "").strip()
    if not site:
        raise ValueError("site is required")
    if not url:
        raise ValueError("url is required")
    if not kind:
        raise ValueError("kind is required")
    return {
        "site": site,
        "url": url,
        "kind": kind,
        "task_id": _int_value(payload.get("task_id"), 0),
        "cb_id": _int_value(payload.get("cb_id"), 0),
        "referer": str(payload.get("referer") or ""),
        "mode": str(payload.get("mode") or ""),
        "method": str(payload.get("method") or "GET"),
        "form": dict(payload.get("form") or {}) or None,
        "extra_headers": dict(payload.get("extra_headers") or {}) or None,
        "preferred_slot_name": str(payload.get("preferred_slot_name") or ""),
        "preferred_slot_idx": _int_value(payload.get("preferred_slot_idx"), -1),
        "allowed_slot_names": [str(name) for name in list(payload.get("allowed_slot_names") or []) if str(name or "").strip()],
    }


def _cache_get_obj(key: str) -> Any:
    payload = CLIENT.get(key, ttl_sec=STATE_TTL_SEC)
    if not payload:
        return None
    try:
        return pickle.loads(payload)
    except Exception as exc:
        raise RuntimeError(f"BAD CACHE PAYLOAD {key}: {type(exc).__name__}: {exc}") from exc


def _cache_set_obj(key: str, value: Any) -> None:
    try:
        payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as exc:
        raise RuntimeError(f"CACHE ENCODE FAILED {key}: {type(exc).__name__}: {exc}") from exc
    CLIENT.set(key, payload, ttl_sec=STATE_TTL_SEC)


def _schedule_key(site: str) -> str:
    site_name = str(site or "").strip()
    if not site_name:
        raise ValueError("schedule key requires site")
    return f"core_crawler:slot_schedule:{site_name}"


def _rr_key(site: str) -> str:
    site_name = str(site or "").strip()
    if not site_name:
        raise ValueError("rr key requires site")
    return f"core_crawler:slot_rr:{site_name}"


def _route_plan_key() -> str:
    return "core_crawler:route_plan"


def _route_plan_lock_key() -> str:
    return "core_crawler:route_plan_lock"


def _window_key(site: str) -> str:
    site_name = str(site or "").strip()
    if not site_name:
        raise ValueError("window key requires site")
    return f"core_crawler:slot_window:{site_name}"


def _window_lock_key(site: str) -> str:
    site_name = str(site or "").strip()
    if not site_name:
        raise ValueError("window lock key requires site")
    return f"core_crawler:slot_window_lock:{site_name}"


def _quarantine_key(site: str) -> str:
    site_name = str(site or "").strip()
    if not site_name:
        raise ValueError("quarantine key requires site")
    return f"core_crawler:slot_quarantine:{site_name}"


def _try_lock(key: str, ttl_sec: float, owner: str) -> str:
    info = CLIENT.lock_try(key, ttl_sec=ttl_sec, owner=owner)
    if not info or not bool(info.get("acquired")):
        return ""
    return str(info.get("token") or "")


def _lock_until(key: str, ttl_sec: float, owner: str, wait_sec: float) -> str:
    deadline = time.time() + max(0.1, float(wait_sec))
    while time.time() < deadline:
        token = _try_lock(key, ttl_sec, owner)
        if token:
            return token
        time.sleep(0.05)
    return ""


def _release_lock(key: str, token: str) -> None:
    if not key or not token:
        return
    try:
        CLIENT.lock_release(key, token=token)
    except Exception:
        pass


def _configured_slot_names() -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for site in ROUTE_SITES:
        cfg = SITE_CONFIGS.get(site)
        if cfg is None:
            continue
        for raw_name in cfg.egress_slots:
            name = str(raw_name or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            ordered.append(name)
    return ordered


def _load_site_quarantine(site: str) -> dict[str, float]:
    raw = _cache_get_obj(_quarantine_key(site)) or {}
    if not isinstance(raw, dict):
        return {}
    now = time.time()
    out: dict[str, float] = {}
    for slot_name, until in raw.items():
        try:
            until_ts = float(until or 0.0)
        except Exception as exc:
            raise RuntimeError(
                f"BAD QUARANTINE STATE {site} {slot_name}: {type(exc).__name__}: {exc}"
            ) from exc
        if until_ts > now:
            out[str(slot_name)] = until_ts
    if out != raw:
        _cache_set_obj(_quarantine_key(site), out)
    return out


def _slot_is_live(slot_name: str, statuses: dict[str, dict[str, Any]]) -> bool:
    name = str(slot_name or "").strip()
    if not name:
        return False
    if name == "direct":
        return True
    return bool((statuses.get(name) or {}).get("alive"))


def _11880_target_active_count(live_count: int) -> int:
    count = max(0, int(live_count))
    if count <= 0:
        return 0
    limited = int(count * float(ONE_ONE_EIGHTY_ACTIVE_TUNNEL_RATIO))
    return min(ONE_ONE_EIGHTY_ACTIVE_TUNNEL_MAX, max(0, limited))


def _load_window_state(site: str, active_names: list[str]) -> dict[str, dict[str, float | int]]:
    raw = _cache_get_obj(_window_key(site)) or {}
    if not isinstance(raw, dict):
        return {}
    now = time.time()
    known_names = {str(name) for name in active_names}
    out: dict[str, dict[str, float | int]] = {}
    for slot_name, row in raw.items():
        name = str(slot_name or "").strip()
        if name not in known_names or not isinstance(row, dict):
            continue
        try:
            active_until = float(row.get("active_until") or 0.0)
            cool_until = float(row.get("cool_until") or 0.0)
            main_requests = max(0, int(row.get("main_requests") or 0))
        except Exception:
            continue
        if active_until <= now or main_requests >= ONE_ONE_EIGHTY_WINDOW_MAIN_REQUEST_LIMIT:
            active_until = 0.0
        if cool_until <= now and active_until <= 0.0:
            cool_until = 0.0
            main_requests = 0
        out[name] = {
            "active_until": active_until,
            "cool_until": cool_until,
            "main_requests": main_requests,
        }
    return out


def _active_window_names(site: str, available: list[str]) -> list[str]:
    now = time.time()
    state = _load_window_state(site, available)
    return [
        name
        for name in available
        if float((state.get(name) or {}).get("active_until") or 0.0) > now
        and int((state.get(name) or {}).get("main_requests") or 0) < ONE_ONE_EIGHTY_WINDOW_MAIN_REQUEST_LIMIT
    ]


def _fallback_11880_windows(site: str, available: list[str]) -> list[str]:
    active_names = _active_window_names(site, available)
    if active_names:
        return list(active_names)
    target_count = _11880_target_active_count(len(available))
    if target_count <= 0:
        return []
    rr_state = _cache_get_obj(_rr_key(site)) or {"pos": 0}
    rr_pos = int(rr_state.get("pos") or 0)
    start_idx = rr_pos % len(available)
    rotated = list(available[start_idx:] + available[:start_idx])
    return rotated[:target_count]


def _activate_11880_windows(site: str, available: list[str]) -> list[str]:
    if len(available) <= 1:
        return list(available)
    lock_key = _window_lock_key(site)
    owner = f"{site}:window:{uuid4().hex}"
    lock_token = _lock_until(lock_key, ROUTE_STATE_LOCK_TTL_SEC, owner, ROUTE_STATE_WAIT_SEC)
    if not lock_token:
        return _fallback_11880_windows(site, available)
    try:
        now = time.time()
        state = _load_window_state(site, available)
        active_names = _active_window_names(site, available)
        target_count = _11880_target_active_count(len(available))
        if len(active_names) > target_count:
            keep_names = set(active_names[:target_count])
            for name in available:
                if name not in keep_names and name in state:
                    state[name]["active_until"] = 0.0
            active_names = [name for name in active_names if name in keep_names]
        if len(active_names) < target_count:
            eligible_names = [
                name
                for name in available
                if name not in active_names
                and float((state.get(name) or {}).get("cool_until") or 0.0) <= now
            ]
            if eligible_names:
                rr_state = _cache_get_obj(_rr_key(site)) or {"pos": 0}
                rr_pos = int(rr_state.get("pos") or 0)
                start_idx = rr_pos % len(eligible_names)
                rotated_names = eligible_names[start_idx:] + eligible_names[:start_idx]
                needed = target_count - len(active_names)
                for name in rotated_names[:needed]:
                    state[name] = {
                        "active_until": now + random.uniform(ONE_ONE_EIGHTY_WINDOW_MIN_SEC, ONE_ONE_EIGHTY_WINDOW_MAX_SEC),
                        "cool_until": now + float(ONE_ONE_EIGHTY_WINDOW_COOLDOWN_SEC),
                        "main_requests": 0,
                    }
                    active_names.append(name)
                _cache_set_obj(_rr_key(site), {"pos": rr_pos + max(1, needed)})
        _cache_set_obj(_window_key(site), state)
        return [name for name in available if name in active_names]
    finally:
        _release_lock(lock_key, lock_token)


def _record_11880_main_request(site: str, slot_name: str) -> None:
    name = str(slot_name or "").strip()
    if site != "11880" or not name:
        return
    lock_key = _window_lock_key(site)
    owner = f"{site}:count:{name}:{uuid4().hex}"
    lock_token = _lock_until(lock_key, ROUTE_STATE_LOCK_TTL_SEC, owner, ROUTE_STATE_WAIT_SEC)
    if not lock_token:
        return
    try:
        cfg = SITE_CONFIGS.get(site)
        configured_names = list(getattr(cfg, "egress_slots", ()) or ()) or [name]
        state = _load_window_state(site, configured_names)
        row = dict(state.get(name) or {})
        row["main_requests"] = max(0, int(row.get("main_requests") or 0)) + 1
        if int(row["main_requests"]) >= ONE_ONE_EIGHTY_WINDOW_MAIN_REQUEST_LIMIT:
            row["active_until"] = 0.0
        state[name] = row
        _cache_set_obj(_window_key(site), state)
    finally:
        _release_lock(lock_key, lock_token)


def _is_11880_main_request(kind: str) -> bool:
    kind_name = str(kind or "").strip().lower()
    if not kind_name:
        return False
    return kind_name not in {"home", "referer"}


def _scheduled_rest_slot(site: str, active_names: list[str], has_quarantine: bool) -> str:
    if has_quarantine or len(active_names) <= 1:
        return ""
    now = time.time()
    state = _cache_get_obj(_schedule_key(site)) or {}
    excluded_name = str(state.get("name") or "")
    until = float(state.get("until") or 0.0)
    is_current = excluded_name in active_names and until > now and until <= (now + SCHEDULE_ROTATE_MAX_SEC + 5.0)
    if is_current:
        return excluded_name
    rr_state = _cache_get_obj(_rr_key(site)) or {"pos": 0}
    rr_pos = int(rr_state.get("pos") or 0)
    excluded_name = active_names[rr_pos % len(active_names)]
    _cache_set_obj(_rr_key(site), {"pos": rr_pos + 1})
    until = now + random.uniform(SCHEDULE_ROTATE_MIN_SEC, SCHEDULE_ROTATE_MAX_SEC)
    _cache_set_obj(_schedule_key(site), {"name": excluded_name, "until": until})
    return excluded_name


def _load_cached_route_plan() -> dict[str, list[str]] | None:
    raw = _cache_get_obj(_route_plan_key()) or {}
    if not isinstance(raw, dict):
        return None
    now = time.time()
    until = float(raw.get("until") or 0.0)
    plan = raw.get("plan") or {}
    if until <= now or not isinstance(plan, dict):
        return None
    out: dict[str, list[str]] = {}
    for site in ROUTE_SITES:
        names = [str(name) for name in list(plan.get(site) or []) if str(name or "").strip()]
        out[site] = names
    return out


def _compute_site_route_plan() -> dict[str, list[str]]:
    all_names = _configured_slot_names()
    if not all_names:
        return {site: [] for site in ROUTE_SITES}
    statuses = load_tunnel_statuses(all_names)
    missing_statuses = [name for name in all_names if name != "direct" and name not in statuses]
    if missing_statuses:
        statuses = refresh_tunnel_statuses(all_names)
    cfg_11880 = SITE_CONFIGS.get("11880")
    cfg_gs = SITE_CONFIGS.get("gs")
    slots_11880 = list(getattr(cfg_11880, "egress_slots", ()) or ())
    slots_gs = list(getattr(cfg_gs, "egress_slots", ()) or ())
    quarantine_11880 = _load_site_quarantine("11880")
    quarantine_gs = _load_site_quarantine("gs")

    live_11880 = [name for name in slots_11880 if _slot_is_live(name, statuses)]
    available_11880 = [name for name in live_11880 if name not in quarantine_11880]
    active_11880 = _activate_11880_windows("11880", available_11880) if available_11880 else []
    if not active_11880 and available_11880 and _11880_target_active_count(len(available_11880)) > 0:
        active_11880 = _fallback_11880_windows("11880", available_11880)
    used_11880 = set(active_11880)

    live_gs = [name for name in slots_gs if _slot_is_live(name, statuses)]
    available_gs = [name for name in live_gs if name not in quarantine_gs and name not in used_11880]
    rest_name = _scheduled_rest_slot("gs", available_gs, bool(quarantine_gs))
    active_gs = [name for name in available_gs if not rest_name or name != rest_name]

    return {
        "11880": list(active_11880),
        "gs": list(active_gs),
    }


def current_site_route_plan() -> dict[str, list[str]]:
    cached = _load_cached_route_plan()
    if cached is not None:
        return cached
    owner = f"route-plan:{uuid4().hex}"
    lock_token = _lock_until(
        _route_plan_lock_key(),
        ROUTE_STATE_LOCK_TTL_SEC,
        owner,
        ROUTE_STATE_WAIT_SEC,
    )
    if not lock_token:
        cached = _load_cached_route_plan()
        if cached is not None:
            return cached
        return _compute_site_route_plan()
    try:
        cached = _load_cached_route_plan()
        if cached is not None:
            return cached
        plan = _compute_site_route_plan()
        _cache_set_obj(
            _route_plan_key(),
            {
                "until": time.time() + float(ROUTE_PLAN_CACHE_SEC),
                "plan": plan,
            },
        )
        return plan
    finally:
        _release_lock(_route_plan_lock_key(), lock_token)


def site_active_slot_names(site: str) -> list[str]:
    return list((current_site_route_plan().get(str(site or "").strip()) or []))


class _BrokerDispatcher:
    def __init__(self, worker_count: int = BROKER_WORKERS, queue_maxsize: int = BROKER_QUEUE_MAX) -> None:
        self._queue_maxsize = max(1, int(queue_maxsize))
        self._jobs: list["multiprocessing.Queue[dict[str, Any] | None]"] = [
            multiprocessing.Queue(maxsize=self._queue_maxsize)
            for _ in range(max(1, int(worker_count)))
        ]
        self._results: "multiprocessing.Queue[dict[str, Any] | None]" = multiprocessing.Queue()
        self._state_mu = threading.Lock()
        self._state_cv = threading.Condition(self._state_mu)
        self._accepted_count = 0
        self._inflight: set[str] = set()
        self._completed: dict[str, dict[str, Any]] = {}
        self._rr_mu = threading.Lock()
        self._site_rr: dict[str, int] = {}
        self._stop = threading.Event()
        self._worker_count = max(1, int(worker_count))
        self._processes: list[multiprocessing.Process] = []
        self._collector: threading.Thread | None = None

    def start(self) -> None:
        self._collector = threading.Thread(
            target=self._collect_results,
            name="core_crawler_browser_broker_collector",
            daemon=True,
        )
        self._collector.start()
        for idx in range(self._worker_count):
            proc = multiprocessing.Process(
                target=_broker_worker_main,
                name=f"core_crawler_browser_broker_{idx}",
                args=(self._jobs[idx], self._results),
                daemon=True,
            )
            proc.start()
            self._processes.append(proc)

    def stop(self) -> None:
        self._stop.set()
        for job_q in self._jobs:
            job_q.put(None)
        for proc in self._processes:
            proc.join(timeout=5.0)
        self._results.put(None)
        if self._collector is not None:
            self._collector.join(timeout=5.0)
        with self._state_cv:
            for request_id in list(self._inflight):
                self._completed[request_id] = {"ok": False, "error": "BROKER_STOPPED", "detail": "dispatcher stopped"}
            self._inflight.clear()
            self._accepted_count = 0
            self._state_cv.notify_all()

    def _pick_slot_for_site(self, site: str, allowed_slot_names: list[str]) -> tuple[str, int]:
        cfg = SITE_CONFIGS.get(site)
        if cfg is None or not allowed_slot_names:
            return "", 0
        total_slots = max(1, len(allowed_slot_names) * int(cfg.sessions_per_egress))
        with self._rr_mu:
            pos = int(self._site_rr.get(site, 0))
            self._site_rr[site] = pos + 1
        logical_idx = pos % total_slots
        egress_idx = logical_idx // int(cfg.sessions_per_egress)
        slot_idx = logical_idx % int(cfg.sessions_per_egress)
        return str(allowed_slot_names[egress_idx]), int(slot_idx)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        request_id = uuid4().hex
        try:
            payload = _normalize_fetch_payload(payload)
        except ValueError as exc:
            return {"ok": False, "error": "BAD_REQUEST", "detail": str(exc)}
        site = str(payload.get("site") or "")
        allowed_slot_names = site_active_slot_names(site)
        if site in SITE_CONFIGS and not allowed_slot_names:
            return {"ok": False, "error": "NO_ACTIVE_SITE_SLOTS", "detail": f"{site} has no active live slots"}
        payload["allowed_slot_names"] = list(allowed_slot_names)
        preferred_slot_name = str(payload.get("preferred_slot_name") or "")
        preferred_slot_idx = _int_value(payload.get("preferred_slot_idx"), -1)
        if site in SITE_CONFIGS and preferred_slot_name not in allowed_slot_names:
            preferred_slot_name = ""
            preferred_slot_idx = -1
        if site in SITE_CONFIGS and not preferred_slot_name:
            preferred_slot_name, preferred_slot_idx = self._pick_slot_for_site(site, allowed_slot_names)
            payload["preferred_slot_name"] = preferred_slot_name
            payload["preferred_slot_idx"] = preferred_slot_idx
        cfg = SITE_CONFIGS.get(site)
        if cfg is None:
            worker_idx = 0
        else:
            try:
                egress_idx = list(cfg.egress_slots).index(str(preferred_slot_name))
            except ValueError:
                egress_idx = 0
            worker_idx = int(egress_idx % self._worker_count)
        with self._state_mu:
            if self._accepted_count >= self._queue_maxsize:
                return {"ok": False, "error": "BROKER_BUSY", "detail": "TRY_AGAIN"}
            self._accepted_count += 1
            self._inflight.add(request_id)
        try:
            self._jobs[worker_idx].put_nowait({"request_id": request_id, "payload": payload})
        except queue.Full:
            with self._state_mu:
                self._inflight.discard(request_id)
                self._accepted_count = max(0, int(self._accepted_count) - 1)
            return {"ok": False, "error": "BROKER_BUSY", "detail": "TRY_AGAIN"}
        if site == "11880" and _is_11880_main_request(str(payload.get("kind") or "")):
            _record_11880_main_request(site, preferred_slot_name)
        return {"ok": True, "accepted": True, "request_id": request_id}

    def poll_result(self, request_id: str) -> dict[str, Any]:
        request_id = str(request_id or "")
        if not request_id:
            return {"ok": False, "error": "BAD_REQUEST", "detail": "request_id required"}
        with self._state_mu:
            ready = self._completed.pop(request_id, None)
            if ready is not None:
                return dict(ready)
            if request_id in self._inflight:
                return {"ok": True, "pending": True}
        return {"ok": False, "error": "NOT_FOUND", "detail": "UNKNOWN_REQUEST_ID"}

    def wait_result(self, request_id: str, timeout_sec: float) -> dict[str, Any]:
        request_id = str(request_id or "")
        if not request_id:
            return {"ok": False, "error": "BAD_REQUEST", "detail": "request_id required"}
        deadline = time.time() + max(0.1, float(timeout_sec or 0.0))
        with self._state_cv:
            while True:
                ready = self._completed.pop(request_id, None)
                if ready is not None:
                    return dict(ready)
                if request_id not in self._inflight:
                    return {"ok": False, "error": "NOT_FOUND", "detail": "UNKNOWN_REQUEST_ID"}
                remaining = deadline - time.time()
                if remaining <= 0:
                    return {"ok": True, "pending": True}
                self._state_cv.wait(timeout=remaining)

    def _collect_results(self) -> None:
        while not self._stop.is_set():
            try:
                item = self._results.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                break
            request_id = str(item.get("request_id") or "")
            if not request_id:
                continue
            response = dict(item.get("response") or {})
            with self._state_cv:
                self._inflight.discard(request_id)
                self._accepted_count = max(0, int(self._accepted_count) - 1)
                self._completed[request_id] = response
                if len(self._completed) > (self._queue_maxsize * 4):
                    doomed = sorted(self._completed.items(), key=lambda row: float((row[1] or {}).get("_completed_ts") or 0.0))
                    for doomed_request_id, _ in doomed[: max(1, len(self._completed) - (self._queue_maxsize * 4))]:
                        self._completed.pop(doomed_request_id, None)
                self._state_cv.notify_all()


def _broker_worker_main(
    jobs: "multiprocessing.Queue[dict[str, Any] | None]",
    results: "multiprocessing.Queue[dict[str, Any] | None]",
) -> None:
    max_inflight = _broker_worker_parallelism()
    router_local = threading.local()
    router_mu = threading.Lock()
    routers: list[BrowserSessionRouter] = []
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=max_inflight,
        thread_name_prefix="core_crawler_broker_fetch",
    )
    active: set[concurrent.futures.Future[None]] = set()

    def _get_router() -> BrowserSessionRouter:
        router = getattr(router_local, "router", None)
        if router is not None:
            return router
        router = BrowserSessionRouter()
        router_local.router = router
        with router_mu:
            routers.append(router)
        return router

    def _flush_done(done_futures: set[concurrent.futures.Future[None]]) -> None:
        for future in done_futures:
            active.discard(future)
            try:
                future.result()
            except Exception:
                pass

    def _run_one(item: dict[str, Any]) -> None:
        request_id = str(item.get("request_id") or "")
        payload = _normalize_fetch_payload(dict(item.get("payload") or {}))
        router = _get_router()
        try:
            result = router.fetch(
                site=str(payload["site"]),
                url=str(payload["url"]),
                kind=str(payload["kind"]),
                task_id=int(payload["task_id"]),
                cb_id=int(payload["cb_id"]),
                referer=str(payload.get("referer") or ""),
                mode=str(payload.get("mode") or ""),
                method=str(payload.get("method") or "GET"),
                form=dict(payload.get("form") or {}) or None,
                extra_headers=dict(payload.get("extra_headers") or {}) or None,
                preferred_slot_name=str(payload.get("preferred_slot_name") or ""),
                preferred_slot_idx=_int_value(payload.get("preferred_slot_idx"), -1),
                allowed_slot_names=[str(name) for name in list(payload.get("allowed_slot_names") or []) if str(name or "").strip()],
            )
            results.put(
                {
                    "request_id": request_id,
                    "response": {"ok": True, "result": asdict(result), "_completed_ts": time.time()},
                }
            )
        except Exception as exc:
            print(
                f"[browser-broker] fail request_id={request_id} "
                f"site={payload.get('site')} cb_id={payload.get('cb_id')} "
                f"error={type(exc).__name__}: {exc}",
                flush=True,
            )
            results.put(
                {
                    "request_id": request_id,
                    "response": {
                        "ok": False,
                        "error": type(exc).__name__,
                        "detail": str(exc),
                        "_completed_ts": time.time(),
                    },
                }
            )

    try:
        stop_requested = False
        while True:
            if active:
                done, _ = concurrent.futures.wait(
                    active,
                    timeout=0.0,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                if done:
                    _flush_done(set(done))
            if stop_requested:
                if not active:
                    break
                done, _ = concurrent.futures.wait(
                    active,
                    timeout=0.1,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                if done:
                    _flush_done(set(done))
                continue
            if len(active) >= max_inflight:
                done, _ = concurrent.futures.wait(
                    active,
                    timeout=0.1,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                if done:
                    _flush_done(set(done))
                continue
            try:
                item = jobs.get(timeout=0.1)
            except queue.Empty:
                continue
            if item is None:
                stop_requested = True
                continue
            active.add(executor.submit(_run_one, item))
    finally:
        executor.shutdown(wait=True, cancel_futures=False)
        for router in list(routers):
            try:
                router.close_all()
            except Exception:
                pass


class _BrokerUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, socket_path: str, dispatcher: _BrokerDispatcher):
        self.dispatcher = dispatcher
        super().__init__(socket_path, _BrokerHandler)


class _BrokerHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        try:
            payload = _recv_json(self.request)
        except ConnectionError:
            return
        action = str(payload.get("action") or "")
        if action == "ping":
            try:
                _send_json(self.request, {"ok": True, "pong": True})
            except BrokenPipeError:
                pass
            return
        if action == "submit":
            response = self.server.dispatcher.submit(payload)
        elif action == "wait_result":
            response = self.server.dispatcher.wait_result(
                str(payload.get("request_id") or ""),
                float(payload.get("timeout_sec") or 0.0),
            )
        elif action == "result":
            response = self.server.dispatcher.poll_result(str(payload.get("request_id") or ""))
        else:
            response = {"ok": False, "error": "BAD_REQUEST", "detail": f"unknown action: {action}"}
        try:
            _send_json(self.request, response)
        except BrokenPipeError:
            pass


def run_browser_broker(socket_path: str = BROKER_SOCKET_PATH) -> None:
    path = Path(socket_path)
    _cleanup_browser_processes()
    try:
        if path.exists() or path.is_socket():
            path.unlink()
    except FileNotFoundError:
        pass

    dispatcher = _BrokerDispatcher()
    dispatcher.start()
    ensure_tunnel_watchdog()
    server = _BrokerUnixServer(str(path), dispatcher)
    try:
        os.chmod(str(path), 0o666)
    except Exception:
        pass

    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        stop_tunnel_watchdog()
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            server.server_close()
        except Exception:
            pass
        dispatcher.stop()
        _cleanup_browser_processes()
        try:
            if path.exists() or path.is_socket():
                path.unlink()
        except Exception:
            pass
