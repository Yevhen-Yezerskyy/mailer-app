# FILE: engine/core_crawler/browser/broker_server.py
# DATE: 2026-03-27
# PURPOSE: Local unix-socket browser broker process for core_crawler fetch requests.

from __future__ import annotations

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
from engine.core_crawler.browser.session_config import BROKER_QUEUE_MAX, BROKER_WORKERS, SITE_CONFIGS
from engine.core_crawler.browser.session_router import BrowserSessionRouter
from engine.core_crawler.tunnels_11880 import ensure_tunnel_watchdog, load_tunnel_statuses, stop_tunnel_watchdog

BROKER_SOCKET_PATH = "/tmp/core_crawler_browser.sock"
STATE_TTL_SEC = 7 * 24 * 60 * 60
SCHEDULE_ROTATE_MIN_SEC = 600.0
SCHEDULE_ROTATE_MAX_SEC = 1200.0
ROUTE_SITES = ("11880", "gs")


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


def _quarantine_key(site: str) -> str:
    site_name = str(site or "").strip()
    if not site_name:
        raise ValueError("quarantine key requires site")
    return f"core_crawler:slot_quarantine:{site_name}"


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


def _site_owned_slot_names() -> dict[str, list[str]]:
    owned = {site: [] for site in ROUTE_SITES}
    slots = _configured_slot_names()
    if not slots:
        return owned
    site_count = len(ROUTE_SITES)
    for idx, slot_name in enumerate(slots):
        site = ROUTE_SITES[idx % site_count]
        owned[site].append(slot_name)
    return owned


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


def site_active_slot_names(site: str) -> list[str]:
    site_name = str(site or "").strip()
    cfg = SITE_CONFIGS.get(site_name)
    owned = _site_owned_slot_names().get(site_name) or []
    if cfg is not None:
        configured = {str(name or "").strip() for name in cfg.egress_slots if str(name or "").strip()}
        owned = [name for name in owned if name in configured]
    if not owned:
        return []
    statuses = load_tunnel_statuses(owned)
    quarantine = _load_site_quarantine(site_name)
    live_owned = [name for name in owned if _slot_is_live(name, statuses)]
    available = [name for name in live_owned if name not in quarantine]
    if not available:
        return []
    rest_name = _scheduled_rest_slot(site_name, available, bool(quarantine))
    if not rest_name:
        return available
    return [name for name in available if name != rest_name]


def current_site_route_plan() -> dict[str, list[str]]:
    return {site: site_active_slot_names(site) for site in ROUTE_SITES}


class _BrokerDispatcher:
    def __init__(self, worker_count: int = BROKER_WORKERS, queue_maxsize: int = BROKER_QUEUE_MAX) -> None:
        self._queue_maxsize = max(1, int(queue_maxsize))
        self._jobs: list["multiprocessing.Queue[dict[str, Any] | None]"] = [
            multiprocessing.Queue(maxsize=self._queue_maxsize)
            for _ in range(max(1, int(worker_count)))
        ]
        self._results: "multiprocessing.Queue[dict[str, Any] | None]" = multiprocessing.Queue()
        self._state_mu = threading.Lock()
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
        with self._state_mu:
            for request_id in list(self._inflight):
                self._completed[request_id] = {"ok": False, "error": "BROKER_STOPPED", "detail": "dispatcher stopped"}
            self._inflight.clear()
            self._accepted_count = 0

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
            with self._state_mu:
                self._inflight.discard(request_id)
                self._accepted_count = max(0, int(self._accepted_count) - 1)
                self._completed[request_id] = response
                if len(self._completed) > (self._queue_maxsize * 4):
                    doomed = sorted(self._completed.items(), key=lambda row: float((row[1] or {}).get("_completed_ts") or 0.0))
                    for doomed_request_id, _ in doomed[: max(1, len(self._completed) - (self._queue_maxsize * 4))]:
                        self._completed.pop(doomed_request_id, None)


def _broker_worker_main(
    jobs: "multiprocessing.Queue[dict[str, Any] | None]",
    results: "multiprocessing.Queue[dict[str, Any] | None]",
) -> None:
    router = BrowserSessionRouter()
    try:
        while True:
            item = jobs.get()
            if item is None:
                break
            request_id = str(item.get("request_id") or "")
            payload = _normalize_fetch_payload(dict(item.get("payload") or {}))
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
    finally:
        router.close_all()


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
    ensure_tunnel_watchdog()
    dispatcher.start()
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
