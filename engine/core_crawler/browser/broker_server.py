# FILE: engine/core_crawler/browser/broker_server.py
# DATE: 2026-03-27
# PURPOSE: Local unix-socket browser broker process for core_crawler fetch requests.

from __future__ import annotations

import json
import multiprocessing
import os
import queue
import socketserver
import struct
import subprocess
import threading
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from engine.core_crawler.browser.session_config import BROKER_QUEUE_MAX, BROKER_WORKERS, SITE_CONFIGS
from engine.core_crawler.browser.session_router import BrowserSessionRouter

BROKER_SOCKET_PATH = "/tmp/core_crawler_browser.sock"


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

    def _pick_slot_for_site(self, site: str) -> tuple[str, int]:
        cfg = SITE_CONFIGS.get(site)
        if cfg is None:
            return "", 0
        total_slots = max(1, len(cfg.egress_slots) * int(cfg.sessions_per_egress))
        with self._rr_mu:
            pos = int(self._site_rr.get(site, 0))
            self._site_rr[site] = pos + 1
        logical_idx = pos % total_slots
        egress_idx = logical_idx // int(cfg.sessions_per_egress)
        slot_idx = logical_idx % int(cfg.sessions_per_egress)
        return str(cfg.egress_slots[egress_idx]), int(slot_idx)

    def _worker_idx_for_slot(self, site: str, slot_name: str, slot_idx: int) -> int:
        cfg = SITE_CONFIGS.get(site)
        if cfg is None:
            return 0
        try:
            egress_idx = list(cfg.egress_slots).index(str(slot_name))
        except ValueError:
            egress_idx = 0
        return int(egress_idx % self._worker_count)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        request_id = uuid4().hex
        payload = dict(payload)
        site = str(payload.get("site") or "")
        preferred_slot_name = str(payload.get("preferred_slot_name") or "")
        preferred_slot_idx = int(payload.get("preferred_slot_idx") or 0)
        if site in SITE_CONFIGS and not preferred_slot_name:
            preferred_slot_name, preferred_slot_idx = self._pick_slot_for_site(site)
            payload["preferred_slot_name"] = preferred_slot_name
            payload["preferred_slot_idx"] = preferred_slot_idx
        worker_idx = self._worker_idx_for_slot(site, preferred_slot_name, preferred_slot_idx)
        with self._state_mu:
            if self._accepted_count >= self._queue_maxsize:
                print(
                    f"[browser-broker] busy request_id={request_id} "
                    f"site={payload.get('site')} kind={payload.get('kind')} "
                    f"task_id={payload.get('task_id')} cb_id={payload.get('cb_id')} "
                    f"accepted={self._accepted_count}/{self._queue_maxsize}",
                    flush=True,
                )
                return {"ok": False, "error": "BROKER_BUSY", "detail": "TRY_AGAIN"}
            self._accepted_count += 1
            self._inflight.add(request_id)
        print(
            f"[browser-broker] enqueue request_id={request_id} "
            f"site={payload.get('site')} kind={payload.get('kind')} "
            f"task_id={payload.get('task_id')} cb_id={payload.get('cb_id')} "
            f"worker={worker_idx} slot={preferred_slot_name}:{preferred_slot_idx} "
            f"accepted={self._accepted_count}/{self._queue_maxsize}",
            flush=True,
        )
        try:
            self._jobs[worker_idx].put_nowait({"request_id": request_id, "payload": payload})
        except queue.Full:
            with self._state_mu:
                self._inflight.discard(request_id)
                self._accepted_count = max(0, int(self._accepted_count) - 1)
            print(
                f"[browser-broker] busy request_id={request_id} "
                f"site={payload.get('site')} kind={payload.get('kind')} "
                f"task_id={payload.get('task_id')} cb_id={payload.get('cb_id')} "
                f"worker={worker_idx} queue_full=1",
                flush=True,
            )
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
            payload = dict(item.get("payload") or {})
            try:
                action = str(payload.get("action") or "fetch")
                if action == "ping":
                    results.put({"request_id": request_id, "response": {"ok": True, "pong": True}})
                    continue
                print(
                    f"[browser-broker] start request_id={request_id} "
                    f"site={payload.get('site')} kind={payload.get('kind')} "
                    f"cb_id={payload.get('cb_id')} url={payload.get('url')}",
                    flush=True,
                )
                result = router.fetch(
                    site=str(payload["site"]),
                    url=str(payload["url"]),
                    kind=str(payload["kind"]),
                    task_id=int(payload["task_id"]),
                    cb_id=int(payload["cb_id"]),
                    referer=str(payload.get("referer") or ""),
                    mode=str(payload.get("mode") or ""),
                    preferred_slot_name=str(payload.get("preferred_slot_name") or ""),
                    preferred_slot_idx=int(payload.get("preferred_slot_idx") or 0),
                )
                print(
                    f"[browser-broker] done request_id={request_id} "
                    f"site={payload.get('site')} cb_id={payload.get('cb_id')} "
                    f"status={result.status} session={result.session_id}",
                    flush=True,
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
        if action in ("fetch", "submit"):
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
    dispatcher.start()
    server = _BrokerUnixServer(str(path), dispatcher)
    try:
        os.chmod(str(path), 0o666)
    except Exception:
        pass

    try:
        server.serve_forever(poll_interval=0.2)
    finally:
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
