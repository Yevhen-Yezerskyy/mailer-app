# FILE: engine/core_crawler/crawler_processor.py
# DATE: 2026-03-29
# PURPOSE: Production core_crawler processor with browser broker plus one-shot pair workers.

from __future__ import annotations

import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

from engine.common.cache.client import CLIENT, _redis_call
from engine.core_crawler.browser.broker_server import current_site_route_plan
from engine.core_crawler.fetch_cb import pending_items_exist
from engine.core_crawler.tunnels_11880 import ensure_tunnel_watchdog, stop_tunnel_watchdog

TICK_SEC = 1.0
CATALOGS = ("11880", "gs")
CATALOG_MAX_WORKERS = {
    "11880": 4,
    "gs": 3,
}
CATALOG_ROUTES_PER_WORKER = {
    "11880": 1,
    "gs": 1,
}


@dataclass
class WorkerProcess:
    process: subprocess.Popen[Any]
    started_at: float


def _scan_redis_keys(pattern: str) -> list[str]:
    cursor = "0"
    found: list[str] = []
    seen: set[str] = set()
    while True:
        reply = _redis_call("SCAN", cursor, "MATCH", str(pattern), "COUNT", 200)
        if not isinstance(reply, list) or len(reply) < 2:
            break
        next_cursor = reply[0]
        raw_keys = reply[1] if isinstance(reply[1], list) else []
        cursor = next_cursor.decode("utf-8", errors="replace") if isinstance(next_cursor, (bytes, bytearray)) else str(next_cursor)
        for raw_key in raw_keys:
            key = raw_key.decode("utf-8", errors="replace") if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
            if not key or key in seen:
                continue
            seen.add(key)
            found.append(key)
        if cursor == "0":
            break
    return found


def _clear_stale_route_worker_locks() -> int:
    keys = _scan_redis_keys("lock:core_crawler:route_worker:*")
    if not keys:
        return 0
    return int(CLIENT.delete_many(keys) or 0)


def _target_parallelism_by_catalog() -> dict[str, int]:
    route_plan = current_site_route_plan()
    targets: dict[str, int] = {}
    for site_name in CATALOGS:
        route_count = max(0, int(len(route_plan.get(site_name) or [])))
        if route_count <= 0:
            targets[site_name] = 0
            continue
        routes_per_worker = max(1, int(CATALOG_ROUTES_PER_WORKER.get(site_name, 1)))
        max_workers = max(1, int(CATALOG_MAX_WORKERS.get(site_name, 1)))
        target = max(1, int((route_count + routes_per_worker - 1) // routes_per_worker))
        targets[site_name] = min(max_workers, target)
    return targets


def _collect_finished_workers(catalog: str, active: list[WorkerProcess]) -> list[WorkerProcess]:
    still_running: list[WorkerProcess] = []
    for worker in active:
        if worker.process.poll() is None:
            still_running.append(worker)
            continue
        print(
            f"[crawler_processor] worker_done catalog={catalog} pid={worker.process.pid} "
            f"rc={worker.process.returncode}",
            flush=True,
        )
    return still_running


def _launch_worker(catalog: str) -> WorkerProcess:
    proc = subprocess.Popen(
        [sys.executable, "-m", "engine.core_crawler.fetch_cb", "--catalog", str(catalog), "--worker-loop"],
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    print(f"[crawler_processor] worker_start catalog={catalog} pid={proc.pid}", flush=True)
    return WorkerProcess(process=proc, started_at=time.time())


def _stop_workers(active_by_catalog: dict[str, list[WorkerProcess]]) -> None:
    live = [
        worker
        for workers in active_by_catalog.values()
        for worker in workers
        if worker.process.poll() is None
    ]
    if not live:
        return
    for worker in live:
        try:
            worker.process.terminate()
        except Exception:
            continue
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if all(worker.process.poll() is not None for worker in live):
            return
        time.sleep(0.1)
    for worker in live:
        if worker.process.poll() is not None:
            continue
        try:
            worker.process.kill()
        except Exception:
            continue


def _trim_workers(active: list[WorkerProcess], target: int) -> list[WorkerProcess]:
    running = list(active)
    excess = max(0, len(running) - max(0, int(target)))
    if excess <= 0:
        return running
    doomed = running[-excess:]
    survivors = running[:-excess]
    for worker in doomed:
        if worker.process.poll() is not None:
            continue
        try:
            worker.process.terminate()
        except Exception:
            continue
    return survivors + [worker for worker in doomed if worker.process.poll() is None]


def main() -> None:
    stop_requested = {"value": False}

    def _handle_signal(signum, _frame) -> None:
        stop_requested["value"] = True
        print(f"[crawler_processor] signal={int(signum)} stop_requested=yes", flush=True)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    active_by_catalog: dict[str, list[WorkerProcess]] = {catalog: [] for catalog in CATALOGS}
    last_targets: dict[str, int] = {catalog: -1 for catalog in CATALOGS}
    try:
        cleared = _clear_stale_route_worker_locks()
        if cleared > 0:
            print(f"[crawler_processor] cleared_stale_route_locks count={cleared}", flush=True)
        ensure_tunnel_watchdog()
        while not stop_requested["value"]:
            targets = _target_parallelism_by_catalog()
            for catalog in CATALOGS:
                active_by_catalog[catalog] = _collect_finished_workers(catalog, active_by_catalog[catalog])
                target = int(targets.get(catalog) or 0)
                if target != last_targets[catalog]:
                    print(f"[crawler_processor] target_parallel catalog={catalog} value={target}", flush=True)
                    last_targets[catalog] = target
                active_by_catalog[catalog] = _trim_workers(active_by_catalog[catalog], target)
                if len(active_by_catalog[catalog]) >= target:
                    continue
                if not pending_items_exist(catalog):
                    continue
                active_by_catalog[catalog].append(_launch_worker(catalog))
            time.sleep(TICK_SEC)
    finally:
        _stop_workers(active_by_catalog)
        stop_tunnel_watchdog()


if __name__ == "__main__":
    main()
