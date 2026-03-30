# FILE: engine/core_crawler/crawler_processor.py
# DATE: 2026-03-29
# PURPOSE: Production core_crawler processor with browser broker plus one-shot pair workers.

from __future__ import annotations

import multiprocessing
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

from engine.core_crawler.browser.broker_server import current_site_route_plan, run_browser_broker
from engine.core_crawler.fetch_cb import pending_items_exist

TICK_SEC = 1.0
CATALOGS = ("11880", "gs")
CATALOG_MAX_WORKERS = {
    "11880": 2,
    "gs": 2,
}
CATALOG_ROUTES_PER_WORKER = {
    "11880": 1,
    "gs": 3,
}


@dataclass
class WorkerProcess:
    process: subprocess.Popen[Any]
    started_at: float


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
        [sys.executable, "-m", "engine.core_crawler.fetch_cb", "--catalog", str(catalog)],
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


def main() -> None:
    stop_requested = {"value": False}

    def _handle_signal(signum, _frame) -> None:
        stop_requested["value"] = True
        print(f"[crawler_processor] signal={int(signum)} stop_requested=yes", flush=True)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    broker = multiprocessing.Process(target=run_browser_broker, name="core_crawler_browser_broker")
    broker.start()
    print(f"[crawler_processor] broker_start pid={broker.pid}", flush=True)

    active_by_catalog: dict[str, list[WorkerProcess]] = {catalog: [] for catalog in CATALOGS}
    last_targets: dict[str, int] = {catalog: -1 for catalog in CATALOGS}
    try:
        while not stop_requested["value"]:
            if not broker.is_alive():
                raise RuntimeError("browser broker stopped unexpectedly")
            targets = _target_parallelism_by_catalog()
            for catalog in CATALOGS:
                active_by_catalog[catalog] = _collect_finished_workers(catalog, active_by_catalog[catalog])
                target = int(targets.get(catalog) or 0)
                if target != last_targets[catalog]:
                    print(f"[crawler_processor] target_parallel catalog={catalog} value={target}", flush=True)
                    last_targets[catalog] = target
                if len(active_by_catalog[catalog]) >= target:
                    continue
                if not pending_items_exist(catalog):
                    continue
                active_by_catalog[catalog].append(_launch_worker(catalog))
            time.sleep(TICK_SEC)
    finally:
        _stop_workers(active_by_catalog)
        if broker.is_alive():
            broker.terminate()
            broker.join(timeout=5.0)
        if broker.is_alive():
            broker.kill()
            broker.join(timeout=5.0)


if __name__ == "__main__":
    main()
