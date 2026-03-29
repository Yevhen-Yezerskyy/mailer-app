# FILE: engine/core_crawler/crawler_processor.py
# DATE: 2026-03-29
# PURPOSE: Production core_crawler processor with browser broker plus one-shot pair workers.

from __future__ import annotations

import multiprocessing
import pickle
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

from engine.common.cache.client import CLIENT
from engine.core_crawler.browser.broker_server import run_browser_broker
from engine.core_crawler.browser.session_config import SITE_CONFIGS
from engine.core_crawler.fetch_cb import pending_items_exist

TICK_SEC = 1.0
QUARANTINE_KEY = "core_crawler:slot_quarantine:global"


@dataclass
class WorkerProcess:
    process: subprocess.Popen[Any]
    started_at: float


def _configured_route_names() -> list[str]:
    route_names: list[str] = []
    seen: set[str] = set()
    for site_name in ("11880", "gs"):
        cfg = SITE_CONFIGS.get(site_name)
        if cfg is None:
            continue
        for name in cfg.egress_slots:
            route_name = str(name or "").strip()
            if not route_name or route_name in seen:
                continue
            seen.add(route_name)
            route_names.append(route_name)
    return route_names


def _load_global_quarantine() -> dict[str, float]:
    payload = CLIENT.get(QUARANTINE_KEY, ttl_sec=24 * 60 * 60)
    if not payload:
        return {}
    raw = pickle.loads(payload)
    if not isinstance(raw, dict):
        return {}
    now = time.time()
    active: dict[str, float] = {}
    for name, until in raw.items():
        until_ts = float(until or 0.0)
        if until_ts > now:
            active[str(name)] = until_ts
    return active


def _target_parallelism() -> int:
    route_names = _configured_route_names()
    if not route_names:
        return 0
    quarantine = _load_global_quarantine()
    if quarantine:
        available_routes = [name for name in route_names if name not in quarantine]
    else:
        available_routes = list(route_names)
        if len(available_routes) > 1:
            available_routes = available_routes[:-1]
    return max(0, int(len(available_routes) * 2))


def _collect_finished_workers(active: list[WorkerProcess]) -> list[WorkerProcess]:
    still_running: list[WorkerProcess] = []
    for worker in active:
        if worker.process.poll() is None:
            still_running.append(worker)
            continue
        print(
            f"[crawler_processor] worker_done pid={worker.process.pid} "
            f"rc={worker.process.returncode}",
            flush=True,
        )
    return still_running


def _launch_worker() -> WorkerProcess:
    proc = subprocess.Popen(
        [sys.executable, "-m", "engine.core_crawler.fetch_cb"],
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    print(f"[crawler_processor] worker_start pid={proc.pid}", flush=True)
    return WorkerProcess(process=proc, started_at=time.time())


def _stop_workers(active: list[WorkerProcess]) -> None:
    live = [worker for worker in active if worker.process.poll() is None]
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

    active: list[WorkerProcess] = []
    last_target = -1
    try:
        while not stop_requested["value"]:
            if not broker.is_alive():
                raise RuntimeError("browser broker stopped unexpectedly")
            active = _collect_finished_workers(active)
            target = _target_parallelism()
            if target != last_target:
                print(f"[crawler_processor] target_parallel={target}", flush=True)
                last_target = target
            if len(active) < target and pending_items_exist():
                active.append(_launch_worker())
            time.sleep(TICK_SEC)
    finally:
        _stop_workers(active)
        if broker.is_alive():
            broker.terminate()
            broker.join(timeout=5.0)
        if broker.is_alive():
            broker.kill()
            broker.join(timeout=5.0)


if __name__ == "__main__":
    main()
