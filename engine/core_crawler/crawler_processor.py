# FILE: engine/core_crawler/crawler_processor.py
# DATE: 2026-03-29
# PURPOSE: Production core_crawler processor with two long-lived site executors and one-shot global pair dispatchers.

from __future__ import annotations

import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from engine.common.cache.client import CLIENT, _redis_call
from engine.core_crawler.browser.broker_server import current_site_route_plan
from engine.core_crawler.browser.session_config import CRAWLER_ACTIVE_TUNNEL_CAP
from engine.core_crawler.tunnels_11880 import ensure_tunnel_watchdog, stop_tunnel_watchdog

TICK_SEC = 5.0
_TZ_BERLIN = ZoneInfo("Europe/Berlin")


@dataclass
class WorkerProcess:
    label: str
    process: subprocess.Popen[Any]
    started_at: float


def _ts() -> str:
    return datetime.now(_TZ_BERLIN).isoformat(timespec="seconds")


def _log(message: str) -> None:
    print(f"{_ts()}\t{message}", flush=True)


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


def _clear_dispatch_queues() -> int:
    keys = _scan_redis_keys("core_crawler:dispatch_queue:*")
    if not keys:
        return 0
    return int(CLIENT.delete_many(keys) or 0)


def _target_parallelism() -> tuple[int, dict[str, int]]:
    route_plan = current_site_route_plan()
    per_site = {
        str(site_name): max(0, int(len(route_plan.get(site_name) or [])))
        for site_name in sorted(route_plan.keys())
    }
    total = min(int(CRAWLER_ACTIVE_TUNNEL_CAP), sum(per_site.values()))
    return total, per_site


def _collect_finished_workers(active: list[WorkerProcess]) -> list[WorkerProcess]:
    still_running: list[WorkerProcess] = []
    for worker in active:
        if worker.process.poll() is None:
            still_running.append(worker)
            continue
        _log(
            f"[crawler_processor] worker_done label={worker.label} "
            f"pid={worker.process.pid} rc={worker.process.returncode}"
        )
    return still_running


def _launch_executor(catalog: str) -> WorkerProcess:
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "engine.core_crawler.fetch_cb",
            "--site-executor",
            "--catalog",
            str(catalog or "").strip(),
        ],
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    label = f"executor:{str(catalog or '').strip()}"
    _log(f"[crawler_processor] worker_start label={label} pid={proc.pid}")
    return WorkerProcess(label=label, process=proc, started_at=time.time())


def _launch_dispatcher() -> WorkerProcess:
    proc = subprocess.Popen(
        [sys.executable, "-m", "engine.core_crawler.fetch_cb", "--dispatcher"],
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    label = "dispatch"
    _log(f"[crawler_processor] worker_start label={label} pid={proc.pid}")
    return WorkerProcess(label=label, process=proc, started_at=time.time())


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


def _ensure_executor(active: list[WorkerProcess], catalog: str) -> list[WorkerProcess]:
    label = f"executor:{str(catalog or '').strip()}"
    for worker in active:
        if worker.label == label and worker.process.poll() is None:
            return active
    active.append(_launch_executor(catalog))
    return active


def main() -> None:
    stop_requested = {"value": False}

    def _handle_signal(signum, _frame) -> None:
        stop_requested["value"] = True
        _log(f"[crawler_processor] signal={int(signum)} stop_requested=yes")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    active_workers: list[WorkerProcess] = []
    active_dispatchers: list[WorkerProcess] = []
    last_total_target = -1
    try:
        cleared = _clear_stale_route_worker_locks()
        if cleared > 0:
            _log(f"[crawler_processor] cleared_stale_route_locks count={cleared}")
        cleared_queues = _clear_dispatch_queues()
        if cleared_queues > 0:
            _log(f"[crawler_processor] cleared_dispatch_queues count={cleared_queues}")
        ensure_tunnel_watchdog()
        while not stop_requested["value"]:
            total_target, per_site = _target_parallelism()
            active_workers = _collect_finished_workers(active_workers)
            active_dispatchers = _collect_finished_workers(active_dispatchers)
            active_workers = _ensure_executor(active_workers, "11880")
            active_workers = _ensure_executor(active_workers, "gs")
            if total_target != last_total_target:
                details = " ".join(f"{site}={count}" for site, count in sorted(per_site.items()))
                _log(f"[crawler_processor] target_parallel total={total_target} {details}".rstrip())
                last_total_target = total_target
            desired_dispatchers = 1 if int(total_target) > 0 else 0
            active_dispatchers = _trim_workers(active_dispatchers, desired_dispatchers)
            if len(active_dispatchers) < desired_dispatchers:
                active_dispatchers.append(_launch_dispatcher())
            time.sleep(TICK_SEC)
    finally:
        _stop_workers(active_workers)
        _stop_workers(active_dispatchers)
        stop_tunnel_watchdog()


if __name__ == "__main__":
    main()
