# FILE: engine/core_crawler/fetch_cb.py
# DATE: 2026-03-29
# PURPOSE: Global pair selector plus site-bound executors for CB crawling on top of task_cb_ratings/cb_crawl_pairs.

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

from engine.common.cache.client import CLIENT, _redis_call
from engine.common.db import fetch_one, get_connection
from engine.core_crawler.browser.broker_server import current_site_route_plan
from engine.core_crawler.browser.fetcher import (
    clear_fetch_route_context,
    close_all_fetch_routers,
    set_fetch_route_context,
)
from engine.core_crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider
from engine.core_crawler.spiders.spider_11880_cb import OneOneEightZeroCBSpider
from engine.core_crawler.tunnels_11880 import load_tunnel_statuses

RETRY_LOCK_TTL_SEC = 10 * 60.0
ROUTE_LOCK_TTL_SEC = 20.0
ROUTE_LOCK_RENEW_SEC = 10.0
ITEM_TIMEOUT_SEC = 360.0
ITEM_LOCK_TTL_SEC = 3 * 60.0
ITEM_LOCK_RENEW_SEC = 15.0
DISPATCH_TICK_SEC = 0.75
DISPATCHER_LOOP_SEC = 1.5
DISPATCH_POOL_LIMIT = 10
DISPATCH_HEAD_LIMIT = 5
ACTIVE_TASK_REFRESH_SEC = 10.0
TASK_EXHAUSTED_TTL_SEC = 10 * 60.0
GS_SLOT_WORKER_MIN_LIFETIME_SEC = 60 * 60.0
GS_SLOT_WORKER_MAX_LIFETIME_SEC = 90 * 60.0


@dataclass(frozen=True)
class QueueItem:
    task_id: int
    cb_id: int
    rate: Optional[int]
    plz: str
    branch_id: int
    branch_name: str
    branch_slug: str
    catalog: str
    lock_key: str
    lock_token: str


@dataclass(frozen=True)
class RouteLease:
    site: str
    slot_name: str
    slot_idx: int
    lock_key: str
    lock_token: str
    launch_id: str = ""


@dataclass(frozen=True)
class RouteLeaseHeartbeat:
    stop_event: Any
    thread: Any


@dataclass(frozen=True)
class ItemTimeoutWatchdog:
    stop_event: Any
    thread: Any


@dataclass(frozen=True)
class ItemLockHeartbeat:
    stop_event: Any
    thread: Any


@dataclass
class ChildWorkerProcess:
    slot_name: str
    process: subprocess.Popen[Any]
    started_at: float


@dataclass
class DispatcherState:
    active_tasks: list[int]
    pool_by_task: dict[int, list[QueueItem]]
    last_active_refresh_at: float


def _make_item(
    task_id: int,
    cb_id: int,
    plz: str,
    branch_id: int,
    branch_name: str,
    branch_slug: str,
    catalog: str,
    rate: Optional[int] = None,
    lock_key: str = "",
    lock_token: str = "",
) -> QueueItem:
    return QueueItem(
        task_id=int(task_id),
        cb_id=int(cb_id),
        rate=int(rate) if rate is not None else None,
        plz=str(plz or "").strip(),
        branch_id=int(branch_id),
        branch_name=str(branch_name or "").strip(),
        branch_slug=str(branch_slug or "").strip(),
        catalog=str(catalog or "").strip(),
        lock_key=str(lock_key or ""),
        lock_token=str(lock_token or ""),
    )


def _pick_active_task_id() -> Optional[int]:
    row = fetch_one(
        """
        SELECT id
        FROM public.aap_audience_audiencetask t
        WHERE t.active = true
        ORDER BY random()
        LIMIT 1
        """,
    )
    return int(row[0]) if row else None


def _list_active_task_ids() -> list[int]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM public.aap_audience_audiencetask t
            WHERE t.active = true
            ORDER BY t.id ASC
            """
        )
        return [int(row[0]) for row in (cur.fetchall() or [])]


def pending_items_exist() -> bool:
    row = fetch_one(
        """
        SELECT 1
        FROM public.aap_audience_audiencetask t
        JOIN public.task_cb_ratings tcr
          ON tcr.task_id = t.id
        JOIN public.cb_crawl_pairs cp
          ON cp.id = tcr.cb_id
        WHERE t.active = true
          AND cp.collected = false
        LIMIT 1
        """,
    )
    return bool(row)


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


def _try_lock_cb(cb_id: int) -> Optional[tuple[str, str]]:
    lock_key = f"core_crawler:cb:{int(cb_id)}"
    owner = f"{os.getpid()}:{int(cb_id)}"
    resp = CLIENT.lock_try(lock_key, ttl_sec=ITEM_LOCK_TTL_SEC, owner=owner)
    if resp and resp.get("acquired") is True and isinstance(resp.get("token"), str):
        return lock_key, str(resp["token"])
    return None


def _task_exhausted_key(task_id: int) -> str:
    return f"core_crawler:task_exhausted:{int(task_id)}"


def _is_task_exhausted_cached(task_id: int) -> bool:
    try:
        return bool(CLIENT.get(_task_exhausted_key(task_id), ttl_sec=int(TASK_EXHAUSTED_TTL_SEC)))
    except Exception:
        return False


def _mark_task_exhausted(task_id: int) -> None:
    try:
        CLIENT.set(
            _task_exhausted_key(task_id),
            str(int(time.time())).encode("ascii"),
            ttl_sec=int(TASK_EXHAUSTED_TTL_SEC),
        )
    except Exception:
        pass


def _route_lock_key(site: str, slot_name: str) -> str:
    site_name = str(site or "").strip()
    tunnel_name = str(slot_name or "").strip()
    if not site_name or not tunnel_name:
        raise ValueError("route lock key requires site and slot_name")
    return f"core_crawler:route_worker:{site_name}:{tunnel_name}"


def _dispatch_queue_key(site: str) -> str:
    site_name = str(site or "").strip()
    if not site_name:
        raise ValueError("dispatch queue key requires site")
    return f"core_crawler:dispatch_queue:{site_name}"


def _encode_queue_item(item: QueueItem) -> bytes:
    payload = {
        "task_id": int(item.task_id),
        "cb_id": int(item.cb_id),
        "rate": int(item.rate) if item.rate is not None else None,
        "plz": str(item.plz or "").strip(),
        "branch_id": int(item.branch_id),
        "branch_name": str(item.branch_name or "").strip(),
        "branch_slug": str(item.branch_slug or "").strip(),
        "catalog": str(item.catalog or "").strip(),
        "lock_key": str(item.lock_key or "").strip(),
        "lock_token": str(item.lock_token or "").strip(),
    }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def _decode_queue_item(raw_payload: bytes | bytearray | str) -> QueueItem | None:
    try:
        if isinstance(raw_payload, (bytes, bytearray)):
            text = bytes(raw_payload).decode("utf-8")
        else:
            text = str(raw_payload)
        data = json.loads(text)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    try:
        return _make_item(
            task_id=int(data["task_id"]),
            cb_id=int(data["cb_id"]),
            rate=int(data["rate"]) if data.get("rate") is not None else None,
            plz=str(data.get("plz") or "").strip(),
            branch_id=int(data["branch_id"]),
            branch_name=str(data.get("branch_name") or "").strip(),
            branch_slug=str(data.get("branch_slug") or "").strip(),
            catalog=str(data.get("catalog") or "").strip(),
            lock_key=str(data.get("lock_key") or "").strip(),
            lock_token=str(data.get("lock_token") or "").strip(),
        )
    except Exception:
        return None


def _queue_length(site: str) -> int:
    reply = _redis_call("LLEN", _dispatch_queue_key(site))
    return int(reply) if isinstance(reply, int) else 0


def _queue_push_item(item: QueueItem, max_depth: int) -> bool:
    if int(max_depth) <= 0:
        return False
    if _queue_length(item.catalog) >= int(max_depth):
        return False
    reply = _redis_call("RPUSH", _dispatch_queue_key(item.catalog), _encode_queue_item(item))
    return isinstance(reply, int) and int(reply) > 0


def _queue_pop_item(site: str) -> QueueItem | None:
    reply = _redis_call("LPOP", _dispatch_queue_key(site))
    if not isinstance(reply, (bytes, bytearray, str)):
        return None
    return _decode_queue_item(reply)


def _claim_specific_route(site: str, slot_name: str) -> RouteLease | None:
    site_name = str(site or "").strip()
    tunnel_name = str(slot_name or "").strip()
    if not site_name or not tunnel_name:
        return None
    active_names = [str(name or "").strip() for name in list((current_site_route_plan().get(site_name) or [])) if str(name or "").strip()]
    if tunnel_name not in active_names:
        return None
    statuses = load_tunnel_statuses([tunnel_name])
    launch_id = str((statuses.get(tunnel_name) or {}).get("launch_id") or "").strip()
    if not launch_id:
        return None
    owner = f"{os.getpid()}:{site_name}:{tunnel_name}"
    lock_key = _route_lock_key(site_name, tunnel_name)
    info = CLIENT.lock_try(lock_key, ttl_sec=ROUTE_LOCK_TTL_SEC, owner=owner)
    if not info or not bool(info.get("acquired")) or not str(info.get("token") or "").strip():
        return None
    return RouteLease(
        site=site_name,
        slot_name=tunnel_name,
        slot_idx=0,
        lock_key=lock_key,
        lock_token=str(info["token"]),
        launch_id=launch_id,
    )


def _claim_route(site: str) -> RouteLease | None:
    site_name = str(site or "").strip()
    available = [str(name or "").strip() for name in list((current_site_route_plan().get(site_name) or [])) if str(name or "").strip()]
    if not available:
        return None
    statuses = load_tunnel_statuses(list(available))
    shuffled = list(available)
    random.shuffle(shuffled)
    for slot_name in shuffled:
        launch_id = str((statuses.get(slot_name) or {}).get("launch_id") or "").strip()
        if not launch_id:
            continue
        owner = f"{os.getpid()}:{site_name}:{slot_name}"
        lock_key = _route_lock_key(site_name, slot_name)
        info = CLIENT.lock_try(lock_key, ttl_sec=ROUTE_LOCK_TTL_SEC, owner=owner)
        if not info or not bool(info.get("acquired")) or not str(info.get("token") or "").strip():
            continue
        return RouteLease(
            site=site_name,
            slot_name=slot_name,
            slot_idx=0,
            lock_key=lock_key,
            lock_token=str(info["token"]),
            launch_id=launch_id,
        )
    return None


def _release_route(route: RouteLease | None) -> None:
    if route is None or not route.lock_key or not route.lock_token:
        return
    try:
        CLIENT.lock_release(route.lock_key, token=route.lock_token)
    except Exception:
        pass


def _route_still_valid(route: RouteLease | None) -> bool:
    if route is None:
        return False
    plan = current_site_route_plan()
    active_names = [str(name or "").strip() for name in list((plan.get(route.site) or [])) if str(name or "").strip()]
    if str(route.slot_name or "").strip() not in active_names:
        return False
    statuses = load_tunnel_statuses([route.slot_name])
    current_launch_id = str((statuses.get(route.slot_name) or {}).get("launch_id") or "").strip()
    if not current_launch_id:
        return False
    return str(route.launch_id or "").strip() == current_launch_id


def _reset_and_release_route(route: RouteLease | None, heartbeat: RouteLeaseHeartbeat | None) -> None:
    try:
        _stop_route_heartbeat(heartbeat)
    except Exception:
        pass
    try:
        _release_route(route)
    except Exception:
        pass


def _start_route_heartbeat(route: RouteLease | None) -> RouteLeaseHeartbeat | None:
    if route is None or not route.lock_key or not route.lock_token:
        return None
    stop_event = threading.Event()

    def _heartbeat() -> None:
        while not stop_event.wait(ROUTE_LOCK_RENEW_SEC):
            try:
                renewed = CLIENT.lock_renew(
                    route.lock_key,
                    ttl_sec=ROUTE_LOCK_TTL_SEC,
                    token=route.lock_token,
                )
            except Exception:
                renewed = False
            if not renewed:
                return

    thread = threading.Thread(
        target=_heartbeat,
        name=f"route_lock:{route.site}:{route.slot_name}",
        daemon=True,
    )
    thread.start()
    return RouteLeaseHeartbeat(stop_event=stop_event, thread=thread)


def _stop_route_heartbeat(heartbeat: RouteLeaseHeartbeat | None) -> None:
    if heartbeat is None:
        return
    try:
        heartbeat.stop_event.set()
    except Exception:
        pass
    try:
        heartbeat.thread.join(timeout=1.0)
    except Exception:
        pass


def _start_item_lock_heartbeat(item: QueueItem) -> ItemLockHeartbeat | None:
    if not item.lock_key or not item.lock_token:
        return None
    stop_event = threading.Event()

    def _heartbeat() -> None:
        while not stop_event.wait(ITEM_LOCK_RENEW_SEC):
            try:
                renewed = CLIENT.lock_renew(
                    item.lock_key,
                    ttl_sec=ITEM_LOCK_TTL_SEC,
                    token=item.lock_token,
                )
            except Exception:
                renewed = False
            if not renewed:
                return

    thread = threading.Thread(
        target=_heartbeat,
        name=f"item_lock:{item.catalog}:{item.cb_id}",
        daemon=True,
    )
    thread.start()
    return ItemLockHeartbeat(stop_event=stop_event, thread=thread)


def _stop_item_lock_heartbeat(heartbeat: ItemLockHeartbeat | None) -> None:
    if heartbeat is None:
        return
    try:
        heartbeat.stop_event.set()
    except Exception:
        pass
    try:
        heartbeat.thread.join(timeout=1.0)
    except Exception:
        pass


def _finalize_item_lock(item: QueueItem, *, release_lock: bool = False) -> None:
    if not item.lock_key or not item.lock_token:
        return
    if _pair_is_collected(item.cb_id):
        try:
            CLIENT.lock_release(item.lock_key, token=item.lock_token)
        except Exception:
            pass
        return
    if bool(release_lock):
        try:
            CLIENT.lock_release(item.lock_key, token=item.lock_token)
        except Exception:
            pass
        return
    try:
        renewed = CLIENT.lock_renew(
            item.lock_key,
            ttl_sec=RETRY_LOCK_TTL_SEC,
            token=item.lock_token,
        )
        if renewed:
            return
    except Exception:
        pass
    try:
        CLIENT.lock_release(item.lock_key, token=item.lock_token)
    except Exception:
        pass


def _release_item_lock(item: QueueItem) -> None:
    if not item.lock_key or not item.lock_token:
        return
    try:
        CLIENT.lock_release(item.lock_key, token=item.lock_token)
    except Exception:
        pass


def _start_item_timeout_watchdog(
    item: QueueItem,
    route: RouteLease | None,
) -> ItemTimeoutWatchdog:
    stop_event = threading.Event()

    def _watchdog() -> None:
        if stop_event.wait(ITEM_TIMEOUT_SEC):
            return
        try:
            print(
                f"[core_crawler] timeout cb_id={item.cb_id} catalog={item.catalog} "
                f"slot={getattr(route, 'slot_name', '')} sec={int(ITEM_TIMEOUT_SEC)}"
            )
        except Exception:
            pass
        try:
            _release_item_lock(item)
        except Exception:
            pass
        try:
            close_all_fetch_routers()
        except Exception:
            pass
        os._exit(124)

    thread = threading.Thread(
        target=_watchdog,
        name=f"item_timeout:{item.catalog}:{item.cb_id}",
        daemon=True,
    )
    thread.start()
    return ItemTimeoutWatchdog(stop_event=stop_event, thread=thread)


def _stop_item_timeout_watchdog(watchdog: ItemTimeoutWatchdog | None) -> None:
    if watchdog is None:
        return
    try:
        watchdog.stop_event.set()
    except Exception:
        pass
    try:
        watchdog.thread.join(timeout=1.0)
    except Exception:
        pass


def _fetch_task_pool(task_id: int, limit: int) -> list[QueueItem]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              tcr.task_id,
              tcr.cb_id,
              tcr.rate,
              ps.plz,
              cp.branch_id,
              bs.branch_name,
              bs.branch_slug,
              bs.catalog
            FROM public.task_cb_ratings tcr
            JOIN public.cb_crawl_pairs cp
              ON cp.id = tcr.cb_id
            JOIN public.plz_sys ps
              ON ps.id = cp.plz_id
            JOIN public.branches_sys bs
              ON bs.id = cp.branch_id
            WHERE tcr.task_id = %s
              AND cp.collected = false
            ORDER BY tcr.rate ASC NULLS LAST, tcr.id ASC
            LIMIT %s
            """,
            (int(task_id), max(1, int(limit))),
        )
        rows = cur.fetchall() or []
    out: list[QueueItem] = []
    for row in rows:
        out.append(
            _make_item(
                task_id=int(row[0]),
                cb_id=int(row[1]),
                rate=int(row[2]) if row[2] is not None else None,
                plz=str(row[3] or "").strip(),
                branch_id=int(row[4]),
                branch_name=str(row[5] or "").strip(),
                branch_slug=str(row[6] or "").strip(),
                catalog=str(row[7] or "").strip(),
            )
        )
    return out


def _lock_candidate_item(candidate: QueueItem) -> QueueItem | None:
    lock_data = _try_lock_cb(candidate.cb_id)
    if not lock_data:
        return None
    return _make_item(
        task_id=int(candidate.task_id),
        cb_id=int(candidate.cb_id),
        rate=int(candidate.rate) if candidate.rate is not None else None,
        plz=str(candidate.plz or "").strip(),
        branch_id=int(candidate.branch_id),
        branch_name=str(candidate.branch_name or "").strip(),
        branch_slug=str(candidate.branch_slug or "").strip(),
        catalog=str(candidate.catalog or "").strip(),
        lock_key=lock_data[0],
        lock_token=lock_data[1],
    )


def _claim_next_item() -> Optional[QueueItem]:
    task_id = _pick_active_task_id()
    if not task_id:
        return None
    for candidate in _fetch_task_pool(task_id, limit=DISPATCH_HEAD_LIMIT):
        locked_item = _lock_candidate_item(candidate)
        if locked_item is not None:
            return locked_item
    return None


def _refresh_dispatcher_active_tasks(state: DispatcherState, force: bool = False) -> None:
    now = time.time()
    if not force and (now - float(state.last_active_refresh_at)) < ACTIVE_TASK_REFRESH_SEC:
        return
    active_tasks = _list_active_task_ids()
    active_set = set(active_tasks)
    state.active_tasks = [
        int(task_id)
        for task_id in active_tasks
        if not _is_task_exhausted_cached(int(task_id))
    ]
    state.pool_by_task = {
        int(task_id): list(state.pool_by_task.get(int(task_id), []) or [])
        for task_id in state.active_tasks
        if int(task_id) in active_set
    }
    state.last_active_refresh_at = now


def _reload_dispatcher_task_pool(state: DispatcherState, task_id: int) -> list[QueueItem]:
    task_key = int(task_id)
    pool = _fetch_task_pool(task_key, limit=DISPATCH_POOL_LIMIT)
    state.pool_by_task[task_key] = list(pool)
    if not pool:
        _mark_task_exhausted(task_key)
        _refresh_dispatcher_active_tasks(state, force=True)
    return pool


def _drop_dispatcher_pool_item(state: DispatcherState, task_id: int, cb_id: int) -> None:
    task_key = int(task_id)
    pool = list(state.pool_by_task.get(task_key) or [])
    if not pool:
        return
    state.pool_by_task[task_key] = [item for item in pool if int(item.cb_id) != int(cb_id)]


def _claim_pooled_item(state: DispatcherState) -> tuple[int, QueueItem] | None:
    _refresh_dispatcher_active_tasks(state)
    if not state.active_tasks:
        return None

    task_id = int(random.choice(state.active_tasks))
    pool = list(state.pool_by_task.get(task_id) or [])
    if len(pool) < DISPATCH_HEAD_LIMIT:
        pool = _reload_dispatcher_task_pool(state, task_id)
    if not pool:
        return None

    for candidate in pool[:DISPATCH_HEAD_LIMIT]:
        locked_item = _lock_candidate_item(candidate)
        if locked_item is not None:
            return task_id, locked_item
    return None


def _pair_is_collected(cb_id: int) -> bool:
    row = fetch_one(
        """
        SELECT collected
        FROM public.cb_crawl_pairs
        WHERE id = %s
        """,
        (int(cb_id),),
    )
    return bool(row and row[0] is True)


def _run_gs_spider(item: QueueItem) -> Any:
    spider = GelbeSeitenCBSpider(
        task_id=int(item.task_id),
        cb_id=int(item.cb_id),
        plz=str(item.plz),
        branch_slug=str(item.branch_slug),
        branch_name=str(item.branch_name),
    )
    spider.run()
    return spider


def _run_11880_spider(item: QueueItem) -> Any:
    spider = OneOneEightZeroCBSpider(
        task_id=int(item.task_id),
        cb_id=int(item.cb_id),
        plz=str(item.plz),
        branch_slug=str(item.branch_slug),
        branch_name=str(item.branch_name),
    )
    spider.run()
    return spider


def _run_spider(item: QueueItem) -> Any | None:
    catalog = str(item.catalog or "").strip().lower()
    if catalog == "gs":
        return _run_gs_spider(item)
    if catalog == "11880":
        return _run_11880_spider(item)
    return None


def _run_item(item: QueueItem, route: RouteLease | None = None) -> dict[str, Any]:
    release_lock = False
    try:
        if route is not None:
            set_fetch_route_context(route.site, route.slot_name, route.slot_idx)
        print(
            f"[core_crawler] start cb_id={item.cb_id} catalog={item.catalog} "
            f"plz={item.plz} branch={item.branch_slug} slot={getattr(route, 'slot_name', '')}"
        )

        if not item.branch_slug or not item.plz:
            print(
                f"[core_crawler] done cb_id={item.cb_id} action=skip_invalid "
                f"catalog={item.catalog} plz={item.plz} branch={item.branch_slug}"
            )
            return

        spider = _run_spider(item)
        if spider is None:
            print(
                f"[core_crawler] done cb_id={item.cb_id} action=skip_unsupported "
                f"catalog={item.catalog}"
            )
            return

        db_action = str(getattr(spider, "_db_action", "") or "")
        db_rows = int(getattr(spider, "_db_rows", 0) or 0)
        final_reason = str(getattr(spider, "_final_reason", "") or "")
        release_lock = final_reason.startswith("FAILED TO PARSE")
        if final_reason.startswith("FETCH EXCEPTION"):
            release_lock = True
        selected = int(len(getattr(spider, "selected_urls", []) or []))
        parsed = int(getattr(spider, "_detail_parsed", 0) or 0)
        print(
            f"[core_crawler] done cb_id={item.cb_id} catalog={item.catalog} "
            f"action={db_action or 'unknown'} rows={db_rows} selected={selected} "
            f"parsed={parsed} reason={final_reason}"
        )

        if not _pair_is_collected(item.cb_id):
            print(
                f"[core_crawler] pending cb_id={item.cb_id} catalog={item.catalog} "
                f"reason={final_reason or 'UNKNOWN'}"
            )
    finally:
        clear_fetch_route_context()
    return {"release_lock": release_lock}


def run_fixed_pair(
    task_id: int,
    cb_id: int,
    plz: str,
    branch_id: int,
    branch_name: str,
    branch_slug: str,
    catalog: str,
) -> None:
    _run_item(
        _make_item(
            task_id=task_id,
            cb_id=cb_id,
            plz=plz,
            branch_id=branch_id,
            branch_name=branch_name,
            branch_slug=branch_slug,
            catalog=catalog,
        )
    )


def _decode_run_one_b64(raw_b64: str) -> dict:
    return json.loads(base64.b64decode(str(raw_b64).encode("ascii")).decode("utf-8"))


def dispatch_run_once(catalog: str = "") -> None:
    item = _claim_next_item()
    if item is None:
        return
    queue_capacity = max(0, int(len(current_site_route_plan().get(item.catalog) or [])))
    if queue_capacity <= 0:
        _release_item_lock(item)
        return
    if not _queue_push_item(item, max_depth=queue_capacity):
        _release_item_lock(item)
        return
    print(
        f"[core_crawler] dispatch cb_id={item.cb_id} catalog={item.catalog} "
        f"rate={item.rate if item.rate is not None else '-'}"
    )


def dispatcher_main() -> None:
    stop_requested = {"value": False}
    state = DispatcherState(active_tasks=[], pool_by_task={}, last_active_refresh_at=0.0)

    def _handle_signal(_signum, _frame) -> None:
        stop_requested["value"] = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    while not stop_requested["value"]:
        try:
            attempt = _claim_pooled_item(state)
            if attempt is None:
                time.sleep(DISPATCHER_LOOP_SEC)
                continue

            task_id, item = attempt
            queue_capacity = max(0, int(len(current_site_route_plan().get(item.catalog) or [])))
            if queue_capacity <= 0:
                _release_item_lock(item)
                time.sleep(DISPATCHER_LOOP_SEC)
                continue
            if not _queue_push_item(item, max_depth=queue_capacity):
                _release_item_lock(item)
                time.sleep(DISPATCHER_LOOP_SEC)
                continue

            _drop_dispatcher_pool_item(state, task_id, item.cb_id)
            print(
                f"[core_crawler] dispatch cb_id={item.cb_id} catalog={item.catalog} "
                f"rate={item.rate if item.rate is not None else '-'}"
            )
        except Exception as exc:
            print(f"[core_crawler] dispatcher_error {type(exc).__name__}: {exc}")
        time.sleep(DISPATCHER_LOOP_SEC)


def _launch_slot_worker(catalog: str, slot_name: str) -> ChildWorkerProcess:
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "engine.core_crawler.fetch_cb",
            "--slot-worker",
            "--catalog",
            str(catalog or "").strip(),
            "--slot-name",
            str(slot_name or "").strip(),
        ],
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
    )
    print(f"[core_crawler] slot_worker_start catalog={catalog} slot={slot_name} pid={process.pid}")
    return ChildWorkerProcess(slot_name=str(slot_name), process=process, started_at=time.time())


def _collect_finished_slot_workers(active: dict[str, ChildWorkerProcess], catalog: str) -> dict[str, ChildWorkerProcess]:
    out: dict[str, ChildWorkerProcess] = {}
    for slot_name, child in active.items():
        if child.process.poll() is None:
            out[slot_name] = child
            continue
        print(
            f"[core_crawler] slot_worker_done catalog={catalog} "
            f"slot={slot_name} pid={child.process.pid} rc={child.process.returncode}"
        )
    return out


def _stop_slot_workers(active: dict[str, ChildWorkerProcess]) -> None:
    live = [child for child in active.values() if child.process.poll() is None]
    if not live:
        return
    for child in live:
        try:
            child.process.terminate()
        except Exception:
            continue
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if all(child.process.poll() is not None for child in live):
            return
        time.sleep(0.1)
    for child in live:
        if child.process.poll() is not None:
            continue
        try:
            child.process.kill()
        except Exception:
            continue


def site_executor_main(catalog: str) -> None:
    catalog_name = str(catalog or "").strip()
    if not catalog_name:
        raise RuntimeError("site executor requires catalog")
    stop_requested = {"value": False}
    active: dict[str, ChildWorkerProcess] = {}

    def _handle_signal(_signum, _frame) -> None:
        stop_requested["value"] = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        while not stop_requested["value"]:
            active = _collect_finished_slot_workers(active, catalog_name)
            desired_slots = [
                str(name or "").strip()
                for name in list((current_site_route_plan().get(catalog_name) or []))
                if str(name or "").strip()
            ]
            desired_set = set(desired_slots)

            for slot_name in list(active.keys()):
                if slot_name in desired_set:
                    continue
                child = active.pop(slot_name)
                try:
                    child.process.terminate()
                except Exception:
                    pass

            for slot_name in desired_slots:
                if slot_name in active:
                    continue
                active[slot_name] = _launch_slot_worker(catalog_name, slot_name)

            time.sleep(1.0)
    finally:
        _stop_slot_workers(active)


def slot_worker_main(catalog: str, slot_name: str) -> None:
    catalog_name = str(catalog or "").strip()
    fixed_slot_name = str(slot_name or "").strip()
    if not catalog_name or not fixed_slot_name:
        raise RuntimeError("slot worker requires catalog and slot name")
    stop_requested = {"value": False}
    active_launch_id = ""
    lifetime_deadline = 0.0
    if catalog_name == "gs":
        lifetime_deadline = time.time() + random.uniform(
            GS_SLOT_WORKER_MIN_LIFETIME_SEC,
            GS_SLOT_WORKER_MAX_LIFETIME_SEC,
        )

    def _handle_signal(_signum, _frame) -> None:
        stop_requested["value"] = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        while not stop_requested["value"]:
            if lifetime_deadline > 0.0 and time.time() >= lifetime_deadline:
                break
            desired_slots = [
                str(name or "").strip()
                for name in list((current_site_route_plan().get(catalog_name) or []))
                if str(name or "").strip()
            ]
            if fixed_slot_name not in set(desired_slots):
                break

            statuses = load_tunnel_statuses([fixed_slot_name])
            current_launch_id = str((statuses.get(fixed_slot_name) or {}).get("launch_id") or "").strip()
            if not current_launch_id:
                break

            if active_launch_id != current_launch_id:
                if active_launch_id:
                    break
                active_launch_id = current_launch_id

            route = RouteLease(
                site=catalog_name,
                slot_name=fixed_slot_name,
                slot_idx=0,
                lock_key="",
                lock_token="",
                launch_id=current_launch_id,
            )
            set_fetch_route_context(catalog_name, fixed_slot_name, 0)

            item = _queue_pop_item(catalog_name)
            if item is None:
                time.sleep(DISPATCH_TICK_SEC)
                continue

            if str(item.catalog or "").strip() != catalog_name:
                _release_item_lock(item)
                time.sleep(DISPATCH_TICK_SEC)
                continue

            item_heartbeat = _start_item_lock_heartbeat(item)
            watchdog = _start_item_timeout_watchdog(item, route)
            finalize_info: dict[str, Any] | None = None
            try:
                finalize_info = _run_item(item, route)
            finally:
                _stop_item_timeout_watchdog(watchdog)
                _stop_item_lock_heartbeat(item_heartbeat)
                _finalize_item_lock(
                    item,
                    release_lock=bool((finalize_info or {}).get("release_lock")),
                )
    finally:
        clear_fetch_route_context()
        close_all_fetch_routers()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-one-b64", default="")
    parser.add_argument("--catalog", default="")
    parser.add_argument("--dispatcher", action="store_true")
    parser.add_argument("--dispatch-once", action="store_true")
    parser.add_argument("--site-executor", action="store_true")
    parser.add_argument("--slot-worker", action="store_true")
    parser.add_argument("--slot-name", default="")
    args = parser.parse_args()

    if args.run_one_b64:
        data = _decode_run_one_b64(args.run_one_b64)
        run_fixed_pair(
            task_id=int(data["task_id"]),
            cb_id=int(data["cb_id"]),
            plz=str(data["plz"] or "").strip(),
            branch_id=int(data["branch_id"]),
            branch_name=str(data["branch_name"] or "").strip(),
            branch_slug=str(data["branch_slug"] or "").strip(),
            catalog=str(data["catalog"] or "").strip(),
        )
        return

    if bool(args.dispatch_once):
        dispatch_run_once(str(args.catalog or "").strip())
        return

    if bool(args.dispatcher):
        dispatcher_main()
        return

    if bool(args.site_executor):
        site_executor_main(str(args.catalog or "").strip())
        return

    if bool(args.slot_worker):
        slot_worker_main(str(args.catalog or "").strip(), str(args.slot_name or "").strip())
        return

    dispatch_run_once(str(args.catalog or "").strip())


if __name__ == "__main__":
    main()
