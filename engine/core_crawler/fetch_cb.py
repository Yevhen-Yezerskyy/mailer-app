# FILE: engine/core_crawler/fetch_cb.py
# DATE: 2026-03-29
# PURPOSE: Simplified CB crawler queue on top of task_cb_ratings/cb_crawl_pairs without Scrapy runtime per pair.

from __future__ import annotations

import argparse
import base64
import json
import os
import random
import signal
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from engine.common.cache.client import CLIENT, _redis_call
from engine.common.db import fetch_one, get_connection
from engine.core_crawler.browser.broker_server import current_site_route_plan
from engine.core_crawler.browser.session_config import CRAWLER_ACTIVE_TUNNEL_CAP
from engine.core_crawler.browser.fetcher import (
    clear_fetch_route_context,
    close_all_fetch_routers,
    reset_fetch_route_session,
    set_fetch_route_context,
)
from engine.core_crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider
from engine.core_crawler.spiders.spider_11880_cb import OneOneEightZeroCBSpider
from engine.core_crawler.tunnels_11880 import load_tunnel_statuses

LOCK_TTL_SEC = 1200.0
RETRY_LOCK_TTL_SEC = 3 * 60 * 60.0
ROUTE_LOCK_TTL_SEC = 20.0
ROUTE_LOCK_RENEW_SEC = 10.0
ITEM_TIMEOUT_SEC = 180.0


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


def _normalize_catalogs(catalogs: Sequence[str] | str | None = None) -> tuple[str, ...]:
    if catalogs is None:
        return ()
    if isinstance(catalogs, str):
        raw_values = [catalogs]
    else:
        raw_values = list(catalogs or [])
    seen: set[str] = set()
    out: list[str] = []
    for raw_value in raw_values:
        value = str(raw_value or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


def _catalog_clause(catalogs: Sequence[str] | str | None = None) -> tuple[str, tuple[Any, ...]]:
    normalized = _normalize_catalogs(catalogs)
    if not normalized:
        return "", ()
    if len(normalized) == 1:
        return " AND bs.catalog = %s ", (normalized[0],)
    placeholders = ", ".join(["%s"] * len(normalized))
    return f" AND bs.catalog IN ({placeholders}) ", tuple(normalized)


def _pick_active_task_id(catalogs: Sequence[str] | str | None = None) -> Optional[int]:
    catalog_sql, catalog_params = _catalog_clause(catalogs)
    row = fetch_one(
        f"""
        SELECT t.id
        FROM public.aap_audience_audiencetask t
        JOIN public.task_cb_ratings tcr
          ON tcr.task_id = t.id
        JOIN public.cb_crawl_pairs cp
          ON cp.id = tcr.cb_id
        JOIN public.branches_sys bs
          ON bs.id = cp.branch_id
        WHERE t.ready = true
          AND t.archived = false
          AND t.collected = false
          AND cp.collected = false
          {catalog_sql}
        ORDER BY random()
        LIMIT 1
        """,
        catalog_params,
    )
    return int(row[0]) if row else None


def pending_items_exist(catalogs: Sequence[str] | str | None = None) -> bool:
    catalog_sql, catalog_params = _catalog_clause(catalogs)
    row = fetch_one(
        f"""
        SELECT 1
        FROM public.task_cb_ratings tcr
        JOIN public.cb_crawl_pairs cp
          ON cp.id = tcr.cb_id
        JOIN public.aap_audience_audiencetask t
          ON t.id = tcr.task_id
        JOIN public.branches_sys bs
          ON bs.id = cp.branch_id
        WHERE t.ready = true
          AND t.archived = false
          AND t.collected = false
          AND cp.collected = false
          {catalog_sql}
        LIMIT 1
        """,
        catalog_params,
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


def _active_route_lock_count() -> int:
    return len(_scan_redis_keys("lock:core_crawler:route_worker:*"))


def _try_lock_cb(cb_id: int) -> Optional[tuple[str, str]]:
    lock_key = f"core_crawler:cb:{int(cb_id)}"
    owner = f"{os.getpid()}:{int(cb_id)}"
    resp = CLIENT.lock_try(lock_key, ttl_sec=LOCK_TTL_SEC, owner=owner)
    if resp and resp.get("acquired") is True and isinstance(resp.get("token"), str):
        return lock_key, str(resp["token"])
    return None


def _route_lock_key(site: str, slot_name: str) -> str:
    site_name = str(site or "").strip()
    tunnel_name = str(slot_name or "").strip()
    if not site_name or not tunnel_name:
        raise ValueError("route lock key requires site and slot_name")
    return f"core_crawler:route_worker:{site_name}:{tunnel_name}"


def _claim_route(site: str) -> RouteLease | None:
    site_name = str(site or "").strip()
    available = [str(name or "").strip() for name in list((current_site_route_plan().get(site_name) or [])) if str(name or "").strip()]
    if not available:
        return None
    statuses = load_tunnel_statuses([name for name in available if name != "direct"])
    shuffled = list(available)
    random.shuffle(shuffled)
    for slot_name in shuffled:
        launch_id = "direct" if slot_name == "direct" else str((statuses.get(slot_name) or {}).get("launch_id") or "").strip()
        if slot_name != "direct" and not launch_id:
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
    if str(route.slot_name or "").strip() == "direct":
        return True
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
        if route is not None:
            reset_fetch_route_session(route.site, route.slot_name, route.slot_idx)
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


def _finalize_item_lock(item: QueueItem) -> None:
    if not item.lock_key or not item.lock_token:
        return
    if _pair_is_collected(item.cb_id):
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
    heartbeat: RouteLeaseHeartbeat | None,
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
            _stop_route_heartbeat(heartbeat)
        except Exception:
            pass
        try:
            _release_route(route)
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


def _claim_next_item(catalogs: Sequence[str] | str | None = None) -> Optional[QueueItem]:
    with get_connection() as conn, conn.cursor() as cur:
        catalog_sql, catalog_params = _catalog_clause(catalogs)
        task_id = _pick_active_task_id(catalogs)
        if not task_id:
            return None

        cur.execute(
            f"""
            SELECT
              tcr.task_id,
              tcr.cb_id,
              tcr.rate
            FROM public.task_cb_ratings tcr
            JOIN public.cb_crawl_pairs cp
              ON cp.id = tcr.cb_id
            JOIN public.branches_sys bs
              ON bs.id = cp.branch_id
            WHERE tcr.task_id = %s
              AND cp.collected = false
              {catalog_sql}
            ORDER BY tcr.rate ASC NULLS LAST, tcr.id ASC
            LIMIT 50
            """,
            (task_id, *catalog_params),
        )
        candidates = cur.fetchall() or []

        for queue_row in candidates:
            cb_id = int(queue_row[1])
            lock_data = _try_lock_cb(cb_id)
            if not lock_data:
                continue

            cur.execute(
                """
                SELECT
                  ps.plz,
                  cp.branch_id,
                  bs.branch_name,
                  bs.branch_slug,
                  bs.catalog
                FROM public.cb_crawl_pairs cp
                JOIN public.plz_sys ps
                  ON ps.id = cp.plz_id
                JOIN public.branches_sys bs
                  ON bs.id = cp.branch_id
                WHERE cp.id = %s
                """,
                (cb_id,),
            )
            meta_row = cur.fetchone()
            if not meta_row:
                CLIENT.lock_release(lock_data[0], token=lock_data[1])
                continue

            return _make_item(
                task_id=int(queue_row[0]),
                cb_id=cb_id,
                rate=int(queue_row[2]) if queue_row[2] is not None else None,
                plz=str(meta_row[0] or "").strip(),
                branch_id=int(meta_row[1]),
                branch_name=str(meta_row[2] or "").strip(),
                branch_slug=str(meta_row[3] or "").strip(),
                catalog=str(meta_row[4] or "").strip(),
                lock_key=lock_data[0],
                lock_token=lock_data[1],
            )

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


def _run_item(item: QueueItem, route: RouteLease | None = None) -> None:
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


def worker_run_once(catalog: str = "") -> None:
    catalog_name = str(catalog or "").strip()
    item = _claim_next_item(catalog_name)
    if not item:
        if catalog_name:
            print(f"[core_crawler] queue empty catalog={catalog_name}; nothing to do")
        else:
            print("[core_crawler] queue empty; nothing to do")
        return

    try:
        _run_item(item)
    finally:
        _finalize_item_lock(item)


def worker_main_loop(catalog: str = "") -> None:
    catalog_name = str(catalog or "").strip()
    stop_requested = {"value": False}
    route: RouteLease | None = None
    heartbeat: RouteLeaseHeartbeat | None = None

    def _handle_signal(_signum, _frame) -> None:
        stop_requested["value"] = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        while not stop_requested["value"]:
            if route is not None and not _route_still_valid(route):
                _reset_and_release_route(route, heartbeat)
                route = None
                heartbeat = None
                clear_fetch_route_context()
                time.sleep(0.25)
                continue

            if route is None and _active_route_lock_count() >= int(CRAWLER_ACTIVE_TUNNEL_CAP):
                time.sleep(0.25)
                continue

            item = _claim_next_item(route.site if route is not None else catalog_name)
            if item is None:
                time.sleep(0.25)
                continue

            if route is None:
                route = _claim_route(item.catalog)
                if route is None:
                    _release_item_lock(item)
                    time.sleep(0.25)
                    continue
                heartbeat = _start_route_heartbeat(route)

            watchdog = _start_item_timeout_watchdog(item, route, heartbeat)
            try:
                _run_item(item, route)
            finally:
                _stop_item_timeout_watchdog(watchdog)
                _finalize_item_lock(item)
                if route is not None and not _route_still_valid(route):
                    _reset_and_release_route(route, heartbeat)
                    route = None
                    heartbeat = None
    finally:
        if route is not None or heartbeat is not None:
            if _route_still_valid(route):
                _stop_route_heartbeat(heartbeat)
                _release_route(route)
            else:
                _reset_and_release_route(route, heartbeat)
        clear_fetch_route_context()
        close_all_fetch_routers()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-one-b64", default="")
    parser.add_argument("--catalog", default="")
    parser.add_argument("--worker-loop", action="store_true")
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

    if bool(args.worker_loop):
        worker_main_loop(str(args.catalog or "").strip())
        return

    worker_run_once(str(args.catalog or "").strip())


if __name__ == "__main__":
    main()
