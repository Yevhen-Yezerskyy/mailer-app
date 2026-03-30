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
import time
from dataclasses import dataclass
from typing import Any, Optional

from engine.common.cache.client import CLIENT
from engine.common.db import fetch_one, get_connection
from engine.core_crawler.browser.broker_server import current_site_route_plan
from engine.core_crawler.browser.fetcher import (
    clear_fetch_route_context,
    close_all_fetch_routers,
    set_fetch_route_context,
)
from engine.core_crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider
from engine.core_crawler.spiders.spider_11880_cb import OneOneEightZeroCBSpider

LOCK_TTL_SEC = 1200.0
RETRY_LOCK_TTL_SEC = 3 * 60 * 60.0
ROUTE_LOCK_TTL_SEC = 2 * 60 * 60.0


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


def _catalog_clause(catalog: str) -> tuple[str, tuple[Any, ...]]:
    catalog_name = str(catalog or "").strip()
    if not catalog_name:
        return "", ()
    return " AND bs.catalog = %s ", (catalog_name,)


def _pick_active_task_id(catalog: str = "") -> Optional[int]:
    catalog_sql, catalog_params = _catalog_clause(catalog)
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


def pending_items_exist(catalog: str = "") -> bool:
    catalog_sql, catalog_params = _catalog_clause(catalog)
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
    shuffled = list(available)
    random.shuffle(shuffled)
    for slot_name in shuffled:
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
        )
    return None


def _release_route(route: RouteLease | None) -> None:
    if route is None or not route.lock_key or not route.lock_token:
        return
    try:
        CLIENT.lock_release(route.lock_key, token=route.lock_token)
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


def _claim_next_item(catalog: str = "") -> Optional[QueueItem]:
    with get_connection() as conn, conn.cursor() as cur:
        catalog_sql, catalog_params = _catalog_clause(catalog)
        task_id = _pick_active_task_id(catalog)
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

    def _handle_signal(_signum, _frame) -> None:
        stop_requested["value"] = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    try:
        while not stop_requested["value"]:
            route = _claim_route(catalog_name)
            if route is None:
                time.sleep(0.25)
                continue
            item = _claim_next_item(catalog_name)
            if item is None:
                _release_route(route)
                time.sleep(0.25)
                continue
            try:
                _run_item(item, route)
            finally:
                _finalize_item_lock(item)
                _release_route(route)
    finally:
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
