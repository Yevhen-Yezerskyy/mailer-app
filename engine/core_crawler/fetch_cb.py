# FILE: engine/core_crawler/fetch_cb.py
# DATE: 2026-03-26
# PURPOSE: Simplified CB crawler queue on top of task_cb_ratings/cb_crawl_pairs.

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from scrapy.crawler import CrawlerProcess

from engine.common.cache.client import CLIENT
from engine.common.db import fetch_one, get_connection
from engine.core_crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider
from engine.core_crawler.spiders.spider_11880_cb import OneOneEightZeroCBSpider

LOCK_TTL_SEC = 1200.0


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


def _pick_active_task_id() -> Optional[int]:
    row = fetch_one(
        """
        SELECT t.id
        FROM public.aap_audience_audiencetask t
        WHERE t.ready = true
          AND t.archived = false
          AND t.collected = false
        ORDER BY random()
        LIMIT 1
        """
    )
    return int(row[0]) if row else None


def _try_lock_cb(cb_id: int) -> Optional[tuple[str, str]]:
    lock_key = f"core_crawler:cb:{int(cb_id)}"
    owner = f"{os.getpid()}:{int(cb_id)}"
    resp = CLIENT.lock_try(lock_key, ttl_sec=LOCK_TTL_SEC, owner=owner)
    if resp and resp.get("acquired") is True and isinstance(resp.get("token"), str):
        return lock_key, str(resp["token"])
    return None


def _release_item_lock(item: QueueItem) -> None:
    try:
        CLIENT.lock_release(item.lock_key, token=item.lock_token)
    except Exception:
        pass


def _claim_next_item() -> Optional[QueueItem]:
    with get_connection() as conn, conn.cursor() as cur:
        task_id = _pick_active_task_id()
        if not task_id:
            return None

        cur.execute(
            """
            SELECT
              tcr.task_id,
              tcr.cb_id,
              tcr.rate
            FROM public.task_cb_ratings tcr
            JOIN public.cb_crawl_pairs cp
              ON cp.id = tcr.cb_id
            WHERE tcr.task_id = %s
              AND cp.collected = false
            ORDER BY tcr.rate ASC NULLS LAST, tcr.id ASC
            LIMIT 50
            """,
            (task_id,),
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

            return QueueItem(
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


def _run_gs_spider(item: QueueItem) -> None:
    process = CrawlerProcess(
        settings={
            "LOG_LEVEL": "ERROR",
            "TELNETCONSOLE_ENABLED": False,
            "DOWNLOAD_DELAY": 2.0,
            "RANDOMIZE_DOWNLOAD_DELAY": True,
            "CONCURRENT_REQUESTS": 1,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
            "AUTOTHROTTLE_ENABLED": True,
            "AUTOTHROTTLE_START_DELAY": 2.0,
            "AUTOTHROTTLE_MAX_DELAY": 30.0,
            "AUTOTHROTTLE_TARGET_CONCURRENCY": 0.5,
            "COOKIES_ENABLED": True,
        }
    )
    process.crawl(
        GelbeSeitenCBSpider,
        task_id=int(item.task_id),
        cb_id=int(item.cb_id),
        plz=str(item.plz),
        branch_slug=str(item.branch_slug),
        branch_name=str(item.branch_name),
    )
    process.start()


def _run_11880_spider(item: QueueItem) -> None:
    process = CrawlerProcess(
        settings={
            "LOG_LEVEL": "ERROR",
            "TELNETCONSOLE_ENABLED": False,
            "COOKIES_ENABLED": True,
        }
    )
    process.crawl(
        OneOneEightZeroCBSpider,
        task_id=int(item.task_id),
        cb_id=int(item.cb_id),
        plz=str(item.plz),
        branch_slug=str(item.branch_slug),
        branch_name=str(item.branch_name),
    )
    process.start()


def _run_spider(item: QueueItem) -> bool:
    catalog = str(item.catalog or "").strip().lower()
    if catalog == "gs":
        _run_gs_spider(item)
        return True
    if catalog == "11880":
        _run_11880_spider(item)
        return True
    return False


def worker_run_once() -> None:
    item = _claim_next_item()
    if not item:
        print("[core_crawler] queue empty; nothing to do")
        return

    try:
        print(
            f"[core_crawler] pop task_id={item.task_id} cb_id={item.cb_id} "
            f"catalog='{item.catalog}' plz='{item.plz}' branch='{item.branch_slug}'"
        )

        if not item.branch_slug or not item.plz:
            print(
                f"[core_crawler] skip invalid meta task_id={item.task_id} cb_id={item.cb_id} "
                f"plz='{item.plz}' branch_slug='{item.branch_slug}'"
            )
            return

        started = _run_spider(item)
        if not started:
            print(
                f"[core_crawler] skip unsupported catalog='{item.catalog}' "
                f"task_id={item.task_id} cb_id={item.cb_id}"
            )
            return

        if not _pair_is_collected(item.cb_id):
            print(
                f"[core_crawler] spider finished without collected flag; "
                f"cb_id={item.cb_id} remains pending"
            )
    finally:
        _release_item_lock(item)
