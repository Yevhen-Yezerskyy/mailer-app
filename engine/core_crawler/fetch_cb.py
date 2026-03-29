# FILE: engine/core_crawler/fetch_cb.py
# DATE: 2026-03-29
# PURPOSE: Simplified CB crawler queue on top of task_cb_ratings/cb_crawl_pairs without Scrapy runtime per pair.

from __future__ import annotations

import argparse
import base64
import json
import os
from dataclasses import dataclass
from typing import Any, Optional

from engine.common.cache.client import CLIENT
from engine.common.db import fetch_one, get_connection
from engine.core_crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider
from engine.core_crawler.spiders.spider_11880_cb import OneOneEightZeroCBSpider

LOCK_TTL_SEC = 1200.0
RETRY_LOCK_TTL_SEC = 3 * 60 * 60.0


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


def pending_items_exist() -> bool:
    row = fetch_one(
        """
        SELECT 1
        FROM public.task_cb_ratings tcr
        JOIN public.cb_crawl_pairs cp
          ON cp.id = tcr.cb_id
        JOIN public.aap_audience_audiencetask t
          ON t.id = tcr.task_id
        WHERE t.ready = true
          AND t.archived = false
          AND t.collected = false
          AND cp.collected = false
        LIMIT 1
        """
    )
    return bool(row)


def _try_lock_cb(cb_id: int) -> Optional[tuple[str, str]]:
    lock_key = f"core_crawler:cb:{int(cb_id)}"
    owner = f"{os.getpid()}:{int(cb_id)}"
    resp = CLIENT.lock_try(lock_key, ttl_sec=LOCK_TTL_SEC, owner=owner)
    if resp and resp.get("acquired") is True and isinstance(resp.get("token"), str):
        return lock_key, str(resp["token"])
    return None


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


def _run_item(item: QueueItem) -> None:
    print(
        f"[core_crawler] start cb_id={item.cb_id} catalog={item.catalog} "
        f"plz={item.plz} branch={item.branch_slug}"
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


def worker_run_once() -> None:
    item = _claim_next_item()
    if not item:
        print("[core_crawler] queue empty; nothing to do")
        return

    try:
        _run_item(item)
    finally:
        _finalize_item_lock(item)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-one-b64", default="")
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

    worker_run_once()


if __name__ == "__main__":
    main()
