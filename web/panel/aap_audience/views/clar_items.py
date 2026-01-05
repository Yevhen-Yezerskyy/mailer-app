# FILE: web/panel/aap_audience/views/clar_items.py
# DATE: 2026-01-05
# PURPOSE:
# - <=50 элементов: прямые вызовы get_city_land / get_branch_str
# - >50 элементов: batch через iter_city_land / iter_branch_str (обязательно схлопываем в dict)
# - update_rate сохранён и используется clar.py

from __future__ import annotations

from typing import Any, Dict, List

from django.db import connection

from mailer_web.format_data import (
    get_city_land,
    get_branch_str,
    iter_city_land,
    iter_branch_str,
)


BATCH_THRESHOLD = 50


def load_sorted_cities(ws_id, user_id, task_id: int) -> List[Dict[str, Any]]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT ct.value_id, ct.rate
            FROM public.crawl_tasks ct
            WHERE ct.task_id = %s
              AND ct.type    = 'city'
            ORDER BY ct.rate ASC
            """,
            [int(task_id)],
        )
        rows = cur.fetchall()

    city_ids = [int(value_id) for value_id, _ in rows]

    if len(city_ids) <= BATCH_THRESHOLD:
        city_map = {cid: get_city_land(cid) for cid in city_ids}
    else:
        # iter_* возвращает generator -> обязательно dict()
        city_map = dict(iter_city_land(city_ids))

    out: List[Dict[str, Any]] = []
    for value_id, rate in rows:
        cid = int(value_id)
        out.append(
            {
                "value_id": cid,
                "value_text": city_map.get(cid, ""),
                "rate": int(rate) if rate is not None else 100,
            }
        )

    return out


def load_sorted_branches(
    ws_id,
    user_id,
    task_id: int,
    *,
    ui_lang: str,
) -> List[Dict[str, Any]]:
    ui_lang = (ui_lang or "").strip().lower() or "ru"

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT ct.value_id, ct.rate
            FROM public.crawl_tasks ct
            WHERE ct.task_id = %s
              AND ct.type    = 'branch'
            ORDER BY ct.rate ASC
            """,
            [int(task_id)],
        )
        rows = cur.fetchall()

    branch_ids = [int(value_id) for value_id, _ in rows]

    if len(branch_ids) <= BATCH_THRESHOLD:
        branch_map = {
            bid: get_branch_str(bid, ui_lang)
            for bid in branch_ids
        }
    else:
        # iter_* возвращает generator -> обязательно dict()
        branch_map = dict(iter_branch_str(branch_ids, ui_lang))

    out: List[Dict[str, Any]] = []
    for value_id, rate in rows:
        bid = int(value_id)
        text = branch_map.get(bid)
        if not text:
            continue
        out.append(
            {
                "value_id": bid,
                "value_text": text,
                "rate": int(rate) if rate is not None else 100,
            }
        )

    return out


def update_rate(
    ws_id,
    user_id,
    task_id: int,
    type_: str,
    value_id: int,
    rate: int,
) -> None:
    type_ = (type_ or "").strip().lower()
    if type_ not in ("city", "branch"):
        return

    try:
        rate_i = int(rate)
    except Exception:
        return

    rate_i = max(1, min(100, rate_i))

    with connection.cursor() as cur:
        cur.execute(
            """
            UPDATE public.crawl_tasks
               SET rate = %s,
                   updated_at = now()
             WHERE task_id  = %s
               AND type     = %s
               AND value_id = %s
            """,
            [rate_i, int(task_id), type_, int(value_id)],
        )
