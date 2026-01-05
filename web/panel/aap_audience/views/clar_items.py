# FILE: web/panel/aap_audience/views/clar_items.py
# DATE: 2026-01-05
# CHANGE:
# - Убран "левый" интерфейс (args/kwargs + _extract_task_id). Оставлены только реальные сигнатуры вызовов.
# - Убраны JOIN'ы для печати cities/branches: value_text теперь формируется централизованно через format_data:
#     cities:  get_city_land(city_id)
#     branches: get_branch_str(branch_id, ui_lang)
# - update_rate оставлен, но с нормальной сигнатурой (ws_id, user_id, task_id, type_, value_id, rate).

from __future__ import annotations

from typing import Any, Dict, List

from django.db import connection

from mailer_web.format_data import get_branch_str, get_city_land


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

    out: List[Dict[str, Any]] = []
    for value_id, rate in rows:
        city_id = int(value_id)
        out.append(
            {
                "value_id": city_id,
                "value_text": get_city_land(city_id),
                "rate": int(rate) if rate is not None else 100,
            }
        )
    return out


def load_sorted_branches(ws_id, user_id, task_id: int, *, ui_lang: str) -> List[Dict[str, Any]]:
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

    out: List[Dict[str, Any]] = []
    for value_id, rate in rows:
        branch_id = int(value_id)
        value_text = get_branch_str(branch_id, ui_lang)
        if not value_text:
            continue
        out.append(
            {
                "value_id": branch_id,
                "value_text": value_text,
                "rate": int(rate) if rate is not None else 100,
            }
        )
    return out


def update_rate(ws_id, user_id, task_id: int, type_: str, value_id: int, rate: int) -> None:
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
