# FILE: web/panel/aap_audience/views/clar_items.py  (обновлено — 2025-12-27)
# (новое — 2025-12-27)
# - Добавлены *args/**kwargs для совместимости со старыми вызовами
# - workspace_id / user_id игнорируются

from __future__ import annotations

from typing import Any, Dict, List

from django.db import connection


def load_sorted_cities(task_id: int, *args, **kwargs) -> List[Dict[str, Any]]:
    """
    Совместимо со старым вызовом:
    load_sorted_cities(ws_id, user_id, task_id)
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                ct.value_id,
                cs.name       AS city_name,
                cs.state_name AS land_name,
                ct.rate
            FROM public.crawl_tasks ct
            JOIN public.cities_sys cs
              ON cs.id = ct.value_id
            WHERE ct.task_id = %s
              AND ct.type    = 'city'
            ORDER BY ct.rate ASC
            """,
            [int(task_id)],
        )
        rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for value_id, city_name, land_name, rate in rows:
        city_name = (city_name or "").strip()
        land_name = (land_name or "").strip()
        value_text = f"{city_name} ({land_name})" if land_name else city_name
        out.append(
            {
                "value_id": int(value_id),
                "value_text": value_text,
                "rate": int(rate) if rate is not None else 100,
                "city_name": city_name,
                "land_name": land_name,
            }
        )
    return out


def load_sorted_branches(*args, **kwargs) -> List[Dict[str, Any]]:
    """
    Совместимо со старым вызовом:
      load_sorted_branches(ws_id, user_id, task_id, ui_lang=ui_lang)
    Здесь принимаем всё через *args/**kwargs, чтобы не словить
    "multiple values for argument 'ui_lang'".
    """
    # task_id: третий позиционный аргумент в старом вызове
    if len(args) >= 3:
        task_id = int(args[2])
    else:
        task_id = int(kwargs.get("task_id"))

    ui_lang = (kwargs.get("ui_lang") or "").strip().lower() or "ru"

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                ct.value_id,
                b.name        AS name_de,
                ui.name_trans AS name_ui,
                ct.rate
            FROM public.crawl_tasks ct
            JOIN public.gb_branches b
              ON b.id = ct.value_id
            LEFT JOIN public.gb_branch_i18n ui
              ON ui.branch_id = ct.value_id
             AND ui.lang = %s
            WHERE ct.task_id = %s
              AND ct.type    = 'branch'
            ORDER BY ct.rate ASC
            """,
            [str(ui_lang), int(task_id)],
        )
        rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for value_id, name_de, name_ui, rate in rows:
        name_de = (name_de or "").strip()
        if not name_de:
            continue

        name_ui = (name_ui or "").strip()

        if ui_lang == "de":
            value_text = name_de
        else:
            value_text = f"{name_de} — {name_ui}" if name_ui else name_de

        out.append(
            {
                "value_id": int(value_id),
                "value_text": value_text,
                "rate": int(rate) if rate is not None else 100,
                "name_de": name_de,
                "name_ui": name_ui,
            }
        )
    return out


def update_rate(task_id: int, type_: str, value_id: int, rate: int, *args, **kwargs) -> None:
    """
    Совместимо со старым вызовом:
    update_rate(ws_id, user_id, task_id, type_, value_id, rate)
    """
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
