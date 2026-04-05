# FILE: engine/core_rate_cities_expand_pairs/rate_cities.py
# DATE: 2026-03-25
# PURPOSE: Standalone city-rating task for the new flow. Picks one task with unrated cities,
# rates up to 50 cities via GPT, stores rates/hash in DB, and returns the batch result.

from __future__ import annotations

import json
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.common.cache.client import CLIENT
from engine.common.db import get_connection
from engine.common.gpt import GPTClient
from engine.common.logs import log
from engine.common.prompts.process import get_prompt, translate_text
from engine.common.utils import h64_text, parse_json_response


BATCH_SIZE = 50
MODEL = "gpt-5.4-mini"
SERVICE_TIER = "flex"
TASK_LOCK_TTL_SEC = 300
TASK_PICK_LIMIT = 200
LOG_FILE = "rate_cities.log"
LOG_FOLDER = "processing"


def _task_lock_key(task_id: int) -> str:
    return f"core_tasks:rate_cities:task:{int(task_id)}"


def _pick_task_ids(limit: int = TASK_PICK_LIMIT) -> Tuple[List[int], int]:
    t0 = time.perf_counter()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT tcr.task_id
            FROM public.task_city_ratings tcr
            JOIN public.aap_audience_audiencetask t
              ON t.id = tcr.task_id
            WHERE tcr.rate IS NULL
              AND t.archived = false
            LIMIT %s
            """
            ,
            (int(limit),),
        )
        rows = cur.fetchall() or []
    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    task_ids = [int(row[0]) for row in rows if row]
    random.shuffle(task_ids)
    return task_ids, elapsed_ms


def _load_task(task_id: int) -> Tuple[Optional[Dict[str, Any]], int]:
    t0 = time.perf_counter()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              t.id,
              t.user_id,
              t.type,
              t.title,
              t.source_product,
              t.source_company,
              t.source_geo,
              (
                SELECT COUNT(*)
                FROM public.task_city_ratings tcr
                WHERE tcr.task_id = t.id
                  AND tcr.rate IS NULL
              )::int AS unrated_cnt
            FROM public.aap_audience_audiencetask t
            WHERE t.id = %s
              AND t.archived = false
            LIMIT 1
            """,
            (int(task_id),),
        )
        row = cur.fetchone()
    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    if not row:
        return None, elapsed_ms

    return {
        "task_id": int(row[0]),
        "user_id": int(row[1]),
        "task_type": str(row[2] or "").strip().lower(),
        "title": str(row[3] or "").strip(),
        "source_product": str(row[4] or "").strip(),
        "source_company": str(row[5] or "").strip(),
        "source_geo": str(row[6] or "").strip(),
        "unrated_cnt": int(row[7] or 0),
    }, elapsed_ms


def _pick_task() -> Tuple[Optional[Dict[str, Any]], Optional[str], Dict[str, int]]:
    owner = f"{os.getpid()}:{int(time.time())}"
    task_ids, pick_task_ids_ms = _pick_task_ids()
    sql_ms: Dict[str, int] = {"pick_task_ids_ms": int(pick_task_ids_ms)}
    for task_id in task_ids:
        resp = CLIENT.lock_try(_task_lock_key(int(task_id)), ttl_sec=TASK_LOCK_TTL_SEC, owner=owner)
        if not resp or resp.get("acquired") is not True or not isinstance(resp.get("token"), str):
            continue

        token = str(resp["token"])
        task, load_task_ms = _load_task(int(task_id))
        sql_ms["load_task_ms"] = int(load_task_ms)
        if not task or int(task.get("unrated_cnt") or 0) <= 0:
            try:
                CLIENT.lock_release(_task_lock_key(int(task_id)), token=token)
            except Exception:
                pass
            continue
        return task, token, sql_ms

    return None, None, sql_ms


def _load_candidates(task_id: int, limit: int) -> Tuple[List[Dict[str, Any]], int]:
    t0 = time.perf_counter()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              cs.id,
              cs.state_name,
              cs.name,
              cs.area_km2,
              cs.pop_total,
              cs.urban_code,
              cs.urban_name,
              cs.travel_code,
              cs.travel_name,
              cs.lat,
              cs.lon
            FROM public.task_city_ratings tcr
            JOIN public.cities_sys cs
              ON cs.id = tcr.city_id
            WHERE tcr.task_id = %s
              AND tcr.rate IS NULL
            LIMIT %s
            """,
            (int(task_id), int(limit)),
        )
        rows = cur.fetchall() or []
    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "id": int(row[0]),
                "state_name": str(row[1] or ""),
                "name": str(row[2] or ""),
                "area_km2": float(row[3]) if row[3] is not None else None,
                "pop_total": int(row[4]) if row[4] is not None else None,
                "urban_code": str(row[5] or ""),
                "urban_name": str(row[6] or ""),
                "travel_code": str(row[7] or ""),
                "travel_name": str(row[8] or ""),
                "lat": float(row[9]) if row[9] is not None else None,
                "lon": float(row[10]) if row[10] is not None else None,
            }
        )
    return out, elapsed_ms


def _build_prompt_name(task_type: str) -> str:
    return "rate_cities_buy" if task_type == "buy" else "rate_cities_sell"


def _build_instructions_text(task_type: str, source_product: str, source_company: str, source_geo: str) -> str:
    product_heading = "Продукт - услуга, что покупается" if task_type == "buy" else "Продукт - услуга, что продается"
    company_heading = "Компания - покупатель" if task_type == "buy" else "Компания - продавец"
    geo_heading = "Ограничения по географии"

    product_de = (translate_text(source_product, "de") or "").strip() or source_product
    company_de = (translate_text(source_company, "de") or "").strip() or source_company
    geo_de = (translate_text(source_geo, "de") or "").strip() or source_geo

    return (
        f"{product_heading}:\n{product_de}\n\n"
        f"{company_heading}:\n{company_de}\n\n"
        f"{geo_heading}:\n{geo_de}"
    ).strip()


def _build_instructions(task_type: str, source_product: str, source_company: str, source_geo: str) -> str:
    prompt_name = _build_prompt_name(task_type)
    base = (get_prompt(prompt_name) or "").strip()
    context_text = _build_instructions_text(task_type, source_product, source_company, source_geo)
    return (base + "\n\n" + context_text).strip()


def _validate_ranked_items(
    candidates: List[Dict[str, Any]],
    rated_items: Optional[List[Dict[str, Any]]],
) -> List[Dict[str, int]]:
    if not rated_items:
        return []

    allowed = {int(item["id"]): str(item["name"]) for item in candidates}
    seen: set[int] = set()
    out: List[Dict[str, int]] = []

    for item in rated_items:
        try:
            city_id = int(item["id"])
            city_name = str(item["name"] or "").strip()
            rate = int(item["rate"])
        except Exception:
            continue

        if city_id not in allowed:
            continue
        if allowed[city_id] != city_name:
            continue
        if city_id in seen:
            continue
        if rate < 1 or rate > 100:
            continue

        seen.add(city_id)
        out.append({"city_id": city_id, "rate": rate})

    return out


def _build_task_hash(source_product: str, source_company: str, source_geo: str) -> int:
    return int(h64_text(str(source_product or "") + str(source_company or "") + str(source_geo or "")))


def _save_rated_items(task_id: int, items: List[Dict[str, int]], hash_task: int) -> Tuple[int, int]:
    if not items:
        return 0, 0

    saved = 0
    t0 = time.perf_counter()
    with get_connection() as conn, conn.cursor() as cur:
        for item in items:
            cur.execute(
                """
                UPDATE public.task_city_ratings
                SET rate = %s,
                    hash_task = %s,
                    updated_at = now()
                WHERE task_id = %s
                  AND city_id = %s
                """,
                (
                    int(item["rate"]),
                    int(hash_task),
                    int(task_id),
                    int(item["city_id"]),
                ),
            )
            saved += int(cur.rowcount or 0)
    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return saved, elapsed_ms


def _log_event(payload: Dict[str, Any]) -> None:
    log(LOG_FILE, folder=LOG_FOLDER, message=json.dumps(payload, ensure_ascii=False, default=str))


def run_once() -> Dict[str, Any]:
    task, task_lock_token, sql_ms = _pick_task()
    if not task:
        result = {"mode": "noop", "reason": "no_task_with_unrated_cities", **sql_ms}
        _log_event({"event": "rate_cities", **result})
        return result

    try:
        candidates, load_candidates_ms = _load_candidates(int(task["task_id"]), BATCH_SIZE)
        sql_ms["load_candidates_ms"] = int(load_candidates_ms)
        if not candidates:
            result = {
                "mode": "noop",
                "reason": "task_has_no_batch",
                "task_id": int(task["task_id"]),
                **sql_ms,
            }
            _log_event({"event": "rate_cities", **result})
            return result

        instructions = _build_instructions(
            str(task["task_type"]),
            str(task["source_product"]),
            str(task["source_company"]),
            str(task["source_geo"]),
        )

        payload = json.dumps({"items_to_rate": candidates}, ensure_ascii=False, separators=(",", ":"))
        response = GPTClient().ask(
            model=MODEL,
            service_tier=SERVICE_TIER,
            user_id=str(task["user_id"]),
            instructions=instructions,
            input=payload,
            use_cache=False,
        )

        data = parse_json_response(response.content or "")
        rated_items = data.get("rated_items") if isinstance(data, dict) else None
        items = _validate_ranked_items(candidates, rated_items if isinstance(rated_items, list) else None)
        hash_task = _build_task_hash(
            str(task["source_product"]),
            str(task["source_company"]),
            str(task["source_geo"]),
        )
        saved_count, save_rated_items_ms = _save_rated_items(int(task["task_id"]), items, hash_task)
        sql_ms["save_rated_items_ms"] = int(save_rated_items_ms)

        result = {
            "mode": "ok" if items else "empty",
            "task_id": int(task["task_id"]),
            "task_type": str(task["task_type"]),
            "title": str(task["title"]),
            "batch_size": len(candidates),
            "unrated_total": int(task["unrated_cnt"]),
            "items": items,
            "saved_count": int(saved_count),
            "hash_task": int(hash_task),
            "raw_count": len(rated_items or []) if isinstance(rated_items, list) else 0,
            **sql_ms,
        }
        _log_event(
            {
                "event": "rate_cities",
                "mode": result["mode"],
                "task_id": result["task_id"],
                "task_type": result["task_type"],
                "batch_size": result["batch_size"],
                "unrated_total": result["unrated_total"],
                "saved_count": result["saved_count"],
                "hash_task": result["hash_task"],
                "raw_count": result["raw_count"],
                "pick_task_ids_ms": result.get("pick_task_ids_ms", 0),
                "load_task_ms": result.get("load_task_ms", 0),
                "load_candidates_ms": result.get("load_candidates_ms", 0),
                "save_rated_items_ms": result.get("save_rated_items_ms", 0),
            }
        )
        return result
    finally:
        if task_lock_token:
            try:
                CLIENT.lock_release(_task_lock_key(int(task["task_id"])), token=task_lock_token)
            except Exception:
                pass


def main() -> None:
    print(json.dumps(run_once(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
