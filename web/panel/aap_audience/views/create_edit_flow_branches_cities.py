# FILE: web/panel/aap_audience/views/create_edit_flow_branches_cities.py
# DATE: 2026-03-23
# PURPOSE: Branches step handler for the create/edit flow.

from __future__ import annotations

import json
import secrets
from typing import Any, Mapping

from django.db import connection, transaction
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.shortcuts import render

from engine.common.cache.client import CLIENT
from engine.common.gpt import GPTClient
from engine.common.utils import h64_text, parse_json_response
from engine.common.prompts.process import get_prompt, translate_text
from mailer_web.format_data import get_branches_sys_translations

from .create_edit_flow_shared import (
    build_flow_render_context,
    build_step_definitions,
    get_flow_config,
)


BRANCH_CLEAN_CHUNK_SIZE = 40
BRANCH_QUERY_LIMIT = BRANCH_CLEAN_CHUNK_SIZE * 10
FORM_TTL_SEC = 24 * 60 * 60
BRANCH_EXPAND_ADJACENT_RU = "Расширь текущий список за счет других использований продукта, которые не были перечислены в описании продукта. Расширь список категорий за счет смежных категорий, похожих категорий, дополнительных синонимов, аналогов. Используй контекст бизнес-справочников."
BRANCH_EXPAND_MIDDLEMEN_RU = "Расширь текущий список за счет релевантных посредников и перекупщиков, оптовых торговцев и покупателей. Если по компании-продавцу понятно, что это экспорт в Германию, расширь список за счет релевантных посредников импорт-экспорт."


def _render_branch_items(records: list[dict[str, Any]], ui_lang: str) -> list[str]:
    if not records:
        return ["Ничего не найдено."]
    if ui_lang == "de":
        return [str(item["branch_name"]) for item in records]
    translated = get_branches_sys_translations([int(item["id"]) for item in records], ui_lang)

    return [
        f"{item['branch_name']} / {translated[int(item['id'])]}"
        if translated.get(int(item["id"])) and translated[int(item["id"])] != str(item["branch_name"])
        else str(item["branch_name"])
        for item in records
    ] or ["Ничего не найдено."]


def _collapse_branch_rows_for_display(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        branch_name = str(row.get("branch_name") or "").strip()
        if not branch_name:
            continue
        key = branch_name.casefold()
        branch_id = int(row["id"])
        if key not in index:
            item = {
                "ids": [branch_id],
                "ids_csv": str(branch_id),
                "rate_display": str(row.get("rate_display") or "-"),
                "branch_name": branch_name,
                "translated_name": str(row.get("translated_name") or "").strip(),
            }
            index[key] = item
            out.append(item)
            continue
        item = index[key]
        item["ids"].append(branch_id)
        item["ids_csv"] = ",".join(str(v) for v in item["ids"])
        if not item["translated_name"] and str(row.get("translated_name") or "").strip():
            item["translated_name"] = str(row.get("translated_name") or "").strip()
    return out


def _collapse_branch_records_for_rating(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in records:
        branch_name = str(item.get("branch_name") or "").strip()
        if not branch_name:
            continue
        key = branch_name.casefold()
        if key in seen:
            continue
        seen.add(key)
        row = {"branch_name": branch_name}
        if item.get("rate") is not None:
            row["rate"] = int(item["rate"])
        out.append(row)
    return out


def handle_branches_cities_step_view(
    request,
    *,
    flow_type: str,
    current_step_key: str,
    item_id: str,
    task,
    saved_values: Mapping[str, Any],
    flow_status: Mapping[str, Any],
):
    flow_conf = get_flow_config(flow_type)
    step_definitions = build_step_definitions(flow_type)
    ui_lang = request.ui_lang_code
    branch_rating_rows: list[dict[str, Any]] = []
    branch_expand_rows: list[dict[str, Any]] = []
    branch_hash_changed = False

    branch_form = str(request.GET.get("branch_form") or "").strip()
    city_form = str(request.GET.get("city_form") or "").strip()

    if request.method == "GET":
        redirect_needed = False

        if not branch_form:
            branch_form = f"bf_{secrets.token_urlsafe(12)}"
            redirect_needed = True
        if not city_form:
            city_form = f"cf_{secrets.token_urlsafe(12)}"
            redirect_needed = True

        branch_key = f"aap:create_flow:branch_form:{branch_form}"
        city_key = f"aap:create_flow:city_form:{city_form}"

        if not CLIENT.get(branch_key, ttl_sec=FORM_TTL_SEC):
            CLIENT.set(
                branch_key,
                json.dumps(
                    {
                        "db_ids": [],
                        "expanded_raw_ids": [],
                        "expanded_clean_ids": [],
                        "conversation_id": "",
                        "response_id": "",
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                ttl_sec=FORM_TTL_SEC,
            )
            if request.GET.get("branch_form"):
                branch_form = f"bf_{secrets.token_urlsafe(12)}"
                branch_key = f"aap:create_flow:branch_form:{branch_form}"
                CLIENT.set(
                    branch_key,
                    json.dumps(
                        {
                            "db_ids": [],
                            "expanded_raw_ids": [],
                            "expanded_clean_ids": [],
                            "conversation_id": "",
                            "response_id": "",
                        },
                        ensure_ascii=False,
                    ).encode("utf-8"),
                    ttl_sec=FORM_TTL_SEC,
                )
                redirect_needed = True

        if not CLIENT.get(city_key, ttl_sec=FORM_TTL_SEC):
            CLIENT.set(city_key, b"1", ttl_sec=FORM_TTL_SEC)
            if request.GET.get("city_form"):
                city_form = f"cf_{secrets.token_urlsafe(12)}"
                city_key = f"aap:create_flow:city_form:{city_form}"
                CLIENT.set(city_key, b"1", ttl_sec=FORM_TTL_SEC)
                redirect_needed = True

        if redirect_needed:
            params = request.GET.copy()
            params["branch_form"] = branch_form
            params["city_form"] = city_form
            return HttpResponseRedirect(f"{request.path}?{params.urlencode()}")

    branch_key = f"aap:create_flow:branch_form:{branch_form}" if branch_form else ""

    branch_instruction = ""
    branch_records: list[dict[str, Any]] = []

    branch_items = _render_branch_items(branch_records, ui_lang) if branch_records else []

    branch_state_payload = CLIENT.get(branch_key, ttl_sec=FORM_TTL_SEC) if branch_key else None
    try:
        branch_state = json.loads((branch_state_payload or b"").decode("utf-8")) if branch_state_payload else {}
    except Exception:
        branch_state = {}
    if not isinstance(branch_state, dict):
        branch_state = {}
    branch_state.setdefault("db_ids", [])
    branch_state.setdefault("expanded_raw_ids", [])
    branch_state.setdefault("expanded_clean_ids", [])
    branch_state.setdefault("conversation_id", "")
    branch_state.setdefault("response_id", "")

    if task:
        current_task_hash = h64_text((task.source_product or "") + (task.source_company or ""))
        with connection.cursor() as cur:
            cur.execute(
                "SELECT tbr.branch_id, tbr.rate, tbr.hash_task, bs.branch_name "
                "FROM task_branch_ratings tbr "
                "JOIN branches_sys bs ON bs.id = tbr.branch_id "
                "WHERE tbr.task_id = %s "
                "ORDER BY tbr.rate ASC NULLS LAST, tbr.branch_id ASC",
                [int(task.id)],
            )
            rows = cur.fetchall() or []

        branch_hash_changed = bool(rows) and any(row[2] != current_task_hash for row in rows)

        translated = (
            get_branches_sys_translations([int(row[0]) for row in rows], ui_lang)
            if rows and ui_lang != "de"
            else {}
        )

        branch_db_ids = [int(row[0]) for row in rows if row and row[3]]
        branch_rating_rows = _collapse_branch_rows_for_display([
            {
                "id": int(row[0]),
                "rate_display": str(row[1]) if row[1] is not None else "-",
                "branch_name": str(row[3] or "").strip(),
                "translated_name": (
                    str(translated.get(int(row[0])) or "").strip()
                    if ui_lang != "de"
                    else ""
                ),
            }
            for row in rows
            if row and row[3]
        ])

        if request.method == "GET" and branch_key:
            branch_state["db_ids"] = branch_db_ids
            CLIENT.set(
                branch_key,
                json.dumps(branch_state, ensure_ascii=False).encode("utf-8"),
                ttl_sec=FORM_TTL_SEC,
            )

        yellow_ids: list[int] = []
        seen_yellow: set[int] = set()
        for branch_id in [int(v) for v in branch_state.get("expanded_clean_ids") or [] if str(v).strip()]:
            if branch_id in seen_yellow:
                continue
            seen_yellow.add(branch_id)
            yellow_ids.append(branch_id)

        if yellow_ids:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, branch_name "
                    "FROM public.branches_sys "
                    "WHERE id = ANY(%s) "
                    "ORDER BY id ASC",
                    [yellow_ids],
                )
                expanded_rows = cur.fetchall() or []

            expanded_map = {int(row[0]): str(row[1] or "").strip() for row in expanded_rows if row and row[1]}
            expanded_translated = (
                get_branches_sys_translations(list(expanded_map.keys()), ui_lang)
                if expanded_map and ui_lang != "de"
                else {}
            )
            branch_expand_rows = _collapse_branch_rows_for_display([
                {
                    "id": branch_id,
                    "rate_display": "-",
                    "branch_name": expanded_map[branch_id],
                    "translated_name": (
                        str(expanded_translated.get(branch_id) or "").strip()
                        if ui_lang != "de"
                        else ""
                    ),
                }
                for branch_id in yellow_ids
                if branch_id in expanded_map
            ])

    if request.method == "POST" and task:
        current_records: list[dict[str, Any]] = []
        product_de = (translate_text(task.source_product or "", "de") or "").strip() or (task.source_product or "").strip()
        company_de = (translate_text(task.source_company or "", "de") or "").strip() or (task.source_company or "").strip()
        action = str(request.POST.get("action") or "").strip()
        if action == "branches_recalc_ratings" and branch_key:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT tbr.branch_id, bs.branch_name, tbr.rate "
                    "FROM task_branch_ratings tbr "
                    "JOIN branches_sys bs ON bs.id = tbr.branch_id "
                    "WHERE tbr.task_id = %s "
                    "ORDER BY tbr.rate ASC NULLS LAST, tbr.branch_id ASC",
                    [int(task.id)],
                )
                rated_rows = cur.fetchall() or []

            items_to_rate = _collapse_branch_records_for_rating(
                [{"branch_name": str(row[1] or "").strip()} for row in rated_rows if row and row[1]]
            )
            rating_map: dict[str, int] = {}
            if items_to_rate:
                resp = GPTClient().ask_dialog(
                    model="gpt-5.4",
                    instructions=get_prompt("create_branches_buy_rate" if flow_type == "buy" else "create_branches_sell_rate"),
                    input=json.dumps(
                        {
                            "what_is_needed" if flow_type == "buy" else "what_is_sold": product_de,
                            "buyer_company" if flow_type == "buy" else "seller_company": company_de,
                            "already_rated_items": [],
                            "items_to_rate": items_to_rate,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    conversation=(str(branch_state.get("conversation_id") or "").strip() or None),
                    previous_response_id=(str(branch_state.get("response_id") or "").strip() or None),
                    user_id=str(request.user.id),
                    service_tier="flex",
                    web_search=False,
                )
                raw = resp.raw if isinstance(resp.raw, dict) else {}
                branch_state["response_id"] = str(raw.get("id") or "").strip()
                conversation = raw.get("conversation")
                branch_state["conversation_id"] = (
                    str(conversation.get("id") or "").strip()
                    if isinstance(conversation, dict)
                    else str(conversation or "").strip()
                )
                data = parse_json_response(resp.content or "")
                rated_items = data.get("rated_items") if isinstance(data, dict) else None
                if isinstance(rated_items, list):
                    for item in rated_items:
                        if not isinstance(item, dict):
                            continue
                        branch_name = str(item.get("branch_name") or "").strip()
                        try:
                            rate = int(item.get("rate"))
                        except Exception:
                            continue
                        if not branch_name or rate < 1 or rate > 20:
                            continue
                        rating_map[branch_name.casefold()] = rate

            if rated_rows:
                hash_task = h64_text((task.source_product or "") + (task.source_company or ""))
                with connection.cursor() as cur:
                    for row in rated_rows:
                        if not row or not row[1]:
                            continue
                        cur.execute(
                            "UPDATE task_branch_ratings "
                            "SET rate = %s, hash_task = %s "
                            "WHERE task_id = %s AND branch_id = %s",
                            [
                                rating_map.get(str(row[1]).strip().casefold(), row[2]),
                                hash_task,
                                int(task.id),
                                int(row[0]),
                            ],
                        )
                CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
            return HttpResponseRedirect(request.get_full_path())

        if action == "branches_ignore_hash" and branch_key:
            with connection.cursor() as cur:
                cur.execute(
                    "UPDATE task_branch_ratings "
                    "SET hash_task = %s "
                    "WHERE task_id = %s",
                    [h64_text((task.source_product or "") + (task.source_company or "")), int(task.id)],
                )
            return HttpResponseRedirect(request.get_full_path())

        if action == "branches_refill" and branch_key:
            with connection.cursor() as cur:
                cur.execute(
                    "DELETE FROM task_branch_ratings "
                    "WHERE task_id = %s",
                    [int(task.id)],
                )
            branch_state["db_ids"] = []
            branch_state["expanded_raw_ids"] = []
            branch_state["expanded_clean_ids"] = []
            branch_state["conversation_id"] = ""
            branch_state["response_id"] = ""
            action = "branches_pick"

        if action == "branches_delete_selected" and branch_key:
            delete_ids: list[int] = []
            for value in str(request.POST.get("branches_delete_ids") or "").split(","):
                value = value.strip()
                if not value:
                    continue
                try:
                    delete_ids.append(int(value))
                except Exception:
                    continue
            delete_ids = list(dict.fromkeys(delete_ids))
            if delete_ids:
                with connection.cursor() as cur:
                    cur.execute(
                        "DELETE FROM task_branch_ratings "
                        "WHERE task_id = %s AND branch_id = ANY(%s)",
                        [int(task.id), delete_ids],
                    )
                branch_state["db_ids"] = [int(v) for v in branch_state.get("db_ids") or [] if int(v) not in delete_ids]
                branch_state["expanded_raw_ids"] = [int(v) for v in branch_state.get("expanded_raw_ids") or [] if int(v) not in delete_ids]
                branch_state["expanded_clean_ids"] = [int(v) for v in branch_state.get("expanded_clean_ids") or [] if int(v) not in delete_ids]
                CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
            return HttpResponseRedirect(request.get_full_path())

        if action == "branches_clear_expand" and branch_key:
            branch_state["expanded_raw_ids"] = []
            branch_state["expanded_clean_ids"] = []
            CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
            return HttpResponseRedirect(request.get_full_path())

        if action == "branches_save" and branch_key:
            yellow_ids: list[int] = []
            seen_ids: set[int] = set()
            for branch_id in [int(v) for v in branch_state.get("expanded_clean_ids") or [] if str(v).strip()]:
                if branch_id in seen_ids:
                    continue
                seen_ids.add(branch_id)
                yellow_ids.append(branch_id)

            if yellow_ids:
                with connection.cursor() as cur:
                    cur.execute(
                        "SELECT bs.branch_name, tbr.rate "
                        "FROM task_branch_ratings tbr "
                        "JOIN branches_sys bs ON bs.id = tbr.branch_id "
                        "WHERE tbr.task_id = %s "
                        "ORDER BY tbr.rate ASC NULLS LAST, tbr.branch_id ASC",
                        [int(task.id)],
                    )
                    rated_rows = cur.fetchall() or []
                    cur.execute(
                        "SELECT id, branch_name "
                        "FROM public.branches_sys "
                        "WHERE id = ANY(%s) "
                        "ORDER BY id ASC",
                        [yellow_ids],
                    )
                    yellow_rows = cur.fetchall() or []

                already_rated_items = _collapse_branch_records_for_rating(
                    [
                        {"branch_name": str(row[0] or "").strip(), "rate": row[1]}
                        for row in rated_rows
                        if row and row[0]
                    ]
                )
                items_to_rate = _collapse_branch_records_for_rating(
                    [
                        {"branch_name": str(row[1] or "").strip()}
                        for row in yellow_rows
                        if row and row[1]
                    ]
                )

                rating_map: dict[str, int] = {}
                if items_to_rate:
                    resp = GPTClient().ask_dialog(
                        model="gpt-5.4",
                        instructions=get_prompt("create_branches_buy_rate" if flow_type == "buy" else "create_branches_sell_rate"),
                        input=json.dumps(
                            {
                                "what_is_needed" if flow_type == "buy" else "what_is_sold": product_de,
                                "buyer_company" if flow_type == "buy" else "seller_company": company_de,
                                "already_rated_items": already_rated_items,
                                "items_to_rate": items_to_rate,
                            },
                            ensure_ascii=False,
                            indent=2,
                        ),
                        conversation=(str(branch_state.get("conversation_id") or "").strip() or None),
                        previous_response_id=(str(branch_state.get("response_id") or "").strip() or None),
                        user_id=str(request.user.id),
                        service_tier="flex",
                        web_search=False,
                    )
                    raw = resp.raw if isinstance(resp.raw, dict) else {}
                    branch_state["response_id"] = str(raw.get("id") or "").strip()
                    conversation = raw.get("conversation")
                    branch_state["conversation_id"] = (
                        str(conversation.get("id") or "").strip()
                        if isinstance(conversation, dict)
                        else str(conversation or "").strip()
                    )
                    data = parse_json_response(resp.content or "")
                    rated_items = data.get("rated_items") if isinstance(data, dict) else None
                    if isinstance(rated_items, list):
                        for item in rated_items:
                            if not isinstance(item, dict):
                                continue
                            branch_name = str(item.get("branch_name") or "").strip()
                            try:
                                rate = int(item.get("rate"))
                            except Exception:
                                continue
                            if not branch_name or rate < 1 or rate > 20:
                                continue
                            rating_map[branch_name.casefold()] = rate

                yellow_insert_rows = []
                for row in yellow_rows:
                    if not row or not row[1]:
                        continue
                    branch_id = int(row[0])
                    branch_name = str(row[1] or "").strip()
                    yellow_insert_rows.append((branch_id, rating_map.get(branch_name.casefold())))

                if yellow_insert_rows:
                    hash_task = h64_text((task.source_product or "") + (task.source_company or ""))
                    with connection.cursor() as cur:
                        cur.execute(
                            "INSERT INTO task_branch_ratings (task_id, branch_id, rate, hash_task) "
                            "VALUES " + ", ".join(["(%s,%s,%s,%s)"] * len(yellow_insert_rows)) + " "
                            "ON CONFLICT (task_id, branch_id) DO NOTHING",
                            [value for branch_id, rate in yellow_insert_rows for value in (int(task.id), branch_id, rate, hash_task)],
                        )
                branch_state["expanded_raw_ids"] = []
                branch_state["expanded_clean_ids"] = []
                merged_db_ids = []
                seen_db_ids = set()
                for branch_id in [int(v) for v in branch_state.get("db_ids") or [] if str(v).strip()] + yellow_ids:
                    if branch_id in seen_db_ids:
                        continue
                    seen_db_ids.add(branch_id)
                    merged_db_ids.append(branch_id)
                branch_state["db_ids"] = merged_db_ids
                CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
            return HttpResponseRedirect(request.get_full_path())

        if action == "branches_pick":
            initial_input = json.dumps(
                {
                    "what_is_needed" if flow_type == "buy" else "what_is_sold": product_de,
                    "buyer_company" if flow_type == "buy" else "seller_company": company_de,
                },
                ensure_ascii=False,
                indent=2,
            )

            previous_response_id = str(branch_state.get("response_id") or "").strip()
            resp = GPTClient().ask_dialog(
                model="gpt-5.4",
                instructions=get_prompt("create_branches_buy" if flow_type == "buy" else "create_branches_sell"),
                input=initial_input,
                conversation=(str(branch_state.get("conversation_id") or "").strip() or None),
                previous_response_id=(previous_response_id or None),
                user_id=str(request.user.id),
                service_tier="flex",
                web_search=False,
            )
            raw = resp.raw if isinstance(resp.raw, dict) else {}
            branch_state["response_id"] = str(raw.get("id") or "").strip()
            conversation = raw.get("conversation")
            branch_state["conversation_id"] = (
                str(conversation.get("id") or "").strip()
                if isinstance(conversation, dict)
                else str(conversation or "").strip()
            )
            condition = str(resp.content or "").strip()

            if condition:
                try:
                    with transaction.atomic(), connection.cursor() as cur:
                        cur.execute("SET LOCAL TRANSACTION READ ONLY")
                        cur.execute(
                            "SELECT id, branch_name "
                            "FROM public.branches_sys "
                            f"WHERE ({condition}) "
                            f"LIMIT {BRANCH_QUERY_LIMIT}"
                        )
                        rows = cur.fetchall() or []
                except Exception:
                    rows = []

                if rows:
                    for row in rows:
                        branch_name = " ".join(str(row[1] or "").split()).strip()
                        if not branch_name:
                            continue
                        current_records.append({"id": int(row[0]), "branch_name": branch_name})

            branch_state["expanded_raw_ids"] = [int(item["id"]) for item in current_records]
            branch_state["expanded_clean_ids"] = []

            if current_records:
                clean_prompt = get_prompt("create_branches_buy_clean" if flow_type == "buy" else "create_branches_sell_clean")
                previous_response_id = str(branch_state.get("response_id") or "").strip()
                resp = GPTClient().ask_dialog(
                    model="gpt-5.4",
                    instructions=clean_prompt,
                    input=json.dumps(
                        {
                            "what_is_needed" if flow_type == "buy" else "what_is_sold": product_de,
                            "buyer_company" if flow_type == "buy" else "seller_company": company_de,
                            "items": current_records,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    conversation=(str(branch_state.get("conversation_id") or "").strip() or None),
                    previous_response_id=(previous_response_id or None),
                    user_id=str(request.user.id),
                    service_tier="flex",
                    web_search=False,
                )
                raw = resp.raw if isinstance(resp.raw, dict) else {}
                branch_state["response_id"] = str(raw.get("id") or "").strip()
                conversation = raw.get("conversation")
                branch_state["conversation_id"] = (
                    str(conversation.get("id") or "").strip()
                    if isinstance(conversation, dict)
                    else str(conversation or "").strip()
                )
                data = parse_json_response(resp.content or "")
                values = data.get("cleaned_items") if isinstance(data, dict) else None

                if isinstance(values, list):
                    by_id = {int(item["id"]): item for item in current_records}
                    by_name = {str(item["branch_name"]).casefold(): item for item in current_records}
                    cleaned_records: list[dict[str, Any]] = []
                    seen = set()
                    for item in values:
                        match = None
                        if isinstance(item, dict):
                            try:
                                branch_id = int(item.get("id"))
                            except Exception:
                                branch_id = None
                            if branch_id is not None:
                                match = by_id.get(branch_id)
                            if match is None:
                                key = " ".join(str(item.get("branch_name") or "").split()).strip().casefold()
                                if key:
                                    match = by_name.get(key)
                        if not match:
                            continue
                        branch_id = int(match["id"])
                        if branch_id in seen:
                            continue
                        seen.add(branch_id)
                        cleaned_records.append({"id": branch_id, "branch_name": str(match["branch_name"])})
                    current_records = cleaned_records

            branch_state["expanded_clean_ids"] = [int(item["id"]) for item in current_records]
            if branch_key:
                CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)

        elif action in {"branches_expand_adjacent", "branches_expand_middlemen", "branches_expand_custom"} and branch_key:
            db_ids = [int(v) for v in branch_state.get("db_ids") or [] if str(v).strip()]
            if not db_ids:
                db_ids = branch_db_ids
            existing_expand_clean_ids = [int(v) for v in branch_state.get("expanded_clean_ids") or [] if str(v).strip()]
            existing_expand_raw_ids = [int(v) for v in branch_state.get("expanded_raw_ids") or [] if str(v).strip()]

            current_ids: list[int] = []
            seen_ids: set[int] = set()
            for branch_id in db_ids + existing_expand_clean_ids:
                if branch_id in seen_ids:
                    continue
                seen_ids.add(branch_id)
                current_ids.append(branch_id)

            current_items: list[dict[str, Any]] = []
            if current_ids:
                with connection.cursor() as cur:
                    cur.execute(
                        "SELECT id, branch_name "
                        "FROM public.branches_sys "
                        "WHERE id = ANY(%s) "
                        "ORDER BY id ASC",
                        [current_ids],
                    )
                    rows = cur.fetchall() or []
                current_map = {int(row[0]): str(row[1] or "").strip() for row in rows if row and row[1]}
                current_items = [
                    {"id": branch_id, "branch_name": current_map[branch_id]}
                    for branch_id in current_ids
                    if branch_id in current_map
                ]

            instruction_ru = BRANCH_EXPAND_ADJACENT_RU
            if action == "branches_expand_middlemen":
                instruction_ru = BRANCH_EXPAND_MIDDLEMEN_RU
            if action == "branches_expand_custom":
                custom = str(request.POST.get("branches_expand_custom") or "").strip()
                instruction_ru = (
                    f'Расширь список категорий, используя указание, сформулированное так: "{custom}". '
                    "Трактуй это самым широким образом, в контексте бизнес-справочников. "
                    "Рассматривай это как расширение свойств и бизнес-характеристик продукта."
                ) if custom else ""

            if instruction_ru:
                instruction_en = (translate_text(instruction_ru, "en") or "").strip() or instruction_ru
                previous_response_id = str(branch_state.get("response_id") or "").strip()
                resp = GPTClient().ask_dialog(
                    model="gpt-5.4",
                    instructions=get_prompt("create_branches_buy_expand" if flow_type == "buy" else "create_branches_sell_expand"),
                    input=json.dumps(
                        {
                            "instruction": instruction_en,
                            "what_is_needed" if flow_type == "buy" else "what_is_sold": product_de,
                            "buyer_company" if flow_type == "buy" else "seller_company": company_de,
                            "items": current_items,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    conversation=(str(branch_state.get("conversation_id") or "").strip() or None),
                    previous_response_id=(previous_response_id or None),
                    user_id=str(request.user.id),
                    service_tier="flex",
                    web_search=False,
                )
                raw = resp.raw if isinstance(resp.raw, dict) else {}
                branch_state["response_id"] = str(raw.get("id") or "").strip()
                conversation = raw.get("conversation")
                branch_state["conversation_id"] = (
                    str(conversation.get("id") or "").strip()
                    if isinstance(conversation, dict)
                    else str(conversation or "").strip()
                )
                data = parse_json_response(resp.content or "")
                condition = str(data.get("extra_sql_condition") or "").strip() if isinstance(data, dict) else ""

                if condition:
                    try:
                        with transaction.atomic(), connection.cursor() as cur:
                            cur.execute("SET LOCAL TRANSACTION READ ONLY")
                            exclude_sql = ""
                            exclude_ids = []
                            exclude_seen = set()
                            for branch_id in db_ids + existing_expand_raw_ids:
                                if branch_id in exclude_seen:
                                    continue
                                exclude_seen.add(branch_id)
                                exclude_ids.append(branch_id)
                            if exclude_ids:
                                exclude_sql = " AND id NOT IN (" + ", ".join(str(int(v)) for v in exclude_ids) + ")"
                            cur.execute(
                                "SELECT id, branch_name "
                                "FROM public.branches_sys "
                                f"WHERE ({condition}){exclude_sql} "
                                f"LIMIT {BRANCH_QUERY_LIMIT}"
                            )
                            rows = cur.fetchall() or []
                    except Exception:
                        rows = []

                    if rows:
                        for row in rows:
                            branch_name = " ".join(str(row[1] or "").split()).strip()
                            if not branch_name:
                                continue
                            current_records.append({"id": int(row[0]), "branch_name": branch_name})

                    merged_expand_raw_ids = []
                    merged_seen = set()
                    for branch_id in existing_expand_raw_ids + [int(item["id"]) for item in current_records]:
                        if branch_id in merged_seen:
                            continue
                        merged_seen.add(branch_id)
                        merged_expand_raw_ids.append(branch_id)
                    branch_state["expanded_raw_ids"] = merged_expand_raw_ids

                    if current_records:
                        clean_prompt = get_prompt("create_branches_buy_clean" if flow_type == "buy" else "create_branches_sell_clean")
                        previous_response_id = str(branch_state.get("response_id") or "").strip()
                        resp = GPTClient().ask_dialog(
                            model="gpt-5.4",
                            instructions=clean_prompt,
                            input=json.dumps(
                                {
                                    "what_is_needed" if flow_type == "buy" else "what_is_sold": product_de,
                                    "buyer_company" if flow_type == "buy" else "seller_company": company_de,
                                    "items": current_records,
                                },
                                ensure_ascii=False,
                                indent=2,
                            ),
                            conversation=(str(branch_state.get("conversation_id") or "").strip() or None),
                            previous_response_id=(previous_response_id or None),
                            user_id=str(request.user.id),
                            service_tier="flex",
                            web_search=False,
                        )
                        raw = resp.raw if isinstance(resp.raw, dict) else {}
                        branch_state["response_id"] = str(raw.get("id") or "").strip()
                        conversation = raw.get("conversation")
                        branch_state["conversation_id"] = (
                            str(conversation.get("id") or "").strip()
                            if isinstance(conversation, dict)
                            else str(conversation or "").strip()
                        )
                        data = parse_json_response(resp.content or "")
                        values = data.get("cleaned_items") if isinstance(data, dict) else None

                        if isinstance(values, list):
                            by_id = {int(item["id"]): item for item in current_records}
                            by_name = {str(item["branch_name"]).casefold(): item for item in current_records}
                            cleaned_records: list[dict[str, Any]] = []
                            seen = set()
                            for item in values:
                                match = None
                                if isinstance(item, dict):
                                    try:
                                        branch_id = int(item.get("id"))
                                    except Exception:
                                        branch_id = None
                                    if branch_id is not None:
                                        match = by_id.get(branch_id)
                                    if match is None:
                                        key = " ".join(str(item.get("branch_name") or "").split()).strip().casefold()
                                        if key:
                                            match = by_name.get(key)
                                if not match:
                                    continue
                                branch_id = int(match["id"])
                                if branch_id in seen:
                                    continue
                                seen.add(branch_id)
                                cleaned_records.append({"id": branch_id, "branch_name": str(match["branch_name"])})
                            current_records = cleaned_records

                merged_expand_clean_ids = []
                merged_seen = set()
                for branch_id in existing_expand_clean_ids + [int(item["id"]) for item in current_records]:
                    if branch_id in merged_seen:
                        continue
                    merged_seen.add(branch_id)
                    merged_expand_clean_ids.append(branch_id)
                branch_state["expanded_clean_ids"] = merged_expand_clean_ids
                CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
        return HttpResponseRedirect(request.get_full_path())

    return render(
        request,
        flow_conf["template_name"],
        build_flow_render_context(
            flow_type=flow_type,
            item_id=item_id,
            task=task,
            saved_values=saved_values,
            step_definitions=step_definitions,
            flow_status=flow_status,
            current_step_key=current_step_key,
            step_template="panels/aap_audience/create/step_branches_cities.html",
            extra_context={
                "branches_cities_step": {
                    "branch_items": branch_items,
                    "branch_rating_rows": branch_rating_rows,
                    "branch_expand_rows": branch_expand_rows,
                    "city_items": [],
                    "branch_instruction": branch_instruction,
                    "city_instruction": "",
                    "branch_conversation_id": "",
                    "branch_response_id": "",
                "branch_records_json": "[]",
                    "branch_rate_modal_base_url": reverse("audience:create_branch_rate_modal") + f"?id={item_id}",
                    "branch_hash_changed": branch_hash_changed,
                    "city_conversation_id": "",
                    "city_response_id": "",
                    "city_records_json": "",
                },
            },
        ),
    )
