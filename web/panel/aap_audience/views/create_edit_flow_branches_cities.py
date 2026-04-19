# FILE: web/panel/aap_audience/views/create_edit_flow_branches_cities.py
# DATE: 2026-03-23
# PURPOSE: Branches step handler for the create/edit flow.

from __future__ import annotations

import json
import re
import secrets
from typing import Any, Callable, Mapping

from django.db import connection, transaction
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.shortcuts import render

from engine.common.cache.client import CLIENT
from engine.common.gpt import GPTClient
from engine.common.utils import h64_text, parse_json_response
from engine.common.translate import get_prompt, translate_text
from mailer_web.format_contact import get_category_title, get_city_title_by_city_id

from .create_edit_flow_gpt_consts import FLOW_GPT_MODEL, FLOW_GPT_SERVICE_TIER
from .create_edit_flow_shared import (
    build_flow_render_context,
    build_step_definitions,
    clear_dialog_state,
    flow_back_url,
    get_flow_config,
    is_gpt_ok,
    mark_flow_gpt_unavailable,
)


BRANCH_CLEAN_CHUNK_SIZE = 40
BRANCH_QUERY_LIMIT = BRANCH_CLEAN_CHUNK_SIZE * 10
FORM_TTL_SEC = 24 * 60 * 60
BRANCH_EXPAND_ADJACENT_RU = "Расширь текущий список за счет других использований продукта, которые не были перечислены в описании продукта. Расширь список категорий за счет смежных категорий, похожих категорий, дополнительных синонимов, аналогов. Используй контекст бизнес-справочников."
BRANCH_EXPAND_MIDDLEMEN_RU = "Расширь текущий список за счет релевантных посредников и перекупщиков, оптовых торговцев и покупателей. Если по компании-продавцу понятно, что это экспорт в Германию, расширь список за счет релевантных посредников импорт-экспорт."
CITY_RADIUS_RE = re.compile(r"__RADIUS_FROM_CITY__\('((?:[^']|'')+)',\s*([0-9]+)\)")


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
            }
            index[key] = item
            out.append(item)
            continue
        item = index[key]
        item["ids"].append(branch_id)
        item["ids_csv"] = ",".join(str(v) for v in item["ids"])
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


def _expand_city_radius_sql(condition: str) -> str:
    value = str(condition or "").strip()
    if not value or "__RADIUS_FROM_CITY__(" not in value:
        return value

    replacements: dict[tuple[str, str], str] = {}
    with connection.cursor() as cur:
        for city_raw, radius_raw in CITY_RADIUS_RE.findall(value):
            key = (city_raw, radius_raw)
            if key in replacements:
                continue
            city = city_raw.replace("''", "'").strip()
            cur.execute(
                "SELECT lat, lon "
                "FROM public.cities_sys "
                "WHERE name ILIKE %s OR name ILIKE %s "
                "ORDER BY pop_total DESC NULLS LAST "
                "LIMIT 1",
                [city, f"{city},%"],
            )
            row = cur.fetchone()
            if not row or row[0] is None or row[1] is None:
                continue
            lat = float(row[0])
            lon = float(row[1])
            radius = int(radius_raw)
            replacements[key] = (
                "6371 * ACOS(LEAST(1.0, GREATEST(-1.0, "
                f"COS(RADIANS({lat})) * COS(RADIANS(lat)) * COS(RADIANS(lon) - RADIANS({lon})) + "
                f"SIN(RADIANS({lat})) * SIN(RADIANS(lat))"
                f"))) <= {radius}"
            )

    def _replace(match: re.Match[str]) -> str:
        return replacements.get((match.group(1), match.group(2)), match.group(0))

    return CITY_RADIUS_RE.sub(_replace, value)


def _request_branch_rating_map(
    request,
    *,
    flow_type: str,
    product_de: str,
    company_de: str,
    branch_state: dict[str, Any],
    already_rated_items: list[dict[str, Any]],
    items_to_rate: list[dict[str, Any]],
) -> tuple[dict[str, int], dict[str, Any], bool]:
    rating_map: dict[str, int] = {}
    if not items_to_rate:
        return rating_map, branch_state, False

    resp = GPTClient().ask_dialog(
        model=FLOW_GPT_MODEL,
        instructions=get_prompt(
            "create_branches_buy_rate" if flow_type == "buy" else "create_branches_sell_rate",
            on_gpt_error=lambda: mark_flow_gpt_unavailable(request),
        ),
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
        service_tier=FLOW_GPT_SERVICE_TIER,
        web_search=True,
    )
    if not is_gpt_ok(resp):
        clear_dialog_state(branch_state)
        mark_flow_gpt_unavailable(request)
        return {}, branch_state, True
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
    return rating_map, branch_state, False


def _ensure_saved_branch_ratings(
    request,
    *,
    task,
    flow_type: str,
    resolve_translated_context: Callable[[], tuple[str, str]],
    branch_state: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    gpt_failed = False
    for _attempt in range(2):
        with connection.cursor() as cur:
            cur.execute(
                "SELECT tbr.branch_id, bs.branch_name, tbr.rate "
                "FROM task_branch_ratings tbr "
                "JOIN branches_sys bs ON bs.id = tbr.branch_id "
                "WHERE tbr.task_id = %s "
                "ORDER BY tbr.rate ASC NULLS LAST, tbr.branch_id ASC",
                [int(task.id)],
            )
            rows = cur.fetchall() or []

        unrated_rows = [row for row in rows if row and row[1] and row[2] is None]
        if not unrated_rows:
            break

        already_rated_items = _collapse_branch_records_for_rating(
            [
                {"branch_name": str(row[1] or "").strip(), "rate": row[2]}
                for row in rows
                if row and row[1] and row[2] is not None
            ]
        )
        items_to_rate = _collapse_branch_records_for_rating(
            [{"branch_name": str(row[1] or "").strip()} for row in unrated_rows if row and row[1]]
        )
        product_de, company_de = resolve_translated_context()
        rating_map, branch_state, gpt_failed = _request_branch_rating_map(
            request,
            flow_type=flow_type,
            product_de=product_de,
            company_de=company_de,
            branch_state=branch_state,
            already_rated_items=already_rated_items,
            items_to_rate=items_to_rate,
        )
        if gpt_failed:
            break
        if not rating_map:
            break

        hash_task = h64_text((task.source_product or "") + (task.source_company or ""))
        updated_any = False
        with connection.cursor() as cur:
            for row in unrated_rows:
                branch_id = int(row[0])
                branch_name = str(row[1] or "").strip()
                rate = rating_map.get(branch_name.casefold())
                if rate is None:
                    continue
                updated_any = True
                cur.execute(
                    "UPDATE task_branch_ratings "
                    "SET rate = %s, hash_task = %s "
                    "WHERE task_id = %s AND branch_id = %s",
                    [rate, hash_task, int(task.id), branch_id],
                )
        if not updated_any:
            break

    return branch_state, gpt_failed


def _current_city_hash(task, geo_text: str) -> int:
    return int(h64_text((task.source_product or "") + (task.source_company or "") + str(geo_text or "")))


def _build_city_rows_context(request, task, geo_text: str) -> dict[str, Any]:
    with connection.cursor() as cur:
        cur.execute(
            "SELECT tcr.city_id, tcr.rate, tcr.hash_task, cs.name, cs.state_name "
            "FROM task_city_ratings tcr "
            "JOIN cities_sys cs ON cs.id = tcr.city_id "
            "WHERE tcr.task_id = %s "
            "ORDER BY tcr.rate ASC NULLS LAST, cs.state_name ASC, cs.name ASC, tcr.city_id ASC",
            [int(task.id)],
        )
        city_rows = cur.fetchall() or []

    city_task_hash = _current_city_hash(task, geo_text)
    city_hash_changed = bool(city_rows) and any(row[2] != city_task_hash for row in city_rows)

    def _city_title(city_id: int, fallback_name: str, fallback_state: str) -> str:
        try:
            title = " ".join(str(get_city_title_by_city_id(city_id, request, land=True)).split()).strip()
        except Exception:
            title = ""
        fallback_city = " ".join(str(fallback_name or "").split()).strip()
        fallback_state_title = " ".join(str(fallback_state or "").split()).strip()
        if fallback_city and fallback_state_title:
            fallback = f"{fallback_city}, {fallback_state_title}"
        else:
            fallback = fallback_city or fallback_state_title
        return title or fallback

    city_rating_rows = [
        {
            "id": int(row[0]),
            "ids_csv": str(int(row[0])),
            "rate_display": str(row[1]) if row[1] is not None else "-",
            "city_title": _city_title(int(row[0]), str(row[3] or "").strip(), str(row[4] or "").strip()),
        }
        for row in city_rows
        if row and row[1] is not None
    ]
    city_expand_rows = [
        {
            "id": int(row[0]),
            "ids_csv": str(int(row[0])),
            "rate_display": "-",
            "city_title": _city_title(int(row[0]), str(row[3] or "").strip(), str(row[4] or "").strip()),
        }
        for row in city_rows
        if row and row[1] is None
    ]
    total_count = len(city_rating_rows) + len(city_expand_rows)
    unrated_count = len(city_expand_rows)
    rated_count = len(city_rating_rows)

    return {
        "city_hash_changed": city_hash_changed,
        "city_rating_rows": city_rating_rows,
        "city_expand_rows": city_expand_rows,
        "city_rating_running": total_count > 0 and unrated_count > 0,
        "city_rating_total_count": total_count,
        "city_rating_unrated_count": unrated_count,
        "city_rating_rated_count": rated_count,
        "city_rating_percent": int((rated_count * 100) / total_count) if total_count else 0,
    }


def handle_branches_step_view(
    request,
    *,
    flow_type: str,
    current_step_key: str,
    item_id: str,
    task,
    saved_values: Mapping[str, Any],
    flow_status: Mapping[str, Any],
):
    return _handle_branches_cities_step_view(
        request,
        flow_type=flow_type,
        current_step_key=current_step_key,
        item_id=item_id,
        task=task,
        saved_values=saved_values,
        flow_status=flow_status,
        view_mode="branches",
    )


def handle_cities_step_view(
    request,
    *,
    flow_type: str,
    current_step_key: str,
    item_id: str,
    task,
    saved_values: Mapping[str, Any],
    flow_status: Mapping[str, Any],
):
    return _handle_branches_cities_step_view(
        request,
        flow_type=flow_type,
        current_step_key=current_step_key,
        item_id=item_id,
        task=task,
        saved_values=saved_values,
        flow_status=flow_status,
        view_mode="cities",
    )


def _handle_branches_cities_step_view(
    request,
    *,
    flow_type: str,
    current_step_key: str,
    item_id: str,
    task,
    saved_values: Mapping[str, Any],
    flow_status: Mapping[str, Any],
    view_mode: str,
):
    flow_conf = get_flow_config(flow_type)
    step_definitions = build_step_definitions(flow_type)
    is_city_partial = request.method == "GET" and str(request.GET.get("cities_partial") or "").strip() == "1"
    branch_rating_rows: list[dict[str, Any]] = []
    branch_expand_rows: list[dict[str, Any]] = []
    branch_hash_changed = False
    city_hash_changed = False
    city_rating_rows: list[dict[str, Any]] = []
    city_expand_rows: list[dict[str, Any]] = []
    city_rating_running = False
    city_rating_total_count = 0
    city_rating_unrated_count = 0
    city_rating_rated_count = 0
    city_rating_percent = 0
    city_probe: dict[str, Any] = {}
    translated_context_cache: tuple[str, str] | None = None
    on_gpt_error = lambda: mark_flow_gpt_unavailable(request)

    def _resolve_product_company_de() -> tuple[str, str]:
        nonlocal translated_context_cache
        if translated_context_cache is not None:
            return translated_context_cache
        if not task:
            translated_context_cache = ("", "")
            return translated_context_cache

        product_de = (translate_text(task.source_product or "", "de", on_gpt_error=on_gpt_error) or "").strip()
        product_de = product_de or (task.source_product or "").strip()
        company_de = (translate_text(task.source_company or "", "de", on_gpt_error=on_gpt_error) or "").strip()
        company_de = company_de or (task.source_company or "").strip()
        translated_context_cache = (product_de, company_de)
        return translated_context_cache

    geo_text = (task.source_geo or "").strip() if task else ""

    branch_form = str(request.GET.get("branch_form") or "").strip()
    city_form = str(request.GET.get("city_form") or "").strip()

    if request.method == "GET":
        redirect_needed = False

        if not branch_form and not is_city_partial:
            branch_form = f"bf_{secrets.token_urlsafe(12)}"
            redirect_needed = True
        if not city_form:
            city_form = f"cf_{secrets.token_urlsafe(12)}"
            if not is_city_partial:
                redirect_needed = True

        branch_key = f"aap:create_flow:branch_form:{branch_form}"
        city_key = f"aap:create_flow:city_form:{city_form}"

        if branch_form and not CLIENT.get(branch_key, ttl_sec=FORM_TTL_SEC):
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
            CLIENT.set(
                city_key,
                json.dumps(
                    {
                        "probe": {},
                        "conversation_id": "",
                        "response_id": "",
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                ttl_sec=FORM_TTL_SEC,
            )
            if request.GET.get("city_form"):
                city_form = f"cf_{secrets.token_urlsafe(12)}"
                city_key = f"aap:create_flow:city_form:{city_form}"
                CLIENT.set(
                    city_key,
                    json.dumps(
                        {
                            "probe": {},
                            "conversation_id": "",
                            "response_id": "",
                        },
                        ensure_ascii=False,
                    ).encode("utf-8"),
                    ttl_sec=FORM_TTL_SEC,
                )
                redirect_needed = True

        if redirect_needed and not is_city_partial:
            params = request.GET.copy()
            params["branch_form"] = branch_form
            params["city_form"] = city_form
            return HttpResponseRedirect(f"{request.path}?{params.urlencode()}")

    branch_key = f"aap:create_flow:branch_form:{branch_form}" if branch_form else ""
    city_key = f"aap:create_flow:city_form:{city_form}" if city_form else ""

    branch_instruction = ""

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

    city_state_payload = CLIENT.get(city_key, ttl_sec=FORM_TTL_SEC) if city_key else None
    try:
        city_state = json.loads((city_state_payload or b"").decode("utf-8")) if city_state_payload else {}
    except Exception:
        city_state = {}
    if not isinstance(city_state, dict):
        city_state = {}
    city_state.setdefault("probe", {})
    city_state.setdefault("conversation_id", "")
    city_state.setdefault("response_id", "")
    city_probe = city_state.get("probe") if isinstance(city_state.get("probe"), dict) else {}
    if not isinstance(city_probe.get("yes_no_options"), list):
        city_probe["yes_no_options"] = []
    if not isinstance(city_probe.get("radio_questions"), list):
        flat_radio_options = city_probe.get("radio_options")
        radio_questions: list[dict[str, Any]] = []
        current_options: list[dict[str, Any]] = []
        if isinstance(flat_radio_options, list):
            for item in flat_radio_options:
                if not isinstance(item, dict):
                    continue
                current_options.append({
                    "label": str(item.get("label") or "").strip(),
                    "checked": bool(item.get("checked")),
                    "sql_condition": str(item.get("sql_condition") or "").strip(),
                })
                if str(item.get("sql_condition") or "").strip() == "":
                    radio_questions.append({"options": current_options})
                    current_options = []
            if current_options:
                radio_questions.append({"options": current_options})
        city_probe["radio_questions"] = radio_questions

    if task and not is_city_partial:
        def _branch_title(branch_id: int, fallback_name: str) -> str:
            try:
                title = " ".join(str(get_category_title(branch_id, request)).split()).strip()
            except Exception:
                title = ""
            return title or fallback_name

        branch_state, branch_ratings_gpt_failed = _ensure_saved_branch_ratings(
            request,
            task=task,
            flow_type=flow_type,
            resolve_translated_context=_resolve_product_company_de,
            branch_state=branch_state,
        )
        if branch_ratings_gpt_failed and request.method != "POST":
            if branch_key:
                CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
            return HttpResponseRedirect(flow_back_url(request, reverse("audience:create_list")))
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

        branch_db_ids = [int(row[0]) for row in rows if row and row[3]]
        branch_rating_rows = _collapse_branch_rows_for_display([
            {
                "id": int(row[0]),
                "rate_display": str(row[1]) if row[1] is not None else "-",
                "branch_name": _branch_title(int(row[0]), str(row[3] or "").strip()),
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
            branch_expand_rows = _collapse_branch_rows_for_display([
                {
                    "id": branch_id,
                    "rate_display": "-",
                    "branch_name": _branch_title(branch_id, expanded_map[branch_id]),
                }
                for branch_id in yellow_ids
                if branch_id in expanded_map
            ])

    if task:
        city_ctx = _build_city_rows_context(request, task, geo_text)
        city_hash_changed = bool(city_ctx["city_hash_changed"])
        city_rating_rows = list(city_ctx["city_rating_rows"])
        city_expand_rows = list(city_ctx["city_expand_rows"])
        city_rating_running = bool(city_ctx["city_rating_running"])
        city_rating_total_count = int(city_ctx["city_rating_total_count"])
        city_rating_unrated_count = int(city_ctx["city_rating_unrated_count"])
        city_rating_rated_count = int(city_ctx["city_rating_rated_count"])
        city_rating_percent = int(city_ctx["city_rating_percent"])

    if request.method == "POST" and task:
        current_records: list[dict[str, Any]] = []
        action = str(request.POST.get("action") or "").strip()
        if action == "cities_apply_selected" and city_key:
            yes_no_options = city_probe.get("yes_no_options") if isinstance(city_probe, dict) else None
            radio_questions = city_probe.get("radio_questions") if isinstance(city_probe, dict) else None
            sql_parts: list[str] = []
            if isinstance(yes_no_options, list):
                for index, item in enumerate(yes_no_options):
                    if not isinstance(item, dict):
                        continue
                    if str(request.POST.get(f"city_probe_yes_{index}") or "").strip() != "1":
                        continue
                    sql_condition = str(item.get("sql_condition") or "").strip()
                    sql_condition = _expand_city_radius_sql(sql_condition)
                    if sql_condition:
                        sql_parts.append(f"({sql_condition})")
            if isinstance(radio_questions, list):
                for question_index, question in enumerate(radio_questions):
                    if not isinstance(question, dict):
                        continue
                    options = question.get("options")
                    if not isinstance(options, list):
                        continue
                    selected_radio = str(request.POST.get(f"city_probe_radio_{question_index}") or "").strip()
                    try:
                        radio_index = int(selected_radio)
                    except Exception:
                        radio_index = None
                    if radio_index is not None and 0 <= radio_index < len(options):
                        item = options[radio_index]
                        if isinstance(item, dict):
                            sql_condition = str(item.get("sql_condition") or "").strip()
                            sql_condition = _expand_city_radius_sql(sql_condition)
                            if sql_condition:
                                sql_parts.append(f"({sql_condition})")

            selected_city_ids: list[int] = []
            try:
                with transaction.atomic(), connection.cursor() as cur:
                    cur.execute("SET LOCAL TRANSACTION READ ONLY")
                    if sql_parts:
                        cur.execute(
                            "SELECT id "
                            "FROM public.cities_sys "
                            f"WHERE {' AND '.join(sql_parts)} "
                            "ORDER BY state_name ASC, name ASC, id ASC"
                        )
                    else:
                        cur.execute(
                            "SELECT id "
                            "FROM public.cities_sys "
                            "ORDER BY state_name ASC, name ASC, id ASC"
                        )
                    selected_city_ids = [int(row[0]) for row in (cur.fetchall() or []) if row]
            except Exception:
                selected_city_ids = []

            if selected_city_ids:
                hash_task = _current_city_hash(task, geo_text)
                with connection.cursor() as cur:
                    cur.execute(
                        "INSERT INTO task_city_ratings (task_id, city_id, rate, hash_task) "
                        "VALUES " + ", ".join(["(%s,%s,%s,%s)"] * len(selected_city_ids)) + " "
                        "ON CONFLICT (task_id, city_id) DO NOTHING",
                        [value for city_id in selected_city_ids for value in (int(task.id), city_id, None, hash_task)],
                    )
            city_state["probe"] = {}
            CLIENT.set(city_key, json.dumps(city_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
            return HttpResponseRedirect(request.get_full_path())

        if action == "cities_apply_all" and city_key:
            with connection.cursor() as cur:
                cur.execute("SELECT id FROM public.cities_sys ORDER BY state_name ASC, name ASC, id ASC")
                all_city_ids = [int(row[0]) for row in (cur.fetchall() or []) if row]
            if all_city_ids:
                hash_task = _current_city_hash(task, geo_text)
                with connection.cursor() as cur:
                    cur.execute(
                        "INSERT INTO task_city_ratings (task_id, city_id, rate, hash_task) "
                        "VALUES " + ", ".join(["(%s,%s,%s,%s)"] * len(all_city_ids)) + " "
                        "ON CONFLICT (task_id, city_id) DO NOTHING",
                        [value for city_id in all_city_ids for value in (int(task.id), city_id, None, hash_task)],
                    )
            city_state["probe"] = {}
            CLIENT.set(city_key, json.dumps(city_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
            return HttpResponseRedirect(request.get_full_path())

        if action == "cities_delete_selected" and city_key:
            delete_ids: list[int] = []
            for value in str(request.POST.get("cities_delete_ids") or "").split(","):
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
                        "DELETE FROM task_city_ratings "
                        "WHERE task_id = %s AND city_id = ANY(%s)",
                        [int(task.id), delete_ids],
                    )
            return HttpResponseRedirect(request.get_full_path())

        if action == "cities_recalc_ratings" and city_key:
            with connection.cursor() as cur:
                cur.execute(
                    "UPDATE task_city_ratings "
                    "SET rate = NULL, hash_task = NULL "
                    "WHERE task_id = %s",
                    [int(task.id)],
                )
            return HttpResponseRedirect(request.get_full_path())

        if action == "cities_refill" and city_key:
            with connection.cursor() as cur:
                cur.execute(
                    "DELETE FROM task_city_ratings "
                    "WHERE task_id = %s",
                    [int(task.id)],
                )
            city_state["probe"] = {}
            city_state["conversation_id"] = ""
            city_state["response_id"] = ""
            CLIENT.set(city_key, json.dumps(city_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
            return HttpResponseRedirect(request.get_full_path())

        if action == "cities_ignore_hash" and city_key:
            with connection.cursor() as cur:
                cur.execute(
                    "UPDATE task_city_ratings "
                    "SET hash_task = %s "
                    "WHERE task_id = %s",
                    [_current_city_hash(task, geo_text), int(task.id)],
                )
            return HttpResponseRedirect(request.get_full_path())

        if action == "cities_pick_refine" and city_key:
            if geo_text:
                city_probe_instructions = "\n\n".join(
                    part for part in (
                        get_prompt("lang_response", on_gpt_error=on_gpt_error).replace(
                            "{LANG}",
                            f"{request.ui_lang_name_en} for all label values only",
                        ).strip(),
                        get_prompt("create_cities_geo_probe", on_gpt_error=on_gpt_error).strip(),
                    )
                    if part
                ).strip()
                resp = GPTClient().ask_dialog(
                    model="standard",
                    instructions=city_probe_instructions,
                    input=json.dumps(
                        {
                            "geo": geo_text,
                            "product": task.source_product or "",
                            "company": task.source_company or "",
                            "available_geo_fields": [
                                "state_name",
                                "name",
                                "pop_total",
                                "lat",
                                "lon",
                            ],
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    conversation=(str(city_state.get("conversation_id") or "").strip() or None),
                    previous_response_id=(str(city_state.get("response_id") or "").strip() or None),
                    user_id=str(request.user.id),
                    service_tier="flex",
                    web_search=True,
                )
                if not is_gpt_ok(resp):
                    clear_dialog_state(city_state)
                    city_state["probe"] = {}
                    CLIENT.set(city_key, json.dumps(city_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
                    mark_flow_gpt_unavailable(request)
                    return HttpResponseRedirect(request.get_full_path())
                raw = resp.raw if isinstance(resp.raw, dict) else {}
                city_state["response_id"] = str(raw.get("id") or "").strip()
                conversation = raw.get("conversation")
                city_state["conversation_id"] = (
                    str(conversation.get("id") or "").strip()
                    if isinstance(conversation, dict)
                    else str(conversation or "").strip()
                )
                data = parse_json_response(resp.content or "")
                if isinstance(data, dict):
                    yes_no_options_raw = data.get("yes_no_options")
                    radio_questions_raw = data.get("radio_questions")
                    yes_no_options = []
                    radio_questions = []
                    if isinstance(yes_no_options_raw, list):
                        for item in yes_no_options_raw:
                            if not isinstance(item, dict):
                                continue
                            label = str(item.get("label") or "").strip()
                            sql_condition = str(item.get("sql_condition") or "").strip()
                            yes_no_options.append({
                                "label": label,
                                "checked": bool(item.get("checked")),
                                "sql_condition": sql_condition,
                            })
                    if isinstance(radio_questions_raw, list):
                        for question in radio_questions_raw:
                            if not isinstance(question, dict):
                                continue
                            options_raw = question.get("options")
                            if not isinstance(options_raw, list):
                                continue
                            options = []
                            for item in options_raw:
                                if not isinstance(item, dict):
                                    continue
                                options.append({
                                    "label": str(item.get("label") or "").strip(),
                                    "checked": bool(item.get("checked")),
                                    "sql_condition": str(item.get("sql_condition") or "").strip(),
                                })
                            radio_questions.append({"options": options})
                    city_state["probe"] = {
                        "yes_no_options": yes_no_options,
                        "radio_questions": radio_questions,
                    }
                    if not yes_no_options and not radio_questions:
                        with connection.cursor() as cur:
                            cur.execute("SELECT id FROM public.cities_sys ORDER BY state_name ASC, name ASC, id ASC")
                            all_city_ids = [int(row[0]) for row in (cur.fetchall() or []) if row]
                        if all_city_ids:
                            hash_task = _current_city_hash(task, geo_text)
                            with connection.cursor() as cur:
                                cur.execute(
                                    "INSERT INTO task_city_ratings (task_id, city_id, rate, hash_task) "
                                    "VALUES " + ", ".join(["(%s,%s,%s,%s)"] * len(all_city_ids)) + " "
                                    "ON CONFLICT (task_id, city_id) DO NOTHING",
                                    [value for city_id in all_city_ids for value in (int(task.id), city_id, None, hash_task)],
                                )
                        city_state["probe"] = {}
                else:
                    city_state["probe"] = {
                        "yes_no_options": [],
                        "radio_questions": [],
                    }
                CLIENT.set(city_key, json.dumps(city_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
            return HttpResponseRedirect(request.get_full_path())

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
                product_de, company_de = _resolve_product_company_de()
                resp = GPTClient().ask_dialog(
                    model=FLOW_GPT_MODEL,
                    instructions=get_prompt(
                        "create_branches_buy_rate" if flow_type == "buy" else "create_branches_sell_rate",
                        on_gpt_error=on_gpt_error,
                    ),
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
                    service_tier=FLOW_GPT_SERVICE_TIER,
                    web_search=True,
                )
                if not is_gpt_ok(resp):
                    clear_dialog_state(branch_state)
                    CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
                    mark_flow_gpt_unavailable(request)
                    return HttpResponseRedirect(request.get_full_path())
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
            CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
            return HttpResponseRedirect(request.get_full_path())

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
                    product_de, company_de = _resolve_product_company_de()
                    resp = GPTClient().ask_dialog(
                        model=FLOW_GPT_MODEL,
                        instructions=get_prompt(
                            "create_branches_buy_rate" if flow_type == "buy" else "create_branches_sell_rate",
                            on_gpt_error=on_gpt_error,
                        ),
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
                        service_tier=FLOW_GPT_SERVICE_TIER,
                        web_search=True,
                    )
                    if not is_gpt_ok(resp):
                        clear_dialog_state(branch_state)
                        CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
                        mark_flow_gpt_unavailable(request)
                        return HttpResponseRedirect(request.get_full_path())
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
            product_de, company_de = _resolve_product_company_de()
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
                model=FLOW_GPT_MODEL,
                instructions=get_prompt(
                    "create_branches_buy" if flow_type == "buy" else "create_branches_sell",
                    on_gpt_error=on_gpt_error,
                ),
                input=initial_input,
                conversation=(str(branch_state.get("conversation_id") or "").strip() or None),
                previous_response_id=(previous_response_id or None),
                user_id=str(request.user.id),
                service_tier=FLOW_GPT_SERVICE_TIER,
                web_search=True,
            )
            if not is_gpt_ok(resp):
                clear_dialog_state(branch_state)
                if branch_key:
                    CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
                mark_flow_gpt_unavailable(request)
                return HttpResponseRedirect(request.get_full_path())
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
                clean_prompt = get_prompt(
                    "create_branches_buy_clean" if flow_type == "buy" else "create_branches_sell_clean",
                    on_gpt_error=on_gpt_error,
                )
                previous_response_id = str(branch_state.get("response_id") or "").strip()
                resp = GPTClient().ask_dialog(
                    model=FLOW_GPT_MODEL,
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
                    service_tier=FLOW_GPT_SERVICE_TIER,
                    web_search=True,
                )
                if not is_gpt_ok(resp):
                    clear_dialog_state(branch_state)
                    if branch_key:
                        CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
                    mark_flow_gpt_unavailable(request)
                    return HttpResponseRedirect(request.get_full_path())
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
                instruction_en = (translate_text(instruction_ru, "en", on_gpt_error=on_gpt_error) or "").strip() or instruction_ru
                product_de, company_de = _resolve_product_company_de()
                previous_response_id = str(branch_state.get("response_id") or "").strip()
                resp = GPTClient().ask_dialog(
                    model=FLOW_GPT_MODEL,
                    instructions=get_prompt(
                        "create_branches_buy_expand" if flow_type == "buy" else "create_branches_sell_expand",
                        on_gpt_error=on_gpt_error,
                    ),
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
                    service_tier=FLOW_GPT_SERVICE_TIER,
                    web_search=True,
                )
                if not is_gpt_ok(resp):
                    clear_dialog_state(branch_state)
                    CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
                    mark_flow_gpt_unavailable(request)
                    return HttpResponseRedirect(request.get_full_path())
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
                        clean_prompt = get_prompt(
                            "create_branches_buy_clean" if flow_type == "buy" else "create_branches_sell_clean",
                            on_gpt_error=on_gpt_error,
                        )
                        previous_response_id = str(branch_state.get("response_id") or "").strip()
                        resp = GPTClient().ask_dialog(
                            model=FLOW_GPT_MODEL,
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
                            service_tier=FLOW_GPT_SERVICE_TIER,
                            web_search=True,
                        )
                        if not is_gpt_ok(resp):
                            clear_dialog_state(branch_state)
                            CLIENT.set(branch_key, json.dumps(branch_state, ensure_ascii=False).encode("utf-8"), ttl_sec=FORM_TTL_SEC)
                            mark_flow_gpt_unavailable(request)
                            return HttpResponseRedirect(request.get_full_path())
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

    params = request.GET.copy()
    if branch_form:
        params["branch_form"] = branch_form
    if city_form:
        params["city_form"] = city_form
    params["cities_partial"] = "1"
    city_partial_url = f"{request.path}?{params.urlencode()}"

    has_branch_rows = bool(branch_rating_rows or branch_expand_rows)
    branches_mode = "work" if has_branch_rows else "empty"
    branches_cities_context = {
        "branches_mode": branches_mode,
        "branch_show_hash_alert": bool(branch_hash_changed and branches_mode == "work"),
        "branch_show_expand_save_actions": bool(branch_expand_rows and branches_mode == "work"),
        "branch_show_expand_controls": bool(branches_mode == "work"),
        "branch_rating_rows": branch_rating_rows,
        "branch_expand_rows": branch_expand_rows,
        "city_items": [],
        "city_rating_rows": city_rating_rows,
        "city_expand_rows": city_expand_rows,
        "city_probe": city_probe,
        "city_hash_changed": city_hash_changed,
        "city_rating_running": city_rating_running,
        "city_rating_total_count": city_rating_total_count,
        "city_rating_unrated_count": city_rating_unrated_count,
        "city_rating_rated_count": city_rating_rated_count,
        "city_rating_percent": city_rating_percent,
        "city_partial_url": city_partial_url,
        "branch_instruction": branch_instruction,
        "city_instruction": "",
        "branch_conversation_id": "",
        "branch_response_id": "",
        "branch_records_json": "[]",
        "branch_rate_modal_base_url": reverse("audience:create_branch_rate_modal") + f"?id={item_id}",
        "branch_hash_changed": branch_hash_changed,
        "city_rate_modal_base_url": reverse("audience:create_city_rate_modal") + f"?id={item_id}",
        "city_conversation_id": "",
        "city_response_id": "",
        "city_records_json": "",
    }

    if is_city_partial:
        return render(
            request,
            "panels/aap_audience/create/step_cities.html",
            {
                "type": flow_type,
                "branches_cities_step": branches_cities_context,
            },
        )

    step_template = (
        "panels/aap_audience/create/step_cities.html"
        if str(view_mode or "").strip() == "cities"
        else "panels/aap_audience/create/step_branches.html"
    )
    return render(
        request,
        flow_conf["template_name"],
        build_flow_render_context(
            request=request,
            flow_type=flow_type,
            item_id=item_id,
            task=task,
            saved_values=saved_values,
            step_definitions=step_definitions,
            flow_status=flow_status,
            current_step_key=current_step_key,
            step_template=step_template,
            extra_context={
                "branches_cities_step": branches_cities_context,
            },
        ),
    )
