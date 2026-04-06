# FILE: web/panel/aap_audience/views/create_edit_flow_mailing_list.py
# DATE: 2026-04-06
# PURPOSE: Mailing-list step handler with status polling and in/out/all filtered contact list.

from __future__ import annotations

from typing import Any, Mapping

from django.db import connection
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import reverse

from engine.common.utils import parse_json_object
from mailer_web.access import encode_id
from mailer_web.format_contact import (
    get_category_title,
    get_city_title,
)

from .create_edit_flow_shared import (
    build_flow_render_context,
    build_step_definitions,
    get_flow_config,
    resolve_task,
)


MAILING_SECTION_IN = "in"
MAILING_SECTION_OUT = "out"
MAILING_SECTION_RATED = "rated"
MAILING_SECTION_ALL = "all"
MAILING_SECTION_KEYS = {
    MAILING_SECTION_IN,
    MAILING_SECTION_OUT,
    MAILING_SECTION_RATED,
    MAILING_SECTION_ALL,
}
MAILING_PAGE_SIZE = 50


def _format_total(value: int) -> str:
    return f"{int(value):,}".replace(",", " ")


def _normalize_mailing_section(value: str) -> str:
    section = str(value or "").strip().lower()
    if section in MAILING_SECTION_KEYS:
        return section
    return MAILING_SECTION_IN


def _get_page_value(raw_value: str) -> int:
    value = str(raw_value or "").strip()
    if value.isdigit():
        page = int(value)
        if page > 0:
            return page
    return 1


def _build_mailing_page_items(*, page: int, total_pages: int) -> list[dict[str, Any]]:
    if total_pages <= 1:
        return []
    out: list[dict[str, Any]] = []
    for number in range(1, total_pages + 1):
        is_edge = (number == 1) or (number == total_pages)
        is_near = abs(number - page) <= 3
        if is_edge or is_near:
            out.append(
                {
                    "kind": "page",
                    "number": number,
                    "is_current": number == page,
                }
            )
            continue
        if not out or out[-1].get("kind") != "gap":
            out.append({"kind": "gap"})
    return out


def _build_mailing_base_url(flow_type: str, item_id: str) -> str:
    route_name = f"audience:create_edit_{flow_type}_mailing_list"
    if item_id:
        return reverse(f"{route_name}_id", args=[item_id])
    return reverse(route_name)


def _build_mailing_section_url(flow_type: str, item_id: str, section: str) -> str:
    base = _build_mailing_base_url(flow_type, item_id)
    return f"{base}?mailing_section={_normalize_mailing_section(section)}"


def _build_mailing_section_urls(flow_type: str, item_id: str) -> dict[str, str]:
    return {
        MAILING_SECTION_IN: _build_mailing_section_url(flow_type, item_id, MAILING_SECTION_IN),
        MAILING_SECTION_OUT: _build_mailing_section_url(flow_type, item_id, MAILING_SECTION_OUT),
        MAILING_SECTION_RATED: _build_mailing_section_url(flow_type, item_id, MAILING_SECTION_RATED),
        MAILING_SECTION_ALL: _build_mailing_section_url(flow_type, item_id, MAILING_SECTION_ALL),
    }


def _mailing_filter_sql(section: str) -> str:
    section_key = _normalize_mailing_section(section)
    if section_key == MAILING_SECTION_IN:
        return "AND sl.rate IS NOT NULL AND sl.rate < %s"
    if section_key == MAILING_SECTION_OUT:
        return "AND sl.rate IS NOT NULL AND sl.rate > %s"
    if section_key == MAILING_SECTION_RATED:
        return "AND sl.rate IS NOT NULL"
    if section_key == MAILING_SECTION_ALL:
        return "AND sl.rate IS NULL"
    return ""


def _build_mailing_row(request, row: tuple[Any, ...]) -> dict[str, Any]:
    company_data = parse_json_object(row[2], field_name="aggr_contacts_cb.company_data")
    norm_data = company_data.get("norm") if isinstance(company_data.get("norm"), dict) else {}
    aggr_contact_id = int(row[0])
    contact_rate = row[6]
    return {
        "aggr_contact_id": aggr_contact_id,
        "aggr_contact_ui_id": encode_id(aggr_contact_id),
        "company_name": str(row[1] or "").strip(),
        "address": str(norm_data.get("address") or "").strip(),
        "company_data": company_data,
        "branch_name": get_category_title(row[3], request),
        "city_title": get_city_title(row[4], request, land=True, plz=False),
        "pair_rate": row[5],
        "contact_rate": contact_rate,
        "contact_rate_display": str(contact_rate) if contact_rate is not None else "-",
        "contact_modal_url": reverse("contact_modal") + f"?id={encode_id(aggr_contact_id)}",
    }


def _fetch_mailing_status(task) -> dict[str, Any]:
    if not task:
        return {
            "is_running": False,
            "is_paused": False,
            "total_count": 0,
            "rated_count": 0,
            "unrated_count": 0,
            "percent": 0,
            "max_rating": None,
            "max_rating_display": "-",
            "total_count_display": _format_total(0),
            "rated_count_display": _format_total(0),
            "rate_limit": 0,
            "rate_limit_display": "0",
        }

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)::int AS total_count,
                COUNT(*) FILTER (WHERE sl.rate IS NOT NULL)::int AS rated_count,
                COUNT(*) FILTER (WHERE sl.rate IS NULL)::int AS unrated_count,
                MAX(sl.rate)::int AS max_rating
            FROM public.sending_lists sl
            WHERE sl.task_id = %s
              AND COALESCE(sl.removed, false) = false
            """,
            [int(task.id)],
        )
        row = cur.fetchone() or [0, 0, 0, None]

    total_count = int(row[0] or 0)
    rated_count = int(row[1] or 0)
    unrated_count = int(row[2] or 0)
    max_rating = int(row[3]) if row[3] is not None else None
    has_unrated = total_count > 0 and unrated_count > 0
    is_task_active = bool(task.active and not task.archived)
    is_running = bool(has_unrated and is_task_active)
    is_paused = bool(has_unrated and not is_task_active)
    percent = int((rated_count * 100) / total_count) if total_count else 0
    rate_limit = int(task.rate_limit or 0)

    return {
        "is_running": is_running,
        "is_paused": is_paused,
        "total_count": total_count,
        "rated_count": rated_count,
        "unrated_count": unrated_count,
        "percent": percent,
        "max_rating": max_rating,
        "max_rating_display": str(max_rating) if max_rating is not None else "-",
        "total_count_display": _format_total(total_count),
        "rated_count_display": _format_total(rated_count),
        "rate_limit": rate_limit,
        "rate_limit_display": str(rate_limit),
    }


def _fetch_mailing_total(task_id: int, *, section: str, query: str, rate_limit: int) -> int:
    section_key = _normalize_mailing_section(section)
    section_sql = _mailing_filter_sql(section_key)
    section_params: list[Any] = []
    if section_key in {MAILING_SECTION_IN, MAILING_SECTION_OUT}:
        section_params.append(int(rate_limit))

    search_query = str(query or "").strip()
    search_like = f"%{search_query}%"

    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*)::int
            FROM public.sending_lists sl
            JOIN public.aggr_contacts_cb ac
              ON ac.id = sl.aggr_contact_cb_id
            WHERE sl.task_id = %s
              AND COALESCE(sl.removed, false) = false
              {section_sql}
              AND (
                    %s = ''
                    OR COALESCE(ac.company_name, '') ILIKE %s
                    OR COALESCE(ac.email, '') ILIKE %s
              )
            """,
            [int(task_id), *section_params, search_query, search_like, search_like],
        )
        row = cur.fetchone()
    return int((row or [0])[0] or 0)


def _fetch_mailing_rows(
    request,
    *,
    task,
    section: str,
    page: int,
    query: str,
) -> dict[str, Any]:
    if not task:
        return {
            "mailing_rows": [],
            "mailing_page": 1,
            "mailing_pages": 1,
            "mailing_total": 0,
            "mailing_total_display": _format_total(0),
            "mailing_has_prev": False,
            "mailing_prev_page": 1,
            "mailing_has_next": False,
            "mailing_next_page": 1,
            "mailing_page_items": [],
            "mailing_query": str(query or "").strip(),
            "mailing_section": _normalize_mailing_section(section),
        }

    section_key = _normalize_mailing_section(section)
    search_query = str(query or "").strip()
    total = _fetch_mailing_total(
        int(task.id),
        section=section_key,
        query=search_query,
        rate_limit=int(task.rate_limit or 0),
    )
    total_pages = max(1, (total + MAILING_PAGE_SIZE - 1) // MAILING_PAGE_SIZE)
    current_page = min(max(1, int(page)), total_pages)
    offset = (current_page - 1) * MAILING_PAGE_SIZE

    section_sql = _mailing_filter_sql(section_key)
    section_params: list[Any] = []
    if section_key in {MAILING_SECTION_IN, MAILING_SECTION_OUT}:
        section_params.append(int(task.rate_limit or 0))

    search_like = f"%{search_query}%"
    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                sl.aggr_contact_cb_id::bigint AS aggr_contact_id,
                ac.company_name AS company_name,
                ac.company_data AS company_data,
                cp.branch_id,
                cp.plz_id,
                sl.rate_cb AS pair_rate,
                sl.rate AS contact_rate
            FROM public.sending_lists sl
            JOIN public.aggr_contacts_cb ac
              ON ac.id = sl.aggr_contact_cb_id
            LEFT JOIN public.cb_crawl_pairs cp
              ON cp.id = sl.cb_id
            WHERE sl.task_id = %s
              AND COALESCE(sl.removed, false) = false
              {section_sql}
              AND (
                    %s = ''
                    OR COALESCE(ac.company_name, '') ILIKE %s
                    OR COALESCE(ac.email, '') ILIKE %s
              )
            ORDER BY
                sl.rate ASC NULLS LAST,
                sl.rate_cb DESC NULLS LAST,
                sl.aggr_contact_cb_id DESC
            LIMIT %s
            OFFSET %s
            """,
            [
                int(task.id),
                *section_params,
                search_query,
                search_like,
                search_like,
                MAILING_PAGE_SIZE,
                offset,
            ],
        )
        rows = cur.fetchall() or []

    return {
        "mailing_rows": [_build_mailing_row(request, row) for row in rows],
        "mailing_page": current_page,
        "mailing_pages": total_pages,
        "mailing_total": total,
        "mailing_total_display": _format_total(total),
        "mailing_has_prev": current_page > 1,
        "mailing_prev_page": current_page - 1,
        "mailing_has_next": current_page < total_pages,
        "mailing_next_page": current_page + 1,
        "mailing_page_items": _build_mailing_page_items(page=current_page, total_pages=total_pages),
        "mailing_query": search_query,
        "mailing_section": section_key,
    }


def mailing_status_view(request):
    flow_type = str(request.GET.get("flow_type") or "").strip().lower()
    item_id = str(request.GET.get("id") or "").strip()
    if flow_type not in {"buy", "sell"}:
        return JsonResponse({"ok": False, "error": "invalid_flow_type"}, status=400)

    task = resolve_task(request, flow_type, item_id)
    if not task:
        return JsonResponse({"ok": False, "error": "task_not_found"}, status=404)

    status = _fetch_mailing_status(task)
    return JsonResponse({"ok": True, **status})


def handle_mailing_list_step_view(
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

    active_section = _normalize_mailing_section(str(request.GET.get("mailing_section") or ""))
    query = str(request.GET.get("q") or "").strip()
    page = _get_page_value(str(request.GET.get("page") or "1"))

    mailing_rows_ctx = _fetch_mailing_rows(
        request,
        task=task,
        section=active_section,
        page=page,
        query=query,
    )
    mailing_status = _fetch_mailing_status(task)

    base_url = _build_mailing_base_url(flow_type, item_id)
    section_urls = _build_mailing_section_urls(flow_type, item_id)
    status_url = (
        reverse("audience:create_mailing_status")
        + f"?flow_type={flow_type}&id={item_id}"
        if item_id
        else ""
    )
    rate_limit_modal_url = (
        reverse("audience:create_rate_limit_modal") + f"?id={item_id}"
        if item_id
        else ""
    )
    pause_info_modal_url = (
        reverse("audience:create_pause_info_modal") + f"?id={item_id}"
        if item_id
        else ""
    )

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
            step_template="panels/aap_audience/create/step_mailing_list.html",
            extra_context={
                "mailing_step": {
                    **mailing_status,
                    **mailing_rows_ctx,
                    "base_url": base_url,
                    "section_urls": section_urls,
                    "active_section": active_section,
                    "status_url": status_url,
                    "rate_limit_modal_url": rate_limit_modal_url,
                    "pause_info_modal_url": pause_info_modal_url,
                },
            },
        ),
    )
