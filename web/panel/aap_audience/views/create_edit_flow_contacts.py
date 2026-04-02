# FILE: web/panel/aap_audience/views/create_edit_flow_contacts.py
# DATE: 2026-04-01
# PURPOSE: Contacts step handlers and partials for the create/edit flow.

from __future__ import annotations

from typing import Any, Mapping

from django.db import connection
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import reverse

from engine.common.utils import parse_json_object
from mailer_web.access import encode_id
from mailer_web.format_contact import get_category_title, get_city_title

from .create_edit_flow_shared import (
    build_flow_render_context,
    build_step_definitions,
    get_flow_config,
    resolve_task,
)

CONTACTS_SECTION_COLLECT = "collect"
CONTACTS_SECTION_ALL = "all"
CONTACTS_SECTION_BRANCH_CITY = "branch_city"
CONTACTS_SECTION_PAIRS = "pairs"

CONTACTS_SECTION_KEYS = {
    CONTACTS_SECTION_COLLECT,
    CONTACTS_SECTION_ALL,
    CONTACTS_SECTION_BRANCH_CITY,
    CONTACTS_SECTION_PAIRS,
}
CONTACTS_ALL_PAGE_SIZE = 50

def _fetch_contacts_total(task_id: int) -> int:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.sending_lists
            WHERE task_id = %s
            """,
            [int(task_id)],
        )
        row = cur.fetchone()
    return int((row or [0])[0] or 0)


def _fetch_contacts_collect_rows(request, task_id: int) -> list[dict[str, Any]]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                sl.aggr_contact_cb_id::bigint AS aggr_contact_id,
                ac.company_name AS company_name,
                ac.company_data AS company_data,
                cp.branch_id,
                cp.plz_id,
                sl.rate_cb AS pair_rate,
                sl.created_at
            FROM public.sending_lists sl
            JOIN public.aggr_contacts_cb ac
              ON ac.id = sl.aggr_contact_cb_id
            LEFT JOIN public.cb_crawl_pairs cp
              ON cp.id = sl.cb_id
            WHERE sl.task_id = %s
              AND COALESCE(sl.removed, false) = false
            ORDER BY
                sl.created_at DESC NULLS LAST,
                sl.aggr_contact_cb_id DESC
            LIMIT 50
            """,
            [int(task_id)],
        )
        rows = cur.fetchall() or []
    return [_build_contact_row(request, row) for row in rows]


def _build_contact_row(request, row: tuple[Any, ...]) -> dict[str, Any]:
    company_data = parse_json_object(row[2], field_name="aggr_contacts_cb.company_data")
    norm_data = company_data.get("norm") if isinstance(company_data.get("norm"), dict) else {}
    aggr_contact_id = int(row[0])
    return {
        "aggr_contact_id": aggr_contact_id,
        "aggr_contact_ui_id": encode_id(aggr_contact_id),
        "company_name": str(row[1] or "").strip(),
        "address": str(norm_data.get("address") or "").strip(),
        "company_data": company_data,
        "branch_name": get_category_title(row[3], request),
        "city_title": get_city_title(row[4], request, land=True, plz=False),
        "pair_rate": row[5],
        "created_at": row[6],
        "contact_modal_url": reverse("contact_modal") + f"?id={encode_id(aggr_contact_id)}",
    }


def _fetch_contacts_all_total(task_id: int, query: str) -> int:
    search_query = str(query or "").strip()
    search_like = f"%{search_query}%"
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.sending_lists sl
            JOIN public.aggr_contacts_cb ac
              ON ac.id = sl.aggr_contact_cb_id
            WHERE sl.task_id = %s
              AND COALESCE(sl.removed, false) = false
              AND (
                    %s = ''
                    OR COALESCE(ac.company_name, '') ILIKE %s
                    OR COALESCE(ac.email, '') ILIKE %s
              )
            """,
            [int(task_id), search_query, search_like, search_like],
        )
        row = cur.fetchone()
    return int((row or [0])[0] or 0)


def _build_contacts_all_page_items(*, page: int, total_pages: int) -> list[dict[str, Any]]:
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


def _get_page_value(raw_value: str) -> int:
    value = str(raw_value or "").strip()
    if value.isdigit():
        page = int(value)
        if page > 0:
            return page
    return 1


def _fetch_contacts_all_rows(request, task_id: int, page: int, query: str) -> dict[str, Any]:
    search_query = str(query or "").strip()
    search_like = f"%{search_query}%"
    total = _fetch_contacts_all_total(int(task_id), search_query)
    total_pages = max(1, (total + CONTACTS_ALL_PAGE_SIZE - 1) // CONTACTS_ALL_PAGE_SIZE)
    current_page = min(max(1, int(page)), total_pages)
    offset = (current_page - 1) * CONTACTS_ALL_PAGE_SIZE

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                sl.aggr_contact_cb_id::bigint AS aggr_contact_id,
                ac.company_name AS company_name,
                ac.company_data AS company_data,
                cp.branch_id,
                cp.plz_id,
                sl.rate_cb AS pair_rate,
                sl.created_at
            FROM public.sending_lists sl
            JOIN public.aggr_contacts_cb ac
              ON ac.id = sl.aggr_contact_cb_id
            LEFT JOIN public.cb_crawl_pairs cp
              ON cp.id = sl.cb_id
            WHERE sl.task_id = %s
              AND COALESCE(sl.removed, false) = false
              AND (
                    %s = ''
                    OR COALESCE(ac.company_name, '') ILIKE %s
                    OR COALESCE(ac.email, '') ILIKE %s
              )
            ORDER BY
                sl.rate_cb DESC NULLS LAST,
                sl.created_at DESC NULLS LAST,
                sl.aggr_contact_cb_id DESC
            LIMIT %s
            OFFSET %s
            """,
            [int(task_id), search_query, search_like, search_like, CONTACTS_ALL_PAGE_SIZE, offset],
        )
        rows = cur.fetchall() or []

    return {
        "contacts_all_rows": [_build_contact_row(request, row) for row in rows],
        "contacts_all_page": current_page,
        "contacts_all_pages": total_pages,
        "contacts_all_total": total,
        "contacts_all_has_prev": current_page > 1,
        "contacts_all_prev_page": current_page - 1,
        "contacts_all_has_next": current_page < total_pages,
        "contacts_all_next_page": current_page + 1,
        "contacts_all_page_items": _build_contacts_all_page_items(page=current_page, total_pages=total_pages),
        "contacts_all_query": search_query,
    }


def _fetch_contacts_branch_city_rows(request, task_id: int) -> dict[str, Any]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                tbr.branch_id::bigint,
                tbr.rate,
                COALESCE(bs.branch_name, '') AS branch_name,
                COALESCE(cnt.contacts_count, 0)::int
            FROM public.task_branch_ratings tbr
            LEFT JOIN public.branches_sys bs
              ON bs.id = tbr.branch_id
            LEFT JOIN (
                SELECT
                    cp.branch_id::bigint AS branch_id,
                    COUNT(*)::int AS contacts_count
                FROM public.sending_lists sl
                JOIN public.cb_crawl_pairs cp
                  ON cp.id = sl.cb_id
                WHERE sl.task_id = %s
                  AND COALESCE(sl.removed, false) = false
                  AND cp.branch_id IS NOT NULL
                GROUP BY cp.branch_id
            ) cnt
              ON cnt.branch_id = tbr.branch_id
            WHERE tbr.task_id = %s
            ORDER BY tbr.rate ASC NULLS LAST, tbr.branch_id ASC
            """,
            [int(task_id), int(task_id)],
        )
        branch_rows = cur.fetchall() or []

        cur.execute(
            """
            SELECT
                tcr.city_id::bigint,
                tcr.rate,
                COALESCE(cs.name, '') AS city_name,
                COALESCE(cs.state_name, '') AS state_name,
                COALESCE(cnt.contacts_count, 0)::int
            FROM public.task_city_ratings tcr
            LEFT JOIN public.cities_sys cs
              ON cs.id = tcr.city_id
            LEFT JOIN (
                SELECT
                    city_map.city_id::bigint AS city_id,
                    COUNT(*)::int AS contacts_count
                FROM public.sending_lists sl
                JOIN public.cb_crawl_pairs cp
                  ON cp.id = sl.cb_id
                LEFT JOIN public.plz_sys ps
                  ON ps.id = cp.plz_id
                LEFT JOIN (
                    SELECT plz, MIN(city_id) AS city_id
                    FROM public.__city__plz_map
                    GROUP BY plz
                ) city_map
                  ON city_map.plz = ps.plz
                WHERE sl.task_id = %s
                  AND COALESCE(sl.removed, false) = false
                  AND city_map.city_id IS NOT NULL
                GROUP BY city_map.city_id
            ) cnt
              ON cnt.city_id = tcr.city_id
            WHERE tcr.task_id = %s
            ORDER BY tcr.rate ASC NULLS LAST, tcr.city_id ASC
            """,
            [int(task_id), int(task_id)],
        )
        city_rows = cur.fetchall() or []

    return {
        "contacts_branch_rows": [
            {
                "branch_id": int(row[0]),
                "rate_display": str(row[1]) if row[1] is not None else "-",
                "branch_name": str(row[2] or "").strip(),
                "contacts_count": int(row[3] or 0),
                "contacts_count_display": _format_contacts_total(int(row[3] or 0)),
            }
            for row in branch_rows
        ],
        "contacts_city_rows": [
            {
                "city_id": int(row[0]),
                "rate_display": str(row[1]) if row[1] is not None else "-",
                "city_name": str(row[2] or "").strip(),
                "state_name": str(row[3] or "").strip(),
                "contacts_count": int(row[4] or 0),
                "contacts_count_display": _format_contacts_total(int(row[4] or 0)),
            }
            for row in city_rows
        ],
    }


def _format_contacts_total(value: int) -> str:
    return f"{int(value):,}".replace(",", " ")


def _normalize_contacts_section(value: str) -> str:
    section = str(value or "").strip().lower()
    if section in CONTACTS_SECTION_KEYS:
        return section
    return CONTACTS_SECTION_COLLECT


def _build_contacts_collect_partial_url(flow_type: str, item_id: str) -> str:
    if not item_id:
        return ""
    return (
        reverse("audience:create_contacts_collect_partial")
        + f"?flow_type={flow_type}&id={item_id}"
    )


def _build_contacts_all_partial_url(flow_type: str, item_id: str) -> str:
    if not item_id:
        return ""
    return reverse("audience:create_contacts_all_partial") + f"?flow_type={flow_type}&id={item_id}"


def _build_contacts_branch_city_partial_url(flow_type: str, item_id: str) -> str:
    if not item_id:
        return ""
    return reverse("audience:create_contacts_branch_city_partial") + f"?flow_type={flow_type}&id={item_id}"


def _build_contacts_pairs_partial_url(flow_type: str, item_id: str) -> str:
    if not item_id:
        return ""
    return reverse("audience:create_contacts_pairs_partial") + f"?flow_type={flow_type}&id={item_id}"


def _is_contacts_active(task) -> bool:
    return bool(task and task.ready and not task.collected and not task.archived)


def _is_contacts_completed(task) -> bool:
    return bool(task and task.collected)


def _build_contacts_step_context(task) -> dict[str, Any]:
    is_active = _is_contacts_active(task)
    is_completed = _is_contacts_completed(task)
    contacts_total = _fetch_contacts_total(int(task.id)) if task else 0
    return {
        "is_active": is_active,
        "is_completed": is_completed,
        "contacts_total": contacts_total,
        "contacts_total_display": _format_contacts_total(contacts_total),
    }


def _build_contacts_section_context(*, request, task, section: str, page: int = 1, query: str = "") -> dict[str, Any]:
    section_key = _normalize_contacts_section(section)
    if section_key == CONTACTS_SECTION_COLLECT:
        return {
            "contacts_collect_rows": _fetch_contacts_collect_rows(request, int(task.id)) if task else [],
        }
    if section_key == CONTACTS_SECTION_ALL:
        if not task:
            return {
                "contacts_all_rows": [],
                "contacts_all_page": 1,
                "contacts_all_pages": 1,
                "contacts_all_total": 0,
                "contacts_all_has_prev": False,
                "contacts_all_prev_page": 1,
                "contacts_all_has_next": False,
                "contacts_all_next_page": 1,
                "contacts_all_page_items": [],
                "contacts_all_query": str(query or "").strip(),
            }
        return _fetch_contacts_all_rows(request, int(task.id), int(page), str(query or "").strip())
    if section_key == CONTACTS_SECTION_BRANCH_CITY:
        if not task:
            return {
                "contacts_branch_rows": [],
                "contacts_city_rows": [],
            }
        return _fetch_contacts_branch_city_rows(request, int(task.id))
    return {}


def _build_contacts_sections_context(*, request, task, flow_type: str, item_id: str) -> dict[str, Any]:
    query = str(request.GET.get("q") or "").strip()
    return {
        "contacts_active_section": CONTACTS_SECTION_COLLECT,
        "contacts_collect_partial_url": _build_contacts_collect_partial_url(flow_type, item_id),
        "contacts_all_partial_url": _build_contacts_all_partial_url(flow_type, item_id),
        "contacts_branch_city_partial_url": _build_contacts_branch_city_partial_url(flow_type, item_id),
        "contacts_pairs_partial_url": _build_contacts_pairs_partial_url(flow_type, item_id),
        "contacts_collect_running": _is_contacts_active(task),
        "contacts_all_running": False,
        "contacts_branch_city_running": False,
        "contacts_pairs_running": False,
        **_build_contacts_section_context(request=request, task=task, section=CONTACTS_SECTION_COLLECT),
        **_build_contacts_section_context(
            request=request,
            task=task,
            section=CONTACTS_SECTION_ALL,
            page=1,
            query=query,
        ),
        **_build_contacts_section_context(
            request=request,
            task=task,
            section=CONTACTS_SECTION_BRANCH_CITY,
        ),
    }


def _resolve_contacts_partial_task(request):
    flow_type = str(request.GET.get("flow_type") or "").strip().lower()
    item_id = str(request.GET.get("id") or "").strip()
    if flow_type not in {"buy", "sell"}:
        return flow_type, item_id, None, 400
    task = resolve_task(request, flow_type, item_id)
    if not task:
        return flow_type, item_id, None, 404
    return flow_type, item_id, task, 200


def _render_contacts_partial(request, *, section: str, template_name: str, page: int = 1, query: str = ""):
    _, _, task, status_code = _resolve_contacts_partial_task(request)
    return render(
        request,
        template_name,
        _build_contacts_section_context(
            request=request,
            task=task,
            section=section,
            page=page,
            query=query,
        ),
        status=status_code,
    )


def contacts_total_view(request):
    flow_type = str(request.GET.get("flow_type") or "").strip().lower()
    item_id = str(request.GET.get("id") or "").strip()
    if flow_type not in {"buy", "sell"}:
        return JsonResponse({"ok": False, "error": "invalid_flow_type"}, status=400)

    task = resolve_task(request, flow_type, item_id)
    if not task:
        return JsonResponse({"ok": False, "error": "task_not_found"}, status=404)

    return JsonResponse(
        {
            "ok": True,
            "is_active": _is_contacts_active(task),
            "contacts_total": _fetch_contacts_total(int(task.id)),
        }
    )


def contacts_collect_partial_view(request):
    return _render_contacts_partial(
        request,
        section=CONTACTS_SECTION_COLLECT,
        template_name="panels/aap_audience/create/_contacts_collect_inner.html",
    )


def contacts_all_partial_view(request):
    page = _get_page_value(str(request.GET.get("page") or "1"))
    query = str(request.GET.get("q") or "").strip()
    return _render_contacts_partial(
        request,
        section=CONTACTS_SECTION_ALL,
        template_name="panels/aap_audience/create/_contacts_all_inner.html",
        page=page,
        query=query,
    )


def contacts_branch_city_partial_view(request):
    return _render_contacts_partial(
        request,
        section=CONTACTS_SECTION_BRANCH_CITY,
        template_name="panels/aap_audience/create/_contacts_branch_city_inner.html",
    )


def contacts_pairs_partial_view(request):
    return _render_contacts_partial(
        request,
        section=CONTACTS_SECTION_PAIRS,
        template_name="panels/aap_audience/create/_contacts_pairs_inner.html",
    )


def handle_contacts_step_view(
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
            step_template="panels/aap_audience/create/step_contacts.html",
            extra_context={
                "contacts_step": _build_contacts_step_context(task),
                **_build_contacts_sections_context(
                    request=request,
                    task=task,
                    flow_type=flow_type,
                    item_id=item_id,
                ),
            },
        ),
    )
