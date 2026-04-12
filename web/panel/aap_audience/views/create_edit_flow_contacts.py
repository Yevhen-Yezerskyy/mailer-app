# FILE: web/panel/aap_audience/views/create_edit_flow_contacts.py
# DATE: 2026-04-03
# PURPOSE: Contacts step handlers and partials with server-routed sub-sections.

from __future__ import annotations

from datetime import timezone
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
    get_city_title_by_city_id,
)

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
RATE_NULL_ORD = 1_000_000_000


def _is_super_workspace_user(request) -> bool:
    user = getattr(request, "user", None)
    ws = getattr(user, "workspace", None)
    return bool(ws and str(getattr(ws, "access_type", "") or "").strip() == "super")


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
                sl.rate_cb ASC NULLS LAST,
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
        "contacts_all_show_paging": total > 0,
        "contacts_all_total": total,
        "contacts_all_total_display": _format_contacts_total(total),
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
                cp.branch_id::bigint AS branch_id,
                SUM(cb_counts.contacts_count)::bigint AS contacts_count
            FROM (
                SELECT
                    sl.cb_id::bigint AS cb_id,
                    COUNT(*)::bigint AS contacts_count
                FROM public.sending_lists sl
                WHERE sl.task_id = %s
                GROUP BY sl.cb_id
                HAVING COUNT(*) > 0
            ) cb_counts
            JOIN public.task_cb_ratings tcb
              ON tcb.task_id = %s
             AND tcb.cb_id = cb_counts.cb_id
            JOIN public.cb_crawl_pairs cp
              ON cp.id = tcb.cb_id
            GROUP BY cp.branch_id
            HAVING SUM(cb_counts.contacts_count) > 0
            """,
            [int(task_id), int(task_id)],
        )
        branch_count_rows = cur.fetchall() or []

        cur.execute(
            """
            SELECT
                tbr.branch_id::bigint,
                tbr.rate
            FROM public.task_branch_ratings tbr
            WHERE tbr.task_id = %s
            ORDER BY tbr.rate ASC NULLS LAST, tbr.branch_id ASC
            """,
            [int(task_id)],
        )
        branch_rating_rows = cur.fetchall() or []

        cur.execute(
            """
            SELECT
                city_map.city_id::bigint AS city_id,
                SUM(cb_counts.contacts_count)::bigint AS contacts_count
            FROM (
                SELECT
                    sl.cb_id::bigint AS cb_id,
                    COUNT(*)::bigint AS contacts_count
                FROM public.sending_lists sl
                WHERE sl.task_id = %s
                GROUP BY sl.cb_id
                HAVING COUNT(*) > 0
            ) cb_counts
            JOIN public.task_cb_ratings tcb
              ON tcb.task_id = %s
             AND tcb.cb_id = cb_counts.cb_id
            JOIN public.cb_crawl_pairs cp
              ON cp.id = tcb.cb_id
            LEFT JOIN public.plz_sys ps
              ON ps.id = cp.plz_id
            JOIN (
                SELECT plz, MIN(city_id) AS city_id
                FROM public.__city__plz_map
                GROUP BY plz
            ) city_map
              ON city_map.plz = ps.plz
            GROUP BY city_map.city_id
            HAVING SUM(cb_counts.contacts_count) > 0
            """,
            [int(task_id), int(task_id)],
        )
        city_count_rows = cur.fetchall() or []

        cur.execute(
            """
            SELECT
                tcr.city_id::bigint,
                tcr.rate
            FROM public.task_city_ratings tcr
            WHERE tcr.task_id = %s
            ORDER BY tcr.rate ASC NULLS LAST, tcr.city_id ASC
            """,
            [int(task_id)],
        )
        city_rating_rows = cur.fetchall() or []

    branch_counts_by_id: dict[int, int] = {
        int(row[0]): int(row[1] or 0) for row in branch_count_rows if row and row[0] is not None
    }
    city_counts_by_id: dict[int, int] = {
        int(row[0]): int(row[1] or 0) for row in city_count_rows if row and row[0] is not None
    }

    def _branch_title(branch_id: int) -> str:
        try:
            return " ".join(str(get_category_title(int(branch_id), request)).split()).strip()
        except Exception:
            return str(int(branch_id))

    def _city_title(city_id: int) -> str:
        try:
            return " ".join(str(get_city_title_by_city_id(int(city_id), request, land=True)).split()).strip()
        except Exception:
            return str(int(city_id))

    branch_rows_collapsed: list[dict[str, Any]] = []
    branch_rows_index: dict[str, dict[str, Any]] = {}
    for row in branch_rating_rows:
        branch_id = int(row[0])
        rate_display = str(row[1]) if row[1] is not None else "-"
        branch_title = _branch_title(branch_id)
        key = branch_title.casefold()
        contacts_count = int(branch_counts_by_id.get(branch_id, 0))
        if key not in branch_rows_index:
            item = {
                "branch_id": branch_id,
                "rate_display": rate_display,
                "branch_title": branch_title,
                "contacts_count": contacts_count,
            }
            branch_rows_index[key] = item
            branch_rows_collapsed.append(item)
            continue
        branch_rows_index[key]["contacts_count"] = int(branch_rows_index[key]["contacts_count"]) + contacts_count

    return {
        "contacts_branch_rows": [
            {**item, "contacts_count_display": _format_contacts_total(int(item["contacts_count"]))}
            for item in branch_rows_collapsed
        ],
        "contacts_city_rows": [
            {
                "city_id": int(row[0]),
                "rate_display": str(row[1]) if row[1] is not None else "-",
                "city_title": _city_title(int(row[0])),
                "contacts_count": int(city_counts_by_id.get(int(row[0]), 0)),
                "contacts_count_display": _format_contacts_total(int(city_counts_by_id.get(int(row[0]), 0))),
            }
            for row in city_rating_rows
        ],
    }


def _fetch_contacts_pairs_rows(request, task_id: int) -> dict[str, Any]:
    show_catalog_column = _is_super_workspace_user(request)
    with connection.cursor() as cur:
        cur.execute(
            """
            WITH first_hole AS MATERIALIZED (
                SELECT
                    COALESCE(tcb.rate::bigint, %s::bigint) AS hole_rate_ord,
                    tcb.id AS hole_id
                FROM public.task_cb_ratings tcb
                JOIN public.cb_crawl_pairs cp
                  ON cp.id = tcb.cb_id
                WHERE tcb.task_id = %s
                  AND COALESCE(cp.collected, false) = false
                ORDER BY tcb.rate ASC NULLS LAST, tcb.id ASC
                LIMIT 1
            )
            SELECT
                tcb.cb_id::bigint AS cb_id,
                tcb.rate AS pair_rate,
                cp.branch_id::bigint AS branch_id,
                cp.plz_id::bigint AS plz_id,
                COALESCE(cp.collected_num, 0)::bigint AS collected_num,
                cp.updated_at,
                COALESCE(bs.catalog, '') AS branch_catalog
            FROM public.task_cb_ratings tcb
            JOIN public.cb_crawl_pairs cp
              ON cp.id = tcb.cb_id
            JOIN public.branches_sys bs
              ON bs.id = cp.branch_id
            LEFT JOIN first_hole fh
              ON true
            WHERE tcb.task_id = %s
              AND COALESCE(cp.collected, false) = true
              AND (
                    fh.hole_id IS NULL
                    OR COALESCE(tcb.rate::bigint, %s::bigint) < fh.hole_rate_ord
                    OR (
                        COALESCE(tcb.rate::bigint, %s::bigint) = fh.hole_rate_ord
                        AND tcb.id < fh.hole_id
                    )
              )
            ORDER BY tcb.rate DESC NULLS LAST, tcb.id DESC
            LIMIT 500
            """,
            [
                int(RATE_NULL_ORD),
                int(task_id),
                int(task_id),
                int(RATE_NULL_ORD),
                int(RATE_NULL_ORD),
            ],
        )
        rows = cur.fetchall() or []

        cur.execute(
            """
            SELECT tbr.branch_id::bigint, tbr.rate
            FROM public.task_branch_ratings tbr
            WHERE tbr.task_id = %s
            """,
            [int(task_id)],
        )
        branch_rate_rows = cur.fetchall() or []

        cur.execute(
            """
            SELECT tcr.city_id::bigint, tcr.rate
            FROM public.task_city_ratings tcr
            WHERE tcr.task_id = %s
            """,
            [int(task_id)],
        )
        city_rate_rows = cur.fetchall() or []

    branch_rate_map: dict[int, int | None] = {
        int(row[0]): (int(row[1]) if row[1] is not None else None)
        for row in branch_rate_rows
        if row and row[0] is not None
    }
    city_rate_map: dict[int, int | None] = {
        int(row[0]): (int(row[1]) if row[1] is not None else None)
        for row in city_rate_rows
        if row and row[0] is not None
    }
    category_title_cache: dict[int, str] = {}
    city_title_cache: dict[int, str] = {}
    city_id_by_plz: dict[int, int | None] = {}

    pairs_rows: list[dict[str, Any]] = []
    for row in rows:
        cb_id = int(row[0])
        pair_rate = row[1]
        branch_id = int(row[2])
        plz_id = int(row[3])
        collected_num = int(row[4] or 0)
        processed_at = row[5]
        branch_catalog = str(row[6] or "").strip().lower()

        if branch_id not in category_title_cache:
            try:
                category_title_cache[branch_id] = " ".join(str(get_category_title(branch_id, request)).split()).strip()
            except Exception:
                category_title_cache[branch_id] = str(branch_id)
        category_title = category_title_cache[branch_id]

        if plz_id not in city_title_cache:
            try:
                city_title_cache[plz_id] = " ".join(str(get_city_title(plz_id, request, land=True, plz=True)).split()).strip()
            except Exception:
                city_title_cache[plz_id] = str(plz_id)
        city_title = city_title_cache[plz_id]

        if plz_id not in city_id_by_plz:
            with connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT cpm.city_id::bigint
                    FROM public.plz_sys ps
                    LEFT JOIN public.__city__plz_map cpm
                      ON cpm.plz = ps.plz
                    WHERE ps.id = %s
                    LIMIT 1
                    """,
                    [plz_id],
                )
                city_id_row = cur.fetchone()
            city_id_by_plz[plz_id] = int(city_id_row[0]) if city_id_row and city_id_row[0] is not None else None

        category_rate_value = branch_rate_map.get(branch_id)
        category_rate = str(category_rate_value) if category_rate_value is not None else "-"
        city_rate_value = city_rate_map.get(int(city_id_by_plz[plz_id])) if city_id_by_plz[plz_id] is not None else None
        city_rate = str(city_rate_value) if city_rate_value is not None else "-"

        pairs_rows.append(
            {
                "cb_id": cb_id,
                "catalog_display": "GS" if branch_catalog == "gs" else ("11880" if branch_catalog == "11880" else "-"),
                "processed_at": processed_at,
                "processed_at_display": (
                    processed_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    if processed_at is not None and hasattr(processed_at, "astimezone")
                    else ""
                ),
                "processed_at_utc_iso": (
                    processed_at.astimezone(timezone.utc).isoformat()
                    if processed_at is not None and hasattr(processed_at, "astimezone")
                    else ""
                ),
                "pair_rate_display": str(pair_rate) if pair_rate is not None else "-",
                "category_with_rate": f"{category_title} ({category_rate})" if category_title else f"({category_rate})",
                "city_with_rate": f"{city_title} ({city_rate})" if city_title else f"({city_rate})",
                "collected_num": collected_num,
                "collected_num_display": _format_contacts_total(collected_num),
            }
        )

    return {
        "contacts_pairs_show_catalog": show_catalog_column,
        "contacts_pairs_rows": pairs_rows,
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


def _contacts_section_route_name(flow_type: str, section: str) -> str:
    suffix_by_section = {
        CONTACTS_SECTION_COLLECT: "contacts",
        CONTACTS_SECTION_ALL: "contacts_all",
        CONTACTS_SECTION_BRANCH_CITY: "contacts_branch_city",
        CONTACTS_SECTION_PAIRS: "contacts_pairs",
    }
    section_key = _normalize_contacts_section(section)
    suffix = suffix_by_section.get(section_key, "contacts")
    return f"audience:create_edit_{flow_type}_{suffix}"


def _build_contacts_section_url(flow_type: str, item_id: str, section: str) -> str:
    route_name = _contacts_section_route_name(flow_type, section)
    if item_id:
        return reverse(f"{route_name}_id", args=[item_id])
    return reverse(route_name)


def _build_contacts_section_urls(flow_type: str, item_id: str) -> dict[str, str]:
    return {
        CONTACTS_SECTION_COLLECT: _build_contacts_section_url(flow_type, item_id, CONTACTS_SECTION_COLLECT),
        CONTACTS_SECTION_ALL: _build_contacts_section_url(flow_type, item_id, CONTACTS_SECTION_ALL),
        CONTACTS_SECTION_BRANCH_CITY: _build_contacts_section_url(flow_type, item_id, CONTACTS_SECTION_BRANCH_CITY),
        CONTACTS_SECTION_PAIRS: _build_contacts_section_url(flow_type, item_id, CONTACTS_SECTION_PAIRS),
    }


def _is_contacts_active(task) -> bool:
    return bool(task and task.active and not task.archived)


def _is_contacts_completed(task) -> bool:
    return False


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
                "contacts_all_show_paging": False,
                "contacts_all_total": 0,
                "contacts_all_total_display": _format_contacts_total(0),
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
    if section_key == CONTACTS_SECTION_PAIRS:
        if not task:
            return {
                "contacts_pairs_show_catalog": _is_super_workspace_user(request),
                "contacts_pairs_rows": [],
            }
        return _fetch_contacts_pairs_rows(request, int(task.id))
    return {}


def _build_active_contacts_context(
    *,
    request,
    task,
    flow_type: str,
    item_id: str,
    active_section: str,
) -> dict[str, Any]:
    section_key = _normalize_contacts_section(active_section)
    query = str(request.GET.get("q") or "").strip()
    page = _get_page_value(str(request.GET.get("page") or "1"))
    section_urls = _build_contacts_section_urls(flow_type, item_id)

    active_partial_url = ""
    if section_key == CONTACTS_SECTION_COLLECT:
        active_partial_url = _build_contacts_collect_partial_url(flow_type, item_id)
    elif section_key == CONTACTS_SECTION_ALL:
        active_partial_url = _build_contacts_all_partial_url(flow_type, item_id)
    elif section_key == CONTACTS_SECTION_BRANCH_CITY:
        active_partial_url = _build_contacts_branch_city_partial_url(flow_type, item_id)
    elif section_key == CONTACTS_SECTION_PAIRS:
        active_partial_url = _build_contacts_pairs_partial_url(flow_type, item_id)

    context = {
        "contacts_active_section": section_key,
        "contacts_section_urls": section_urls,
        "contacts_collect_running": _is_contacts_active(task),
        "contacts_active_partial_url": active_partial_url,
    }
    context.update(
        _build_contacts_section_context(
            request=request,
            task=task,
            section=section_key,
            page=page,
            query=query,
        )
    )
    return context


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
    flow_type, item_id, task, status_code = _resolve_contacts_partial_task(request)
    context = _build_contacts_section_context(
        request=request,
        task=task,
        section=section,
        page=page,
        query=query,
    )
    if flow_type in {"buy", "sell"}:
        context["contacts_section_urls"] = _build_contacts_section_urls(flow_type, item_id)
        context["type"] = flow_type
    return render(
        request,
        template_name,
        context,
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


def contacts_ready_view(request):
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
            "ready": bool(task.ready),
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
    contacts_section: str,
):
    flow_conf = get_flow_config(flow_type)
    step_definitions = build_step_definitions(flow_type)
    active_section = _normalize_contacts_section(contacts_section)
    pause_info_modal_url = (
        reverse("audience:create_pause_info_modal") + f"?id={item_id}"
        if item_id
        else ""
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
            step_template="panels/aap_audience/create/step_contacts.html",
            extra_context={
                "contacts_step": _build_contacts_step_context(task),
                "pause_info_modal_url": pause_info_modal_url,
                **_build_active_contacts_context(
                    request=request,
                    task=task,
                    flow_type=flow_type,
                    item_id=item_id,
                    active_section=active_section,
                ),
            },
        ),
    )
