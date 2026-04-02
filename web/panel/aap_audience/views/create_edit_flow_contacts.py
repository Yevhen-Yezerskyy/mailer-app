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
            LEFT JOIN public.plz_sys ps
              ON ps.id = cp.plz_id
            LEFT JOIN (
                SELECT m.plz, MIN(m.city_id) AS city_id
                FROM public.__city__plz_map m
                GROUP BY m.plz
            ) city_map
              ON city_map.plz = ps.plz
            WHERE sl.task_id = %s
              AND COALESCE(sl.removed, false) = false
            ORDER BY
                sl.created_at DESC NULLS LAST,
                sl.aggr_contact_cb_id DESC
            LIMIT 500
            """,
            [int(task_id)],
        )
        rows = cur.fetchall() or []
        out: list[dict[str, Any]] = []
    for row in rows:
        company_name = str(row[1] or "").strip()
        company_data = parse_json_object(row[2], field_name="aggr_contacts_cb.company_data")
        norm_data = company_data.get("norm") if isinstance(company_data.get("norm"), dict) else {}
        address = str(norm_data.get("address") or "").strip()

        out.append(
            {
                "aggr_contact_id": int(row[0]),
                "aggr_contact_ui_id": encode_id(int(row[0])),
                "company_name": company_name,
                "address": address,
                "company_data": company_data,
                "branch_name": get_category_title(row[3], request),
                "city_title": get_city_title(row[4], request, land=True, plz=False),
                "pair_rate": row[5],
                "created_at": row[6],
                "contact_modal_url": reverse("contact_modal") + f"?id={encode_id(int(row[0]))}",
            }
        )
    return out


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


def _build_contacts_section_context(*, request, task, section: str) -> dict[str, Any]:
    section_key = _normalize_contacts_section(section)
    if section_key == CONTACTS_SECTION_COLLECT:
        return {
            "contacts_collect_rows": _fetch_contacts_collect_rows(request, int(task.id)) if task else [],
        }
    return {}


def _build_contacts_sections_context(*, request, task, flow_type: str, item_id: str) -> dict[str, Any]:
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


def _render_contacts_partial(request, *, section: str, template_name: str):
    _, _, task, status_code = _resolve_contacts_partial_task(request)
    return render(
        request,
        template_name,
        _build_contacts_section_context(request=request, task=task, section=section),
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
    return _render_contacts_partial(
        request,
        section=CONTACTS_SECTION_ALL,
        template_name="panels/aap_audience/create/_contacts_all_inner.html",
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
