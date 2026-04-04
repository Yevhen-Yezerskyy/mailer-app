# FILE: web/panel/aap_audience/views/create_edit_flow.py
# DATE: 2026-03-23
# PURPOSE: Dispatcher for the shared create/edit audience flow.

from __future__ import annotations

from django.shortcuts import redirect

from .create_edit_flow_branches_cities import (
    handle_branches_step_view,
    handle_cities_step_view,
)
from .create_edit_flow_contacts import handle_contacts_step_view
from .create_edit_flow_mailing_list import handle_mailing_list_step_view
from .create_edit_flow_status import build_flow_step_states
from .create_edit_flow_shared import (
    FLOW_STEP_ORDER,
    TEXT_STEP_KEYS,
    build_edit_url,
    build_step_definitions,
    maybe_update_title_on_geo_enter,
    resolve_task,
    task_saved_values,
)
from .create_edit_flow_text import handle_text_step_view


def create_edit_flow_view(
    request,
    *,
    flow_type: str,
    step_key: str,
    item_id: str = "",
    contacts_section: str = "collect",
):
    step_definitions = build_step_definitions(flow_type)
    requested_step = (step_key or "product").strip().lower()

    task = resolve_task(request, flow_type, item_id)
    task = maybe_update_title_on_geo_enter(request, requested_step=requested_step, task=task)
    saved_values = task_saved_values(task)
    flow_status = build_flow_step_states(
        step_order=FLOW_STEP_ORDER,
        step_definitions=step_definitions,
        requested_step_key=requested_step,
        saved_values=saved_values,
        url_builder=lambda current_key: build_edit_url(flow_type, item_id, current_key),
    )
    current_step_key = str(flow_status["current_step_key"] or "product")
    if current_step_key != requested_step:
        return redirect(build_edit_url(flow_type, item_id, current_step_key))

    if current_step_key in TEXT_STEP_KEYS:
        return handle_text_step_view(
            request,
            flow_type=flow_type,
            current_step_key=current_step_key,
            item_id=item_id,
            task=task,
            saved_values=saved_values,
            flow_status=flow_status,
        )
    if current_step_key == "branches":
        return handle_branches_step_view(
            request,
            flow_type=flow_type,
            current_step_key=current_step_key,
            item_id=item_id,
            task=task,
            saved_values=saved_values,
            flow_status=flow_status,
        )
    if current_step_key == "cities":
        return handle_cities_step_view(
            request,
            flow_type=flow_type,
            current_step_key=current_step_key,
            item_id=item_id,
            task=task,
            saved_values=saved_values,
            flow_status=flow_status,
        )
    if current_step_key == "contacts":
        return handle_contacts_step_view(
            request,
            flow_type=flow_type,
            current_step_key=current_step_key,
            item_id=item_id,
            task=task,
            saved_values=saved_values,
            flow_status=flow_status,
            contacts_section=contacts_section,
        )
    return handle_mailing_list_step_view(
        request,
        flow_type=flow_type,
        current_step_key=current_step_key,
        item_id=item_id,
        task=task,
        saved_values=saved_values,
        flow_status=flow_status,
    )
