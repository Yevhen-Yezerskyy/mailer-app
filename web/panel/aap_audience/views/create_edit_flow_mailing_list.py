# FILE: web/panel/aap_audience/views/create_edit_flow_mailing_list.py
# DATE: 2026-03-23
# PURPOSE: Mailing-list step handler for the create/edit flow.

from __future__ import annotations

from typing import Any, Mapping

from django.shortcuts import render

from .create_edit_flow_shared import (
    build_flow_render_context,
    build_step_definitions,
    get_flow_config,
)


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
            step_template="panels/aap_audience/create/step_placeholder.html",
            extra_context={
                "placeholder_step": {
                    "title": str(step_definitions["mailing_list"].get("summary_label") or ""),
                    "text": str("Раздел пока в разработке."),
                },
            },
        ),
    )
