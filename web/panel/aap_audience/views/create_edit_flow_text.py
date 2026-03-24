# FILE: web/panel/aap_audience/views/create_edit_flow_text.py
# DATE: 2026-03-23
# PURPOSE: Text-step handler for product/company/geo inside the create/edit flow.

from __future__ import annotations

import json
from typing import Any, Mapping

from django.shortcuts import redirect, render

from engine.common.gpt import GPTClient
from mailer_web.access import encode_id

from .create_edit_flow_status import build_flow_step_states
from .create_edit_flow_shared import (
    TEXT_STEP_KEYS,
    TASK_CREATION_STEP_KEYS,
    build_current_step_context,
    build_edit_url,
    build_flow_render_context,
    build_step_definitions,
    create_task,
    get_flow_config,
    has_insertable_company_tasks,
    parse_ai_json,
    prompt_instructions,
    reset_section_dialog,
    resolve_task,
    session_key,
    task_saved_values,
)


def _run_section_dialog(request, *, flow_type: str, step_def: Mapping[str, Any], item_id: str, value: str, command: str):
    state_key = session_key(request, flow_type, item_id, str(step_def["json_key"]))
    state = request.session.get(state_key, {}) or {}
    if step_def["json_key"] == "geo":
        task = resolve_task(request, flow_type, item_id)
        payload = json.dumps(
            {
                "geo": value,
                "product": (task.source_product or "") if task else "",
                "company": (task.source_company or "") if task else "",
                "command": command,
            },
            ensure_ascii=False,
        )
    else:
        payload = f"{step_def['input_label']}:\n{value}\n\nКОМАНДА:\n{command}"

    resp = GPTClient().ask_dialog(
        model="gpt-5.4",
        instructions=prompt_instructions(request, step_def["prompt_key"]),
        input=payload,
        conversation=str(state.get("conversation_id") or ""),
        previous_response_id=str(state.get("response_id") or ""),
        user_id=step_def["user_id"],
        service_tier="flex",
    )
    new_value, new_advice, new_question = parse_ai_json(resp.content or "", str(step_def["json_key"]))

    raw = resp.raw if isinstance(resp.raw, dict) else {}
    response_id = str(raw.get("id") or "").strip()
    conv_val = raw.get("conversation")
    conversation_id = ""
    if isinstance(conv_val, dict):
        conversation_id = str(conv_val.get("id") or "").strip()
    elif conv_val is not None:
        conversation_id = str(conv_val).strip()

    request.session[state_key] = {
        "conversation_id": conversation_id or str(state.get("conversation_id") or ""),
        "response_id": response_id or str(state.get("response_id") or ""),
    }
    request.session.modified = True
    return new_value, new_advice, new_question


def _handle_text_step_action(
    *,
    request,
    flow_type: str,
    item_id: str,
    action: str,
    task,
    step_definitions: Mapping[str, Mapping[str, Any]],
    working_values: dict[str, str],
    ai_command_display_map: dict[str, str],
    ai_advice_map: dict[str, str],
    ai_question_map: dict[str, str],
):
    for step_key in TEXT_STEP_KEYS:
        step_def = step_definitions[step_key]
        field_name = str(step_def["field_name"])
        field_value = working_values[field_name]

        if action == step_def["process_action"]:
            try:
                new_value, new_advice, new_question = _run_section_dialog(
                    request,
                    flow_type=flow_type,
                    step_def=step_def,
                    item_id=item_id,
                    value=field_value,
                    command=ai_command_display_map[step_key],
                )
                if new_value:
                    working_values[field_name] = new_value
                ai_advice_map[step_key] = new_advice
                ai_question_map[step_key] = new_question
            except Exception:
                pass
            return task, None, True

        if action == step_def["save_action"] and field_value:
            if task:
                setattr(task, field_name, field_value)
                task.save(update_fields=[field_name, "updated_at"])
                ai_command_display_map[step_key] = ""
                ai_advice_map[step_key] = "__saved__"
                ai_question_map[step_key] = ""
                return task, None, True

            if step_key not in TASK_CREATION_STEP_KEYS:
                return task, None, True

            task = create_task(
                request,
                flow_type=flow_type,
                title="",
                **{field_name: field_value},
            )
            return task, redirect(build_edit_url(flow_type, encode_id(int(task.id)), step_key)), True

        if action == step_def["reset_action"]:
            reset_section_dialog(request, flow_type=flow_type, item_id=item_id, step_key=step_key)
            working_values[field_name] = ""
            ai_command_display_map[step_key] = ""
            ai_advice_map[step_key] = ""
            ai_question_map[step_key] = ""
            return task, None, True

    return task, None, False


def handle_text_step_view(
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

    working_values = {
        "source_product": str(saved_values["source_product"] or ""),
        "source_company": str(saved_values["source_company"] or ""),
        "source_geo": str(saved_values["source_geo"] or ""),
    }
    ai_command_display_map = {key: "" for key in TEXT_STEP_KEYS}
    ai_advice_map = {key: "" for key in TEXT_STEP_KEYS}
    ai_question_map = {key: "" for key in TEXT_STEP_KEYS}

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        for step_key in TEXT_STEP_KEYS:
            step_def = step_definitions[step_key]
            field_name = str(step_def["field_name"])
            command_field = str(step_def["command_field"])
            working_values[field_name] = (request.POST.get(field_name) or saved_values[field_name]).strip()
            ai_command_display_map[step_key] = (request.POST.get(command_field) or "").strip()

        if action == "close":
            return redirect("audience:create_list")

        task, redirect_response, handled = _handle_text_step_action(
            request=request,
            flow_type=flow_type,
            item_id=item_id,
            action=action,
            task=task,
            step_definitions=step_definitions,
            working_values=working_values,
            ai_command_display_map=ai_command_display_map,
            ai_advice_map=ai_advice_map,
            ai_question_map=ai_question_map,
        )
        if redirect_response is not None:
            return redirect_response
        if not handled:
            task = resolve_task(request, flow_type, item_id) if item_id else task

        task = resolve_task(request, flow_type, item_id) if item_id else task
        saved_values = task_saved_values(task)
        flow_status = build_flow_step_states(
            step_order=tuple(step_definitions.keys()),
            step_definitions=step_definitions,
            requested_step_key=current_step_key,
            saved_values=saved_values,
            url_builder=lambda step_key: build_edit_url(flow_type, item_id, step_key),
        )
        current_step_key = str(flow_status["current_step_key"] or "product")

    has_company_insert = has_insertable_company_tasks(request, task) if current_step_key == "company" else False
    current_step = build_current_step_context(
        flow_type=flow_type,
        item_id=item_id,
        step_definitions=step_definitions,
        flow_step_states=list(flow_status["step_states"]),
        current_step_key=current_step_key,
        working_values=working_values,
        saved_values=saved_values,
        ai_command_display_map=ai_command_display_map,
        ai_advice_map=ai_advice_map,
        ai_question_map=ai_question_map,
        has_insertable_company_tasks=has_company_insert,
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
            step_template="panels/aap_audience/create/step_form.html",
            extra_context={
                "current_step": current_step,
            },
        ),
    )
