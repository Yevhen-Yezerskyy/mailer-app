# FILE: web/panel/aap_audience/views/create_edit_flow_text.py
# DATE: 2026-03-23
# PURPOSE: Text-step handler for product/company/geo inside the create/edit flow.

from __future__ import annotations

import json
from typing import Any, Mapping

from django.shortcuts import redirect, render

from engine.common.gpt import GPTClient
from mailer_web.access import encode_id

from .create_edit_flow_gpt_consts import FLOW_GPT_MODEL, FLOW_GPT_SERVICE_TIER
from .create_edit_flow_shared import (
    FLOW_GPT_UNAVAILABLE_TEXT,
    TEXT_STEP_KEYS,
    TASK_CREATION_STEP_KEYS,
    build_current_step_context,
    build_edit_url,
    build_flow_render_context,
    build_step_definitions,
    clear_dialog_state,
    create_task,
    get_flow_config,
    has_insertable_company_tasks,
    is_gpt_ok,
    mark_flow_gpt_unavailable,
    parse_ai_json,
    prompt_instructions,
    reset_section_dialog,
    resolve_task,
    session_key,
    task_saved_values,
)


def _text_draft_session_key(request, *, flow_type: str, item_id: str) -> str:
    return session_key(request, flow_type, item_id, "text_draft")


def _read_text_draft(request, *, flow_type: str, item_id: str) -> tuple[dict[str, str], dict[str, str], dict[str, str], dict[str, str]]:
    key = _text_draft_session_key(request, flow_type=flow_type, item_id=item_id)
    payload = request.session.get(key, {}) or {}
    if not isinstance(payload, dict):
        payload = {}
    payload_working_values = payload.get("working_values") if isinstance(payload.get("working_values"), dict) else {}
    payload_ai_command_display_map = (
        payload.get("ai_command_display_map") if isinstance(payload.get("ai_command_display_map"), dict) else {}
    )
    payload_ai_advice_map = payload.get("ai_advice_map") if isinstance(payload.get("ai_advice_map"), dict) else {}
    payload_ai_question_map = payload.get("ai_question_map") if isinstance(payload.get("ai_question_map"), dict) else {}

    working_values = {
        "source_product": str(payload_working_values.get("source_product") or ""),
        "source_company": str(payload_working_values.get("source_company") or ""),
        "source_geo": str(payload_working_values.get("source_geo") or ""),
    }
    ai_command_display_map = {
        key_name: str(payload_ai_command_display_map.get(key_name) or "")
        for key_name in TEXT_STEP_KEYS
    }
    ai_advice_map = {
        key_name: str(payload_ai_advice_map.get(key_name) or "")
        for key_name in TEXT_STEP_KEYS
    }
    ai_question_map = {
        key_name: str(payload_ai_question_map.get(key_name) or "")
        for key_name in TEXT_STEP_KEYS
    }
    return working_values, ai_command_display_map, ai_advice_map, ai_question_map


def _write_text_draft(
    request,
    *,
    flow_type: str,
    item_id: str,
    working_values: Mapping[str, str],
    ai_command_display_map: Mapping[str, str],
    ai_advice_map: Mapping[str, str],
    ai_question_map: Mapping[str, str],
) -> None:
    key = _text_draft_session_key(request, flow_type=flow_type, item_id=item_id)
    request.session[key] = {
        "working_values": {
            "source_product": str(working_values.get("source_product") or ""),
            "source_company": str(working_values.get("source_company") or ""),
            "source_geo": str(working_values.get("source_geo") or ""),
        },
        "ai_command_display_map": {k: str(ai_command_display_map.get(k) or "") for k in TEXT_STEP_KEYS},
        "ai_advice_map": {k: str(ai_advice_map.get(k) or "") for k in TEXT_STEP_KEYS},
        "ai_question_map": {k: str(ai_question_map.get(k) or "") for k in TEXT_STEP_KEYS},
    }
    request.session.modified = True


def _clear_text_draft(request, *, flow_type: str, item_id: str) -> None:
    key = _text_draft_session_key(request, flow_type=flow_type, item_id=item_id)
    request.session.pop(key, None)
    request.session.modified = True


def _run_section_dialog(
    request,
    *,
    flow_type: str,
    step_def: Mapping[str, Any],
    item_id: str,
    value: str,
    command: str,
) -> tuple[str, str, str, bool]:
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
        model=FLOW_GPT_MODEL,
        instructions=prompt_instructions(request, step_def["prompt_key"]),
        input=payload,
        conversation=str(state.get("conversation_id") or ""),
        previous_response_id=str(state.get("response_id") or ""),
        user_id=step_def["user_id"],
        service_tier=FLOW_GPT_SERVICE_TIER,
        web_search=True,
    )
    if not is_gpt_ok(resp):
        clear_dialog_state(state)
        request.session[state_key] = state
        request.session.modified = True
        mark_flow_gpt_unavailable(request)
        return "", "", "", True
    new_value, new_advice, new_question = parse_ai_json(resp.content or "", str(step_def["json_key"]))
    error_marker = str(FLOW_GPT_UNAVAILABLE_TEXT or "").strip().casefold()
    combined_text = "\n".join(
        (
            str(new_value or ""),
            str(new_advice or ""),
            str(new_question or ""),
        )
    ).casefold()
    if error_marker and error_marker in combined_text:
        clear_dialog_state(state)
        request.session[state_key] = state
        request.session.modified = True
        mark_flow_gpt_unavailable(request)
        return "", "", "", True

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
    return new_value, new_advice, new_question, False


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
    saved_values: Mapping[str, Any],
):
    for step_key in TEXT_STEP_KEYS:
        step_def = step_definitions[step_key]
        field_name = str(step_def["field_name"])
        field_value = working_values[field_name]

        if action == step_def["process_action"]:
            try:
                new_value, new_advice, new_question, gpt_failed = _run_section_dialog(
                    request,
                    flow_type=flow_type,
                    step_def=step_def,
                    item_id=item_id,
                    value=field_value,
                    command=ai_command_display_map[step_key],
                )
                if gpt_failed:
                    working_values[field_name] = str(saved_values.get(field_name) or "")
                    ai_command_display_map[step_key] = ""
                    ai_advice_map[step_key] = ""
                    ai_question_map[step_key] = ""
                    return task, None, True, False
                if new_value:
                    working_values[field_name] = new_value
                ai_advice_map[step_key] = new_advice
                ai_question_map[step_key] = new_question
            except Exception:
                working_values[field_name] = str(saved_values.get(field_name) or "")
                ai_command_display_map[step_key] = ""
                ai_advice_map[step_key] = ""
                ai_question_map[step_key] = ""
                return task, None, True, False
            return task, None, True, True

        if action == step_def["save_action"] and field_value:
            if task:
                setattr(task, field_name, field_value)
                task.save(update_fields=[field_name, "updated_at"])
                ai_command_display_map[step_key] = ""
                ai_advice_map[step_key] = ""
                ai_question_map[step_key] = ""
                return task, None, True, False

            if step_key not in TASK_CREATION_STEP_KEYS:
                return task, None, True, False

            task = create_task(
                request,
                flow_type=flow_type,
                title="",
                **{field_name: field_value},
            )
            return task, redirect(build_edit_url(flow_type, encode_id(int(task.id)), step_key)), True, False

        if action == step_def["reset_action"]:
            reset_section_dialog(request, flow_type=flow_type, item_id=item_id, step_key=step_key)
            working_values[field_name] = str(saved_values.get(field_name) or "")
            ai_command_display_map[step_key] = ""
            ai_advice_map[step_key] = ""
            ai_question_map[step_key] = ""
            return task, None, True, False

    return task, None, False, False


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
            _clear_text_draft(request, flow_type=flow_type, item_id=item_id)
            return redirect("audience:create_list")

        task, redirect_response, handled, keep_draft = _handle_text_step_action(
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
            saved_values=saved_values,
        )
        if redirect_response is not None:
            _clear_text_draft(request, flow_type=flow_type, item_id=item_id)
            return redirect_response
        if handled and keep_draft:
            _write_text_draft(
                request,
                flow_type=flow_type,
                item_id=item_id,
                working_values=working_values,
                ai_command_display_map=ai_command_display_map,
                ai_advice_map=ai_advice_map,
                ai_question_map=ai_question_map,
            )
        else:
            _clear_text_draft(request, flow_type=flow_type, item_id=item_id)
        return redirect(request.get_full_path())

    draft_working_values, draft_ai_command_display_map, draft_ai_advice_map, draft_ai_question_map = _read_text_draft(
        request,
        flow_type=flow_type,
        item_id=item_id,
    )
    for field_name in ("source_product", "source_company", "source_geo"):
        draft_value = str(draft_working_values.get(field_name) or "")
        if draft_value:
            working_values[field_name] = draft_value
    for step_key in TEXT_STEP_KEYS:
        ai_command_display_map[step_key] = str(draft_ai_command_display_map.get(step_key) or "")
        ai_advice_map[step_key] = str(draft_ai_advice_map.get(step_key) or "")
        ai_question_map[step_key] = str(draft_ai_question_map.get(step_key) or "")

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
            request=request,
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
