# FILE: web/panel/aap_audience/views/create_edit_flow.py
# DATE: 2026-03-23
# PURPOSE: Dispatcher for the shared create/edit audience flow.

from __future__ import annotations

from django.db import connection
from django.shortcuts import redirect
from django.urls import reverse

from engine.core_status.is_active import clear_is_more_needed_full_cache
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
    build_contact_rating_hash_alert_context,
    flow_back_url,
    build_step_definitions,
    current_contact_rating_hash,
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

    task = resolve_task(request, flow_type, item_id, include_archived=bool(item_id))
    if item_id and not task:
        return redirect(reverse("audience:create_list"))
    is_archived_task = bool(task and task.archived)
    archive_allowed_steps = {"contacts", "mailing_list"}
    if is_archived_task and requested_step not in archive_allowed_steps:
        return redirect(build_edit_url(flow_type, item_id, "contacts"))

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "toggle_user_active":
            if task and not bool(task.archived) and bool(task.ready):
                task.user_active = not bool(task.user_active)
                task.save(update_fields=["user_active", "updated_at"])
            return redirect(request.get_full_path())
        if action in {"contacts_rating_recalc", "contacts_rating_restart", "contacts_rating_ignore_hash"}:
            if task and not bool(task.archived):
                if action == "contacts_rating_recalc":
                    with connection.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE public.sending_lists
                            SET rate = NULL,
                                rating_hash = NULL,
                                updated_at = now()
                            WHERE task_id = %s
                              AND COALESCE(removed, false) = false
                            """,
                            [int(task.id)],
                        )
                    clear_is_more_needed_full_cache(int(task.id))
                elif action == "contacts_rating_restart":
                    with connection.cursor() as cur:
                        cur.execute(
                            "DELETE FROM public.sending_lists WHERE task_id = %s",
                            [int(task.id)],
                        )
                    task.ready = False
                    task.save(update_fields=["ready", "updated_at"])
                    clear_is_more_needed_full_cache(int(task.id))
                elif action == "contacts_rating_ignore_hash":
                    task_hash = current_contact_rating_hash(task)
                    with connection.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE public.sending_lists
                            SET rating_hash = %s,
                                updated_at = now()
                            WHERE task_id = %s
                              AND COALESCE(removed, false) = false
                              AND rate IS NOT NULL
                            """,
                            [int(task_hash), int(task.id)],
                        )
            return redirect(request.get_full_path())

    task, geo_title_gpt_failed = maybe_update_title_on_geo_enter(
        request,
        requested_step=requested_step,
        task=task,
    )
    if geo_title_gpt_failed and request.method != "POST":
        return redirect(flow_back_url(request, reverse("audience:create_list")))
    saved_values = task_saved_values(task)
    flow_status = build_flow_step_states(
        step_order=FLOW_STEP_ORDER,
        step_definitions=step_definitions,
        requested_step_key=requested_step,
        saved_values=saved_values,
        url_builder=lambda current_key: build_edit_url(flow_type, item_id, current_key),
    )
    if is_archived_task:
        forced_step = requested_step if requested_step in archive_allowed_steps else "contacts"
        flow_status["current_step_key"] = forced_step
        for step in flow_status.get("step_states") or []:
            key = str(step.get("key") or "").strip()
            is_current = key == forced_step
            step["is_current"] = is_current
            step["is_clickable"] = (key in archive_allowed_steps) and (not is_current)
            if key not in archive_allowed_steps:
                step["is_available"] = False

    current_step_key = str(flow_status["current_step_key"] or "product")
    if not is_archived_task and current_step_key != requested_step:
        return redirect(build_edit_url(flow_type, item_id, current_step_key))
    rating_hash_alert = build_contact_rating_hash_alert_context(task)

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
            rating_hash_alert=rating_hash_alert,
        )
    return handle_mailing_list_step_view(
        request,
        flow_type=flow_type,
        current_step_key=current_step_key,
        item_id=item_id,
        task=task,
        saved_values=saved_values,
        flow_status=flow_status,
        rating_hash_alert=rating_hash_alert,
    )
