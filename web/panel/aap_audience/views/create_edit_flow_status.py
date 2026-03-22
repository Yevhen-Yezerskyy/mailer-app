# FILE: web/panel/aap_audience/views/create_edit_flow_status.py
# DATE: 2026-03-21
# PURPOSE: Shared flow-step status resolver for create/edit audience flow screens.

from __future__ import annotations

from typing import Callable, Mapping, Sequence


def _norm_text(value) -> str:
    return " ".join(str(value or "").split()).strip()


def _is_step_complete(step_def: Mapping[str, object], saved_values: Mapping[str, object]) -> bool:
    completion_type = str(step_def.get("completion_type") or "text").strip().lower()
    field_name = str(step_def.get("completion_field") or "").strip()
    value = saved_values.get(field_name) if field_name else None

    if completion_type == "always":
        return True
    if completion_type == "truthy":
        return bool(value)
    if completion_type == "never":
        return False
    return bool(_norm_text(value))


def _visible_step_keys(step_order: Sequence[str], step_definitions: Mapping[str, Mapping[str, object]]) -> list[str]:
    return [key for key in step_order if bool(step_definitions.get(key, {}).get("visible"))]


def _selectable_step_keys(step_order: Sequence[str], step_definitions: Mapping[str, Mapping[str, object]]) -> list[str]:
    return [
        key
        for key in step_order
        if bool(step_definitions.get(key, {}).get("visible")) and bool(step_definitions.get(key, {}).get("implemented", True))
    ]


def _first_unmet_dependency(
    step_key: str,
    step_definitions: Mapping[str, Mapping[str, object]],
    completed_keys: set[str],
) -> str:
    step_def = step_definitions.get(step_key, {})
    for dep in tuple(step_def.get("depends_on") or ()):
        if dep in completed_keys:
            continue
        nested = _first_unmet_dependency(dep, step_definitions, completed_keys)
        return nested or dep
    return ""


def resolve_current_step_key(
    *,
    step_order: Sequence[str],
    step_definitions: Mapping[str, Mapping[str, object]],
    requested_step_key: str,
    saved_values: Mapping[str, object],
) -> str:
    completed_keys = {
        key
        for key in step_order
        if key in step_definitions and _is_step_complete(step_definitions[key], saved_values)
    }
    selectable_keys = []
    for key in _selectable_step_keys(step_order, step_definitions):
        step_def = step_definitions.get(key, {})
        depends_on = tuple(step_def.get("depends_on") or ())
        is_available = bool(step_def.get("always_available")) or all(dep in completed_keys for dep in depends_on)
        if is_available:
            selectable_keys.append(key)

    if not selectable_keys:
        return ""

    if requested_step_key in selectable_keys:
        return requested_step_key
    fallback_key = _first_unmet_dependency(requested_step_key, step_definitions, completed_keys)
    if fallback_key in selectable_keys:
        return fallback_key
    return selectable_keys[0]


def build_flow_step_states(
    *,
    step_order: Sequence[str],
    step_definitions: Mapping[str, Mapping[str, object]],
    requested_step_key: str,
    saved_values: Mapping[str, object],
    url_builder: Callable[[str], str],
) -> dict[str, object]:
    current_step_key = resolve_current_step_key(
        step_order=step_order,
        step_definitions=step_definitions,
        requested_step_key=requested_step_key,
        saved_values=saved_values,
    )

    completed_keys = {
        key
        for key in step_order
        if key in step_definitions and _is_step_complete(step_definitions[key], saved_values)
    }

    step_states: list[dict[str, object]] = []
    for key in step_order:
        step_def = step_definitions.get(key, {})
        if not bool(step_def.get("visible")):
            continue
        depends_on = tuple(step_def.get("depends_on") or ())
        is_implemented = bool(step_def.get("implemented", True))
        is_available = bool(step_def.get("always_available")) or all(dep in completed_keys for dep in depends_on)
        step_states.append(
            {
                "key": key,
                "label": step_def.get("nav_label") or key,
                "url": url_builder(key),
                "is_current": key == current_step_key,
                "is_complete": key in completed_keys,
                "is_available": is_available,
                "is_clickable": bool(is_available and is_implemented),
                "is_implemented": is_implemented,
            }
        )

    return {
        "current_step_key": current_step_key,
        "completed_keys": completed_keys,
        "step_states": step_states,
    }


def get_next_step_key(step_states: Sequence[Mapping[str, object]], current_step_key: str) -> str:
    keys = [
        str(step.get("key") or "").strip()
        for step in step_states
        if (
            str(step.get("key") or "").strip()
            and bool(step.get("is_implemented", True))
            and bool(step.get("is_available", False))
        )
    ]
    if not keys:
        return ""
    if current_step_key not in keys:
        return keys[0]

    idx = keys.index(current_step_key)
    return keys[(idx + 1) % len(keys)]
