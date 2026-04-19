# FILE: web/panel/aap_settings/views/sending.py
# DATE: 2026-01-19
# PURPOSE: Settings → Sending: автосоздание SendingSettings для workspace + сохранение value_json из формы.
# CHANGE: SendingSettings.workspace -> SendingSettings.workspace_id.

from __future__ import annotations

import json
from typing import Any, Dict

from django.shortcuts import redirect, render
from django.utils.translation import gettext as _trans

from engine.common.cache.client import CLIENT
from panel.aap_settings.models import (
    GlobalSendingSettings,
    SendingSettings,
)


DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun", "hol"]


def _ws_window_cache_key(workspace_id: str) -> str:
    return f"core_status:campaign:window:ws:{str(workspace_id)}"


def _global_window_cache_key() -> str:
    return "core_status:campaign:window:global"


def _invalidate_status_window_cache(workspace_id: str) -> None:
    CLIENT.delete_many(
        [
            _ws_window_cache_key(str(workspace_id)),
            _global_window_cache_key(),
        ]
    )


def _guard(request):
    ws = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws or not getattr(user, "is_authenticated", False):
        return None
    return ws


def _normalize_value_json(data: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = data if isinstance(data, dict) else {}
    for k in DAY_KEYS:
        if not isinstance(out.get(k), list):
            out[k] = []
    return out


def _global_value_json() -> Dict[str, Any]:
    obj = GlobalSendingSettings.objects.filter(singleton_key=1).first()
    data = obj.global_global_window if (obj and isinstance(obj.global_global_window, dict)) else {}
    return _normalize_value_json(data)


def sending_settings_view(request):
    ws = _guard(request)
    if not ws:
        return redirect("/")

    obj = SendingSettings.objects.filter(workspace_id=ws).first()

    errors = []

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "reset_defaults":
            SendingSettings.objects.filter(workspace_id=ws).delete()
            _invalidate_status_window_cache(str(ws))
            return redirect(request.path)

        raw = (request.POST.get("value_json") or "").strip()
        try:
            data = json.loads(raw) if raw else None
        except Exception:
            data = None

        ok = True
        if not isinstance(data, dict):
            ok = False
        else:
            for k in DAY_KEYS:
                if not isinstance(data.get(k, []), list):
                    ok = False
                    break

        if not ok:
            errors.append(_trans("Неверный JSON."))
        else:
            data = _normalize_value_json(data)
            if obj is None:
                obj = SendingSettings.objects.create(workspace_id=ws, value_json=data)
            else:
                obj.value_json = data
                obj.save(update_fields=["value_json", "updated_at"])
            _invalidate_status_window_cache(str(ws))
            return redirect(request.path)

    value_json = obj.value_json if obj is not None else _global_value_json()
    value_json = _normalize_value_json(value_json)
    global_value_json = _global_value_json()
    has_custom_settings = obj is not None

    ctx = {
        "errors": errors,
        "value_json_str": json.dumps(value_json, ensure_ascii=False),
        "global_value_json_str": json.dumps(global_value_json, ensure_ascii=False),
        "has_custom_settings": has_custom_settings,
        "day_labels": [
            ("mon", _trans("Понедельник")),
            ("tue", _trans("Вторник")),
            ("wed", _trans("Среда")),
            ("thu", _trans("Четверг")),
            ("fri", _trans("Пятница")),
            ("sat", _trans("Суббота")),
            ("sun", _trans("Воскресенье")),
            ("hol", _trans("Праздники")),
        ],
    }
    return render(request, "panels/aap_settings/sending.html", ctx)


def sending_reset_modal_view(request):
    ws = _guard(request)
    if not ws:
        return redirect("/")

    has_custom_settings = SendingSettings.objects.filter(workspace_id=ws).exists()
    return render(
        request,
        "panels/aap_settings/modal_sending_reset.html",
        {
            "status": "ok",
            "has_custom_settings": has_custom_settings,
        },
    )
