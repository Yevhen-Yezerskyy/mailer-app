# FILE: web/panel/aap_settings/views/sending.py
# DATE: 2026-01-18
# PURPOSE: Settings → Sending: автосоздание SendingSettings для workspace + сохранение value_json из формы.
# CHANGE: FIX: "_" используется ТОЛЬКО как gettext, created не затирает переводчик.

from __future__ import annotations

import json
from typing import Any, Dict

from django.shortcuts import redirect, render
from django.utils.translation import gettext as _

from panel.aap_settings.models import SendingSettings


DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun", "hol"]


def _guard(request):
    ws = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws or not getattr(user, "is_authenticated", False):
        return None
    return ws


def _default_value_json() -> Dict[str, Any]:
    return {
        "mon": [],
        "tue": [{"from": "09:00", "to": "12:00"}],
        "wed": [{"from": "09:00", "to": "12:00"}],
        "thu": [{"from": "09:00", "to": "12:00"}],
        "fri": [],
        "sat": [],
        "sun": [],
        "hol": [],
    }


def sending_settings_view(request):
    ws = _guard(request)
    if not ws:
        return redirect("/")

    obj, created_flag = SendingSettings.objects.get_or_create(
        workspace=ws,
        defaults={"value_json": _default_value_json()},
    )

    errors = []

    if request.method == "POST":
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
            errors.append(_("Неверный JSON."))
        else:
            for k in DAY_KEYS:
                data.setdefault(k, [])
            obj.value_json = data
            obj.save(update_fields=["value_json", "updated_at"])
            return redirect(request.path)

    ctx = {
        "errors": errors,
        "value_json_str": json.dumps(obj.value_json or _default_value_json(), ensure_ascii=False),
        "day_labels": [
            ("mon", _("Понедельник")),
            ("tue", _("Вторник")),
            ("wed", _("Среда")),
            ("thu", _("Четверг")),
            ("fri", _("Пятница")),
            ("sat", _("Суббота")),
            ("sun", _("Воскресенье")),
            ("hol", _("Праздники")),
        ],
    }
    return render(request, "panels/aap_settings/sending.html", ctx)
