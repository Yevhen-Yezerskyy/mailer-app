# FILE: web/panel/aap_audience/views/modal_status_task.py
# DATE: 2026-01-02
# NEW:
# - модалка по rate_contacts.id (obfuscated id в GET id)
# - без проверок workspace/user/task
# - JOIN rate_contacts -> raw_contacts_aggr, выводим pretty JSON company_data

from __future__ import annotations

import json

from django.db import connection
from django.shortcuts import render

from mailer_web.access import decode_id


def _pretty_json(v) -> str:
    if v is None:
        return "{}"
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, indent=2, sort_keys=False)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return "{}"
        try:
            obj = json.loads(s)
            return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=False)
        except Exception:
            return s
    try:
        return json.dumps(v, ensure_ascii=False, indent=2, sort_keys=False)
    except Exception:
        return str(v)


def modal_status_task_view(request):
    token = (request.GET.get("id") or "").strip()
    if not token:
        return render(
            request,
            "panels/aap_audience/modal_status_task.html",
            {"status": "empty", "company_name": "", "rate_cl": None, "rate_cb": None, "company_json": "{}"},
        )

    try:
        rc_id = int(decode_id(token))
    except Exception:
        rc_id = 0

    if rc_id <= 0:
        return render(
            request,
            "panels/aap_audience/modal_status_task.html",
            {"status": "empty", "company_name": "", "rate_cl": None, "rate_cb": None, "company_json": "{}"},
        )

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                rc.id,
                rc.contact_id,
                rc.rate_cl,
                rc.rate_cb,
                rca.company_name,
                rca.company_data
            FROM public.rate_contacts rc
            LEFT JOIN public.raw_contacts_aggr rca
              ON rca.id = rc.contact_id
            WHERE rc.id = %s
            LIMIT 1
            """,
            [int(rc_id)],
        )
        row = cur.fetchone()

    if not row:
        return render(
            request,
            "panels/aap_audience/modal_status_task.html",
            {"status": "empty", "company_name": "", "rate_cl": None, "rate_cb": None, "company_json": "{}"},
        )

    _rc_id, _contact_id, rate_cl, rate_cb, company_name, company_data = row

    return render(
        request,
        "panels/aap_audience/modal_status_task.html",
        {
            "status": "done",
            "company_name": (company_name or "").strip(),
            "rate_cl": rate_cl,
            "rate_cb": rate_cb,
            "company_json": _pretty_json(company_data),
        },
    )
