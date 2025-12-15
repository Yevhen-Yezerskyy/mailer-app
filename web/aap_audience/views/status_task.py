# FILE: web/aap_audience/views/status_task.py  (обновлено) 2025-12-15
# Fix: company_data(jsonb) из raw_contacts_gb парсим в dict (иначе в шаблоне .items пусто).

import json

from django.core.paginator import Paginator
from django.db import connection
from django.shortcuts import render, get_object_or_404

from aap_audience.models import AudienceTask


def _to_dict(v):
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            x = json.loads(v)
            return x if isinstance(x, dict) else {}
        except Exception:
            return {}
    return {}


def status_task_view(request, task_id: int):
    ws_id = request.workspace_id
    user = request.user

    task = get_object_or_404(
        AudienceTask,
        id=task_id,
        workspace_id=ws_id,
        user=user,
    )

    page = int(request.GET.get("page", 1))
    per_page = 20

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(rc.id)
            FROM queue_sys qs
            JOIN raw_contacts_gb rc
              ON rc.cb_crawler_id = qs.cb_crawler_id
            WHERE qs.task_id = %s
              AND qs.status = 'collected'
            """,
            [task_id],
        )
        total = cur.fetchone()[0] or 0

        cur.execute(
            """
            SELECT
                rc.id,
                rc.company_name,
                rc.email,
                cc.city_name,
                cc.branch_slug,
                rc.company_data
            FROM queue_sys qs
            JOIN cb_crawler cc
              ON cc.id = qs.cb_crawler_id
            JOIN raw_contacts_gb rc
              ON rc.cb_crawler_id = cc.id
            WHERE qs.task_id = %s
              AND qs.status = 'collected'
            ORDER BY rc.id
            LIMIT %s OFFSET %s
            """,
            [task_id, per_page, (page - 1) * per_page],
        )

        rows = []
        for r in cur.fetchall():
            rows.append(
                {
                    "id": r[0],
                    "company_name": r[1],
                    "email": r[2],
                    "city": r[3],
                    "branch": r[4],
                    "company_data": _to_dict(r[5]),
                }
            )

    paginator = Paginator(range(total), per_page)
    page_obj = paginator.get_page(page)

    return render(
        request,
        "panels/aap_audience/status_task.html",
        {
            "task": task,
            "rows": rows,
            "total": total,
            "page_obj": page_obj,
        },
    )
