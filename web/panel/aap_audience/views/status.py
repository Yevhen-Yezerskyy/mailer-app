# FILE: web/panel/aap_audience/views/status.py
# DATE: 2026-01-01
# CHANGE:
# - вынесена выборка задач в _get_tasks() (единый стиль)
# - добавлен фильтр archived=false
# - логика страницы НЕ изменена

from __future__ import annotations

from django.db import connection
from django.shortcuts import render

from mailer_web.access import encode_id
from panel.aap_audience.models import AudienceTask


def _get_tasks(request):
    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return AudienceTask.objects.none()
    return (
        AudienceTask.objects
        .filter(
            workspace_id=ws_id,
            user=user,
            run_processing=True,
            archived=False,
        )
        .order_by("-created_at")
    )


def _fetch_contacts_stats(task_ids: list[int]) -> dict[int, dict[str, int]]:
    """
    Returns:
      { task_id: {"total": int, "rated": int}, ... }
    """
    if not task_ids:
        return {}

    sql = """
        SELECT
            task_id,
            COUNT(*)::int AS total_cnt,
            SUM(
                CASE
                    WHEN rate_cl IS NOT NULL AND hash_task IS NOT NULL THEN 1
                    ELSE 0
                END
            )::int AS rated_cnt
        FROM public.rate_contacts
        WHERE task_id = ANY(%s::bigint[])
        GROUP BY task_id
    """

    out: dict[int, dict[str, int]] = {}
    with connection.cursor() as cur:
        cur.execute(sql, [task_ids])
        for task_id, total_cnt, rated_cnt in cur.fetchall():
            out[int(task_id)] = {
                "total": int(total_cnt or 0),
                "rated": int(rated_cnt or 0),
            }
    return out


def status_view(request):
    tasks = _get_tasks(request)
    stats = _fetch_contacts_stats([int(t.id) for t in tasks])

    for t in tasks:
        t.ui_id = encode_id(int(t.id))
        s = stats.get(int(t.id), {"total": 0, "rated": 0})
        t.contacts_total = int(s.get("total") or 0)
        t.contacts_rated = int(s.get("rated") or 0)

    return render(
        request,
        "panels/aap_audience/status.html",
        {"tasks": tasks},
    )
