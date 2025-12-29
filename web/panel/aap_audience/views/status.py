# FILE: web/panel/aap_audience/views/status.py  (обновлено — 2025-12-29)
# Смысл:
# - status-страница показывает ТОЛЬКО задачи с run_processing=true
# - для каждой задачи подтягивает из public.rate_contacts:
#   * contacts_total: всего контактов по task_id
#   * contacts_rated: контактов с rate_cl IS NOT NULL и hash_task IS NOT NULL
# - ui_id остаётся obfuscated (encode_id)

from __future__ import annotations

from django.db import connection
from django.shortcuts import render

from mailer_web.access import encode_id
from panel.aap_audience.models import AudienceTask


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
            out[int(task_id)] = {"total": int(total_cnt or 0), "rated": int(rated_cnt or 0)}
    return out


def status_view(request):
    ws_id = request.workspace_id
    user = request.user

    if not ws_id or not getattr(user, "is_authenticated", False):
        tasks = AudienceTask.objects.none()
        stats = {}
    else:
        tasks = (
            AudienceTask.objects.filter(workspace_id=ws_id, user=user, run_processing=True)
            .order_by("-created_at")[:50]
        )
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
