# FILE: web/panel/aap_audience/views/status.py
# DATE: 2026-01-01
# CHANGE:
# - status list: добавлены
#   * rating_active (есть активный __tasks_rating contacts/contacts_update)
#   * contacts_rated (валидный hash_task) + buckets 1-30/31-70/71-100 + проценты
#   * criteria_changed (есть rate_contacts.hash_task != current_hash для хоть одной записи)
# - archived=false фильтр сохранён, остальная логика страницы сохранена

from __future__ import annotations

from django.db import connection
from django.shortcuts import render

from engine.common.utils import h64_text
from mailer_web.access import encode_id
from panel.aap_audience.models import AudienceTask


def _get_tasks(request):
    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return AudienceTask.objects.none()
    return (
        AudienceTask.objects.filter(
            workspace_id=ws_id,
            user=user,
            run_processing=True,
            archived=False,
        ).order_by("-created_at")
    )


def _pct(part: int, total: int) -> int:
    if not total:
        return 0
    return int(round((int(part) * 100.0) / float(int(total))))


def _fetch_contacts_stats_and_buckets(task_ids: list[int]) -> dict[int, dict[str, int]]:
    """
    Returns:
      { task_id: {
          "total": int,
          "rated": int,                 # rate_cl NOT NULL + валидный hash_task
          "b1": int, "b2": int, "b3": int,
        }, ... }
    """
    if not task_ids:
        return {}

    sql = """
        SELECT
            task_id,

            COUNT(*)::int AS total_cnt,

            SUM(
                CASE
                    WHEN rate_cl IS NOT NULL
                     AND hash_task IS NOT NULL
                     AND hash_task NOT IN (-1,0,1)
                    THEN 1
                    ELSE 0
                END
            )::int AS rated_cnt,

            SUM(
                CASE
                    WHEN rate_cl BETWEEN 1 AND 30
                     AND hash_task IS NOT NULL
                     AND hash_task NOT IN (-1,0,1)
                    THEN 1
                    ELSE 0
                END
            )::int AS b1_cnt,

            SUM(
                CASE
                    WHEN rate_cl BETWEEN 31 AND 70
                     AND hash_task IS NOT NULL
                     AND hash_task NOT IN (-1,0,1)
                    THEN 1
                    ELSE 0
                END
            )::int AS b2_cnt,

            SUM(
                CASE
                    WHEN rate_cl BETWEEN 71 AND 100
                     AND hash_task IS NOT NULL
                     AND hash_task NOT IN (-1,0,1)
                    THEN 1
                    ELSE 0
                END
            )::int AS b3_cnt

        FROM public.rate_contacts
        WHERE task_id = ANY(%s::bigint[])
        GROUP BY task_id
    """

    out: dict[int, dict[str, int]] = {}
    with connection.cursor() as cur:
        cur.execute(sql, [task_ids])
        for task_id, total_cnt, rated_cnt, b1_cnt, b2_cnt, b3_cnt in cur.fetchall():
            out[int(task_id)] = {
                "total": int(total_cnt or 0),
                "rated": int(rated_cnt or 0),
                "b1": int(b1_cnt or 0),
                "b2": int(b2_cnt or 0),
                "b3": int(b3_cnt or 0),
            }
    return out


def _fetch_rating_active(task_ids: list[int]) -> set[int]:
    if not task_ids:
        return set()

    sql = """
        SELECT DISTINCT task_id::int
        FROM public.__tasks_rating
        WHERE task_id = ANY(%s::int[])
          AND type IN ('contacts','contacts_update')
          AND done = false
    """
    with connection.cursor() as cur:
        cur.execute(sql, [task_ids])
        return {int(r[0]) for r in cur.fetchall()}


def _fetch_criteria_changed(pairs: list[tuple[int, int]]) -> set[int]:
    """
    pairs: [(task_id, current_hash), ...]
    Возвращает task_id где найден хотя бы 1 валидный rc.hash_task != current_hash
    """
    if not pairs:
        return set()

    values_sql = ", ".join(["(%s,%s)"] * len(pairs))
    params: list[int] = []
    for task_id, ch in pairs:
        params.extend((int(task_id), int(ch)))

    sql = f"""
        WITH hashes(task_id, current_hash) AS (VALUES {values_sql})
        SELECT DISTINCT rc.task_id::int
        FROM public.rate_contacts rc
        JOIN hashes h ON h.task_id = rc.task_id
        WHERE rc.hash_task IS NOT NULL
          AND rc.hash_task NOT IN (-1,0,1)
          AND rc.hash_task IS DISTINCT FROM h.current_hash
    """

    with connection.cursor() as cur:
        cur.execute(sql, params)
        return {int(r[0]) for r in cur.fetchall()}


def status_view(request):
    tasks = _get_tasks(request)

    task_ids = [int(t.id) for t in tasks]
    stats = _fetch_contacts_stats_and_buckets(task_ids)

    pairs: list[tuple[int, int]] = []
    for t in tasks:
        pairs.append((int(t.id), int(h64_text((t.task or "") + (t.task_client or "")))))

    rating_active_ids = _fetch_rating_active(task_ids)
    criteria_changed_ids = _fetch_criteria_changed(pairs)

    for t in tasks:
        t.ui_id = encode_id(int(t.id))

        s = stats.get(int(t.id), {"total": 0, "rated": 0, "b1": 0, "b2": 0, "b3": 0})
        t.contacts_total = int(s.get("total") or 0)
        t.contacts_rated = int(s.get("rated") or 0)

        t.rated_1_30_cnt = int(s.get("b1") or 0)
        t.rated_31_70_cnt = int(s.get("b2") or 0)
        t.rated_71_100_cnt = int(s.get("b3") or 0)

        t.rated_1_30_pct = _pct(t.rated_1_30_cnt, t.contacts_rated)
        t.rated_31_70_pct = _pct(t.rated_31_70_cnt, t.contacts_rated)
        t.rated_71_100_pct = _pct(t.rated_71_100_cnt, t.contacts_rated)

        t.rating_active = int(t.id) in rating_active_ids
        t.criteria_changed = int(t.id) in criteria_changed_ids

    return render(
        request,
        "panels/aap_audience/status.html",
        {"tasks": tasks},
    )
