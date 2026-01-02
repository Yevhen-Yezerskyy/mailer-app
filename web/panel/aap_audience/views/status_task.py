# FILE: web/panel/aap_audience/views/status_task.py
# DATE: 2026-01-02
# CHANGE:
# - восстановлена логика правой верхней карточки: rating_any_exists / rating_active_exists / criteria_changed
# - добавлены buckets 1-30/31-70/71-100 + проценты
# - сохранена модалка: ui_id = encode_id(rate_contacts.id) в нижних таблицах
# - остальное (таблицы rated/all + paging) без лишних изменений

from __future__ import annotations

import math
from typing import Any

from django.db import connection
from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from engine.common.utils import h64_text
from mailer_web.access import encode_id, resolve_pk_or_redirect
from panel.aap_audience.models import AudienceTask

PAGE_SIZE = 50


def _safe_int(v: Any, default: int = 1) -> int:
    try:
        x = int(str(v or "").strip())
        return x if x > 0 else default
    except Exception:
        return default


def _qall(sql: str, params: list[Any]) -> list[dict]:
    with connection.cursor() as cur:
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _pct(part: int, total: int) -> int:
    if not total:
        return 0
    return int(round((int(part) * 100.0) / float(int(total))))


def _format_contact_rows(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        branches = r.get("branches") or []
        addr_list = r.get("address_list") or []
        out.append(
            {
                "ui_id": encode_id(int(r.get("rate_contact_id") or 0)),  # для модалки (rate_contacts.id)
                "contact_id": int(r.get("contact_id") or 0),
                "company_name": (r.get("company_name") or "").strip(),
                "branches_str": ", ".join(str(x) for x in branches)
                if isinstance(branches, (list, tuple))
                else str(branches),
                "address_first": (addr_list[0] if isinstance(addr_list, (list, tuple)) and addr_list else "") or "",
                "rate_cl": r.get("rate_cl"),
                "rate_cb_100": int(round((float(r.get("rate_cb") or 0) / 100.0)))
                if r.get("rate_cb") is not None
                else None,
            }
        )
    return out


def _fetch_contacts_total_and_rated(task_id: int) -> tuple[int, int]:
    # rated = rate_cl NOT NULL + валидный hash_task
    sql = """
        SELECT
            COUNT(*)::int AS total_cnt,
            SUM(
                CASE
                    WHEN rate_cl IS NOT NULL
                     AND hash_task IS NOT NULL
                     AND hash_task NOT IN (-1,0,1)
                    THEN 1
                    ELSE 0
                END
            )::int AS rated_cnt
        FROM public.rate_contacts
        WHERE task_id = %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, [int(task_id)])
        row = cur.fetchone()
        if not row:
            return 0, 0
        return int(row[0] or 0), int(row[1] or 0)


def _fetch_rated_buckets(task_id: int) -> tuple[int, int, int]:
    sql = """
        SELECT
            SUM(CASE WHEN rate_cl BETWEEN 1 AND 30 THEN 1 ELSE 0 END)::int AS c1,
            SUM(CASE WHEN rate_cl BETWEEN 31 AND 70 THEN 1 ELSE 0 END)::int AS c2,
            SUM(CASE WHEN rate_cl BETWEEN 71 AND 100 THEN 1 ELSE 0 END)::int AS c3
        FROM public.rate_contacts
        WHERE task_id = %s
          AND rate_cl IS NOT NULL
          AND hash_task IS NOT NULL
          AND hash_task NOT IN (-1,0,1)
    """
    with connection.cursor() as cur:
        cur.execute(sql, [int(task_id)])
        row = cur.fetchone() or (0, 0, 0)
        return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)


def _fetch_contacts_rated(task_id: int, *, page: int) -> tuple[int, list[dict]]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.rate_contacts rc
            WHERE rc.task_id = %s
              AND rc.rate_cl IS NOT NULL
              AND rc.hash_task IS NOT NULL
              AND rc.hash_task NOT IN (-1,0,1)
            """,
            [int(task_id)],
        )
        total = int((cur.fetchone() or [0])[0] or 0)

    offset = (page - 1) * PAGE_SIZE
    rows = _qall(
        """
        SELECT
            rc.id AS rate_contact_id,
            rc.contact_id,
            rca.company_name,
            rca.branches,
            rca.address_list,
            rc.rate_cl,
            rc.rate_cb
        FROM public.rate_contacts rc
        JOIN public.raw_contacts_aggr rca ON rca.id = rc.contact_id
        WHERE rc.task_id = %s
          AND rc.rate_cl IS NOT NULL
          AND rc.hash_task IS NOT NULL
          AND rc.hash_task NOT IN (-1,0,1)
        ORDER BY rc.rate_cl ASC, rc.contact_id ASC
        LIMIT %s OFFSET %s
        """,
        [int(task_id), int(PAGE_SIZE), int(offset)],
    )
    return total, _format_contact_rows(rows)


def _fetch_contacts_all(task_id: int, *, page: int) -> tuple[int, list[dict]]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.rate_contacts rc
            WHERE rc.task_id = %s
            """,
            [int(task_id)],
        )
        total = int((cur.fetchone() or [0])[0] or 0)

    offset = (page - 1) * PAGE_SIZE
    rows = _qall(
        """
        SELECT
            rc.id AS rate_contact_id,
            rc.contact_id,
            rca.company_name,
            rca.branches,
            rca.address_list,
            rc.rate_cl,
            rc.rate_cb
        FROM public.rate_contacts rc
        JOIN public.raw_contacts_aggr rca ON rca.id = rc.contact_id
        WHERE rc.task_id = %s
        ORDER BY rc.rate_cb ASC NULLS LAST, rc.contact_id ASC
        LIMIT %s OFFSET %s
        """,
        [int(task_id), int(PAGE_SIZE), int(offset)],
    )
    return total, _format_contact_rows(rows)


def _rating_any_exists(task_id: int) -> bool:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM public.__tasks_rating
            WHERE task_id = %s
              AND type IN ('contacts','contacts_update')
            LIMIT 1
            """,
            [int(task_id)],
        )
        return cur.fetchone() is not None


def _rating_active_exists(task_id: int) -> bool:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM public.__tasks_rating
            WHERE task_id = %s
              AND type IN ('contacts','contacts_update')
              AND done = false
            LIMIT 1
            """,
            [int(task_id)],
        )
        return cur.fetchone() is not None


def _criteria_changed(task_id: int, current_hash: int) -> bool:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM public.rate_contacts
            WHERE task_id = %s
              AND hash_task IS NOT NULL
              AND hash_task NOT IN (-1,0,1)
              AND hash_task IS DISTINCT FROM %s
            LIMIT 1
            """,
            [int(task_id), int(current_hash)],
        )
        return cur.fetchone() is not None


def _rating_insert(task_id: int, type_: str, hash_task: int) -> None:
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.__tasks_rating (task_id, type, hash_task, done, created_at, updated_at)
            VALUES (%s, %s, %s, false, now(), now())
            """,
            [int(task_id), str(type_), int(hash_task)],
        )


def status_task_view(request):
    res = resolve_pk_or_redirect(request, AudienceTask, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res
    pk = int(res)

    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return HttpResponseRedirect("../")

    task = AudienceTask.objects.filter(id=pk, workspace_id=ws_id, user=user).first()
    if task is None:
        return HttpResponseRedirect("../")

    task.ui_id = encode_id(int(task.id))
    current_hash = int(h64_text((task.task or "") + (task.task_client or "")))

    # actions (правая верхняя карточка)
    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "rating_start_contacts":
            if (not _rating_any_exists(int(task.id))) and (not _rating_active_exists(int(task.id))):
                _rating_insert(int(task.id), "contacts", current_hash)
            return redirect(f"{request.path}?id={task.ui_id}")

        if action == "rating_next_1000":
            if _rating_any_exists(int(task.id)) and (not _rating_active_exists(int(task.id))):
                AudienceTask.objects.filter(id=task.id, workspace_id=ws_id, user=user).update(
                    subscribers_limit=int(task.subscribers_limit or 0) + 1000
                )
                _rating_insert(int(task.id), "contacts", current_hash)
            return redirect(f"{request.path}?id={task.ui_id}")

        if action == "rating_start_contacts_update":
            if not _rating_active_exists(int(task.id)):
                _rating_insert(int(task.id), "contacts_update", current_hash)
            return redirect(f"{request.path}?id={task.ui_id}")

    contacts_total, contacts_rated = _fetch_contacts_total_and_rated(int(task.id))

    rated_1_30_cnt = rated_31_70_cnt = rated_71_100_cnt = 0
    rated_1_30_pct = rated_31_70_pct = rated_71_100_pct = 0
    if contacts_rated > 0:
        rated_1_30_cnt, rated_31_70_cnt, rated_71_100_cnt = _fetch_rated_buckets(int(task.id))
        rated_1_30_pct = _pct(rated_1_30_cnt, contacts_rated)
        rated_31_70_pct = _pct(rated_31_70_cnt, contacts_rated)
        rated_71_100_pct = _pct(rated_71_100_cnt, contacts_rated)

    tab = (request.GET.get("tab") or "rated").strip()
    if tab not in ("rated", "all"):
        tab = "rated"

    rated_page = _safe_int(request.GET.get("p_rated"), 1)
    all_page = _safe_int(request.GET.get("p_all"), 1)

    rated_count, rated_rows = _fetch_contacts_rated(int(task.id), page=rated_page)
    all_count, all_rows = _fetch_contacts_all(int(task.id), page=all_page)

    rating_any_exists = _rating_any_exists(int(task.id))
    rating_active_exists = _rating_active_exists(int(task.id))
    criteria_changed = False
    if (not rating_active_exists) and rating_any_exists:
        criteria_changed = _criteria_changed(int(task.id), current_hash)

    return render(
        request,
        "panels/aap_audience/status_task.html",
        {
            "t": task,
            "contacts_total": contacts_total,
            "contacts_rated": contacts_rated,
            "rated_1_30_cnt": rated_1_30_cnt,
            "rated_31_70_cnt": rated_31_70_cnt,
            "rated_71_100_cnt": rated_71_100_cnt,
            "rated_1_30_pct": rated_1_30_pct,
            "rated_31_70_pct": rated_31_70_pct,
            "rated_71_100_pct": rated_71_100_pct,
            "rating_any_exists": rating_any_exists,
            "rating_active_exists": rating_active_exists,
            "criteria_changed": criteria_changed,
            "tab": tab,
            "rated_rows": rated_rows,
            "all_rows": all_rows,
            "rated_count": rated_count,
            "all_count": all_count,
            "rated_page": rated_page,
            "all_page": all_page,
            "rated_pages": max(1, int(math.ceil(rated_count / float(PAGE_SIZE))) if rated_count else 1),
            "all_pages": max(1, int(math.ceil(all_count / float(PAGE_SIZE))) if all_count else 1),
            "page_size": PAGE_SIZE,
        },
    )
