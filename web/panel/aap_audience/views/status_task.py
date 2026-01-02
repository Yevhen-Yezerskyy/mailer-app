# FILE: web/panel/aap_audience/views/status_task.py
# DATE: 2026-01-02
# CHANGE:
# - rows для нижних таблиц: добавлен ui_id = encode_id(rate_contacts.id)
# - SELECT: добавлен rc.id AS rate_contact_id
# - остальное не трогал

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


def _format_contact_rows(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        branches = r.get("branches") or []
        addr_list = r.get("address_list") or []
        out.append(
            {
                "ui_id": encode_id(int(r.get("rate_contact_id") or 0)),  # NEW: для модалки
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


def _fetch_contacts_stats(task_id: int) -> tuple[int, int]:
    sql = """
        SELECT
            COUNT(*)::int AS total_cnt,
            SUM(
                CASE
                    WHEN rate_cl IS NOT NULL AND hash_task IS NOT NULL THEN 1
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


def _fetch_contacts_rated(task_id: int, *, page: int) -> tuple[int, list[dict]]:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.rate_contacts rc
            WHERE rc.task_id = %s
              AND rc.rate_cl IS NOT NULL
              AND rc.hash_task IS NOT NULL
            """,
            [int(task_id)],
        )
        total = int((cur.fetchone() or [0])[0] or 0)

    offset = (page - 1) * PAGE_SIZE
    rows = _qall(
        """
        SELECT
            rc.id AS rate_contact_id,  -- NEW
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
            rc.id AS rate_contact_id,  -- NEW
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


def _contacts_rating_exists(task_id: int) -> bool:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM public.__tasks_rating
            WHERE task_id = %s
              AND type = 'contacts'
              AND done = false
            LIMIT 1
            """,
            [int(task_id)],
        )
        return cur.fetchone() is not None


def status_task_view(request):
    res = resolve_pk_or_redirect(request, AudienceTask, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res
    pk = int(res)

    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return HttpResponseRedirect("../")

    try:
        t = AudienceTask.objects.get(id=pk, workspace_id=ws_id, user=user)
    except Exception:
        return HttpResponseRedirect("../")

    t.ui_id = encode_id(int(t.id))

    if request.method == "POST" and request.POST.get("action") == "start_contacts":
        if not _contacts_rating_exists(int(t.id)):
            hash_task = int(h64_text((t.task or "") + (t.task_client or "")))
            with connection.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.__tasks_rating (task_id, type, hash_task, done, created_at, updated_at)
                    VALUES (%s, 'contacts', %s, false, now(), now())
                    """,
                    [int(t.id), int(hash_task)],
                )
        return redirect(f"{request.path}?id={t.ui_id}")

    contacts_total, contacts_rated = _fetch_contacts_stats(int(t.id))

    tab = (request.GET.get("tab") or "rated").strip()
    if tab not in ("rated", "all"):
        tab = "rated"

    rated_page = _safe_int(request.GET.get("p_rated"), 1)
    all_page = _safe_int(request.GET.get("p_all"), 1)

    rated_count, rated_rows = _fetch_contacts_rated(int(t.id), page=rated_page)
    all_count, all_rows = _fetch_contacts_all(int(t.id), page=all_page)

    contacts_rating_exists = _contacts_rating_exists(int(t.id))

    return render(
        request,
        "panels/aap_audience/status_task.html",
        {
            "t": t,
            "contacts_total": contacts_total,
            "contacts_rated": contacts_rated,
            "contacts_rating_exists": contacts_rating_exists,
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
