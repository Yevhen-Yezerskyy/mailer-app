# FILE: web/panel/aap_audience/views/status_task.py  (обновлено — 2026-01-12)
# CHANGE:
# - Убрана логика "начать рейтингование".
# - Добавлены кнопки "+100/+200/+500/+1000" (только когда рейтингование не запущено и criteria_changed=false).
# - Добавлена кнопка "Удалить все рейтинги" (всегда доступна): rate_cl/hash_task -> NULL; все __tasks_rating -> done=true; subscribers_limit -> 0.
# - Кнопки бакетов (1-30/31-70/71-100) сделаны ссылками на нужную страницу вкладки rated.
# - В выборке строк для таблиц добавлены rc.rate_cb/rc.rate_cl и прокинуты в build_contact_packet(..., rate_cb=..., rate_cl=...)
#   для корректного ключа кеша format_data.

from __future__ import annotations

import math
from typing import Any, Optional

from django.db import connection
from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from engine.common.utils import h64_text
from mailer_web.access import encode_id, resolve_pk_or_redirect
from mailer_web.format_data import build_contact_packet
from panel.aap_audience.models import AudienceTask

PAGE_SIZE = 50


def _safe_int(v: Any, default: int = 1) -> int:
    try:
        x = int(str(v or "").strip())
        return x if x > 0 else default
    except Exception:
        return default


def _pct(part: int, total: int) -> int:
    if not total:
        return 0
    return int(round((int(part) * 100.0) / float(int(total))))


def _fetch_contacts_total_and_rated(task_id: int) -> tuple[int, int]:
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


def _count_rated_lt(task_id: int, *, lt_rate: int) -> int:
    sql = """
        SELECT COUNT(*)::int
        FROM public.rate_contacts rc
        WHERE rc.task_id = %s
          AND rc.rate_cl IS NOT NULL
          AND rc.hash_task IS NOT NULL
          AND rc.hash_task NOT IN (-1,0,1)
          AND rc.rate_cl < %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, [int(task_id), int(lt_rate)])
        return int((cur.fetchone() or [0])[0] or 0)


def _page_for_offset(offset_cnt: int) -> int:
    if offset_cnt <= 0:
        return 1
    return int(1 + (int(offset_cnt) // int(PAGE_SIZE)))


def _packets_for_rows(rows: list[tuple[int, Any, Any]], *, ui_lang: str) -> list[dict]:
    out: list[dict] = []
    for rc_id, rate_cb, rate_cl in rows:
        p = build_contact_packet(int(rc_id), ui_lang, rate_cb=rate_cb, rate_cl=rate_cl)
        p["ui_id"] = encode_id(int(rc_id))
        out.append(p)
    return out


def _fetch_contacts_rated(task_id: int, *, page: int, ui_lang: str) -> tuple[int, list[dict]]:
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
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT rc.id::bigint, rc.rate_cb, rc.rate_cl
            FROM public.rate_contacts rc
            WHERE rc.task_id = %s
              AND rc.rate_cl IS NOT NULL
              AND rc.hash_task IS NOT NULL
              AND rc.hash_task NOT IN (-1,0,1)
            ORDER BY rc.rate_cl ASC, rc.contact_id ASC
            LIMIT %s OFFSET %s
            """,
            [int(task_id), int(PAGE_SIZE), int(offset)],
        )
        rows = [(int(r[0]), r[1], r[2]) for r in (cur.fetchall() or [])]

    return total, _packets_for_rows(rows, ui_lang=ui_lang)


def _fetch_contacts_all(task_id: int, *, page: int, ui_lang: str) -> tuple[int, list[dict]]:
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
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT rc.id::bigint, rc.rate_cb, rc.rate_cl
            FROM public.rate_contacts rc
            WHERE rc.task_id = %s
            ORDER BY rc.rate_cb ASC NULLS LAST, rc.contact_id ASC
            LIMIT %s OFFSET %s
            """,
            [int(task_id), int(PAGE_SIZE), int(offset)],
        )
        rows = [(int(r[0]), r[1], r[2]) for r in (cur.fetchall() or [])]

    return total, _packets_for_rows(rows, ui_lang=ui_lang)


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


def _ratings_clear(task_id: int) -> None:
    with connection.cursor() as cur:
        cur.execute(
            """
            UPDATE public.rate_contacts
            SET rate_cl = NULL,
                hash_task = NULL,
                updated_at = now()
            WHERE task_id = %s
            """,
            [int(task_id)],
        )
        cur.execute(
            """
            UPDATE public.__tasks_rating
            SET done = true,
                updated_at = now()
            WHERE task_id = %s
              AND type IN ('contacts','contacts_update')
              AND done = false
            """,
            [int(task_id)],
        )
        cur.execute(
            """
            UPDATE public.aap_audience_audiencetask
            SET subscribers_limit = 0,
                updated_at = now()
            WHERE id = %s
            """,
            [int(task_id)],
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

    ui_lang = getattr(request, "LANGUAGE_CODE", "") or "ru"

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "rating_clear_ratings":
            _ratings_clear(int(task.id))
            return redirect(f"{request.path}?id={task.ui_id}")

        if action in ("rating_add_100", "rating_add_200", "rating_add_500", "rating_add_1000"):
            add = 0
            if action == "rating_add_100":
                add = 100
            elif action == "rating_add_200":
                add = 200
            elif action == "rating_add_500":
                add = 500
            elif action == "rating_add_1000":
                add = 1000

            active_now = _rating_active_exists(int(task.id))
            any_now = _rating_any_exists(int(task.id))
            changed_now = False
            if (not active_now) and any_now:
                changed_now = _criteria_changed(int(task.id), current_hash)

            if (not active_now) and (not changed_now) and add > 0:
                AudienceTask.objects.filter(id=task.id, workspace_id=ws_id, user=user).update(
                    subscribers_limit=int(task.subscribers_limit or 0) + int(add)
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
    bucket_1_30_page = bucket_31_70_page = bucket_71_100_page = 1

    if contacts_rated > 0:
        rated_1_30_cnt, rated_31_70_cnt, rated_71_100_cnt = _fetch_rated_buckets(int(task.id))
        rated_1_30_pct = _pct(rated_1_30_cnt, contacts_rated)
        rated_31_70_pct = _pct(rated_31_70_cnt, contacts_rated)
        rated_71_100_pct = _pct(rated_71_100_cnt, contacts_rated)

        bucket_1_30_page = _page_for_offset(_count_rated_lt(int(task.id), lt_rate=1))
        bucket_31_70_page = _page_for_offset(_count_rated_lt(int(task.id), lt_rate=31))
        bucket_71_100_page = _page_for_offset(_count_rated_lt(int(task.id), lt_rate=71))

    tab = (request.GET.get("tab") or "rated").strip()
    if tab not in ("rated", "all"):
        tab = "rated"

    rated_page = _safe_int(request.GET.get("p_rated"), 1)
    all_page = _safe_int(request.GET.get("p_all"), 1)

    rated_count, rated_rows = _fetch_contacts_rated(int(task.id), page=rated_page, ui_lang=ui_lang)
    all_count, all_rows = _fetch_contacts_all(int(task.id), page=all_page, ui_lang=ui_lang)

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
            "bucket_1_30_page": bucket_1_30_page,
            "bucket_31_70_page": bucket_31_70_page,
            "bucket_71_100_page": bucket_71_100_page,
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
