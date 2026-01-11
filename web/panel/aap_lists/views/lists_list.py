# FILE: web/panel/aap_lists/views/lists_list.py
# DATE: 2026-01-11
# PURPOSE: /panel/lists/lists/list/?id=... — управление конкретным списком рассылки.
# CHANGE:
# - POST-экшены НЕ сбрасывают mode/поиск/страницу: редирект на request.get_full_path()
# - clear_search сбрасывает только поиск, но сохраняет id+mode
# - убран весь мусор с подменой request.GET / dummy resolve_pk (используем contact_id напрямую)

from __future__ import annotations

import math
from typing import Any, Optional

from django.db import connection
from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from mailer_web.access import encode_id, resolve_pk_or_redirect
from mailer_web.format_data import build_contact_packet
from panel.aap_audience.models import AudienceTask
from panel.aap_lists.models import MailingList

PAGE_SIZE = 50
DEFAULT_RATE_MAX = 50


def _safe_int(v: Any, default: int) -> int:
    try:
        x = int(str(v or "").strip())
        return x if x > 0 else default
    except Exception:
        return default


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None, None
    return ws_id, user


def _get_list_or_redirect(request, ws_id, user):
    if not request.GET.get("id"):
        return HttpResponseRedirect("../")

    res = resolve_pk_or_redirect(request, MailingList, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res

    obj = (
        MailingList.objects.filter(id=int(res), workspace_id=ws_id, user=user, archived=False)
        .prefetch_related("audience_tasks")
        .first()
    )
    if obj is None:
        return HttpResponseRedirect("../")
    return obj


def _get_one_task(ml: MailingList) -> Optional[AudienceTask]:
    return ml.audience_tasks.all().first()


def _list_active_cnt(list_id: int) -> int:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.lists_contacts
            WHERE list_id = %s AND active = true
            """,
            [int(list_id)],
        )
        return int((cur.fetchone() or [0])[0] or 0)


def _ws_upsert_many(ws_id, contact_ids: list[int]) -> None:
    if not contact_ids:
        return
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.ws_contacts (workspace_id, contact_id, active, reason, created_at)
            SELECT %s::uuid, x::bigint, true, NULL, now()
            FROM unnest(%s::bigint[]) AS x
            ON CONFLICT (workspace_id, contact_id)
            DO UPDATE SET active = EXCLUDED.active
            """,
            [ws_id, contact_ids],
        )


def _lists_insert_many(list_id: int, contact_ids: list[int]) -> None:
    if not contact_ids:
        return
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.lists_contacts (list_id, contact_id, active, reason, added_at)
            SELECT %s::bigint, x::bigint, true, NULL, now()
            FROM unnest(%s::bigint[]) AS x
            ON CONFLICT (list_id, contact_id)
            DO UPDATE SET active = EXCLUDED.active
            """,
            [int(list_id), contact_ids],
        )


def _lists_delete_many(list_id: int, contact_ids: list[int]) -> None:
    if not contact_ids:
        return
    with connection.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.lists_contacts
            WHERE list_id = %s AND contact_id = ANY(%s::bigint[])
            """,
            [int(list_id), contact_ids],
        )


def _lists_set_active(list_id: int, contact_ids: list[int], active: bool) -> None:
    if not contact_ids:
        return
    with connection.cursor() as cur:
        cur.execute(
            """
            UPDATE public.lists_contacts
            SET active = %s, reason = NULL
            WHERE list_id = %s AND contact_id = ANY(%s::bigint[])
            """,
            [bool(active), int(list_id), contact_ids],
        )


def _bulk_add_by_rate(task_id: int, list_id: int, ws_id, *, rate_max: int) -> None:
    # membership (upsert active=true)
    with connection.cursor() as cur:
        cur.execute(
            """
            WITH candidates AS (
              SELECT DISTINCT rc.contact_id::bigint AS contact_id
              FROM public.rate_contacts rc
              WHERE rc.task_id = %s
                AND rc.rate_cl IS NOT NULL
                AND rc.rate_cl BETWEEN 1 AND %s
            )
            INSERT INTO public.lists_contacts (list_id, contact_id, active, reason, added_at)
            SELECT %s::bigint, c.contact_id, true, NULL, now()
            FROM candidates c
            ON CONFLICT (list_id, contact_id)
            DO UPDATE SET active = EXCLUDED.active
            """,
            [int(task_id), int(rate_max), int(list_id)],
        )

    # ws catalog (upsert active=true)
    with connection.cursor() as cur:
        cur.execute(
            """
            WITH candidates AS (
              SELECT DISTINCT rc.contact_id::bigint AS contact_id
              FROM public.rate_contacts rc
              WHERE rc.task_id = %s
                AND rc.rate_cl IS NOT NULL
                AND rc.rate_cl BETWEEN 1 AND %s
            )
            INSERT INTO public.ws_contacts (workspace_id, contact_id, active, reason, created_at)
            SELECT %s::uuid, c.contact_id, true, NULL, now()
            FROM candidates c
            ON CONFLICT (workspace_id, contact_id)
            DO UPDATE SET active = EXCLUDED.active
            """,
            [int(task_id), int(rate_max), ws_id],
        )


def _fetch_branch_ids_by_text(q: str, ui_lang: str, limit: int = 200) -> list[int]:
    s = (q or "").strip()
    if not s:
        return []

    lang = (ui_lang or "ru").strip().lower()
    pat = f"%{s}%"

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT b.id::int
            FROM public.gb_branches b
            LEFT JOIN public.gb_branch_i18n i
              ON i.branch_id = b.id AND i.lang = %s
            WHERE b.name ILIKE %s
               OR i.name_trans ILIKE %s
            ORDER BY b.id ASC
            LIMIT %s
            """,
            [lang, pat, pat, int(limit)],
        )
        return [int(r[0]) for r in cur.fetchall()]


def _packets_for_rc_ids(rc_ids: list[int], *, ui_lang: str) -> list[dict]:
    out: list[dict] = []
    for rc_id in rc_ids:
        p = build_contact_packet(int(rc_id), ui_lang)
        p["ui_id"] = encode_id(int(rc_id))  # для модалки
        out.append(p)
    return out


def _fetch_rows(
    *,
    task_id: int,
    list_id: int,
    mode: str,  # "list" | "audience"
    page: int,
    ui_lang: str,
    q_email: str,
    q_company: str,
    q_branch: str,
    q_addr: str,
    q_plz: str,
    in_list_only: bool,
    search_active: bool,
) -> tuple[int, list[dict]]:
    branch_ids = _fetch_branch_ids_by_text(q_branch, ui_lang) if q_branch else []

    where = ["rc.task_id = %s"]
    params: list[Any] = [int(task_id)]

    # режимы (работают только если поиск не активен)
    if not search_active:
        if mode == "list":
            where.append("lc.contact_id IS NOT NULL")
        else:  # audience
            where.append("lc.contact_id IS NULL")
    else:
        # поиск: если in_list_only включен — ограничиваемся членами списка
        if in_list_only:
            where.append("lc.contact_id IS NOT NULL")

    need_join_aggr = False
    if q_email:
        need_join_aggr = True
        where.append("ra.email ILIKE %s")
        params.append(f"%{q_email.strip()}%")

    if q_company:
        need_join_aggr = True
        where.append("ra.company_name ILIKE %s")
        params.append(f"%{q_company.strip()}%")

    if q_plz:
        need_join_aggr = True
        where.append("EXISTS (SELECT 1 FROM unnest(ra.plz_list) p WHERE p ILIKE %s)")
        params.append(f"%{q_plz.strip()}%")

    if q_addr:
        need_join_aggr = True
        where.append("EXISTS (SELECT 1 FROM unnest(ra.address_list) a WHERE a ILIKE %s)")
        params.append(f"%{q_addr.strip()}%")

    if branch_ids:
        need_join_aggr = True
        where.append("ra.branches && %s::int[]")
        params.append(branch_ids)

    where_sql = " AND ".join(where)

    join_aggr_sql = "JOIN public.raw_contacts_aggr ra ON ra.id = rc.contact_id" if need_join_aggr else ""
    join_lc_sql = "LEFT JOIN public.lists_contacts lc ON lc.list_id = %s AND lc.contact_id = rc.contact_id"
    params_with_list = [int(list_id)] + params

    # total
    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*)::int
            FROM public.rate_contacts rc
            {join_lc_sql}
            {join_aggr_sql}
            WHERE {where_sql}
            """,
            params_with_list,
        )
        total = int((cur.fetchone() or [0])[0] or 0)

    offset = (int(page) - 1) * PAGE_SIZE

    # ids + membership flags
    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              rc.id::bigint,
              rc.contact_id::bigint,
              (lc.contact_id IS NOT NULL) AS in_list,
              lc.active AS list_active
            FROM public.rate_contacts rc
            {join_lc_sql}
            {join_aggr_sql}
            WHERE {where_sql}
            ORDER BY
              (rc.rate_cl IS NULL) ASC,
              rc.rate_cl ASC,
              rc.rate_cb ASC NULLS LAST,
              rc.contact_id ASC
            LIMIT %s OFFSET %s
            """,
            params_with_list + [int(PAGE_SIZE), int(offset)],
        )
        rows = cur.fetchall()

    rc_ids = [int(r[0]) for r in rows]
    packets = _packets_for_rc_ids(rc_ids, ui_lang=ui_lang)

    meta_by_rc: dict[int, dict[str, Any]] = {}
    for rc_id, contact_id, in_list, list_active in rows:
        meta_by_rc[int(rc_id)] = {
            "contact_id": int(contact_id),
            "in_list": bool(in_list),
            "list_active": None if list_active is None else bool(list_active),
        }

    out: list[dict] = []
    for p in packets:
        ratings = p.get("ratings") or {}
        rid = ratings.get("rate_contact_id")
        if rid is None:
            continue
        p["meta"] = meta_by_rc.get(int(rid), {"contact_id": None, "in_list": False, "list_active": None})
        out.append(p)

    return total, out


def lists_list_view(request):
    ws_id, user = _guard(request)
    if not ws_id:
        return redirect("/")

    ml = _get_list_or_redirect(request, ws_id, user)
    if isinstance(ml, HttpResponseRedirect):
        return ml
    ml.ui_id = encode_id(int(ml.id))

    task = _get_one_task(ml)
    if task is None:
        return redirect("/panel/lists/lists/")
    task.ui_id = encode_id(int(task.id))

    ui_lang = getattr(request, "LANGUAGE_CODE", "") or "ru"

    def _get_mode() -> str:
        m = (request.GET.get("mode") or "list").strip().lower()
        return m if m in ("list", "audience") else "list"

    # -----------------------
    # POST actions (важно: НЕ сбрасываем текущий mode/поиск/страницу)
    # -----------------------
    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        def _redir_same():
            return redirect(request.get_full_path())

        def _redir_clean_search():
            # сбрасываем только поиск/страницу/галки поиска; сохраняем id+mode
            mode = _get_mode()
            return redirect(f"{request.path}?id={request.GET.get('id') or ''}&mode={mode}")

        if action == "bulk_add_by_rate":
            rate_max = _safe_int(request.POST.get("rate_max"), DEFAULT_RATE_MAX)
            if rate_max > 100:
                rate_max = 100
            _bulk_add_by_rate(int(task.id), int(ml.id), ws_id, rate_max=rate_max)
            return _redir_same()

        if action == "clear_search":
            return _redir_clean_search()

        # row actions (работаем по contact_id)
        cid_s = (request.POST.get("contact_id") or "").strip()
        contact_id = None
        if cid_s:
            try:
                contact_id = int(cid_s)
            except Exception:
                contact_id = None

        if action in ("include", "exclude", "subscribe", "unsubscribe") and contact_id:
            if action == "include":
                _lists_insert_many(int(ml.id), [int(contact_id)])
                _ws_upsert_many(ws_id, [int(contact_id)])
            elif action == "exclude":
                _lists_delete_many(int(ml.id), [int(contact_id)])
            elif action == "subscribe":
                _lists_set_active(int(ml.id), [int(contact_id)], True)
            elif action == "unsubscribe":
                _lists_set_active(int(ml.id), [int(contact_id)], False)
            return _redir_same()

        return _redir_same()

    # -----------------------
    # GET state
    # -----------------------
    mode = _get_mode()
    page = _safe_int(request.GET.get("p"), 1)

    q_email = (request.GET.get("q_email") or "").strip()
    q_company = (request.GET.get("q_company") or "").strip()
    q_branch = (request.GET.get("q_branch") or "").strip()
    q_addr = (request.GET.get("q_addr") or "").strip()
    q_plz = (request.GET.get("q_plz") or "").strip()

    # default checked, но только если параметр вообще есть (чтобы "по умолчанию checked" работал)
    if "in_list" in request.GET:
        in_list_only = (request.GET.get("in_list") or "").strip() in ("1", "true", "on", "yes")
    else:
        in_list_only = True

    search_active = any([q_email, q_company, q_branch, q_addr, q_plz]) or ("in_list" in request.GET)

    total, rows = _fetch_rows(
        task_id=int(task.id),
        list_id=int(ml.id),
        mode=mode,
        page=page,
        ui_lang=ui_lang,
        q_email=q_email,
        q_company=q_company,
        q_branch=q_branch,
        q_addr=q_addr,
        q_plz=q_plz,
        in_list_only=bool(in_list_only),
        search_active=bool(search_active),
    )

    pages = max(1, int(math.ceil(total / float(PAGE_SIZE))) if total else 1)
    list_active_cnt = _list_active_cnt(int(ml.id))

    return render(
        request,
        "panels/aap_lists/lists_list.html",
        {
            "ml": ml,
            "task": task,
            "list_active_cnt": list_active_cnt,
            "mode": mode,
            "rows": rows,
            "count": total,
            "page": page,
            "pages": pages,
            "page_size": PAGE_SIZE,
            "q_email": q_email,
            "q_company": q_company,
            "q_branch": q_branch,
            "q_addr": q_addr,
            "q_plz": q_plz,
            "in_list_only": in_list_only,
            "search_active": search_active,
            "default_rate_max": DEFAULT_RATE_MAX,
        },
    )
