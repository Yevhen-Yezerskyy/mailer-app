# FILE: web/panel/aap_lists/views/contacts.py
# DATE: 2026-01-12
# PURPOSE: /panel/lists/contacts/ — контакты workspace (ws_contacts) + "букет" + поиск + пагинация.
# CHANGE:
# - добавлен фильтр "Неподписанные" (?unsub=1) = wc.active=false
# - "букет" сверху слева: только неархивные списки
# - в таблице branches_html (2-я колонка)
# - "Забыть" показываем только если контакт ни в каких списках ИЛИ только в архивных списках

from __future__ import annotations

import math
from typing import Any, Optional

from django.db import connection
from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from mailer_web.access import encode_id, resolve_pk_or_redirect
from mailer_web.format_data import get_contact

PAGE_SIZE = 50


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


def _lang_candidates(ui_lang: str) -> list[str]:
    x = (ui_lang or "").strip().lower()
    if not x:
        return ["ru", "rus", "en", "eng", "de", "deu"]
    if x in ("ru", "rus"):
        return ["ru", "rus"]
    if x in ("en", "eng"):
        return ["en", "eng"]
    if x in ("de", "deu"):
        return ["de", "deu"]
    if x in ("uk", "ukr"):
        return ["uk", "ukr"]
    return [x]


def _fetch_branch_ids_by_text(q: str, ui_lang: str, limit: int = 400) -> list[int]:
    s = (q or "").strip()
    if not s:
        return []

    pat = f"%{s}%"
    langs = _lang_candidates(ui_lang)

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT b.id::int
            FROM public.gb_branches b
            LEFT JOIN public.gb_branch_i18n i
              ON i.branch_id = b.id AND i.lang = ANY(%s::text[])
            WHERE b.name ILIKE %s
               OR i.name_original ILIKE %s
               OR i.name_trans ILIKE %s
            ORDER BY b.id ASC
            LIMIT %s
            """,
            [langs, pat, pat, pat, int(limit)],
        )
        return [int(r[0]) for r in (cur.fetchall() or [])]


def _qs_params_from_request(request) -> dict:
    return {
        "q_email": (request.GET.get("q_email") or "").strip(),
        "q_company": (request.GET.get("q_company") or "").strip(),
        "q_branch": (request.GET.get("q_branch") or "").strip(),
        "q_addr": (request.GET.get("q_addr") or "").strip(),
        "q_plz": (request.GET.get("q_plz") or "").strip(),
    }


def _is_search_active(q: dict) -> bool:
    return any([q.get("q_email"), q.get("q_company"), q.get("q_branch"), q.get("q_addr"), q.get("q_plz")])


def _fetch_lists_for_workspace(ws_id) -> list[dict]:
    # ВАЖНО: в "букете" показываем ТОЛЬКО неархивные списки
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT id::bigint, title::text, created_at
            FROM public.aap_lists_mailinglist
            WHERE workspace_id = %s::uuid
              AND archived = false
            ORDER BY created_at DESC
            """,
            [ws_id],
        )
        out: list[dict] = []
        for lid, title, _created_at in cur.fetchall() or []:
            out.append(
                {
                    "id": int(lid),
                    "ui_id": encode_id(int(lid)),
                    "title": (title or "").strip() or str(int(lid)),
                }
            )
        return out


def _fetch_counts_by_list(ws_id, list_ids: list[int]) -> dict[int, int]:
    if not list_ids:
        return {}
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT lc.list_id::bigint, COUNT(*)::int
            FROM public.lists_contacts lc
            JOIN public.aap_lists_mailinglist ml
              ON ml.id = lc.list_id AND ml.workspace_id = %s::uuid
            WHERE lc.list_id = ANY(%s::bigint[])
            GROUP BY lc.list_id
            """,
            [ws_id, list_ids],
        )
        return {int(r[0]): int(r[1] or 0) for r in (cur.fetchall() or [])}


def _fetch_count_no_list(ws_id) -> int:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.ws_contacts wc
            WHERE wc.workspace_id = %s::uuid
              AND NOT EXISTS (
                SELECT 1
                FROM public.lists_contacts lc
                JOIN public.aap_lists_mailinglist ml
                  ON ml.id = lc.list_id AND ml.workspace_id = wc.workspace_id
                WHERE lc.contact_id = wc.contact_id
              )
            """,
            [ws_id],
        )
        return int((cur.fetchone() or [0])[0] or 0)


def _fetch_count_unsub(ws_id) -> int:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM public.ws_contacts wc
            WHERE wc.workspace_id = %s::uuid
              AND wc.active = false
            """,
            [ws_id],
        )
        return int((cur.fetchone() or [0])[0] or 0)


def _decode_list_id_or_redirect(request):
    """
    list в query передаётся как encode_id от aap_lists_mailinglist.id
    """
    if not request.GET.get("list"):
        return None

    from panel.aap_lists.models import MailingList  # локальный import

    res = resolve_pk_or_redirect(request, MailingList, param="list")
    if isinstance(res, HttpResponseRedirect):
        return res
    return int(res)


def _fetch_lists_for_contacts(ws_id, contact_ids: list[int]) -> dict[int, list[dict]]:
    """
    Возвращает для каждого contact_id список списков (включая архивные).
    """
    if not contact_ids:
        return {}

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              lc.contact_id::bigint,
              ml.title::text,
              ml.archived::bool
            FROM public.lists_contacts lc
            JOIN public.aap_lists_mailinglist ml
              ON ml.id = lc.list_id AND ml.workspace_id = %s::uuid
            WHERE lc.contact_id = ANY(%s::bigint[])
            ORDER BY ml.archived ASC, ml.created_at DESC, ml.id DESC
            """,
            [ws_id, contact_ids],
        )

        out: dict[int, list[dict]] = {}
        for cid, title, archived in cur.fetchall() or []:
            t = (title or "").strip()
            if not t:
                continue
            out.setdefault(int(cid), []).append({"title": t, "archived": bool(archived)})
        return out


def _lists_str(items: list[dict]) -> str:
    if not items:
        return ""
    parts: list[str] = []
    for it in items:
        t = (it.get("title") or "").strip()
        if not t:
            continue
        if bool(it.get("archived")):
            parts.append(f"{t} (архив)")
        else:
            parts.append(t)
    return ", ".join(parts)


def _forget_allowed(items: list[dict]) -> bool:
    # показывать кнопку "Забыть" только если:
    # - нет списков вообще, или
    # - все списки архивные
    if not items:
        return True
    return all(bool(x.get("archived")) for x in items)


def _fetch_rows(
    *,
    ws_id,
    list_id: Optional[int],
    no_list: bool,
    unsub: bool,
    page: int,
    ui_lang: str,
    q: dict,
) -> tuple[int, list[dict]]:
    branch_ids = _fetch_branch_ids_by_text(q.get("q_branch") or "", ui_lang) if q.get("q_branch") else []

    where = ["wc.workspace_id = %s::uuid"]
    params: list[Any] = [ws_id]

    if unsub:
        where.append("wc.active = false")

    if list_id is not None:
        where.append(
            """
            EXISTS (
              SELECT 1
              FROM public.lists_contacts lc2
              JOIN public.aap_lists_mailinglist ml2
                ON ml2.id = lc2.list_id AND ml2.workspace_id = wc.workspace_id
              WHERE lc2.contact_id = wc.contact_id AND lc2.list_id = %s::bigint
            )
            """
        )
        params.append(int(list_id))

    if (list_id is None) and no_list:
        where.append(
            """
            NOT EXISTS (
              SELECT 1
              FROM public.lists_contacts lc3
              JOIN public.aap_lists_mailinglist ml3
                ON ml3.id = lc3.list_id AND ml3.workspace_id = wc.workspace_id
              WHERE lc3.contact_id = wc.contact_id
            )
            """
        )

    need_join_aggr = False

    if q.get("q_email"):
        need_join_aggr = True
        where.append("COALESCE(ra.email, '') ILIKE %s")
        params.append(f"%{q['q_email']}%")

    if q.get("q_company"):
        need_join_aggr = True
        where.append("COALESCE(ra.company_name, '') ILIKE %s")
        params.append(f"%{q['q_company']}%")

    if q.get("q_plz"):
        need_join_aggr = True
        where.append("EXISTS (SELECT 1 FROM unnest(ra.plz_list) p WHERE p ILIKE %s)")
        params.append(f"%{q['q_plz']}%")

    if q.get("q_addr"):
        need_join_aggr = True
        where.append("COALESCE(ra.address_list[1], '') ILIKE %s")
        params.append(f"%{q['q_addr']}%")

    if q.get("q_branch"):
        need_join_aggr = True
        if branch_ids:
            where.append("ra.branches && %s::int[]")
            params.append(branch_ids)
        else:
            where.append("FALSE")

    where_sql = " AND ".join(where)
    join_ra_sql = "JOIN public.raw_contacts_aggr ra ON ra.id = wc.contact_id" if need_join_aggr else ""

    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*)::int
            FROM public.ws_contacts wc
            {join_ra_sql}
            WHERE {where_sql}
            """,
            params,
        )
        total = int((cur.fetchone() or [0])[0] or 0)

    offset = (int(page) - 1) * PAGE_SIZE

    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT wc.contact_id::bigint, wc.active::bool
            FROM public.ws_contacts wc
            {join_ra_sql}
            WHERE {where_sql}
            LIMIT %s OFFSET %s
            """,
            params + [int(PAGE_SIZE), int(offset)],
        )
        base_rows = cur.fetchall() or []

    contact_ids = [int(r[0]) for r in base_rows]
    lists_by_contact = _fetch_lists_for_contacts(ws_id, contact_ids)

    out: list[dict] = []
    for contact_id, active in base_rows:
        aggr_id = int(contact_id)
        c = get_contact(aggr_id, ui_lang) or {}

        lst = lists_by_contact.get(aggr_id) or []

        out.append(
            {
                "aggr_id": aggr_id,
                "ui_id": encode_id(int(aggr_id)),
                "active": bool(active),

                "company_name": (c.get("company_name") or "").strip(),
                "address": (c.get("address") or "").strip(),
                "city_land": (c.get("city_land") or "").strip(),
                "email": (c.get("email") or "").strip(),
                "branches_html": c.get("branches_html") or "",

                "lists": lst,
                "lists_str": _lists_str(lst),
                "forget_allowed": _forget_allowed(lst),
            }
        )

    return total, out


def contacts_view(request):
    ws_id, _user = _guard(request)
    if not ws_id:
        return redirect("/")

    ui_lang = getattr(request, "LANGUAGE_CODE", "") or "ru"

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        cid = (request.POST.get("contact_id") or "").strip()

        contact_id = None
        if cid:
            try:
                contact_id = int(cid)
            except Exception:
                contact_id = None

        def _redir_same():
            return redirect(request.get_full_path())

        if action in ("forget", "subscribe", "unsubscribe") and contact_id:
            if action == "forget":
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        DELETE FROM public.ws_contacts
                        WHERE workspace_id = %s::uuid AND contact_id = %s::bigint
                        """,
                        [ws_id, int(contact_id)],
                    )
                return _redir_same()

            if action == "subscribe":
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE public.ws_contacts
                        SET active = true, reason = NULL
                        WHERE workspace_id = %s::uuid AND contact_id = %s::bigint
                        """,
                        [ws_id, int(contact_id)],
                    )
                return _redir_same()

            if action == "unsubscribe":
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE public.ws_contacts
                        SET active = false, reason = NULL
                        WHERE workspace_id = %s::uuid AND contact_id = %s::bigint
                        """,
                        [ws_id, int(contact_id)],
                    )
                return _redir_same()

        return _redir_same()

    q = _qs_params_from_request(request)
    search_active = _is_search_active(q)

    # filters
    list_id: Optional[int] = None
    list_ui = (request.GET.get("list") or "").strip()
    no_list = (request.GET.get("nol") or "").strip() in ("1", "true", "on", "yes")
    unsub = (request.GET.get("unsub") or "").strip() in ("1", "true", "on", "yes")

    if list_ui:
        decoded = _decode_list_id_or_redirect(request)
        if isinstance(decoded, HttpResponseRedirect):
            return decoded
        list_id = int(decoded) if decoded is not None else None
        no_list = False

    filter_active = (list_id is not None) or bool(no_list) or bool(unsub)

    # bouquet (неархивные списки)
    lists = _fetch_lists_for_workspace(ws_id)
    list_ids = [int(x["id"]) for x in lists]
    cnt_by_list = _fetch_counts_by_list(ws_id, list_ids)
    no_list_cnt = _fetch_count_no_list(ws_id)
    unsub_cnt = _fetch_count_unsub(ws_id)

    for it in lists:
        it["cnt"] = int(cnt_by_list.get(int(it["id"]), 0))
        it["is_active"] = (list_id is not None) and (int(it["id"]) == int(list_id))

    no_list_item = {"cnt": int(no_list_cnt), "is_active": bool(no_list)}
    unsub_item = {"cnt": int(unsub_cnt), "is_active": bool(unsub)}

    # page + rows
    page = _safe_int(request.GET.get("p"), 1)

    total, rows = _fetch_rows(
        ws_id=ws_id,
        list_id=list_id,
        no_list=bool(no_list),
        unsub=bool(unsub),
        page=page,
        ui_lang=ui_lang,
        q=q,
    )

    pages = max(1, int(math.ceil(total / float(PAGE_SIZE))) if total else 1)
    if page > pages:
        page = pages
        total, rows = _fetch_rows(
            ws_id=ws_id,
            list_id=list_id,
            no_list=bool(no_list),
            unsub=bool(unsub),
            page=page,
            ui_lang=ui_lang,
            q=q,
        )

    return render(
        request,
        "panels/aap_lists/contacts.html",
        {
            "lists": lists,
            "no_list_item": no_list_item,
            "unsub_item": unsub_item,
            "filter_active": filter_active,
            "list_filter_ui": list_ui,
            "no_list": no_list,
            "unsub": unsub,
            "rows": rows,
            "count": total,
            "page": page,
            "pages": pages,
            "page_size": PAGE_SIZE,
            "search_active": search_active,
            "q_email": q["q_email"],
            "q_company": q["q_company"],
            "q_branch": q["q_branch"],
            "q_addr": q["q_addr"],
            "q_plz": q["q_plz"],
        },
    )
