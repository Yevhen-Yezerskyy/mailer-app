# FILE: web/panel/aap_audience/views/contacts_manage.py
# DATE: 2026-04-23
# PURPOSE: AAP Audience "Блокировка кантактов": unique workspace contacts with search, paging, and block/unblock modal flow.

from __future__ import annotations

from typing import Any

from django.db import connection
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme
from django.utils.translation import gettext as _trans

from mailer_web.access import decode_id, encode_id

PAGE_SIZE = 50


def _format_total(value: int) -> str:
    return f"{int(value):,}".replace(",", " ")


def _get_page_value(raw_value: str) -> int:
    value = str(raw_value or "").strip()
    if value.isdigit():
        page = int(value)
        if page > 0:
            return page
    return 1


def _build_page_items(*, page: int, total_pages: int) -> list[dict[str, Any]]:
    if total_pages <= 1:
        return []
    out: list[dict[str, Any]] = []
    for number in range(1, total_pages + 1):
        is_edge = number in (1, total_pages)
        is_near = abs(number - page) <= 3
        if is_edge or is_near:
            out.append(
                {
                    "kind": "page",
                    "number": number,
                    "is_current": number == page,
                }
            )
            continue
        if not out or out[-1].get("kind") != "gap":
            out.append({"kind": "gap"})
    return out


def _is_truthy(raw_value: str) -> bool:
    return str(raw_value or "").strip().lower() in {"1", "true", "on", "yes"}


def _safe_next_url(request, raw_next: str) -> str:
    next_url = str(raw_next or "").strip()
    fallback = reverse("audience:contacts_manage")
    if not next_url:
        return fallback
    if not url_has_allowed_host_and_scheme(next_url, allowed_hosts={request.get_host()}):
        return fallback
    if not next_url.startswith("/panel/audience/contacts-manage/"):
        return fallback
    return next_url


def _parse_contact_id(token: str) -> int:
    try:
        return int(decode_id(str(token or "").strip()))
    except Exception:
        return 0


def _contact_exists_in_workspace(workspace_id, contact_id: int) -> bool:
    if not workspace_id or int(contact_id) <= 0:
        return False
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM public.sending_lists sl
            JOIN public.aap_audience_audiencetask t
              ON t.id = sl.task_id
            JOIN public.aggr_contacts_cb ac
              ON ac.id = sl.aggr_contact_cb_id
            WHERE t.workspace_id = %s::uuid
              AND sl.aggr_contact_cb_id = %s
              AND COALESCE(ac.blocked, false) = false
              AND COALESCE(ac.wrong_email, false) = false
            LIMIT 1
            """,
            [workspace_id, int(contact_id)],
        )
        row = cur.fetchone()
    return bool(row)


def _load_contact_modal_payload(workspace_id, contact_id: int) -> dict[str, Any] | None:
    if not workspace_id or int(contact_id) <= 0:
        return None
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                ac.id::bigint,
                ac.company_name,
                ac.email
            FROM public.aggr_contacts_cb ac
            WHERE ac.id = %s
              AND COALESCE(ac.blocked, false) = false
              AND COALESCE(ac.wrong_email, false) = false
              AND EXISTS (
                  SELECT 1
                  FROM public.sending_lists sl
                  JOIN public.aap_audience_audiencetask t
                    ON t.id = sl.task_id
                  WHERE t.workspace_id = %s::uuid
                    AND sl.aggr_contact_cb_id = ac.id
              )
            LIMIT 1
            """,
            [int(contact_id), workspace_id],
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "contact_id": int(row[0]),
        "company_name": str(row[1] or "").strip(),
        "email": str(row[2] or "").strip(),
    }


def _fetch_total(workspace_id, query: str, *, blocked_only: bool, has_sends: bool) -> int:
    search_query = str(query or "").strip()
    search_like = f"%{search_query}%"
    with connection.cursor() as cur:
        cur.execute(
            """
            WITH contact_stats AS (
                SELECT
                    sl.aggr_contact_cb_id::bigint AS aggr_contact_id,
                    BOOL_OR(COALESCE(sl.removed, false)) AS has_removed,
                    BOOL_OR(NOT COALESCE(sl.removed, false)) AS has_active
                FROM public.sending_lists sl
                JOIN public.aap_audience_audiencetask t
                  ON t.id = sl.task_id
                JOIN public.aggr_contacts_cb ac
                  ON ac.id = sl.aggr_contact_cb_id
                WHERE t.workspace_id = %s::uuid
                  AND (
                        %s = ''
                        OR COALESCE(ac.company_name, '') ILIKE %s
                        OR COALESCE(ac.email, '') ILIKE %s
                  )
                  AND COALESCE(ac.blocked, false) = false
                  AND COALESCE(ac.wrong_email, false) = false
                GROUP BY sl.aggr_contact_cb_id
            ),
            filtered AS (
                SELECT cs.aggr_contact_id
                FROM contact_stats cs
                WHERE (%s = false OR cs.has_removed = true)
                  AND (
                        %s = false
                        OR EXISTS (
                            SELECT 1
                            FROM public.sending_log lg
                            JOIN public.campaigns_campaigns c
                              ON c.id = lg.campaign_id
                            WHERE c.workspace_id = %s::uuid
                              AND lg.aggr_contact_cb_id = cs.aggr_contact_id
                              AND lg.status = 'SEND'
                            LIMIT 1
                        )
                  )
            )
            SELECT COUNT(*)::int
            FROM filtered
            """,
            [
                workspace_id,
                search_query,
                search_like,
                search_like,
                bool(blocked_only),
                bool(has_sends),
                workspace_id,
            ],
        )
        row = cur.fetchone()
    return int((row or [0])[0] or 0)


def _fetch_rows(
    _request,
    workspace_id,
    *,
    page: int,
    query: str,
    blocked_only: bool,
    has_sends: bool,
) -> dict[str, Any]:
    search_query = str(query or "").strip()
    total = _fetch_total(
        workspace_id,
        search_query,
        blocked_only=bool(blocked_only),
        has_sends=bool(has_sends),
    )
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    current_page = min(max(1, int(page)), total_pages)
    offset = (current_page - 1) * PAGE_SIZE
    search_like = f"%{search_query}%"

    with connection.cursor() as cur:
        cur.execute(
            """
            WITH contact_stats AS (
                SELECT
                    sl.aggr_contact_cb_id::bigint AS aggr_contact_id,
                    BOOL_OR(COALESCE(sl.removed, false)) AS has_removed,
                    BOOL_OR(NOT COALESCE(sl.removed, false)) AS has_active,
                    BOOL_OR(COALESCE(sl.removed, false)) AS is_blocked
                FROM public.sending_lists sl
                JOIN public.aap_audience_audiencetask t
                  ON t.id = sl.task_id
                JOIN public.aggr_contacts_cb ac
                  ON ac.id = sl.aggr_contact_cb_id
                WHERE t.workspace_id = %s::uuid
                  AND (
                        %s = ''
                        OR COALESCE(ac.company_name, '') ILIKE %s
                        OR COALESCE(ac.email, '') ILIKE %s
                  )
                  AND COALESCE(ac.blocked, false) = false
                  AND COALESCE(ac.wrong_email, false) = false
                GROUP BY sl.aggr_contact_cb_id
            ),
            filtered AS (
                SELECT
                    cs.aggr_contact_id,
                    cs.is_blocked
                FROM contact_stats cs
                WHERE (%s = false OR cs.has_removed = true)
                  AND (
                        %s = false
                        OR EXISTS (
                            SELECT 1
                            FROM public.sending_log lg
                            JOIN public.campaigns_campaigns c
                              ON c.id = lg.campaign_id
                            WHERE c.workspace_id = %s::uuid
                              AND lg.aggr_contact_cb_id = cs.aggr_contact_id
                              AND lg.status = 'SEND'
                            LIMIT 1
                        )
                  )
            ),
            paged AS (
                SELECT
                    f.aggr_contact_id,
                    f.is_blocked
                FROM filtered f
                ORDER BY
                    f.aggr_contact_id
                LIMIT %s
                OFFSET %s
            ),
            list_titles AS (
                SELECT
                    sl.aggr_contact_cb_id::bigint AS aggr_contact_id,
                    ARRAY_AGG(
                        DISTINCT COALESCE(NULLIF(TRIM(t.title), ''), '#' || t.id::text)
                        ORDER BY COALESCE(NULLIF(TRIM(t.title), ''), '#' || t.id::text)
                    ) AS titles
                FROM public.sending_lists sl
                JOIN public.aap_audience_audiencetask t
                  ON t.id = sl.task_id
                JOIN paged p
                  ON p.aggr_contact_id = sl.aggr_contact_cb_id
                WHERE t.workspace_id = %s::uuid
                GROUP BY sl.aggr_contact_cb_id
            ),
            send_times AS (
                SELECT
                    lg.aggr_contact_cb_id::bigint AS aggr_contact_id,
                    ARRAY_AGG(
                        DISTINCT COALESCE(lg.processed_at, lg.created_at)
                        ORDER BY COALESCE(lg.processed_at, lg.created_at) DESC
                    ) AS sent_times
                FROM public.sending_log lg
                JOIN public.campaigns_campaigns c
                  ON c.id = lg.campaign_id
                JOIN paged p
                  ON p.aggr_contact_id = lg.aggr_contact_cb_id
                WHERE c.workspace_id = %s::uuid
                  AND lg.status = 'SEND'
                GROUP BY lg.aggr_contact_cb_id
            )
            SELECT
                p.aggr_contact_id::bigint,
                ac.company_name,
                ac.email,
                COALESCE(p.is_blocked, false) AS is_blocked,
                lt.titles,
                st.sent_times
            FROM paged p
            JOIN public.aggr_contacts_cb ac
              ON ac.id = p.aggr_contact_id
            LEFT JOIN list_titles lt
              ON lt.aggr_contact_id = p.aggr_contact_id
            LEFT JOIN send_times st
              ON st.aggr_contact_id = p.aggr_contact_id
            ORDER BY p.aggr_contact_id
            """,
            [
                workspace_id,
                search_query,
                search_like,
                search_like,
                bool(blocked_only),
                bool(has_sends),
                workspace_id,
                PAGE_SIZE,
                offset,
                workspace_id,
                workspace_id,
            ],
        )
        raw_rows = cur.fetchall() or []

    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        aggr_contact_id = int(row[0])
        list_titles = row[4] if isinstance(row[4], list) else []
        send_times = row[5] if isinstance(row[5], list) else []
        rows.append(
            {
                "aggr_contact_id": aggr_contact_id,
                "aggr_contact_ui_id": encode_id(aggr_contact_id),
                "company_name": str(row[1] or "").strip(),
                "email": str(row[2] or "").strip(),
                "is_blocked": bool(row[3]),
                "list_titles": [str(x or "").strip() for x in list_titles if str(x or "").strip()],
                "send_times": [x for x in send_times if x is not None],
                "contact_modal_url": reverse("contact_modal") + f"?id={encode_id(aggr_contact_id)}",
            }
        )

    return {
        "rows": rows,
        "total": total,
        "total_display": _format_total(total),
        "page": current_page,
        "pages": total_pages,
        "has_prev": current_page > 1,
        "prev_page": current_page - 1,
        "has_next": current_page < total_pages,
        "next_page": current_page + 1,
        "page_items": _build_page_items(page=current_page, total_pages=total_pages),
        "show_paging": total > 0,
    }


def contacts_manage_toggle_modal_view(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    token = str(request.GET.get("id") or "").strip()
    mode = str(request.GET.get("mode") or "block").strip().lower()
    mode = "unblock" if mode == "unblock" else "block"
    next_url = _safe_next_url(request, str(request.GET.get("next") or "").strip())

    if not ws_id or not getattr(user, "is_authenticated", False):
        return render(
            request,
            "panels/aap_audience/modal_contact_block_toggle.html",
            {"status": "error"},
            status=403,
        )

    contact_id = _parse_contact_id(token)
    contact = _load_contact_modal_payload(ws_id, int(contact_id)) if contact_id > 0 else None
    if not contact:
        return render(
            request,
            "panels/aap_audience/modal_contact_block_toggle.html",
            {"status": "error"},
            status=404,
        )

    modal_title = _trans("Заблокировать контакт") if mode == "block" else _trans("Разблокировать контакт")
    return render(
        request,
        "panels/aap_audience/modal_contact_block_toggle.html",
        {
            "status": "ok",
            "mode": mode,
            "modal_title": modal_title,
            "post_url": reverse("audience:contacts_manage"),
            "action_name": "contact_toggle_block",
            "contact_ui_id": token,
            "company_name": contact["company_name"],
            "email": contact["email"],
            "next_url": next_url,
        },
    )


def contacts_manage_view(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)

    if request.method == "POST":
        action = str(request.POST.get("action") or "").strip()
        if action == "contact_toggle_block":
            mode = str(request.POST.get("mode") or "block").strip().lower()
            mode = "unblock" if mode == "unblock" else "block"
            contact_id = _parse_contact_id(str(request.POST.get("id") or "").strip())
            next_url = _safe_next_url(request, str(request.POST.get("next") or "").strip())
            if (
                ws_id
                and getattr(user, "is_authenticated", False)
                and int(contact_id) > 0
                and _contact_exists_in_workspace(ws_id, int(contact_id))
            ):
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE public.sending_lists sl
                        SET removed = %s,
                            updated_at = now()
                        FROM public.aap_audience_audiencetask t
                        WHERE t.id = sl.task_id
                          AND t.workspace_id = %s::uuid
                          AND sl.aggr_contact_cb_id = %s
                        """,
                        [bool(mode == "block"), ws_id, int(contact_id)],
                    )
            return redirect(next_url)

    if not ws_id or not getattr(user, "is_authenticated", False):
        return render(
            request,
            "panels/aap_audience/contacts_manage.html",
            {
                "contacts_rows": [],
                "contacts_total": 0,
                "contacts_total_display": _format_total(0),
                "contacts_page": 1,
                "contacts_pages": 1,
                "contacts_has_prev": False,
                "contacts_prev_page": 1,
                "contacts_has_next": False,
                "contacts_next_page": 1,
                "contacts_page_items": [],
                "contacts_show_paging": False,
                "contacts_query": "",
                "contacts_blocked_only": False,
                "contacts_has_sends": False,
                "contacts_manage_url": reverse("audience:contacts_manage"),
            },
            status=403,
        )

    query = str(request.GET.get("q") or "").strip()
    page = _get_page_value(str(request.GET.get("page") or "1"))
    blocked_only = _is_truthy(str(request.GET.get("blocked") or "").strip())
    has_sends = _is_truthy(str(request.GET.get("has_sends") or "").strip())
    payload = _fetch_rows(
        request,
        ws_id,
        page=page,
        query=query,
        blocked_only=blocked_only,
        has_sends=has_sends,
    )

    return render(
        request,
        "panels/aap_audience/contacts_manage.html",
        {
            "contacts_rows": payload["rows"],
            "contacts_total": payload["total"],
            "contacts_total_display": payload["total_display"],
            "contacts_page": payload["page"],
            "contacts_pages": payload["pages"],
            "contacts_has_prev": payload["has_prev"],
            "contacts_prev_page": payload["prev_page"],
            "contacts_has_next": payload["has_next"],
            "contacts_next_page": payload["next_page"],
            "contacts_page_items": payload["page_items"],
            "contacts_show_paging": payload["show_paging"],
            "contacts_query": query,
            "contacts_blocked_only": blocked_only,
            "contacts_has_sends": has_sends,
            "contacts_manage_url": reverse("audience:contacts_manage"),
            "contacts_current_url": request.get_full_path(),
        },
    )
