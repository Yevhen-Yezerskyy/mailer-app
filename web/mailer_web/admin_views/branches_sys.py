# FILE: web/mailer_web/admin_views/branches_sys.py  (обновлено — 2026-02-10)
# CHANGE: view_edit теперь принимает sys_id как аргумент (Django передаёт kwargs напрямую).

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlencode

from django.contrib import messages
from django.db import connection, transaction
from django.http import HttpRequest, HttpResponse, HttpResponseRedirect

from .utils import AdminPage, render_admin


T_SYS = "public.branches_raw_sys"
T_11880 = "public.branches_raw_11880"
T_GS = "public.branches_raw_gs"


@dataclass(frozen=True)
class SysRow:
    id: int
    label: str
    label_rus: Optional[str]
    cnt_11880: int
    cnt_gs: int
    is_active: bool


@dataclass(frozen=True)
class BrRow:
    id: int
    label: str
    label_rus: Optional[str]


def _to_int(v: Any, default: int) -> int:
    try:
        return int(str(v))
    except Exception:
        return default


def _norm_active(v: str) -> str:
    v = (v or "").strip().lower()
    if v in ("1", "true", "t", "yes", "y", "on"):
        return "1"
    if v in ("0", "false", "f", "no", "n", "off"):
        return "0"
    if v in ("all", "*", "any"):
        return "all"
    return "1"


def _norm_sort(v: str) -> str:
    v = (v or "").strip().lower()
    if v in ("label", "label_rus"):
        return v
    return "label"


def _qs_without_page(params: dict[str, str]) -> str:
    p = {k: v for k, v in params.items() if k != "page" and v != ""}
    return urlencode(p)


def _fetch_sys_rows(
    *,
    q: str,
    active: str,
    sort: str,
    limit: int,
    offset: int,
) -> tuple[int, list[SysRow]]:
    where = []
    args: list[Any] = []

    if active == "1":
        where.append("is_active = true")
    elif active == "0":
        where.append("is_active = false")

    q = (q or "").strip()
    if q:
        where.append("(label ILIKE %s OR COALESCE(label_rus,'') ILIKE %s)")
        like = f"%{q}%"
        args.extend([like, like])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    order_sql = (
        "ORDER BY label ASC, id ASC"
        if sort == "label"
        else "ORDER BY label_rus ASC NULLS LAST, label ASC, id ASC"
    )

    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {T_SYS} {where_sql}", args)
        total = int(cur.fetchone()[0])

        cur.execute(
            f"""
            SELECT
              id,
              label,
              label_rus,
              COALESCE(array_length(br_11880, 1), 0) AS cnt_11880,
              COALESCE(array_length(br_gs, 1), 0) AS cnt_gs,
              is_active
            FROM {T_SYS}
            {where_sql}
            {order_sql}
            LIMIT %s OFFSET %s
            """,
            [*args, int(limit), int(offset)],
        )
        rows = [
            SysRow(
                id=int(r[0]),
                label=str(r[1]),
                label_rus=(str(r[2]) if r[2] is not None and str(r[2]).strip() else None),
                cnt_11880=int(r[3]),
                cnt_gs=int(r[4]),
                is_active=bool(r[5]),
            )
            for r in cur.fetchall()
        ]

    return total, rows


def _toggle_sys_active(sys_id: int) -> bool:
    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute(f"SELECT is_active FROM {T_SYS} WHERE id=%s FOR UPDATE", [sys_id])
            row = cur.fetchone()
            if not row:
                raise ValueError("not_found")
            new_val = not bool(row[0])
            cur.execute(f"UPDATE {T_SYS} SET is_active=%s WHERE id=%s", [new_val, sys_id])
    return new_val


def _create_sys(label: str, label_rus: str, is_active: bool) -> int:
    label = (label or "").strip()
    label_rus = (label_rus or "").strip()
    if not label:
        raise ValueError("label_required")

    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {T_SYS} (label, label_rus, br_11880, br_gs, is_active)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                [label, (label_rus if label_rus else None), [], [], bool(is_active)],
            )
            return int(cur.fetchone()[0])


def _save_sys_fields(sys_id: int, label: str, label_rus: str, is_active: bool) -> None:
    label = (label or "").strip()
    label_rus = (label_rus or "").strip()
    if not label:
        raise ValueError("label_required")

    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {T_SYS}
                SET label=%s, label_rus=%s, is_active=%s
                WHERE id=%s
                """,
                [label, (label_rus if label_rus else None), bool(is_active), sys_id],
            )
            if cur.rowcount == 0:
                raise ValueError("not_found")


def _fetch_sys_one(sys_id: int) -> tuple[int, str, Optional[str], list[int], list[int], bool]:
    with connection.cursor() as cur:
        cur.execute(
            f"SELECT id, label, label_rus, br_11880, br_gs, is_active FROM {T_SYS} WHERE id=%s",
            [sys_id],
        )
        row = cur.fetchone()
        if not row:
            raise ValueError("not_found")
        br_11880 = list(row[3] or [])
        br_gs = list(row[4] or [])
        return (
            int(row[0]),
            str(row[1]),
            (str(row[2]) if row[2] is not None and str(row[2]).strip() else None),
            [int(x) for x in br_11880],
            [int(x) for x in br_gs],
            bool(row[5]),
        )


def _fetch_br_available(*, table: str, exclude_ids: list[int], q: str) -> list[BrRow]:
    q = (q or "").strip()
    where = ["is_active = true", "NOT (id = ANY(%s))"]
    args: list[Any] = [exclude_ids]

    if q:
        where.append("(label ILIKE %s OR COALESCE(label_rus,'') ILIKE %s)")
        like = f"%{q}%"
        args.extend([like, like])

    where_sql = "WHERE " + " AND ".join(where)

    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, label, label_rus
            FROM {table}
            {where_sql}
            ORDER BY label ASC, id ASC
            """,
            args,
        )
        return [
            BrRow(
                id=int(r[0]),
                label=str(r[1]),
                label_rus=(str(r[2]) if r[2] is not None and str(r[2]).strip() else None),
            )
            for r in cur.fetchall()
        ]


def _fetch_br_selected(*, table: str, ids: list[int]) -> list[BrRow]:
    if not ids:
        return []
    with connection.cursor() as cur:
        cur.execute(
            f"""
            SELECT id, label, label_rus
            FROM {table}
            WHERE id = ANY(%s)
            ORDER BY label ASC, id ASC
            """,
            [ids],
        )
        return [
            BrRow(
                id=int(r[0]),
                label=str(r[1]),
                label_rus=(str(r[2]) if r[2] is not None and str(r[2]).strip() else None),
            )
            for r in cur.fetchall()
        ]


def _sys_add_11880(sys_id: int, br_id: int) -> None:
    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {T_SYS}
                SET br_11880 = array_append(br_11880, %s)
                WHERE id=%s AND NOT (%s = ANY(br_11880))
                """,
                [br_id, sys_id, br_id],
            )


def _sys_remove_11880(sys_id: int, br_id: int) -> None:
    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute(f"UPDATE {T_SYS} SET br_11880 = array_remove(br_11880, %s) WHERE id=%s", [br_id, sys_id])


def _sys_add_gs(sys_id: int, br_id: int) -> None:
    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {T_SYS}
                SET br_gs = array_append(br_gs, %s)
                WHERE id=%s AND NOT (%s = ANY(br_gs))
                """,
                [br_id, sys_id, br_id],
            )


def _sys_remove_gs(sys_id: int, br_id: int) -> None:
    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute(f"UPDATE {T_SYS} SET br_gs = array_remove(br_gs, %s) WHERE id=%s", [br_id, sys_id])


def view_list(request: HttpRequest) -> HttpResponse:
    next_url = (request.POST.get("next") or request.get_full_path() or "/admin/").strip()

    if request.method == "POST":
        toggle_id = _to_int(request.POST.get("toggle_id"), 0)
        if toggle_id > 0:
            try:
                new_val = _toggle_sys_active(toggle_id)
                messages.success(request, f"is_active → {'true' if new_val else 'false'} (id={toggle_id})")
            except ValueError:
                messages.error(request, f"Запись не найдена (id={toggle_id}).")
        return HttpResponseRedirect(next_url)

    q = (request.GET.get("q") or "").strip()
    active = _norm_active(request.GET.get("active") or "1")
    sort = _norm_sort(request.GET.get("sort") or "label")

    per_page = _to_int(request.GET.get("per_page"), 200)
    if per_page <= 0:
        per_page = 200
    if per_page > 1000:
        per_page = 1000

    page_n = _to_int(request.GET.get("page"), 1)
    if page_n <= 0:
        page_n = 1

    offset = (page_n - 1) * per_page
    total, rows = _fetch_sys_rows(q=q, active=active, sort=sort, limit=per_page, offset=offset)

    pages = max(1, (total + per_page - 1) // per_page)
    if page_n > pages:
        page_n = pages

    params = {"q": q, "active": active, "sort": sort, "per_page": str(per_page)}
    qs = _qs_without_page(params)

    return render_admin(
        request,
        template="admin/serenity/branches_sys_list.html",
        context={
            "title": "Бранчи SYS",
            "q": q,
            "active": active,
            "sort": sort,
            "per_page": per_page,
            "rows": rows,
            "total": total,
            "page": page_n,
            "pages": pages,
            "qs": qs,
        },
    )


def view_edit(request: HttpRequest, sys_id: int) -> HttpResponse:
    if sys_id <= 0:
        messages.error(request, "Неверный id.")
        return HttpResponseRedirect("/admin/serenity/branches-sys/")

    next_url = (request.POST.get("next") or request.get_full_path() or "/admin/").strip()

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        item_id = _to_int(request.POST.get("item_id"), 0)

        try:
            if action == "save":
                label = request.POST.get("label") or ""
                label_rus = request.POST.get("label_rus") or ""
                is_active = (request.POST.get("is_active") or "").strip().lower() in ("1", "true", "t", "yes", "y", "on")
                _save_sys_fields(sys_id, label, label_rus, is_active)
                messages.success(request, "Сохранено.")
                return HttpResponseRedirect(next_url)

            if action == "add_11880" and item_id > 0:
                _sys_add_11880(sys_id, item_id)
                return HttpResponseRedirect(next_url)

            if action == "del_11880" and item_id > 0:
                _sys_remove_11880(sys_id, item_id)
                return HttpResponseRedirect(next_url)

            if action == "add_gs" and item_id > 0:
                _sys_add_gs(sys_id, item_id)
                return HttpResponseRedirect(next_url)

            if action == "del_gs" and item_id > 0:
                _sys_remove_gs(sys_id, item_id)
                return HttpResponseRedirect(next_url)

        except ValueError as e:
            if str(e) == "label_required":
                messages.error(request, "label обязателен.")
            elif str(e) == "not_found":
                messages.error(request, "Запись не найдена.")
            else:
                messages.error(request, "Ошибка сохранения.")
        return HttpResponseRedirect(next_url)

    (sid, label, label_rus, br_11880, br_gs, is_active) = _fetch_sys_one(sys_id)

    q11880 = (request.GET.get("q11880") or "").strip()
    qgs = (request.GET.get("qgs") or "").strip()

    left_11880 = _fetch_br_available(table=T_11880, exclude_ids=br_11880, q=q11880)
    right_11880 = _fetch_br_selected(table=T_11880, ids=br_11880)

    left_gs = _fetch_br_available(table=T_GS, exclude_ids=br_gs, q=qgs)
    right_gs = _fetch_br_selected(table=T_GS, ids=br_gs)

    return render_admin(
        request,
        template="admin/serenity/branches_sys_edit.html",
        context={
            "title": f"Бранчи SYS: {label}",
            "sys_id": sid,
            "label": label,
            "label_rus": label_rus or "",
            "is_active": is_active,
            "q11880": q11880,
            "qgs": qgs,
            "left_11880": left_11880,
            "right_11880": right_11880,
            "left_gs": left_gs,
            "right_gs": right_gs,
        },
    )


def view_new(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return render_admin(request, template="admin/serenity/branches_sys_new.html", context={"title": "Бранчи SYS: добавить"})

    try:
        label = request.POST.get("label") or ""
        label_rus = request.POST.get("label_rus") or ""
        is_active = (request.POST.get("is_active") or "").strip().lower() in ("1", "true", "t", "yes", "y", "on")
        new_id = _create_sys(label, label_rus, is_active)
        return HttpResponseRedirect(f"/admin/serenity/branches-sys/{new_id}/")
    except ValueError as e:
        if str(e) == "label_required":
            messages.error(request, "label обязателен.")
        else:
            messages.error(request, "Ошибка добавления.")
        return HttpResponseRedirect("/admin/serenity/branches-sys/new/")


page = AdminPage(
    route="serenity/branches-sys/",
    name="serenity_branches_sys",
    title="Бранчи SYS",
    nav_section="Бранчи",
    nav_label="SYS",
    view=view_list,
)

page_edit = AdminPage(
    route="serenity/branches-sys/<int:sys_id>/",
    name="serenity_branches_sys_edit",
    title="Бранчи SYS: изменить",
    nav_section="",
    nav_label="",
    view=view_edit,
)

page_new = AdminPage(
    route="serenity/branches-sys/new/",
    name="serenity_branches_sys_new",
    title="Бранчи SYS: добавить",
    nav_section="",
    nav_label="",
    view=view_new,
)
