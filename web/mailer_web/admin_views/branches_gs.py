# FILE: web/mailer_web/admin_views/branches_gs.py  (новое — 2026-02-10)
# PURPOSE: Админ-страница "Бранчи GS (GelbeSeiten)": поиск/фильтр/сортировка/пэйджинг (200), toggle is_active. Без GPT/перевода.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlencode

from django.contrib import messages
from django.db import connection, transaction
from django.http import HttpRequest, HttpResponse, HttpResponseRedirect

from .utils import AdminPage, render_admin


TABLE = "public.branches_raw_gs"


@dataclass(frozen=True)
class Row:
    id: int
    slug: str
    label: str
    label_rus: Optional[str]
    is_active: bool


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
    return "1"  # default: только активные


def _norm_sort(v: str) -> str:
    v = (v or "").strip().lower()
    if v in ("label", "label_rus"):
        return v
    return "label"


def _qs_without_page(params: dict[str, str]) -> str:
    p = {k: v for k, v in params.items() if k != "page" and v != ""}
    return urlencode(p)


def _fetch_rows(
    *,
    q: str,
    active: str,
    sort: str,
    limit: int,
    offset: int,
) -> tuple[int, list[Row]]:
    where = []
    args: list[Any] = []

    if active == "1":
        where.append("is_active = true")
    elif active == "0":
        where.append("is_active = false")

    q = (q or "").strip()
    if q:
        where.append("(slug ILIKE %s OR label ILIKE %s OR COALESCE(label_rus,'') ILIKE %s)")
        like = f"%{q}%"
        args.extend([like, like, like])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    order_sql = (
        "ORDER BY label ASC, id ASC"
        if sort == "label"
        else "ORDER BY label_rus ASC NULLS LAST, label ASC, id ASC"
    )

    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE} {where_sql}", args)
        total = int(cur.fetchone()[0])

        cur.execute(
            f"""
            SELECT id, slug, label, label_rus, is_active
            FROM {TABLE}
            {where_sql}
            {order_sql}
            LIMIT %s OFFSET %s
            """,
            [*args, int(limit), int(offset)],
        )
        rows = [
            Row(
                id=int(r[0]),
                slug=str(r[1]),
                label=str(r[2]),
                label_rus=(str(r[3]) if r[3] is not None and str(r[3]).strip() else None),
                is_active=bool(r[4]),
            )
            for r in cur.fetchall()
        ]

    return total, rows


def _toggle_active(branch_id: int) -> bool:
    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute(f"SELECT is_active FROM {TABLE} WHERE id=%s FOR UPDATE", [branch_id])
            row = cur.fetchone()
            if not row:
                raise ValueError("not_found")
            new_val = not bool(row[0])
            cur.execute(f"UPDATE {TABLE} SET is_active=%s WHERE id=%s", [new_val, branch_id])
    return new_val


def view(request: HttpRequest) -> HttpResponse:
    next_url = (request.POST.get("next") or request.get_full_path() or "/admin/").strip()

    if request.method == "POST":
        toggle_id = _to_int(request.POST.get("toggle_id"), 0)
        if toggle_id > 0:
            try:
                new_val = _toggle_active(toggle_id)
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
    total, rows = _fetch_rows(q=q, active=active, sort=sort, limit=per_page, offset=offset)

    pages = max(1, (total + per_page - 1) // per_page)
    if page_n > pages:
        page_n = pages

    params = {"q": q, "active": active, "sort": sort, "per_page": str(per_page)}
    qs = _qs_without_page(params)

    return render_admin(
        request,
        template="admin/serenity/branches_gs.html",
        context={
            "title": "Бранчи GS",
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


page = AdminPage(
    route="serenity/branches-gs/",
    name="serenity_branches_gs",
    title="Бранчи GS",
    nav_section="Бранчи",
    nav_label="GS",
    view=view,
)
