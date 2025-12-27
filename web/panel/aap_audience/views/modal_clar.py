# FILE: web/panel/aap_audience/views/modal_clar.py  (новое — 2025-12-27)
# PURPOSE: HTML-фрагмент для модалки clar. Показывает города/категории по task (ui_id) и mode (cities|branches).

from __future__ import annotations

from django.db import connection
from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from mailer_web.access import resolve_pk_or_redirect
from panel.aap_audience.models import AudienceTask


def _load_items(ws_id, user_id: int, task_id: int, type_: str):
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT value_id, value_text, rate
            FROM crawl_tasks_labeled
            WHERE workspace_id = %s
              AND user_id      = %s
              AND task_id      = %s
              AND type         = %s
            ORDER BY rate ASC, value_text ASC
            """,
            [str(ws_id), int(user_id), int(task_id), str(type_)],
        )
        rows = cur.fetchall()
    return [{"value_id": r[0], "value_text": r[1], "rate": r[2]} for r in rows]


def _is_running(task_id: int, rating_type: str) -> bool:
    """
    __tasks_rating: append-only.
    running = есть хотя бы одна запись done=false для данного type.
    """
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM public.__tasks_rating
            WHERE task_id = %s
              AND type = %s
              AND done = false
            LIMIT 1
            """,
            [int(task_id), str(rating_type)],
        )
        return cur.fetchone() is not None


def modal_clar_view(request):
    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return redirect("/")

    mode = (request.GET.get("mode") or "").strip().lower()
    if mode not in ("cities", "branches"):
        mode = "cities"

    if not request.GET.get("id"):
        return redirect("/")

    res = resolve_pk_or_redirect(request, AudienceTask, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res

    pk = int(res)
    task = AudienceTask.objects.filter(id=pk, workspace_id=ws_id, user=user).first()
    if task is None:
        return redirect("/")

    if mode == "cities":
        items = _load_items(ws_id, user.id, task.id, "city")
        running = _is_running(task.id, "geo")
        title = "Города"
    else:
        items = _load_items(ws_id, user.id, task.id, "branch")
        running = _is_running(task.id, "branches")
        title = "Категории"

    if items:
        status = "done"
    else:
        status = "running" if running else "empty"

    return render(
        request,
        "panels/aap_audience/modal_clar.html",
        {
            "task": task,
            "mode": mode,
            "title": title,
            "items": items,
            "status": status,
        },
    )
