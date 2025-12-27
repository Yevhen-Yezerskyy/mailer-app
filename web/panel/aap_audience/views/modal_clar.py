# FILE: web/panel/aap_audience/views/modal_clar.py  (обновлено — 2025-12-27)
# PURPOSE: HTML-фрагмент для модалки clar. Показывает города/категории по task (ui_id) и mode (cities|branches).
#          Источник: crawl_tasks + joins (НЕ crawl_tasks_labeled).

from __future__ import annotations

from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from mailer_web.access import resolve_pk_or_redirect
from panel.aap_audience.models import AudienceTask

from .clar_items import load_sorted_branches, load_sorted_cities


def _is_running(task_id: int, rating_type: str) -> bool:
    """
    __tasks_rating: append-only.
    running = есть хотя бы одна запись done=false для данного type.
    """
    from django.db import connection

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

    ui_lang = getattr(request, "LANGUAGE_CODE", "") or "ru"

    if mode == "cities":
        items = load_sorted_cities(ws_id, user.id, task.id)
        running = _is_running(task.id, "geo")
        title = "Города"
    else:
        items = load_sorted_branches(ws_id, user.id, task.id, ui_lang=ui_lang)
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
