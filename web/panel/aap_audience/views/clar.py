# FILE: web/panel/aap_audience/views/clar.py  (обновлено — 2025-12-27)
# (новое — 2025-12-27)
# - Для статусов "в обработке" добавлен прогресс (%):
#   branches: count(crawl_tasks where task_id + type='branch' + hash_task) / 790 * 100
#   geo(cities): count(crawl_tasks where task_id + type='city' + hash_task) / 1326 * 100
# - Процент НЕ ограничивается сверху (если 1000% — значит баг виден всем).
# - Прогресс считается по hash_task из последней running-записи (__tasks_rating done=false).

from __future__ import annotations

from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from engine.common.utils import h64_text
from mailer_web.access import encode_id, resolve_pk_or_redirect
from panel.aap_audience.forms import AudienceClarBuyForm, AudienceClarSellForm
from panel.aap_audience.models import AudienceTask

from panel.aap_audience.views.clar_items import (
    load_sorted_branches,
    load_sorted_cities,
    update_rate,
)


def _get_tasks(request):
    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return AudienceTask.objects.none()
    return (
        AudienceTask.objects
        .filter(workspace_id=ws_id, user=user, archived=False)
        .order_by("-created_at")
    )


def _with_ui_ids(tasks):
    for t in tasks:
        t.ui_id = encode_id(int(t.id))
    return tasks


def _get_edit_task_or_redirect(request):
    if request.GET.get("state") != "edit":
        return None, None

    ws_id = request.workspace_id
    user = request.user

    if not ws_id or not getattr(user, "is_authenticated", False):
        return None, redirect(request.path)

    if not request.GET.get("id"):
        return None, redirect(request.path)

    res = resolve_pk_or_redirect(request, AudienceTask, param="id")
    if isinstance(res, HttpResponseRedirect):
        return None, res

    pk = int(res)
    task = AudienceTask.objects.filter(id=pk, workspace_id=ws_id, user=user).first()
    if task is None:
        return None, redirect(request.path)

    return task, None


def _tasks_rating_fetch(task_id: int):
    """
    {
      "has_any": bool,
      "branches": {"running": bool, "last_done_hash": int|None, "progress": int|None, "running_hash": int|None},
      "geo":      {"running": bool, "last_done_hash": int|None, "progress": int|None, "running_hash": int|None},
    }
    """
    from django.db import connection

    BRANCHES_TOTAL = 790
    GEO_TOTAL = 1326

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT type, hash_task, done, updated_at
            FROM public.__tasks_rating
            WHERE task_id = %s
              AND type IN ('branches','geo')
            ORDER BY updated_at DESC
            """,
            [int(task_id)],
        )
        rows = cur.fetchall()

    out = {
        "has_any": False,
        "branches": {
            "running": False,
            "last_done_hash": None,
            "progress": None,
            "running_hash": None,
        },
        "geo": {"running": False, "last_done_hash": None, "progress": None, "running_hash": None},
    }
    if not rows:
        return out

    out["has_any"] = True

    seen_last_done = {"branches": False, "geo": False}
    seen_running = {"branches": False, "geo": False}

    for type_, hash_task, done, _updated_at in rows:
        if type_ not in ("branches", "geo"):
            continue

        if done is False and not seen_running[type_]:
            out[type_]["running"] = True
            out[type_]["running_hash"] = hash_task
            seen_running[type_] = True

        if done is True and not seen_last_done[type_]:
            out[type_]["last_done_hash"] = hash_task
            seen_last_done[type_] = True

    def _progress(crawl_type: str, hash_task: int, total: int) -> int:
        if not hash_task:
            return 0
        if not total:
            return 0

        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM public.crawl_tasks
                WHERE task_id = %s
                  AND type = %s
                  AND hash_task = %s
                """,
                [int(task_id), str(crawl_type), int(hash_task)],
            )
            cnt = int(cur.fetchone()[0] or 0)

        return int(round((cnt * 100.0) / float(total)))

    if out["branches"]["running"] and out["branches"]["running_hash"]:
        out["branches"]["progress"] = _progress(
            "branch",
            int(out["branches"]["running_hash"]),
            BRANCHES_TOTAL,
        )

    if out["geo"]["running"] and out["geo"]["running_hash"]:
        out["geo"]["progress"] = _progress(
            "city",
            int(out["geo"]["running_hash"]),
            GEO_TOTAL,
        )

    return out


def _tasks_rating_insert(task_id: int, type_: str, hash_task: int):
    from django.db import connection

    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.__tasks_rating (task_id, type, hash_task, done, created_at, updated_at)
            VALUES (%s, %s, %s, false, now(), now())
            """,
            [int(task_id), str(type_), int(hash_task)],
        )


def _bind_tasks_statuses(tasks):
    """
    Для списка tasks:
    - t.rating: текущее состояние обработки (running/last_done_hash/progress)
    - t.rating_hashes: текущие хеши (task+branches / task+geo)
    """
    for t in tasks:
        t.rating = _tasks_rating_fetch(int(t.id))
        t.rating_hashes = {
            "branches": h64_text((t.task or "") + (t.task_branches or "")),
            "geo": h64_text((t.task or "") + (t.task_geo or "")),
        }


def _bind_task_top_items(ws_id, user_id: int, tasks, ui_lang: str):
    """
    Для таблицы (ТОП-15):
    - t.clar_city_items
    - t.clar_branch_items
    Берём первые 15 по rate.
    """
    for t in tasks:
        t.clar_city_items = load_sorted_cities(ws_id, user_id, int(t.id))[:15]
        t.clar_branch_items = load_sorted_branches(ws_id, user_id, int(t.id), ui_lang=ui_lang)[:15]


def clar_view(request):
    ws_id = request.workspace_id
    user = request.user
    ui_lang = getattr(request, "LANGUAGE_CODE", "") or "ru"

    edit_task, r = _get_edit_task_or_redirect(request)
    if r is not None:
        return r

    state = "edit" if edit_task else ""
    form = None
    rating = None
    rating_hashes = None

    edit_cities = []
    edit_branches = []

    if edit_task:
        FormClass = AudienceClarBuyForm if edit_task.type == "buy" else AudienceClarSellForm

        if request.method == "POST":
            action = (request.POST.get("action") or "").strip()

            if action == "cancel":
                return redirect(request.path)

            if action == "toggle_processing":
                AudienceTask.objects.filter(
                    id=edit_task.id, workspace_id=ws_id, user=user
                ).update(run_processing=not edit_task.run_processing)
                return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")

            if action in ("rate_city", "rate_branch"):
                type_ = "city" if action == "rate_city" else "branch"
                value_id = (request.POST.get("value_id") or "").strip()
                rate_val = (request.POST.get("rate") or "").strip()

                if not value_id or not rate_val:
                    return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")

                try:
                    update_rate(
                        ws_id=ws_id,
                        user_id=int(user.id),
                        task_id=int(edit_task.id),
                        type_=type_,
                        value_id=int(value_id),
                        rate=int(rate_val),
                    )
                except Exception:
                    pass

                return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")

            hash_branches = h64_text((edit_task.task or "") + (edit_task.task_branches or ""))
            hash_geo = h64_text((edit_task.task or "") + (edit_task.task_geo or ""))

            if action == "rating_start_all":
                _tasks_rating_insert(int(edit_task.id), "branches", hash_branches)
                _tasks_rating_insert(int(edit_task.id), "geo", hash_geo)
                return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")

            if action == "rating_restart_branches":
                _tasks_rating_insert(int(edit_task.id), "branches", hash_branches)
                return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")

            if action == "rating_restart_geo":
                _tasks_rating_insert(int(edit_task.id), "geo", hash_geo)
                return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")

            if action == "save":
                form = FormClass(request.POST)
                if form.is_valid():
                    cd = form.cleaned_data
                    AudienceTask.objects.filter(
                        id=edit_task.id, workspace_id=ws_id, user=user
                    ).update(
                        title=(cd["title"] or "").strip(),
                        task=(cd["task"] or "").strip(),
                        task_client=(cd["task_client"] or "").strip(),
                        task_branches=(cd["task_branches"] or "").strip(),
                        task_geo=(cd["task_geo"] or "").strip(),
                    )
                return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")

            return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")

        form = FormClass(
            initial={
                "title": edit_task.title or "",
                "task": edit_task.task or "",
                "task_client": edit_task.task_client or "",
                "task_branches": edit_task.task_branches or "",
                "task_geo": edit_task.task_geo or "",
            }
        )

        rating = _tasks_rating_fetch(int(edit_task.id))
        rating_hashes = {
            "branches": h64_text((edit_task.task or "") + (edit_task.task_branches or "")),
            "geo": h64_text((edit_task.task or "") + (edit_task.task_geo or "")),
        }

        edit_cities = load_sorted_cities(ws_id, user.id, int(edit_task.id))
        edit_branches = load_sorted_branches(ws_id, user.id, int(edit_task.id), ui_lang=ui_lang)

    tasks = _with_ui_ids(_get_tasks(request))
    if ws_id and getattr(user, "is_authenticated", False) and tasks:
        _bind_tasks_statuses(tasks)
        _bind_task_top_items(ws_id, user.id, tasks, ui_lang=ui_lang)

    return render(
        request,
        "panels/aap_audience/clar.html",
        {
            "tasks": tasks,
            "state": state,
            "form": form,
            "edit_task": edit_task,
            "rating": rating,
            "rating_hashes": rating_hashes,
            "ui_lang": ui_lang,
            "edit_cities": edit_cities,
            "edit_branches": edit_branches,
        },
    )
