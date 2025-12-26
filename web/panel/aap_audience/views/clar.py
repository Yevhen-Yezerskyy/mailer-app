# FILE: web/panel/aap_audience/views/clar.py  (обновлено — 2025-12-26)
# CHANGE:
# - rating branches/geo полностью независим от edit_task.run_processing
# - __tasks_rating: только INSERT (append-only), done не трогаем
# - хеш только через engine.common.utils.h64_text
# - UI-ветки/тексты остаются в шаблоне; view отдаёт только данные/состояния

from __future__ import annotations

from django.db import connection
from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from engine.common.utils import h64_text
from mailer_web.access import encode_id, resolve_pk_or_redirect
from panel.aap_audience.forms import AudienceClarBuyForm, AudienceClarSellForm
from panel.aap_audience.models import AudienceTask


def _load_all_crawl_items_for_tasks(workspace_id, user_id, task_ids):
    task_ids = [int(x) for x in task_ids if x]
    if not task_ids:
        return {}

    out = {tid: {"city": [], "branch": []} for tid in task_ids}

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT task_id, type, value_id, value_text, rate
            FROM crawl_tasks_labeled
            WHERE workspace_id = %s
              AND user_id      = %s
              AND task_id = ANY(%s)
            ORDER BY task_id ASC, type ASC, rate ASC, value_text ASC
            """,
            [str(workspace_id), int(user_id), task_ids],
        )
        rows = cur.fetchall()

    for task_id, type_, value_id, value_text, rate in rows:
        if task_id in out and type_ in ("city", "branch"):
            out[task_id][type_].append(
                {"value_id": value_id, "value_text": value_text, "rate": rate}
            )

    return out


def _get_tasks(request):
    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return AudienceTask.objects.none()
    return (
        AudienceTask.objects.filter(workspace_id=ws_id, user=user)
        .order_by("-created_at")[:50]
    )


def _with_ui_ids(tasks):
    for t in tasks:
        t.ui_id = encode_id(int(t.id))
    return tasks


def _bind_clar_items(ws_id, user_id, tasks):
    task_ids = [t.id for t in tasks]
    all_items = _load_all_crawl_items_for_tasks(ws_id, user_id, task_ids) if task_ids else {}

    for t in tasks:
        bucket = all_items.get(t.id, {"city": [], "branch": []})
        t.clar_city_items = bucket["city"]
        t.clar_branch_items = bucket["branch"]


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
      "branches": {"running": bool, "last_done_hash": int|None},
      "geo":      {"running": bool, "last_done_hash": int|None},
    }
    """
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
        "branches": {"running": False, "last_done_hash": None},
        "geo": {"running": False, "last_done_hash": None},
    }
    if not rows:
        return out

    out["has_any"] = True

    seen_last_done = {"branches": False, "geo": False}
    for type_, hash_task, done, _updated_at in rows:
        if type_ not in ("branches", "geo"):
            continue

        if done is False:
            out[type_]["running"] = True

        if done is True and not seen_last_done[type_]:
            out[type_]["last_done_hash"] = hash_task
            seen_last_done[type_] = True

    return out


def _tasks_rating_insert(task_id: int, type_: str, hash_task: int):
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.__tasks_rating (task_id, type, hash_task, done, created_at, updated_at)
            VALUES (%s, %s, %s, false, now(), now())
            """,
            [int(task_id), str(type_), int(hash_task)],
        )


def clar_view(request):
    ws_id = request.workspace_id
    user = request.user

    edit_task, r = _get_edit_task_or_redirect(request)
    if r is not None:
        return r

    state = "edit" if edit_task else ""
    form = None
    rating = None
    rating_hashes = None

    if edit_task:
        FormClass = AudienceClarBuyForm if edit_task.type == "buy" else AudienceClarSellForm

        if request.method == "POST":
            action = request.POST.get("action")

            if action == "cancel":
                return redirect(request.path)

            if action == "toggle_processing":
                AudienceTask.objects.filter(
                    id=edit_task.id, workspace_id=ws_id, user=user
                ).update(run_processing=not edit_task.run_processing)
                return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")

            # rating actions (append-only inserts)
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
                        title=cd["title"].strip(),
                        task=cd["task"].strip(),
                        task_client=cd["task_client"].strip(),
                        task_branches=cd["task_branches"].strip(),
                        task_geo=cd["task_geo"].strip(),
                    )
                    return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")
            else:
                return redirect(f"{request.path}?state=edit&id={encode_id(int(edit_task.id))}")

        else:
            form = FormClass(
                initial={
                    "title": edit_task.title or "",
                    "task": edit_task.task or "",
                    "task_client": edit_task.task_client or "",
                    "task_branches": edit_task.task_branches or "",
                    "task_geo": edit_task.task_geo or "",
                }
            )

        # rating data для шаблона — независимо от run_processing
        rating = _tasks_rating_fetch(int(edit_task.id))
        rating_hashes = {
            "branches": h64_text((edit_task.task or "") + (edit_task.task_branches or "")),
            "geo": h64_text((edit_task.task or "") + (edit_task.task_geo or "")),
        }

    tasks = _with_ui_ids(_get_tasks(request))
    if ws_id and getattr(user, "is_authenticated", False) and tasks:
        _bind_clar_items(ws_id, user.id, tasks)

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
        },
    )
