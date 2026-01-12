# FILE: web/panel/aap_lists/views/lists.py
# DATE: 2026-01-12
# PURPOSE: /panel/lists/lists/ — списки рассылок: add/edit/archive + статистика по контактам/рейтингам.
# CHANGE:
# - добавлены агрегаты: общий total/rated/buckets по rate_contacts (все таски пользователя) и per-task для таблицы
# - в таблице: показываем размер списка (COUNT(*) по lists_contacts), аудитория: total/rated/buckets
# - кнопку "Удалить" показываем только если в списке 0 контактов (COUNT(*)==0)

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from django.db import connection
from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from mailer_web.access import encode_id, resolve_pk_or_redirect
from panel.aap_audience.models import AudienceTask
from panel.aap_lists.forms import MailingListForm
from panel.aap_lists.models import MailingList


def _guard(request):
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None, None
    return ws_id, user


def _pct(part: int, total: int) -> int:
    if not total:
        return 0
    return int(round((int(part) * 100.0) / float(int(total))))


def _lists_qs(ws_id, user):
    return (
        MailingList.objects
        .filter(workspace_id=ws_id, user=user, archived=False)
        .order_by("-created_at")
        .prefetch_related("audience_tasks")
    )


def _tasks_qs(ws_id, user):
    return (
        AudienceTask.objects
        .filter(workspace_id=ws_id, user=user, archived=False)
        .order_by("-created_at")
    )


def _with_ui_ids(items):
    for it in items:
        it.ui_id = encode_id(int(it.id))
    return items


def _bind_one_task(items):
    for it in items:
        it.one_task = it.audience_tasks.all().first()
    return items


def _audience_choices(ws_id, user):
    # ⛔ никаких encode_id — только реальные PK
    return [(str(t.id), t.title or str(t.id)) for t in _tasks_qs(ws_id, user)]


def _get_edit_obj(request, ws_id, user):
    if request.GET.get("state") != "edit":
        return None
    if not request.GET.get("id"):
        return None

    res = resolve_pk_or_redirect(request, MailingList, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res

    return (
        MailingList.objects
        .filter(id=int(res), workspace_id=ws_id, user=user, archived=False)
        .prefetch_related("audience_tasks")
        .first()
    )


def _fetch_lists_contacts_cnt(list_ids: List[int]) -> Dict[int, int]:
    if not list_ids:
        return {}
    sql = """
        SELECT lc.list_id::bigint, COUNT(*)::int
        FROM public.lists_contacts lc
        WHERE lc.list_id = ANY(%s)
        GROUP BY lc.list_id
    """
    out: Dict[int, int] = {}
    with connection.cursor() as cur:
        cur.execute(sql, [list_ids])
        for lid, cnt in cur.fetchall() or []:
            out[int(lid)] = int(cnt or 0)
    return out


def _fetch_rate_stats_by_task(task_ids: List[int]) -> Dict[int, Dict[str, int]]:
    """
    Rated: rate_cl IS NOT NULL AND rate_cl <> 0
    Buckets: 1-30 / 31-70 / 71-100 по rate_cl (только rate_cl, без hash_task).
    """
    if not task_ids:
        return {}

    sql = """
        SELECT
            rc.task_id::bigint AS task_id,
            COUNT(*)::int AS total_cnt,
            SUM(CASE WHEN rc.rate_cl IS NOT NULL AND rc.rate_cl <> 0 THEN 1 ELSE 0 END)::int AS rated_cnt,
            SUM(CASE WHEN rc.rate_cl BETWEEN 1 AND 30 THEN 1 ELSE 0 END)::int AS c1,
            SUM(CASE WHEN rc.rate_cl BETWEEN 31 AND 70 THEN 1 ELSE 0 END)::int AS c2,
            SUM(CASE WHEN rc.rate_cl BETWEEN 71 AND 100 THEN 1 ELSE 0 END)::int AS c3
        FROM public.rate_contacts rc
        WHERE rc.task_id = ANY(%s)
        GROUP BY rc.task_id
    """

    out: Dict[int, Dict[str, int]] = {}
    with connection.cursor() as cur:
        cur.execute(sql, [task_ids])
        for row in cur.fetchall() or []:
            task_id = int(row[0])
            out[task_id] = {
                "total": int(row[1] or 0),
                "rated": int(row[2] or 0),
                "c1": int(row[3] or 0),
                "c2": int(row[4] or 0),
                "c3": int(row[5] or 0),
            }
    return out


def _sum_overall(task_stats: Dict[int, Dict[str, int]]) -> Dict[str, int]:
    total = sum(int(v.get("total", 0) or 0) for v in task_stats.values())
    rated = sum(int(v.get("rated", 0) or 0) for v in task_stats.values())
    c1 = sum(int(v.get("c1", 0) or 0) for v in task_stats.values())
    c2 = sum(int(v.get("c2", 0) or 0) for v in task_stats.values())
    c3 = sum(int(v.get("c3", 0) or 0) for v in task_stats.values())
    return {"total": total, "rated": rated, "c1": c1, "c2": c2, "c3": c3}


def _apply_task_stats_to_list_items(items, task_stats: Dict[int, Dict[str, int]], list_cnt: Dict[int, int]):
    for it in items:
        it.contacts_total = int(list_cnt.get(int(it.id), 0))

        t = getattr(it, "one_task", None)
        st = task_stats.get(int(t.id), {}) if t else {}

        it.task_contacts_total = int(st.get("total", 0))
        it.task_contacts_rated = int(st.get("rated", 0))

        it.rated_1_30_cnt = int(st.get("c1", 0))
        it.rated_31_70_cnt = int(st.get("c2", 0))
        it.rated_71_100_cnt = int(st.get("c3", 0))

        it.rated_1_30_pct = _pct(it.rated_1_30_cnt, it.task_contacts_rated)
        it.rated_31_70_pct = _pct(it.rated_31_70_cnt, it.task_contacts_rated)
        it.rated_71_100_pct = _pct(it.rated_71_100_cnt, it.task_contacts_rated)

    return items


def lists_view(request):
    ws_id, user = _guard(request)
    if not ws_id:
        return redirect("/")

    edit_obj = _get_edit_obj(request, ws_id, user)
    if isinstance(edit_obj, HttpResponseRedirect):
        return edit_obj

    state = "edit" if edit_obj else ""
    choices = _audience_choices(ws_id, user)

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        if action == "cancel":
            return redirect(request.path)

        if action == "delete":
            post_id = (request.POST.get("id") or "").strip()
            if post_id:
                q = request.GET.copy()
                q["id"] = post_id
                request.GET = q

            res = resolve_pk_or_redirect(request, MailingList, param="id")
            if isinstance(res, HttpResponseRedirect):
                return res

            MailingList.objects.filter(
                id=int(res),
                workspace_id=ws_id,
                user=user,
            ).update(archived=True)

            return redirect(request.path)

        form = MailingListForm(request.POST, audience_choices=choices)
        if not form.is_valid():
            lists = _bind_one_task(_with_ui_ids(_lists_qs(ws_id, user)))

            task_ids = [int(t.id) for t in _tasks_qs(ws_id, user)]
            task_stats = _fetch_rate_stats_by_task(task_ids)
            overall = _sum_overall(task_stats)

            list_ids = [int(x.id) for x in lists]
            list_cnt = _fetch_lists_contacts_cnt(list_ids)
            _apply_task_stats_to_list_items(lists, task_stats, list_cnt)

            return render(
                request,
                "panels/aap_lists/lists.html",
                {
                    "lists": lists,
                    "state": state,
                    "form": form,
                    "edit_obj": edit_obj,
                    "contacts_total": int(overall["total"]),
                    "contacts_rated": int(overall["rated"]),
                    "rated_1_30_cnt": int(overall["c1"]),
                    "rated_31_70_cnt": int(overall["c2"]),
                    "rated_71_100_cnt": int(overall["c3"]),
                    "rated_1_30_pct": _pct(int(overall["c1"]), int(overall["rated"])),
                    "rated_31_70_pct": _pct(int(overall["c2"]), int(overall["rated"])),
                    "rated_71_100_pct": _pct(int(overall["c3"]), int(overall["rated"])),
                },
            )

        title = (form.cleaned_data.get("title") or "").strip()
        task_pk = int(form.cleaned_data["audience_task_id"])

        task_obj = AudienceTask.objects.filter(
            id=task_pk,
            workspace_id=ws_id,
            user=user,
            archived=False,
        ).first()
        if task_obj is None:
            return redirect(request.path)

        if action == "add":
            obj = MailingList.objects.create(
                workspace_id=ws_id,
                user=user,
                title=title,
            )
            obj.audience_tasks.set([task_obj])
            return redirect(request.path)

        if action == "save":
            post_id = (request.POST.get("id") or "").strip()
            if post_id:
                q = request.GET.copy()
                q["id"] = post_id
                request.GET = q

            res = resolve_pk_or_redirect(request, MailingList, param="id")
            if isinstance(res, HttpResponseRedirect):
                return res

            obj = MailingList.objects.filter(
                id=int(res),
                workspace_id=ws_id,
                user=user,
                archived=False,
            ).first()
            if obj is None:
                return redirect(request.path)

            obj.title = title
            obj.save(update_fields=["title", "updated_at"])
            obj.audience_tasks.set([task_obj])

            return redirect(f"{request.path}?state=edit&id={encode_id(int(obj.id))}")

        return redirect(request.path)

    # GET
    init = {"title": "", "audience_task_id": ""}
    if edit_obj:
        init["title"] = edit_obj.title or ""
        one_task = edit_obj.audience_tasks.all().first()
        if one_task:
            init["audience_task_id"] = str(one_task.id)

    form = MailingListForm(initial=init, audience_choices=choices)
    lists = _bind_one_task(_with_ui_ids(_lists_qs(ws_id, user)))

    task_ids = [int(t.id) for t in _tasks_qs(ws_id, user)]
    task_stats = _fetch_rate_stats_by_task(task_ids)
    overall = _sum_overall(task_stats)

    list_ids = [int(x.id) for x in lists]
    list_cnt = _fetch_lists_contacts_cnt(list_ids)
    _apply_task_stats_to_list_items(lists, task_stats, list_cnt)

    return render(
        request,
        "panels/aap_lists/lists.html",
        {
            "lists": lists,
            "state": state,
            "form": form,
            "edit_obj": edit_obj,
            "contacts_total": int(overall["total"]),
            "contacts_rated": int(overall["rated"]),
            "rated_1_30_cnt": int(overall["c1"]),
            "rated_31_70_cnt": int(overall["c2"]),
            "rated_71_100_cnt": int(overall["c3"]),
            "rated_1_30_pct": _pct(int(overall["c1"]), int(overall["rated"])),
            "rated_31_70_pct": _pct(int(overall["c2"]), int(overall["rated"])),
            "rated_71_100_pct": _pct(int(overall["c3"]), int(overall["rated"])),
        },
    )
