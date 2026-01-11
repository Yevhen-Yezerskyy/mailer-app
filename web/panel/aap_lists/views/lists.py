# FILE: web/panel/aap_lists/views/lists.py
# DATE: 2026-01-11
# PURPOSE: /panel/lists/lists/ — списки рассылок: add/edit/archive.
#          В селекте audience_task_id = РЕАЛЬНЫЙ pk (НЕ encode).
# CHANGE:
# - add: больше не залипаем в edit после добавления (redirect на чистый URL)
# - в списке есть ui_id для ссылок (edit/manage)

from __future__ import annotations

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
            return render(
                request,
                "panels/aap_lists/lists.html",
                {
                    "lists": lists,
                    "state": state,
                    "form": form,
                    "edit_obj": edit_obj,
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

    return render(
        request,
        "panels/aap_lists/lists.html",
        {
            "lists": lists,
            "state": state,
            "form": form,
            "edit_obj": edit_obj,
        },
    )
