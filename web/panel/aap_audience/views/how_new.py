# FILE: web/panel/aap_audience/views/how.py  (обновлено — 2025-12-24)
# CHANGE:
# - switch edit identifier from line_id -> id (obid)
# - use centralized resolver: mailer_web.access.resolve_pk_or_redirect(request, AudienceTask)
# - never expose raw t.id in URLs: add t.ui_id = encode_id(t.id) for template
# - keep core HOW logic (state/type/forms/GPT/save) unchanged

import json
from typing import Any, Tuple

from django.shortcuts import render, redirect
from django.http import HttpResponseRedirect

from panel.aap_audience.forms import (
    AudienceHowSellForm,
    AudienceHowBuyForm,
    AudienceEditSellForm,
    AudienceEditBuyForm,
)
from panel.aap_audience.models import AudienceTask
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt

from mailer_web.access import encode_id, resolve_pk_or_redirect


# ---------- helpers ----------

def _redirect_how(request, *, type_: str):
    return redirect(f"{request.path}?state=how&type={type_}")


def _canonical_redirect(request):
    return redirect(f"{request.path}?state=how&type=sell")


def _parse_how_json(raw: str, fallback: dict) -> dict:
    try:
        data = json.loads((raw or "").strip())
        if not isinstance(data, dict):
            raise ValueError
        return data
    except Exception:
        return {
            "what": fallback.get("what", ""),
            "who": fallback.get("who", ""),
            "geo": fallback.get("geo", ""),
            "questions": {},
            "hints": {},
        }


def _parse_task_split_json(raw: str, fallback_task: str) -> dict:
    try:
        data = json.loads((raw or "").strip())
        if not isinstance(data, dict):
            raise ValueError

        what = (data.get("what") or "").strip()
        who = (data.get("who") or "").strip()
        geo = (data.get("geo") or "").strip()

        # минимальная страховка
        if not any([what, who, geo]):
            raise ValueError

        return {"what": what, "who": who, "geo": geo}
    except Exception:
        return {"what": (fallback_task or "").strip(), "who": "", "geo": ""}


def _clean_multiline(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "")
    lines = [line.strip() for line in text.split("\n")]
    lines = [l for l in lines if l]
    return "\n".join(lines)


def _get_form_class(type_: str):
    return AudienceHowBuyForm if type_ == "buy" else AudienceHowSellForm


def _get_edit_form_class(type_: str):
    return AudienceEditBuyForm if type_ == "buy" else AudienceEditSellForm


def _get_system_prompt(type_: str) -> str:
    return "audience_how_system_buy" if type_ == "buy" else "audience_how_system_sell"


def _get_tasks(request):
    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return AudienceTask.objects.none()
    return AudienceTask.objects.filter(workspace_id=ws_id, user=user).order_by("-created_at")[:50]


def _with_ui_ids(tasks):
    # IMPORTANT: template must use t.ui_id for URLs; never raw t.id
    for t in tasks:
        t.ui_id = encode_id(int(t.id))
    return tasks


def _get_task_obj_or_redirect(request, *, pk: int):
    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None

    try:
        return AudienceTask.objects.get(id=int(pk), workspace_id=ws_id, user=user)
    except Exception:
        return None


def _build_task_text(*, what: str, who: str, geo: str) -> str:
    pieces = [
        (what or "").replace("\n", " ").strip(),
        (who or "").replace("\n", " ").strip(),
        (geo or "").replace("\n", " ").strip(),
    ]
    return " ".join(p for p in pieces if p).strip()


def _gpt_fill_task_fields(
    *,
    client: GPTClient,
    user_id: Any,
    task_text: str,
) -> Tuple[str, str, str, str]:
    title_resp = client.ask(
        model="nano",
        instructions=get_prompt("audience_how_name"),
        input=task_text,
        user_id=user_id,
        service_tier="flex",
    )
    title = (title_resp.content or "").strip()

    branches_resp = client.ask(
        model="maxi",
        instructions=get_prompt("audience_how_branches"),
        input=task_text,
        user_id=user_id,
        service_tier="flex",
    )
    task_branches = _clean_multiline(branches_resp.content or "")

    geo_resp = client.ask(
        model="maxi",
        instructions=get_prompt("audience_how_geo"),
        input=task_text,
        user_id=user_id,
        service_tier="flex",
    )
    task_geo = _clean_multiline(geo_resp.content or "")

    client_resp = client.ask(
        model="maxi",
        instructions=get_prompt("audience_how_client"),
        input=task_text,
        user_id=user_id,
        service_tier="flex",
    )
    task_client = _clean_multiline(client_resp.content or "")

    return title, task_branches, task_geo, task_client


def _gpt_split_task_to_3(*, client: GPTClient, user_id: Any, task_text: str) -> dict:
    resp = client.ask(
        model="maxi",
        instructions=get_prompt("audience_how_split"),
        input=(task_text or "").strip(),
        user_id=user_id,
        service_tier="flex",
    )
    return _parse_task_split_json(resp.content or "", task_text or "")


# ---------- handlers ----------

def _handle_how(request, *, type_: str):
    FormClass = _get_form_class(type_)
    system_prompt = _get_system_prompt(type_)
    tasks = _with_ui_ids(_get_tasks(request))

    if request.method == "GET":
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {"form": FormClass(), "state": "how", "type": type_, "tasks": tasks},
        )

    # POST
    action = request.POST.get("action")

    if action == "clear":
        return _redirect_how(request, type_=type_)

    form = FormClass(request.POST)
    if not form.is_valid():
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {"form": form, "state": "how", "type": type_, "tasks": tasks},
        )

    payload = {
        "what": form.cleaned_data.get("what", "") or "",
        "who": form.cleaned_data.get("who", "") or "",
        "geo": form.cleaned_data.get("geo", "") or "",
    }

    if not any(payload.values()):
        form.add_error(None, "Заполните хотя бы одно поле.")
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {"form": form, "state": "how", "type": type_, "tasks": tasks},
        )

    # save -> ADD handler (создание в БД)
    if action == "save":
        return _handle_add(request, type_=type_, payload=payload, form=form)

    # process -> GPT dialog (no DB)
    if action != "process":
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {"form": form, "state": "how", "type": type_, "tasks": tasks},
        )

    user_input = f"""
Тип задачи: {type_}

WHAT:
{payload['what']}

WHO:
{payload['who']}

GEO:
{payload['geo']}

Верни ТОЛЬКО JSON строго по SYSTEM_PROMPT.
"""

    client = GPTClient()
    resp = client.ask(
        model="maxi",
        instructions=get_prompt(system_prompt),
        input=user_input,
        user_id=request.user.id,
        service_tier="flex",
    )

    result = _parse_how_json(resp.content, payload)

    updated_form = FormClass(
        initial={
            "what": result.get("what", payload["what"]),
            "who": result.get("who", payload["who"]),
            "geo": result.get("geo", payload["geo"]),
        }
    )

    return render(
        request,
        "panels/aap_audience/how_new.html",
        {
            "form": updated_form,
            "state": "how",
            "type": type_,
            "tasks": tasks,
            "q_what": result.get("questions", {}).get("what", ""),
            "h_what": result.get("hints", {}).get("what", ""),
            "q_who": result.get("questions", {}).get("who", ""),
            "h_who": result.get("hints", {}).get("who", ""),
            "q_geo": result.get("questions", {}).get("geo", ""),
            "h_geo": result.get("hints", {}).get("geo", ""),
        },
    )


def _handle_add(request, *, type_: str, payload: dict, form):
    if request.method != "POST":
        return _redirect_how(request, type_=type_)

    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return _redirect_how(request, type_=type_)

    task_text = _build_task_text(
        what=payload.get("what", ""),
        who=payload.get("who", ""),
        geo=payload.get("geo", ""),
    )
    if not task_text:
        form.add_error(None, "Заполните хотя бы одно поле.")
        tasks = _with_ui_ids(_get_tasks(request))
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {"form": form, "state": "how", "type": type_, "tasks": tasks},
        )

    client = GPTClient()
    title, task_branches, task_geo, task_client = _gpt_fill_task_fields(
        client=client,
        user_id=user.id,
        task_text=task_text,
    )

    AudienceTask.objects.create(
        workspace_id=ws_id,
        user=user,
        type=type_,
        task=task_text,
        title=title,
        task_branches=task_branches,
        task_geo=task_geo,
        task_client=task_client,
    )

    return _redirect_how(request, type_=type_)


def _handle_edit(request, *, type_: str, pk: int):
    tasks = _with_ui_ids(_get_tasks(request))
    obj = _get_task_obj_or_redirect(request, pk=int(pk))
    if not obj:
        # rule: if missed workspace / not found -> clean URL (no GET)
        return redirect(request.path)

    FormClass = _get_edit_form_class(type_)

    if request.method == "GET":
        client = GPTClient()
        parts = _gpt_split_task_to_3(
            client=client,
            user_id=request.user.id,
            task_text=(obj.task or ""),
        )

        form = FormClass(
            initial={
                "what": parts.get("what", ""),
                "who": parts.get("who", ""),
                "geo": parts.get("geo", ""),
                "title": obj.title or "",
                "task_client": obj.task_client or "",
                "task_branches": obj.task_branches or "",
                "task_geo": obj.task_geo or "",
            }
        )

        return render(
            request,
            "panels/aap_audience/how_new.html",
            {
                "form": form,
                "state": "edit",
                "type": type_,
                "id": encode_id(int(obj.id)),
                "tasks": tasks,
            },
        )

    # POST edit
    action = request.POST.get("action")

    if action == "cancel":
        return _redirect_how(request, type_=type_)

    form = FormClass(request.POST)
    if not form.is_valid():
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {
                "form": form,
                "state": "edit",
                "type": type_,
                "id": encode_id(int(obj.id)),
                "tasks": tasks,
            },
        )

    # собрать task обратно из 3 полей (без GPT)
    task_text = _build_task_text(
        what=form.cleaned_data.get("what", ""),
        who=form.cleaned_data.get("who", ""),
        geo=form.cleaned_data.get("geo", ""),
    )

    obj.type = type_
    obj.task = task_text
    obj.title = (form.cleaned_data.get("title") or "").strip()
    obj.task_client = (form.cleaned_data.get("task_client") or "").strip()
    obj.task_branches = (form.cleaned_data.get("task_branches") or "").strip()
    obj.task_geo = (form.cleaned_data.get("task_geo") or "").strip()
    obj.save(update_fields=["type", "task", "title", "task_client", "task_branches", "task_geo", "updated_at"])

    return _redirect_how(request, type_=type_)


# ---------- view ----------

def how_view(request):
    state = request.GET.get("state")
    type_ = request.GET.get("type")

    if type_ not in ("sell", "buy"):
        return _canonical_redirect(request)

    if state not in ("how", "add", "edit"):
        return _canonical_redirect(request)

    if request.method == "GET" and state == "add":
        return _redirect_how(request, type_=type_)

    if state == "edit":
        res = resolve_pk_or_redirect(request, AudienceTask, param="id")
        if isinstance(res, HttpResponseRedirect):
            return res
        return _handle_edit(request, type_=type_, pk=int(res))

    return _handle_how(request, type_=type_)
