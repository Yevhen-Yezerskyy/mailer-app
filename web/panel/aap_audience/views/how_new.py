# FILE: web/panel/aap_audience/views/how.py  (новое — 2025-12-23)
# PURPOSE: FSM HOW/ADD/EDIT в одном URL: how=GPT-диалог без БД, add=создание AudienceTask (4 GPT поля), edit=заглушка. Внизу — список задач.

import json
from typing import Any, Tuple

from django.shortcuts import render, redirect

from panel.aap_audience.forms import AudienceHowSellForm, AudienceHowBuyForm
from panel.aap_audience.models import AudienceTask
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt


# ---------- helpers ----------

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


def _clean_multiline(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "")
    lines = [line.strip() for line in text.split("\n")]
    lines = [l for l in lines if l]
    return "\n".join(lines)


def _get_form_class(type_: str):
    return AudienceHowBuyForm if type_ == "buy" else AudienceHowSellForm


def _get_system_prompt(type_: str) -> str:
    return "audience_how_system_buy" if type_ == "buy" else "audience_how_system_sell"


def _get_tasks(request):
    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return AudienceTask.objects.none()
    return AudienceTask.objects.filter(workspace_id=ws_id, user=user).order_by("-created_at")[:50]


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
    """
    4 GPT-запроса как в старом HOW:
    - title (nano)
    - task_branches (maxi)
    - task_geo (maxi)
    - task_client (maxi)
    """
    title_resp = client.ask(
        model="nano",
        instructions=get_prompt("audience_how_name"),
        input=task_text,
        user_id=user_id,
    )
    title = (title_resp.content or "").strip()

    branches_resp = client.ask(
        model="maxi",
        instructions=get_prompt("audience_how_branches"),
        input=task_text,
        user_id=user_id,
    )
    task_branches = _clean_multiline(branches_resp.content or "")

    geo_resp = client.ask(
        model="maxi",
        instructions=get_prompt("audience_how_geo"),
        input=task_text,
        user_id=user_id,
    )
    task_geo = _clean_multiline(geo_resp.content or "")

    client_resp = client.ask(
        model="maxi",
        instructions=get_prompt("audience_how_client"),
        input=task_text,
        user_id=user_id,
    )
    task_client = _clean_multiline(client_resp.content or "")

    return title, task_branches, task_geo, task_client


# ---------- handlers ----------

def _handle_how(request, *, type_: str):
    FormClass = _get_form_class(type_)
    system_prompt = _get_system_prompt(type_)
    tasks = _get_tasks(request)

    if request.method == "GET":
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {"form": FormClass(), "state": "how", "type": type_, "tasks": tasks},
        )

    # POST
    action = request.POST.get("action")

    if action == "clear":
        return _canonical_redirect(request)

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

    client = GPTClient(service_tier="flex")
    resp = client.ask(
        model="maxi",
        instructions=get_prompt(system_prompt),
        input=user_input,
        user_id=request.user.id,
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
    # add = только POST (создание записи)
    if request.method != "POST":
        return _canonical_redirect(request)

    ws_id = request.workspace_id
    user = request.user
    if not ws_id or not getattr(user, "is_authenticated", False):
        return _canonical_redirect(request)

    task_text = _build_task_text(
        what=payload.get("what", ""),
        who=payload.get("who", ""),
        geo=payload.get("geo", ""),
    )
    if not task_text:
        form.add_error(None, "Заполните хотя бы одно поле.")
        tasks = _get_tasks(request)
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {"form": form, "state": "how", "type": type_, "tasks": tasks},
        )

    client = GPTClient(service_tier="flex")
    title, task_branches, task_geo, task_client = _gpt_fill_task_fields(
        client=client,
        user_id=user.id,
        task_text=task_text,
    )

    obj = AudienceTask.objects.create(
        workspace_id=ws_id,
        user=user,
        type=type_,
        task=task_text,
        title=title,
        task_branches=task_branches,
        task_geo=task_geo,
        task_client=task_client,
    )

    # предусмотреть edit (пока не включаем)
    _edit_url = f"{request.path}?state=edit&type={type_}&line_id={obj.id}"

    # сейчас: редирект на дефолтный GET
    return _canonical_redirect(request)


def _handle_edit_stub(request, *, type_: str, line_id: str):
    # пока заглушка
    tasks = _get_tasks(request)
    return render(
        request,
        "panels/aap_audience/how_new.html",
        {
            "state": "edit",
            "type": type_,
            "line_id": line_id,
            "tasks": tasks,
            "edit_stub": True,
        },
    )


# ---------- view ----------

def how_view(request):
    state = request.GET.get("state")
    type_ = request.GET.get("type")
    line_id = request.GET.get("line_id")

    if type_ not in ("sell", "buy"):
        return _canonical_redirect(request)

    if state not in ("how", "add", "edit"):
        return _canonical_redirect(request)

    if state == "edit" and not line_id:
        return _canonical_redirect(request)

    # GET: state=add не существует
    if request.method == "GET" and state == "add":
        return _canonical_redirect(request)

    if state == "edit":
        return _handle_edit_stub(request, type_=type_, line_id=str(line_id))

    # state=how (и POST save/process/clear внутри)
    return _handle_how(request, type_=type_)
