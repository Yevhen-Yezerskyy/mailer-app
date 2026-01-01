# FILE: web/panel/aap_audience/views/how.py  (обновлено — 2025-12-25)
# Смысл: state=how "save" создаёт AudienceTask и редиректит на edit.
# GPT:
# - title: 1 вызов (mini)
# - range: 1 вызов (maxi) -> JSON {"Client":"","Branches":"","Geo":""}
# Ошибка парсинга JSON -> поля = "Processing error" и едет в БД.
# Delete: ранний guard в how_view.

import json
from typing import Any, Tuple, Literal

from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render

from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt, LANG_MAP

from mailer_web.access import encode_id, resolve_pk_or_redirect
from panel.aap_audience.forms import (
    AudienceEditBuyForm,
    AudienceEditSellForm,
    AudienceHowBuyForm,
    AudienceHowSellForm,
)
from panel.aap_audience.models import AudienceTask


# ---------- helpers ----------

def _redirect_how(request, *, type_: str):
    return redirect(f"{request.path}?state=how&type={type_}")


def _canonical_redirect(request):
    return redirect(f"{request.path}?state=how&type=sell")


def _redirect_edit(request, *, type_: str, pk: int):
    return redirect(f"{request.path}?state=edit&type={type_}&id={encode_id(int(pk))}")


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
    if type_ == "buy":
        return "audience_how_system_buy"
    if type_ == "sell":
        return "audience_how_system_sell"
    raise ValueError(f"Invalid audience type: {type_!r}")


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


def _get_task_obj_or_none(request, *, pk: int):
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
    task_type: Literal["buy", "sell"],
    request: Any,
) -> Tuple[str, str, str, str]:
    
    lang_code = (request.LANGUAGE_CODE or "en").lower()
    lang_name = LANG_MAP.get(lang_code, lang_code)

    title_resp = client.ask(
        model="mini",
        instructions=get_prompt("audience_how_name")+ f"\n\nAnswer strictly in {lang_name}.",
        input=task_text,
        user_id=user_id,
        service_tier="flex",
    )
    title = (title_resp.content or "").strip()

    # range: один вызов, maxi, JSON
    prompt_name = "audience_how_range_buy" if task_type == "buy" else "audience_how_range_sell"

    range_resp = client.ask(
        model="maxi",
        instructions=get_prompt(prompt_name)+ f"\n\nAnswer strictly in {lang_name}.",
        input=task_text,  # нетронутый текст
        user_id=user_id,
        service_tier="flex",
    )

    task_branches = "Processing error"
    task_geo = "Processing error"
    task_client = "Processing error"

    try:
        raw = (range_resp.content or "").strip()
        data = json.loads(raw) if raw else {}

        b = data.get("Branches", "")
        g = data.get("Geo", "")
        c = data.get("Seller", "") if task_type == "buy" else data.get("Client", "")
        
        task_branches = _clean_multiline(b if isinstance(b, str) else str(b or ""))
        task_geo = _clean_multiline(g if isinstance(g, str) else str(g or ""))
        task_client = _clean_multiline(c if isinstance(c, str) else str(c or ""))

        if not task_branches:
            task_branches = "Processing error"
        if not task_geo:
            task_geo = "Processing error"
        if not task_client:
            task_client = "Processing error"
    except Exception:
        pass

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
            "panels/aap_audience/how.html",
            {"form": FormClass(), "state": "how", "type": type_, "tasks": tasks},
        )

    action = request.POST.get("action")

    if action == "clear":
        return _redirect_how(request, type_=type_)

    form = FormClass(request.POST)
    if not form.is_valid():
        return render(
            request,
            "panels/aap_audience/how.html",
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
            "panels/aap_audience/how.html",
            {"form": form, "state": "how", "type": type_, "tasks": tasks},
        )

    if action == "save":
        return _handle_add_then_edit(request, type_=type_, payload=payload, form=form)

    if action != "process":
        return render(
            request,
            "panels/aap_audience/how.html",
            {"form": form, "state": "how", "type": type_, "tasks": tasks},
        )

    lang_code = (request.LANGUAGE_CODE or "en").lower()
    lang_name = LANG_MAP.get(lang_code, lang_code)
    if type_=="sell":
        user_input = f"""{payload['what']} {payload['who']} {payload['geo']}"""
    else:
        user_input = f""" [what]: {payload['what']} [who]: {payload['who']} [geo]: {payload['geo']}"""

    client = GPTClient()
    resp = client.ask(
        model="maxi",
        instructions=get_prompt(system_prompt) + f"\n\nAnswer strictly in {lang_name}.",
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
        "panels/aap_audience/how.html",
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


def _handle_add_then_edit(request, *, type_: str, payload: dict, form):
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
            "panels/aap_audience/how.html",
            {"form": form, "state": "how", "type": type_, "tasks": tasks},
        )

    client = GPTClient()
    title, task_branches, task_geo, task_client = _gpt_fill_task_fields(
        client=client,
        user_id=user.id,
        task_text=task_text,
        task_type=type_,
        request = request,
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

    return _redirect_edit(request, type_=type_, pk=int(obj.id))


def _handle_edit(request, *, type_: str, pk: int):
    tasks = _with_ui_ids(_get_tasks(request))
    obj = _get_task_obj_or_none(request, pk=int(pk))
    if not obj:
        return _redirect_how(request, type_=type_)

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
            "panels/aap_audience/how.html",
            {"form": form, "state": "edit", "type": type_, "id": encode_id(int(obj.id)), "tasks": tasks},
        )

    action = request.POST.get("action")

    if action == "cancel":
        return _redirect_how(request, type_=type_)

    form = FormClass(request.POST)
    if not form.is_valid():
        return render(
            request,
            "panels/aap_audience/how.html",
            {"form": form, "state": "edit", "type": type_, "id": encode_id(int(obj.id)), "tasks": tasks},
        )

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

    # --- DELETE guard (soft archive) ---
    if request.method == "POST" and request.POST.get("action") == "delete":
        post_id = (request.POST.get("id") or "").strip()
        if post_id and not request.GET.get("id"):
            q = request.GET.copy()
            q["id"] = post_id
            request.GET = q

        res = resolve_pk_or_redirect(request, AudienceTask, param="id")
        if isinstance(res, HttpResponseRedirect):
            return res

        pk = int(res)
        ws_id = request.workspace_id
        user = request.user
        if ws_id and getattr(user, "is_authenticated", False):
            AudienceTask.objects.filter(
                id=pk,
                workspace_id=ws_id,
                user=user,
            ).update(
                archived=True,
                run_processing=False,
            )

        return redirect(request.get_full_path())
    # --- /DELETE guard ---

    if request.method == "GET" and state == "add":
        return _redirect_how(request, type_=type_)

    if state == "edit":
        res = resolve_pk_or_redirect(request, AudienceTask, param="id")
        if isinstance(res, HttpResponseRedirect):
            return res
        return _handle_edit(request, type_=type_, pk=int(res))

    return _handle_how(request, type_=type_)
