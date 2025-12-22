# FILE: web/panel/aap_audience/views/how.py
# DATE: 2025-12-22
# PURPOSE:
# HOW-view с явной FSM:
# - state=how  (реализован)
# - state=prepare/edit (зарезервирован, пока не реализован)
#
# Жёсткая валидация state и type через GET.
# GPT-промпт выбирается по type:
#   - sell -> audience_how_system_sell
#   - buy  -> audience_how_system_buy
#
# Реализованы действия HOW:
# - process
# - clear
# - save (переход в следующий state, пока заглушка)

import json
from django.shortcuts import render, redirect

from panel.aap_audience.forms import AudienceHowSellForm, AudienceHowBuyForm
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt


# ---------- helpers ----------

def _canonical_redirect(request, *, state="how", type_="sell"):
    return redirect(f"{request.path}?state={state}&type={type_}")


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


def _get_form_class(type_: str):
    return AudienceHowBuyForm if type_ == "buy" else AudienceHowSellForm


def _get_system_prompt(type_: str):
    if type_ == "buy":
        return "audience_how_system_buy"
    return "audience_how_system_sell"


# ---------- view ----------

def how_view(request):
    # ----- FSM guards -----
    state = request.GET.get("state")
    type_ = request.GET.get("type")

    if state != "how":
        return _canonical_redirect(request, state="how", type_=type_ or "sell")

    if type_ not in ("sell", "buy"):
        return _canonical_redirect(request, state="how", type_="sell")

    FormClass = _get_form_class(type_)
    system_prompt = _get_system_prompt(type_)

    # ---------- GET ----------
    if request.method == "GET":
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {
                "form": FormClass(),
                "state": state,
                "type": type_,
            },
        )

    # ---------- POST ----------
    action = request.POST.get("action")

    # ---- clear ----
    if action == "clear":
        return _canonical_redirect(request, state="how", type_=type_)

    form = FormClass(request.POST)

    # ---- validation error ----
    if not form.is_valid():
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {
                "form": form,
                "state": state,
                "type": type_,
            },
        )

    # ---- save (переход в следующий state, пока заглушка) ----
    if action == "save":
        # здесь позже:
        # - финальный GPT
        # - сохранение AudienceTask
        # - redirect на state=prepare&id=...
        return redirect("/panel/audience/prepare/")  # TEMP placeholder

    # ---- process ----
    if action != "process":
        return render(
            request,
            "panels/aap_audience/how_new.html",
            {
                "form": form,
                "state": state,
                "type": type_,
            },
        )

    payload = {
        "what": form.cleaned_data.get("what", "") or "",
        "who": form.cleaned_data.get("who", "") or "",
        "geo": form.cleaned_data.get("geo", "") or "",
    }

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
        tier="maxi",
        instructions=get_prompt(system_prompt),
        input=user_input,
        user_id=request.user.id,
    )

    result = _parse_how_json(resp.content, payload)

    # ВАЖНО:
    # GPT → новая форма (без error-состояния)
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
            "state": state,
            "type": type_,
            "q_what": result.get("questions", {}).get("what", ""),
            "h_what": result.get("hints", {}).get("what", ""),
            "q_who": result.get("questions", {}).get("who", ""),
            "h_who": result.get("hints", {}).get("who", ""),
            "q_geo": result.get("questions", {}).get("geo", ""),
            "h_geo": result.get("hints", {}).get("geo", ""),
        },
    )
