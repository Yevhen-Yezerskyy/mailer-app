# FILE: aap_audience/views/how.py  (новое) 2025-12-11

import json

from django.shortcuts import render, redirect, get_object_or_404

from aap_audience.forms import AudienceHowForm
from aap_audience.models import AudienceTask
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt


def _parse_how_json(raw: str, fallback: dict) -> dict:
    """
    Аккуратно парсим JSON от HOW-промпта.
    Если что-то не так — возвращаем fallback.
    """
    try:
        data = json.loads((raw or "").strip())
        if not isinstance(data, dict):
            raise ValueError("not a dict")
        return data
    except Exception:
        return {
            "what": fallback.get("what", ""),
            "who": fallback.get("who", ""),
            "geo": fallback.get("geo", ""),
            "questions": {},
            "hints": {},
        }


# ---- ФУНКЦИЯ ДЛЯ УДАЛЕНИЯ ДВОЙНЫХ/ПУСТЫХ СТРОК ----
def _clean_multiline(text: str) -> str:
    """
    Убирает пустые строки, двойные переносы,
    нормализует многострочный GPT-ответ.
    """
    if not text:
        return ""
    text = text.replace("\r", "")
    lines = [line.strip() for line in text.split("\n")]
    lines = [l for l in lines if l]  # убираем пустые строки
    return "\n".join(lines)


def how_view(request):
    ws_id = request.workspace_id
    user = request.user

    client = GPTClient()

    # все сохранённые задачи для таблицы
    tasks = AudienceTask.objects.filter(workspace_id=ws_id, user=user)

    # -------------------------
    # GET: обычный / режим edit
    # -------------------------
    if request.method == "GET":
        edit_id = request.GET.get("edit")
        if edit_id:
            obj = get_object_or_404(
                AudienceTask,
                id=edit_id,
                workspace_id=ws_id,
                user=user,
            )

            # разложим сохранённый task обратно в what/who/geo через HOW-промпт
            gpt_resp = client.ask(
                tier="maxi",
                workspace_id=ws_id,
                user_id=user.id,
                system=get_prompt("audience_how_system"),
                user=obj.task,
                with_web=False,
                endpoint="audience_how_prepare",
            )

            parsed = _parse_how_json(gpt_resp.content, {"what": "", "who": "", "geo": ""})

            initial = {
                "what": parsed.get("what", ""),
                "who": parsed.get("who", ""),
                "geo": parsed.get("geo", ""),
                "question_what": parsed.get("questions", {}).get("what", ""),
                "hint_what": parsed.get("hints", {}).get("what", ""),
                "question_who": parsed.get("questions", {}).get("who", ""),
                "hint_who": parsed.get("hints", {}).get("who", ""),
                "question_geo": parsed.get("questions", {}).get("geo", ""),
                "hint_geo": parsed.get("hints", {}).get("geo", ""),
                "edit_id": obj.id,
            }
            form = AudienceHowForm(initial=initial)
        else:
            form = AudienceHowForm()

        return render(
            request,
            "panels/aap_audience/how.html",
            {"form": form, "tasks": tasks},
        )

    # -------------------------
    # POST
    # -------------------------

    # Сначала обработка удаления, без форм и GPT
    if request.POST.get("mode") == "delete":
        delete_id = request.POST.get("delete_id")
        if delete_id:
            AudienceTask.objects.filter(
                id=delete_id,
                workspace_id=ws_id,
                user=user,
            ).delete()
        return redirect(request.path)

    # дальше обычная форма HOW
    form = AudienceHowForm(request.POST)
    if not form.is_valid():
        return render(
            request,
            "panels/aap_audience/how.html",
            {"form": form, "tasks": tasks},
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
            {"form": form, "tasks": tasks},
        )

    # ----- НОРМАЛИЗАЦИЯ HOW (maxi + web) -----
    user_message = f"""
Пользователь ввёл три поля.

Что продаём:
{payload['what']}

Кто продавец:
{payload['who']}

География:
{payload['geo']}

Выполни нормализацию строго по SYSTEM_PROMPT и верни ТОЛЬКО JSON.
"""

    how_resp = client.ask(
        tier="maxi",
        workspace_id=ws_id,
        user_id=user.id,
        system=get_prompt("audience_how_system"),
        user=user_message,
        with_web=True,
        endpoint="audience_how_prepare",
    )

    result = _parse_how_json(how_resp.content, payload)

    # ----- СОХРАНЕНИЕ / ОБНОВЛЕНИЕ ЗАДАЧИ -----
    if request.POST.get("mode") == "save":
        # одна строка без WHAT/WHO/GEO, куски через пробел
        pieces = [
            (result.get("what", "") or "").replace("\n", " ").strip(),
            (result.get("who", "") or "").replace("\n", " ").strip(),
            (result.get("geo", "") or "").replace("\n", " ").strip(),
        ]
        task_text = " ".join(p for p in pieces if p).strip()

        # title
        title_resp = client.ask(
            tier="nano",
            workspace_id=ws_id,
            user_id=user.id,
            system=get_prompt("audience_how_name"),
            user=task_text,
            with_web=False,
            endpoint="audience_task_title",
        )
        title = (title_resp.content or "").strip()

        # branches
        branches_resp = client.ask(
            tier="maxi",
            workspace_id=ws_id,
            user_id=user.id,
            system=get_prompt("audience_how_branches"),
            user=task_text,
            with_web=True,
            endpoint="audience_task_branches",
        )
        task_branches = _clean_multiline(branches_resp.content or "")

        # geo
        geo_resp = client.ask(
            tier="maxi",
            workspace_id=ws_id,
            user_id=user.id,
            system=get_prompt("audience_how_geo"),
            user=task_text,
            with_web=True,
            endpoint="audience_task_geo",
        )
        task_geo = _clean_multiline(geo_resp.content or "")

        # client  <-- НОВОЕ
        client_resp = client.ask(
            tier="maxi",
            workspace_id=ws_id,
            user_id=user.id,
            system=get_prompt("audience_how_client"),
            user=task_text,
            with_web=True,
            endpoint="audience_task_client",
        )
        task_client = _clean_multiline(client_resp.content or "")

        edit_id = form.cleaned_data.get("edit_id")

        if edit_id:
            obj = get_object_or_404(
                AudienceTask,
                id=edit_id,
                workspace_id=ws_id,
                user=user,
            )
            obj.task = task_text
            obj.title = title
            obj.task_branches = task_branches
            obj.task_geo = task_geo
            obj.task_client = task_client  # <-- НОВОЕ
            obj.save()
        else:
            AudienceTask.objects.create(
                workspace_id=ws_id,
                user=user,
                task=task_text,
                title=title,
                task_branches=task_branches,
                task_geo=task_geo,
                task_client=task_client,  # <-- НОВОЕ
            )

        return redirect(request.path)

    # ----- ПРОСТО ОБРАБОТАТЬ (без сохранения) -----
    updated_form = AudienceHowForm(initial={
        "what": result.get("what", payload["what"]),
        "who": result.get("who", payload["who"]),
        "geo": result.get("geo", payload["geo"]),
        "question_what": result.get("questions", {}).get("what", ""),
        "hint_what": result.get("hints", {}).get("what", ""),
        "question_who": result.get("questions", {}).get("who", ""),
        "hint_who": result.get("hints", {}).get("who", ""),
        "question_geo": result.get("questions", {}).get("geo", ""),
        "hint_geo": result.get("hints", {}).get("geo", ""),
        "edit_id": form.cleaned_data.get("edit_id"),
    })

    return render(
        request,
        "panels/aap_audience/how.html",
        {"form": updated_form, "tasks": tasks},
    )
