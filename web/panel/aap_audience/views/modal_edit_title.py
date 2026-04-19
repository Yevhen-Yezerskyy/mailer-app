# FILE: web/panel/aap_audience/views/modal_edit_title.py
# DATE: 2026-03-22
# PURPOSE: Modal form for editing and suggesting the audience title inside create/edit flow.

from __future__ import annotations

from django.http import JsonResponse
from django.shortcuts import render
from django.utils.translation import gettext as _trans

from engine.common.gpt import GPTClient
from engine.common.translate import get_prompt
from mailer_web.access import decode_id
from panel.aap_audience.models import AudienceTask
from .create_edit_flow_gpt_consts import FLOW_GPT_MODEL, FLOW_GPT_SERVICE_TIER
from .create_edit_flow_shared import FLOW_GPT_UNAVAILABLE_TEXT, is_gpt_ok, mark_flow_gpt_unavailable

def _resolve_task(request, token: str):
    if not token:
        return None
    try:
        pk = int(decode_id(token))
    except Exception:
        return None
    return (
        AudienceTask.objects.filter(
            id=pk,
            workspace_id=request.workspace_id,
            archived=False,
        ).first()
    )


def _can_suggest(task) -> bool:
    return bool((task.source_product or "").strip() and (task.source_company or "").strip())


def _prompt_instructions(request, prompt_key: str) -> str:
    lang_name = request.ui_lang_name_en
    on_gpt_error = lambda: mark_flow_gpt_unavailable(request)
    lang_prompt = get_prompt("lang_response", on_gpt_error=on_gpt_error).replace("{LANG}", lang_name).strip()
    prompt_text = get_prompt(prompt_key, on_gpt_error=on_gpt_error).strip()
    return "\n\n".join(part for part in (lang_prompt, prompt_text) if part).strip()


def _title_prompt_key(task) -> str:
    return "create_buy_title" if str(task.type or "").strip() == "buy" else "create_sell_title"


def _title_user_id(task) -> str:
    suffix = "buy" if str(task.type or "").strip() == "buy" else "sell"
    return f"panel.audience.create_edit_{suffix}.title"


def _title_input(task) -> str:
    return (
        f"PRODUCT:\n{(task.source_product or '').strip()}\n\n"
        f"COMPANY:\n{(task.source_company or '').strip()}\n\n"
        f"GEO:\n{(task.source_geo or '').strip()}"
    )


def _display_title(task) -> str:
    title = (task.title or "").strip()
    if title:
        return title
    return f"{_trans('Список рассылки')} #{int(task.id)}"


def modal_edit_title_view(request):
    token = (request.POST.get("id") or request.GET.get("id") or "").strip()
    task = _resolve_task(request, token)

    if request.method == "POST":
        if not task:
            return JsonResponse({"ok": False, "error": str(_trans("Запись не найдена."))}, status=404)

        action = (request.POST.get("action") or "").strip()

        if action == "suggest_title":
            if not _can_suggest(task):
                return JsonResponse(
                    {
                        "ok": False,
                        "error": str(_trans("Для предложения названия нужно сохранить продукт и компанию.")),
                    },
                    status=400,
                )

            resp = GPTClient().ask(
                model=FLOW_GPT_MODEL,
                instructions=_prompt_instructions(request, _title_prompt_key(task)),
                input=_title_input(task),
                user_id=_title_user_id(task),
                service_tier=FLOW_GPT_SERVICE_TIER,
                web_search=False,
            )
            if not is_gpt_ok(resp):
                mark_flow_gpt_unavailable(request)
                return JsonResponse(
                    {
                        "ok": False,
                        "error": str(_trans(FLOW_GPT_UNAVAILABLE_TEXT)),
                        "gpt_unavailable": True,
                        "popup_text": FLOW_GPT_UNAVAILABLE_TEXT,
                    },
                    status=503,
                )
            title = (resp.content or "").strip()
            if not title:
                return JsonResponse(
                    {"ok": False, "error": str(_trans("Не удалось предложить название."))},
                    status=400,
                )
            return JsonResponse({"ok": True, "title": title})

        if action == "save_title":
            title = (request.POST.get("title") or "").strip()
            if not title:
                return JsonResponse(
                    {"ok": False, "error": str(_trans("Введите название списка рассылки."))},
                    status=400,
                )
            task.title = title
            task.save(update_fields=["title", "updated_at"])
            return JsonResponse({"ok": True, "title": title})

        return JsonResponse({"ok": False, "error": str(_trans("Неизвестное действие."))}, status=400)

    return render(
        request,
        "panels/aap_audience/modal_edit_title.html",
        {
            "task": task,
            "task_id_token": token,
            "title_value": _display_title(task) if task else "",
            "suggest_enabled": bool(task and _can_suggest(task)),
        },
    )
