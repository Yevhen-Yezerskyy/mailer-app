# FILE: web/panel/aap_audience/views/modal_edit_title.py
# DATE: 2026-03-22
# PURPOSE: Modal form for editing and suggesting the audience title inside create/edit flow.

from __future__ import annotations

from django.http import JsonResponse
from django.shortcuts import render
from django.utils.translation import get_language, gettext as _

from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt
from mailer_web.access import decode_id
from panel.aap_audience.models import AudienceTask

from .create_edit_flow import LANG_RESPONSE_NAMES


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
    lang_code = (getattr(request, "LANGUAGE_CODE", "") or get_language() or "en").lower()
    lang_key = lang_code.split("-")[0].split("_")[0]
    lang_name = LANG_RESPONSE_NAMES.get(lang_key, "English")
    lang_prompt = get_prompt("lang_response").replace("{LANG}", lang_name).strip()
    prompt_text = get_prompt(prompt_key).strip()
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
    return f"{_('Список рассылки')} #{int(task.id)}"


def modal_edit_title_view(request):
    token = (request.POST.get("id") or request.GET.get("id") or "").strip()
    task = _resolve_task(request, token)

    if request.method == "POST":
        if not task:
            return JsonResponse({"ok": False, "error": str(_("Запись не найдена."))}, status=404)

        action = (request.POST.get("action") or "").strip()

        if action == "suggest_title":
            if not _can_suggest(task):
                return JsonResponse(
                    {
                        "ok": False,
                        "error": str(_("Для предложения названия нужно сохранить продукт и компанию.")),
                    },
                    status=400,
                )

            resp = GPTClient().ask(
                model="gpt-5.4",
                instructions=_prompt_instructions(request, _title_prompt_key(task)),
                input=_title_input(task),
                user_id=_title_user_id(task),
                service_tier="flex",
                web_search=False,
            )
            title = (resp.content or "").strip()
            if not title:
                return JsonResponse(
                    {"ok": False, "error": str(_("Не удалось предложить название."))},
                    status=400,
                )
            return JsonResponse({"ok": True, "title": title})

        if action == "save_title":
            title = (request.POST.get("title") or "").strip()
            if not title:
                return JsonResponse(
                    {"ok": False, "error": str(_("Введите название списка рассылки."))},
                    status=400,
                )
            task.title = title
            task.save(update_fields=["title", "updated_at"])
            return JsonResponse({"ok": True, "title": title})

        return JsonResponse({"ok": False, "error": str(_("Неизвестное действие."))}, status=400)

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
