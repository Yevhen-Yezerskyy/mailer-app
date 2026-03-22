# FILE: web/panel/aap_audience/views/create_edit_flow.py
# DATE: 2026-03-21
# PURPOSE: Shared create/edit flow engine for buy/sell audience flows with reusable step config and status logic.

from __future__ import annotations

import json
from typing import Any, Mapping

from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.text import format_lazy
from django.utils.translation import get_language, gettext_lazy as _

from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt
from mailer_web.access import decode_id, encode_id
from panel.aap_audience.models import AudienceTask

from .create_edit_flow_status import build_flow_step_states, get_next_step_key


FLOW_STEP_ORDER = (
    "product",
    "company",
    "geo",
    "branches_cities",
    "contacts",
    "mailing_list",
)

TEXT_STEP_KEYS = ("product", "company", "geo")
TASK_CREATION_STEP_KEYS = frozenset({"product", "company"})

LANG_RESPONSE_NAMES = {
    "ru": "Russian",
    "rus": "Russian",
    "de": "German",
    "deu": "German",
    "uk": "Ukrainian",
    "ukr": "Ukrainian",
    "en": "English",
    "eng": "English",
}

STEP_URL_PARTS = {
    "product": "product",
    "company": "company",
    "geo": "geo",
    "branches_cities": "branches_cities",
    "contacts": "contacts",
    "mailing_list": "mailing_list",
}

COMMON_AI_HELP_TEXT = _(
    "В процессе обработки ИИ будет задавать вопросы и давать подсказки для уточнения описания. "
    "Поскольку основные варианты уже указаны в начальном описании, часть подсказок может быть "
    "нерелевантной. При необходимости добавьте информацию в описание или в команду для ассистента ИИ."
)

COMMON_FUTURE_STEPS = {
    "branches_cities": {
        "nav_label": _("Категории / Города"),
        "summary_label": _("Категории / Города"),
        "dirty_label": _("Категории / Города"),
    },
    "contacts": {
        "nav_label": _("Сбор контактов"),
        "summary_label": _("Сбор контактов"),
        "dirty_label": _("Сбор контактов"),
    },
    "mailing_list": {
        "nav_label": _("Список рассылки"),
        "summary_label": _("Список рассылки"),
        "dirty_label": _("Список рассылки"),
    },
}

BASE_STEP_DEFINITIONS = {
    "product": {
        "field_name": "source_product",
        "command_field": "product_ai_command",
        "json_key": "product",
        "input_label": "ПРОДУКТ",
        "process_action": "process_product",
        "save_action": "save_product",
        "reset_action": "reset_product_context",
        "completion_type": "text",
        "completion_field": "source_product",
        "visible": True,
        "implemented": True,
        "always_available": True,
    },
    "company": {
        "field_name": "source_company",
        "command_field": "company_ai_command",
        "json_key": "company",
        "input_label": "COMPANY",
        "process_action": "process_company",
        "save_action": "save_company",
        "reset_action": "reset_company_context",
        "completion_type": "text",
        "completion_field": "source_company",
        "visible": True,
        "implemented": True,
        "always_available": True,
    },
    "geo": {
        "field_name": "source_geo",
        "command_field": "geo_ai_command",
        "json_key": "geo",
        "input_label": "GEO",
        "process_action": "process_geo",
        "save_action": "save_geo",
        "reset_action": "reset_geo_context",
        "completion_type": "text",
        "completion_field": "source_geo",
        "visible": True,
        "implemented": True,
        "depends_on": ("product", "company"),
    },
    "branches_cities": {
        "completion_type": "never",
        "visible": True,
        "implemented": True,
        "is_placeholder_view": True,
        "depends_on": ("product", "company", "geo"),
        "placeholder_title": _("Категории / Города"),
        "placeholder_text": _("Раздел пока в разработке."),
    },
    "contacts": {
        "completion_type": "never",
        "visible": True,
        "implemented": True,
        "is_placeholder_view": True,
        "depends_on": ("product", "company", "geo"),
        "placeholder_title": _("Сбор контактов"),
        "placeholder_text": _("Раздел пока в разработке."),
    },
    "mailing_list": {
        "completion_type": "never",
        "visible": True,
        "implemented": True,
        "is_placeholder_view": True,
        "depends_on": ("product", "company", "geo"),
        "placeholder_title": _("Список рассылки"),
        "placeholder_text": _("Раздел пока в разработке."),
    },
}


def _make_ai_help_paragraphs(subject):
    return (
        format_lazy(
            _(
                "Введите команду для работы с {subject} — что нужно добавить, удалить или изменить. "
                "Указанная команда будет применена к текущему описанию."
            ),
            subject=subject,
        ),
        format_lazy(
            _(
                "Вы также можете задать вопрос ИИ-ассистенту в контексте работы над {subject}. "
                "Ответ на вопрос будет дан отдельно и не изменит текст описания."
            ),
            subject=subject,
        ),
        COMMON_AI_HELP_TEXT,
    )


def _text_step(
    *,
    prompt_key: str,
    user_id: str,
    nav_label,
    summary_label,
    dirty_label,
    editor_label,
    placeholder,
    ai_subject,
):
    return {
        "prompt_key": prompt_key,
        "user_id": user_id,
        "nav_label": nav_label,
        "summary_label": summary_label,
        "dirty_label": dirty_label,
        "editor_label": editor_label,
        "placeholder": placeholder,
        "ai_help_paragraphs": _make_ai_help_paragraphs(ai_subject),
    }


FLOW_TYPE_CONFIG = {
    "sell": {
        "type": "sell",
        "template_name": "panels/aap_audience/create/flow.html",
        "edit_url_name": "audience:create_edit_sell",
        "edit_url_name_id": "audience:create_edit_sell_id",
        "dialog_session_prefix": "create_edit_sell_dialog",
        "mode_class": "YY-STATUS_GREEN",
        "mode_label": _("Поиск клиентов / покупателей"),
        "page_title": _("Список рассылки: поиск клиентов / покупателей"),
        "steps": {
            "product": _text_step(
                prompt_key="create_sell_product",
                user_id="panel.audience.create_edit_sell.product",
                nav_label=_("Продукт"),
                summary_label=_("Продукт / услуга"),
                dirty_label=_("Продукт / услуга"),
                editor_label=_("Продукт / услуга. Что продаётся? Где применяется? Кто покупатель?"),
                placeholder=_(
                    "Для корректного выбора бизнес-категорий и рейтингования потенциальных клиентов необходимо "
                    "сформировать описание продукта или услуги. Введите описание продукта / услуги. Можно "
                    "использовать ссылку / ссылки в формате https://www.example.com. Можно воспользоваться "
                    "помощью ИИ-ассистента.\n\n"
                    "«Обработать» — помощь ИИ.\n"
                    "При нажатии на кнопку «Обработать» ИИ-ассистент развернёт описание, сделает его более "
                    "подробным, структурированным и удобным для машинной обработки. В дальнейшем любые "
                    "добавленные вами данные будут автоматически встроены в структуру описания. Удалённые "
                    "элементы также будут корректно исключены. Результат обработки не сохраняется автоматически.\n\n"
                    "«Сохранить» — сохранить описание\n"
                    "Кнопка «Сохранить» записывает текущую версию описания для дальнейшего использования."
                ),
                ai_subject=_("описанием продукта или услуги"),
            ),
            "company": _text_step(
                prompt_key="create_sell_company",
                user_id="panel.audience.create_edit_sell.company",
                nav_label=_("Компания"),
                summary_label=_("Компания - продавец"),
                dirty_label=_("Компания"),
                editor_label=_("Компания - продавец. Название, адрес, страна, размер, опыт, специализация."),
                placeholder=_(
                    "Для корректного выбора бизнес-категорий и рейтингования потенциальных клиентов необходимо "
                    "сформировать описание компании-продавца. Укажите название компании, страну и адрес. "
                    "Уточните специализацию компании, опыт работы, возраст и размер компании. Укажите сайт "
                    "компании. Используйте ссылки в формате: https://www.example.com. При необходимости можно "
                    "воспользоваться помощью ИИ-ассистента.\n\n"
                    "«Обработать» — помощь ИИ\n"
                    "При нажатии на кнопку «Обработать» ИИ-ассистент развернёт описание, сделает его более "
                    "подробным, структурированным и удобным для машинной обработки. В дальнейшем любые "
                    "добавленные вами данные будут встроены в описание. Удалённые элементы также будут "
                    "корректно исключены. Результат обработки автоматически не сохраняется.\n\n"
                    "«Сохранить» — сохранить описание\n"
                    "Кнопка «Сохранить» записывает текущую версию описания для дальнейшего использования."
                ),
                ai_subject=_("описанием компании"),
            ),
            "geo": _text_step(
                prompt_key="create_sell_geo",
                user_id="panel.audience.create_edit_sell.geo",
                nav_label=_("География"),
                summary_label=_("География. Ограничения, предпочтения, приоритеты."),
                dirty_label=_("География"),
                editor_label=_("География. Ограничения, предпочтения, приоритеты."),
                placeholder=_(
                    "Для корректного отбора и приоритезации городов необходимо сформировать географические "
                    "критерии.\n"
                    "Если существуют конкретные ограничения по географии поиска клиентов, укажите их. Это "
                    "может быть конкретный город, радиус вокруг города, федеральная земля, агломерация и т. д.\n\n"
                    "При необходимости можно воспользоваться помощью ИИ-ассистента.\n\n"
                    "«Обработать» — помощь ИИ\n"
                    "При нажатии на кнопку «Обработать» ИИ-ассистент развернёт географические предпочтения "
                    "и ограничения, сделает их более подробными и конкретными. В дальнейшем любые добавленные "
                    "вами данные будут встроены в описание. Удалённые элементы также будут корректно исключены. "
                    "Результат обработки автоматически не сохраняется.\n\n"
                    "«Сохранить» — сохранить географические ограничения и предпочтения\n"
                    "Кнопка «Сохранить» записывает текущую версию географических критериев для дальнейшего использования."
                ),
                ai_subject=_("описанием географии"),
            ),
            **COMMON_FUTURE_STEPS,
        },
    },
    "buy": {
        "type": "buy",
        "template_name": "panels/aap_audience/create/flow.html",
        "edit_url_name": "audience:create_edit_buy",
        "edit_url_name_id": "audience:create_edit_buy_id",
        "dialog_session_prefix": "create_edit_buy_dialog",
        "mode_class": "YY-STATUS_YELLOW",
        "mode_label": _("Поиск поставщиков / подрядчиков"),
        "page_title": _("Список рассылки: поиск поставщиков / подрядчиков"),
        "steps": {
            "product": _text_step(
                prompt_key="create_buy_product",
                user_id="panel.audience.create_edit_buy.product",
                nav_label=_("Продукт"),
                summary_label=_("Продукт / услуга"),
                dirty_label=_("Продукт / услуга"),
                editor_label=_(
                    "Продукт / услуга. Что нужно купить / заказать? Кто поставляет / выполняет работы?"
                ),
                placeholder=_(
                    "Для корректного выбора бизнес-категорий и рейтингования потенциальных поставщиков и "
                    "подрядчиков необходимо сформировать описание закупаемого продукта, услуги или работ. "
                    "Введите описание того, что нужно купить, заказать или отдать на подряд. Можно "
                    "использовать ссылку / ссылки в формате https://www.example.com. Можно воспользоваться "
                    "помощью ИИ-ассистента.\n\n"
                    "«Обработать» — помощь ИИ.\n"
                    "При нажатии на кнопку «Обработать» ИИ-ассистент развернёт описание, сделает его более "
                    "подробным, структурированным и удобным для машинной обработки. В дальнейшем любые "
                    "добавленные вами данные будут автоматически встроены в структуру описания. Удалённые "
                    "элементы также будут корректно исключены. Результат обработки не сохраняется автоматически.\n\n"
                    "«Сохранить» — сохранить описание\n"
                    "Кнопка «Сохранить» записывает текущую версию описания для дальнейшего использования."
                ),
                ai_subject=_("описанием закупаемого продукта, услуги или работ"),
            ),
            "company": _text_step(
                prompt_key="create_buy_company",
                user_id="panel.audience.create_edit_buy.company",
                nav_label=_("Компания"),
                summary_label=_("Компания-заказчик"),
                dirty_label=_("Компания-заказчик"),
                editor_label=_("Компания-заказчик. Название, адрес, страна, размер, опыт, специализация."),
                placeholder=_(
                    "Для корректного выбора бизнес-категорий и рейтингования потенциальных поставщиков и "
                    "подрядчиков необходимо сформировать описание компании-заказчика. Укажите название компании, "
                    "страну и адрес. Уточните специализацию компании, опыт работы, возраст и размер компании. "
                    "Укажите сайт компании. Используйте ссылки в формате: https://www.example.com. При "
                    "необходимости можно воспользоваться помощью ИИ-ассистента.\n\n"
                    "«Обработать» — помощь ИИ\n"
                    "При нажатии на кнопку «Обработать» ИИ-ассистент развернёт описание, сделает его более "
                    "подробным, структурированным и удобным для машинной обработки. В дальнейшем любые "
                    "добавленные вами данные будут встроены в описание. Удалённые элементы также будут "
                    "корректно исключены. Результат обработки автоматически не сохраняется.\n\n"
                    "«Сохранить» — сохранить описание\n"
                    "Кнопка «Сохранить» записывает текущую версию описания для дальнейшего использования."
                ),
                ai_subject=_("описанием компании-заказчика"),
            ),
            "geo": _text_step(
                prompt_key="create_buy_geo",
                user_id="panel.audience.create_edit_buy.geo",
                nav_label=_("География"),
                summary_label=_("География. Ограничения, предпочтения, приоритеты."),
                dirty_label=_("География"),
                editor_label=_("География. Ограничения, предпочтения, приоритеты."),
                placeholder=_(
                    "Для корректного отбора и приоритезации городов необходимо сформировать географические "
                    "критерии поиска поставщиков и подрядчиков.\n"
                    "Если существуют конкретные ограничения по географии поиска поставщиков и подрядчиков, "
                    "укажите их. Это может быть конкретный город, радиус вокруг города, федеральная земля, "
                    "агломерация и т. д.\n\n"
                    "При необходимости можно воспользоваться помощью ИИ-ассистента.\n\n"
                    "«Обработать» — помощь ИИ\n"
                    "При нажатии на кнопку «Обработать» ИИ-ассистент развернёт географические предпочтения "
                    "и ограничения, сделает их более подробными и конкретными. В дальнейшем любые добавленные "
                    "вами данные будут встроены в описание. Удалённые элементы также будут корректно исключены. "
                    "Результат обработки автоматически не сохраняется.\n\n"
                    "«Сохранить» — сохранить географические ограничения и предпочтения\n"
                    "Кнопка «Сохранить» записывает текущую версию географических критериев для дальнейшего использования."
                ),
                ai_subject=_("описанием географии поиска поставщиков и подрядчиков"),
            ),
            **COMMON_FUTURE_STEPS,
        },
    },
}


def _get_flow_config(flow_type: str) -> dict[str, Any]:
    return FLOW_TYPE_CONFIG[flow_type]


def _build_step_definitions(flow_type: str) -> dict[str, dict[str, Any]]:
    flow_conf = _get_flow_config(flow_type)
    step_defs: dict[str, dict[str, Any]] = {}
    for key in FLOW_STEP_ORDER:
        merged = dict(BASE_STEP_DEFINITIONS.get(key, {}))
        merged.update(flow_conf["steps"].get(key, {}))
        step_defs[key] = merged
    return step_defs


def _build_edit_url(flow_type: str, item_id: str, step_key: str) -> str:
    step_part = STEP_URL_PARTS.get(step_key, "product")
    route_name = f"audience:create_edit_{flow_type}_{step_part}"
    if item_id:
        return reverse(f"{route_name}_id", args=[item_id])
    return reverse(route_name)


def _session_key(request, flow_type: str, item_id: str, section: str) -> str:
    flow_conf = _get_flow_config(flow_type)
    return (
        f"{flow_conf['dialog_session_prefix']}:"
        f"{request.workspace_id}:{request.user.id}:{item_id or 'new'}:{section}"
    )


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


def _has_technical_title(task) -> bool:
    if not task:
        return False
    return f"#{int(task.id)}" in str(task.title or "")


def _suggest_title_for_task(request, task) -> str:
    resp = GPTClient().ask(
        model="gpt-5.4",
        instructions=_prompt_instructions(request, _title_prompt_key(task)),
        input=_title_input(task),
        user_id=_title_user_id(task),
        service_tier="flex",
        web_search=False,
    )
    return str(resp.content or "").strip()


def _maybe_update_title_on_geo_enter(request, *, requested_step: str, task):
    if request.method == "POST" or requested_step != "geo" or not task or not _has_technical_title(task):
        return task
    try:
        title = _suggest_title_for_task(request, task)
    except Exception:
        return task
    if not title:
        return task
    AudienceTask.objects.filter(id=task.id).update(title=title)
    task.title = title
    return task


def _parse_ai_json(text: str, main_key: str) -> tuple[str, str, str]:
    raw = (text or "").strip()
    if not raw:
        return "", "", ""
    try:
        data = json.loads(raw)
    except Exception:
        s = raw.find("{")
        e = raw.rfind("}")
        if s == -1 or e == -1 or e <= s:
            return "", raw, ""
        try:
            data = json.loads(raw[s : e + 1])
        except Exception:
            return "", raw, ""

    if not isinstance(data, dict):
        return "", raw, ""

    main_value = str(data.get(main_key) or "").strip()
    advice_answer = str(data.get("advice_answer") or "").strip()
    advice_question = str(data.get("advice_question") or "").strip()
    advice_legacy = str(data.get("advice") or "").strip()
    if advice_answer or advice_question:
        return main_value, advice_answer, advice_question
    return main_value, advice_legacy, ""


def _resolve_task(request, flow_type: str, item_id: str):
    if not item_id:
        return None
    try:
        pk = int(decode_id(item_id))
    except Exception:
        return None
    return (
        AudienceTask.objects.filter(
            id=pk,
            workspace_id=request.workspace_id,
            archived=False,
            type=flow_type,
        ).first()
    )


def _run_section_dialog(request, *, flow_type: str, step_def: Mapping[str, Any], item_id: str, value: str, command: str):
    state_key = _session_key(request, flow_type, item_id, str(step_def["json_key"]))
    state = request.session.get(state_key, {}) or {}
    if step_def["json_key"] == "geo":
        task = _resolve_task(request, flow_type, item_id)
        payload = json.dumps(
            {
                "geo": value,
                "product": (task.source_product or "") if task else "",
                "company": (task.source_company or "") if task else "",
                "command": command,
            },
            ensure_ascii=False,
        )
    else:
        payload = f"{step_def['input_label']}:\n{value}\n\nКОМАНДА:\n{command}"

    resp = GPTClient().ask_dialog(
        model="gpt-5.4",
        instructions=_prompt_instructions(request, step_def["prompt_key"]),
        input=payload,
        conversation=str(state.get("conversation_id") or ""),
        previous_response_id=str(state.get("response_id") or ""),
        user_id=step_def["user_id"],
        service_tier="flex",
    )
    new_value, new_advice, new_question = _parse_ai_json(resp.content or "", str(step_def["json_key"]))

    raw = resp.raw if isinstance(resp.raw, dict) else {}
    response_id = str(raw.get("id") or "").strip()
    conv_val = raw.get("conversation")
    conversation_id = ""
    if isinstance(conv_val, dict):
        conversation_id = str(conv_val.get("id") or "").strip()
    elif conv_val is not None:
        conversation_id = str(conv_val).strip()

    request.session[state_key] = {
        "conversation_id": conversation_id or str(state.get("conversation_id") or ""),
        "response_id": response_id or str(state.get("response_id") or ""),
    }
    request.session.modified = True
    return new_value, new_advice, new_question


def _reset_section_dialog(request, *, flow_type: str, item_id: str, step_key: str):
    request.session.pop(_session_key(request, flow_type, item_id, step_key), None)
    request.session.modified = True


def _create_task(request, *, flow_type: str, title: str, **extra_fields):
    task = AudienceTask.objects.create(
        workspace_id=request.workspace_id,
        user=request.user,
        task="",
        title=title,
        task_branches="",
        task_geo="",
        type=flow_type,
        **extra_fields,
    )
    title_value = f"{_('Список рассылки')} #{int(task.id)}"
    AudienceTask.objects.filter(id=task.id).update(title=title_value)
    task.title = title_value
    return task


def _task_saved_values(task) -> dict[str, Any]:
    return {
        "title": (task.title or "") if task else "",
        "source_product": (task.source_product or "") if task else "",
        "source_company": (task.source_company or "") if task else "",
        "source_geo": (task.source_geo or "") if task else "",
        "task_branches": (task.task_branches or "") if task else "",
        "task_geo": (task.task_geo or "") if task else "",
        "run_processing": bool(task.run_processing) if task else False,
    }


def _has_insertable_company_tasks(request, current_task) -> bool:
    queryset = AudienceTask.objects.filter(
        workspace_id=request.workspace_id,
        archived=False,
    ).order_by("-updated_at")
    if current_task:
        queryset = queryset.exclude(id=current_task.id)
    for source_company in queryset.values_list("source_company", flat=True):
        if str(source_company or "").strip():
            return True
    return False


def _handle_step_action(
    *,
    request,
    flow_type: str,
    item_id: str,
    action: str,
    task,
    step_definitions: Mapping[str, Mapping[str, Any]],
    working_values: dict[str, str],
    ai_command_display_map: dict[str, str],
    ai_advice_map: dict[str, str],
    ai_question_map: dict[str, str],
):
    for step_key in TEXT_STEP_KEYS:
        step_def = step_definitions[step_key]
        field_name = str(step_def["field_name"])
        field_value = working_values[field_name]

        if action == step_def["process_action"]:
            try:
                new_value, new_advice, new_question = _run_section_dialog(
                    request,
                    flow_type=flow_type,
                    step_def=step_def,
                    item_id=item_id,
                    value=field_value,
                    command=ai_command_display_map[step_key],
                )
                if new_value:
                    working_values[field_name] = new_value
                ai_advice_map[step_key] = new_advice
                ai_question_map[step_key] = new_question
            except Exception:
                pass
            return task, None, True

        if action == step_def["save_action"] and field_value:
            if task:
                setattr(task, field_name, field_value)
                task.save(update_fields=[field_name, "updated_at"])
                ai_command_display_map[step_key] = ""
                ai_advice_map[step_key] = "__saved__"
                ai_question_map[step_key] = ""
                return task, None, True

            if step_key not in TASK_CREATION_STEP_KEYS:
                return task, None, True

            task = _create_task(
                request,
                flow_type=flow_type,
                title="",
                **{field_name: field_value},
            )
            return task, redirect(_build_edit_url(flow_type, encode_id(int(task.id)), step_key)), True

        if action == step_def["reset_action"]:
            _reset_section_dialog(request, flow_type=flow_type, item_id=item_id, step_key=step_key)
            working_values[field_name] = ""
            ai_command_display_map[step_key] = ""
            ai_advice_map[step_key] = ""
            ai_question_map[step_key] = ""
            return task, None, True

    return task, None, False


def _build_current_step_context(
    *,
    flow_type: str,
    item_id: str,
    step_definitions: Mapping[str, Mapping[str, Any]],
    flow_step_states: list[dict[str, Any]],
    current_step_key: str,
    working_values: Mapping[str, str],
    saved_values: Mapping[str, Any],
    ai_command_display_map: Mapping[str, str],
    ai_advice_map: Mapping[str, str],
    ai_question_map: Mapping[str, str],
    has_insertable_company_tasks: bool,
) -> dict[str, Any]:
    current_step = step_definitions[current_step_key]
    next_step_key = get_next_step_key(flow_step_states, current_step_key)
    next_step = step_definitions.get(next_step_key, current_step)

    field_name = str(current_step["field_name"])
    command_field = str(current_step["command_field"])

    return {
        "key": current_step_key,
        "field_name": field_name,
        "command_field": command_field,
        "editor_label": current_step["editor_label"],
        "placeholder": current_step["placeholder"],
        "value": working_values.get(field_name, ""),
        "saved_value": saved_values.get(field_name, ""),
        "process_action": current_step["process_action"],
        "save_action": current_step["save_action"],
        "reset_action": current_step["reset_action"],
        "ai_command_value": "",
        "ai_command_display": ai_command_display_map[current_step_key],
        "ai_advice": ai_advice_map[current_step_key],
        "ai_question": ai_question_map[current_step_key],
        "ai_help_paragraphs": current_step["ai_help_paragraphs"],
        "next_url": _build_edit_url(flow_type, item_id, next_step_key),
        "next_save_label": next_step.get("summary_label") or next_step.get("nav_label") or "",
        "next_footer_label": next_step.get("nav_label") or next_step.get("summary_label") or "",
        "show_insert_company_option": bool(current_step_key == "company" and has_insertable_company_tasks),
        "show_insert_company_button": bool(
            current_step_key == "company"
            and has_insertable_company_tasks
            and not str(working_values.get(field_name, "") or "").strip()
        ),
        "insert_company_modal_url": (
            reverse("audience:create_company_insert_modal") + (f"?id={item_id}" if item_id else "")
        ),
    }


def _build_placeholder_step_context(
    *,
    step_definitions: Mapping[str, Mapping[str, Any]],
    current_step_key: str,
) -> dict[str, str] | None:
    current_step = step_definitions.get(current_step_key, {})
    if not bool(current_step.get("is_placeholder_view")):
        return None
    return {
        "title": str(current_step.get("placeholder_title") or current_step.get("summary_label") or current_step_key),
        "text": str(current_step.get("placeholder_text") or _("Раздел пока в разработке.")),
    }


def create_edit_flow_view(request, *, flow_type: str, step_key: str, item_id: str = ""):
    flow_conf = _get_flow_config(flow_type)
    step_definitions = _build_step_definitions(flow_type)
    requested_step = (step_key or "product").strip().lower()

    task = _resolve_task(request, flow_type, item_id)
    task = _maybe_update_title_on_geo_enter(request, requested_step=requested_step, task=task)
    saved_values = _task_saved_values(task)
    flow_status = build_flow_step_states(
        step_order=FLOW_STEP_ORDER,
        step_definitions=step_definitions,
        requested_step_key=requested_step,
        saved_values=saved_values,
        url_builder=lambda step_key: _build_edit_url(flow_type, item_id, step_key),
    )
    current_step_key = str(flow_status["current_step_key"] or "product")
    if current_step_key != requested_step:
        return redirect(_build_edit_url(flow_type, item_id, current_step_key))

    working_values = {
        "source_product": str(saved_values["source_product"] or ""),
        "source_company": str(saved_values["source_company"] or ""),
        "source_geo": str(saved_values["source_geo"] or ""),
    }
    ai_command_display_map = {key: "" for key in TEXT_STEP_KEYS}
    ai_advice_map = {key: "" for key in TEXT_STEP_KEYS}
    ai_question_map = {key: "" for key in TEXT_STEP_KEYS}

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()

        for step_key in TEXT_STEP_KEYS:
            step_def = step_definitions[step_key]
            field_name = str(step_def["field_name"])
            command_field = str(step_def["command_field"])
            working_values[field_name] = (request.POST.get(field_name) or saved_values[field_name]).strip()
            ai_command_display_map[step_key] = (request.POST.get(command_field) or "").strip()

        if action == "close":
            return redirect("audience:create_list")

        else:
            task, redirect_response, handled = _handle_step_action(
                request=request,
                flow_type=flow_type,
                item_id=item_id,
                action=action,
                task=task,
                step_definitions=step_definitions,
                working_values=working_values,
                ai_command_display_map=ai_command_display_map,
                ai_advice_map=ai_advice_map,
                ai_question_map=ai_question_map,
            )
            if redirect_response is not None:
                return redirect_response
            if not handled:
                task = _resolve_task(request, flow_type, item_id) if item_id else task

        task = _resolve_task(request, flow_type, item_id) if item_id else task
        saved_values = _task_saved_values(task)
        flow_status = build_flow_step_states(
            step_order=FLOW_STEP_ORDER,
            step_definitions=step_definitions,
            requested_step_key=current_step_key,
            saved_values=saved_values,
            url_builder=lambda step_key: _build_edit_url(flow_type, item_id, step_key),
        )
        current_step_key = str(flow_status["current_step_key"] or "product")

    if current_step_key in TEXT_STEP_KEYS:
        has_insertable_company_tasks = _has_insertable_company_tasks(request, task) if current_step_key == "company" else False
        current_step = _build_current_step_context(
            flow_type=flow_type,
            item_id=item_id,
            step_definitions=step_definitions,
            flow_step_states=list(flow_status["step_states"]),
            current_step_key=current_step_key,
            working_values=working_values,
            saved_values=saved_values,
            ai_command_display_map=ai_command_display_map,
            ai_advice_map=ai_advice_map,
            ai_question_map=ai_question_map,
            has_insertable_company_tasks=has_insertable_company_tasks,
        )
    else:
        current_step = None
    placeholder_step = _build_placeholder_step_context(
        step_definitions=step_definitions,
        current_step_key=current_step_key,
    )

    summary_items = [
        {
            "key": "product",
            "label": step_definitions["product"]["summary_label"],
            "value": saved_values["source_product"],
        },
        {
            "key": "company",
            "label": step_definitions["company"]["summary_label"],
            "value": saved_values["source_company"],
        },
        {
            "key": "geo",
            "label": step_definitions["geo"]["summary_label"],
            "value": saved_values["source_geo"],
        },
    ]

    flow_js_config = {
        "labels": {
            **{
                key: str(step_definitions[key].get("dirty_label") or step_definitions[key].get("summary_label") or key)
                for key in FLOW_STEP_ORDER
                if step_definitions.get(key)
            },
        },
        "requiredStepKeys": ["product", "company", "geo"],
        "missingTitle": str(_("Не заполнены обязательные разделы")),
        "missingText": str(_("Не заполнены или не сохранены разделы:")),
        "closeLabel": str(_("Отменить")),
    }
    display_title = saved_values["title"] or str(_("Новый список рассылки"))

    return render(
        request,
        flow_conf["template_name"],
        {
            "type": flow_type,
            "is_placeholder": bool(placeholder_step),
            "status": current_step_key,
            "task": task,
            "task_id_token": item_id,
            "saved_title": saved_values["title"],
            "display_title": display_title,
            "flow_mode_class": flow_conf["mode_class"],
            "flow_mode_label": flow_conf["mode_label"],
            "page_title": flow_conf["page_title"],
            "flow_close_url": reverse("audience:create_list"),
            "flow_step_states": flow_status["step_states"],
            "flow_current_step_key": current_step_key,
            "current_step": current_step,
            "placeholder_step": placeholder_step,
            "summary_items": summary_items,
            "flow_js_config": flow_js_config,
        },
    )
