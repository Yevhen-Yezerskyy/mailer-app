# FILE: web/panel/aap_audience/views/create_edit_flow_shared.py
# DATE: 2026-03-23
# PURPOSE: Shared config and helpers for the create/edit flow dispatcher and step handlers.

from __future__ import annotations

import json
from urllib.parse import urlsplit
from typing import Any, Mapping

from django.db import connection
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme
from django.utils.text import format_lazy
from django.utils.translation import gettext_lazy as _trans

from engine.common.gpt import GPTClient
from engine.common.translate import get_prompt
from engine.common.utils import h64_text
from mailer_web.access import decode_id
from panel.aap_campaigns.models import Campaign
from panel.aap_audience.models import AudienceTask

from .create_edit_flow_gpt_consts import FLOW_GPT_MODEL, FLOW_GPT_SERVICE_TIER

FLOW_STEP_ORDER = (
    "product",
    "company",
    "geo",
    "branches",
    "cities",
    "contacts",
    "mailing_list",
)

TEXT_STEP_KEYS = ("product", "company", "geo")
TASK_CREATION_STEP_KEYS = frozenset({"product", "company"})
FLOW_GPT_UNAVAILABLE_SESSION_KEY = "aap:create_flow:gpt_unavailable_popup_text"
FLOW_GPT_UNAVAILABLE_TEXT = _trans("Ассистент ИИ сейчас недоступен. Повторите попытку в течение пяти минут.")

STEP_URL_PARTS = {
    "product": "product",
    "company": "company",
    "geo": "geo",
    "branches": "branches",
    "cities": "cities",
    "contacts": "contacts",
    "mailing_list": "mailing_list",
}

COMMON_AI_HELP_TEXT = _trans(
    "В процессе обработки ИИ будет задавать вопросы и давать подсказки для уточнения описания. "
    "Поскольку основные варианты уже указаны в начальном описании, часть подсказок может быть "
    "нерелевантной. При необходимости добавьте информацию в описание или в команду для ассистента ИИ."
)

COMMON_FUTURE_STEPS = {
    "branches": {
        "nav_label": _trans("Категории"),
        "summary_label": _trans("Категории"),
        "dirty_label": _trans("Категории"),
    },
    "cities": {
        "nav_label": _trans("Города"),
        "summary_label": _trans("Города"),
        "dirty_label": _trans("Города"),
    },
    "contacts": {
        "nav_label": _trans("Контакты"),
        "summary_label": _trans("Сбор контактов"),
        "dirty_label": _trans("Сбор контактов"),
    },
    "mailing_list": {
        "nav_label": _trans("Рейтинг"),
        "summary_label": _trans("Список рассылки"),
        "dirty_label": _trans("Список рассылки"),
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
    "branches": {
        "completion_type": "never",
        "visible": True,
        "implemented": True,
        "depends_on": ("product", "company", "geo"),
    },
    "cities": {
        "completion_type": "never",
        "visible": True,
        "implemented": True,
        "depends_on": ("product", "company", "geo"),
    },
    "contacts": {
        "completion_type": "never",
        "visible": True,
        "implemented": True,
        "depends_on": ("product", "company", "geo"),
    },
    "mailing_list": {
        "completion_type": "never",
        "visible": True,
        "implemented": True,
        "depends_on": ("product", "company", "geo"),
    },
}


def _make_ai_help_paragraphs(subject):
    return (
        format_lazy(
            _trans(
                "Введите команду для работы с {subject} — что нужно добавить, удалить или изменить. "
                "Указанная команда будет применена к текущему описанию."
            ),
            subject=subject,
        ),
        format_lazy(
            _trans(
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
        "mode_label": _trans("Поиск клиентов / покупателей"),
        "page_title": _trans("Список рассылки: поиск клиентов / покупателей"),
        "steps": {
            "product": _text_step(
                prompt_key="create_sell_product",
                user_id="panel.audience.create_edit_sell.product",
                nav_label=_trans("Продукт"),
                summary_label=_trans("Продукт / услуга"),
                dirty_label=_trans("Продукт / услуга"),
                editor_label=_trans("Продукт / услуга. Что продаётся? Где применяется? Кто покупатель?"),
                placeholder=_trans(
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
                ai_subject=_trans("описанием продукта или услуги"),
            ),
            "company": _text_step(
                prompt_key="create_sell_company",
                user_id="panel.audience.create_edit_sell.company",
                nav_label=_trans("Компания"),
                summary_label=_trans("Компания - продавец"),
                dirty_label=_trans("Компания"),
                editor_label=_trans("Компания - продавец. Название, адрес, страна, размер, опыт, специализация."),
                placeholder=_trans(
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
                ai_subject=_trans("описанием компании"),
            ),
            "geo": _text_step(
                prompt_key="create_sell_geo",
                user_id="panel.audience.create_edit_sell.geo",
                nav_label=_trans("География"),
                summary_label=_trans("География. Ограничения, предпочтения, приоритеты."),
                dirty_label=_trans("География"),
                editor_label=_trans("География. Ограничения, предпочтения, приоритеты."),
                placeholder=_trans(
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
                ai_subject=_trans("описанием географии"),
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
        "mode_label": _trans("Поиск поставщиков / подрядчиков"),
        "page_title": _trans("Список рассылки: поиск поставщиков / подрядчиков"),
        "steps": {
            "product": _text_step(
                prompt_key="create_buy_product",
                user_id="panel.audience.create_edit_buy.product",
                nav_label=_trans("Продукт"),
                summary_label=_trans("Продукт / услуга"),
                dirty_label=_trans("Продукт / услуга"),
                editor_label=_trans(
                    "Продукт / услуга. Что нужно купить / заказать? Кто поставляет / выполняет работы?"
                ),
                placeholder=_trans(
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
                ai_subject=_trans("описанием закупаемого продукта, услуги или работ"),
            ),
            "company": _text_step(
                prompt_key="create_buy_company",
                user_id="panel.audience.create_edit_buy.company",
                nav_label=_trans("Компания"),
                summary_label=_trans("Компания-заказчик"),
                dirty_label=_trans("Компания-заказчик"),
                editor_label=_trans("Компания-заказчик. Название, адрес, страна, размер, опыт, специализация."),
                placeholder=_trans(
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
                ai_subject=_trans("описанием компании-заказчика"),
            ),
            "geo": _text_step(
                prompt_key="create_buy_geo",
                user_id="panel.audience.create_edit_buy.geo",
                nav_label=_trans("География"),
                summary_label=_trans("География. Ограничения, предпочтения, приоритеты."),
                dirty_label=_trans("География"),
                editor_label=_trans("География. Ограничения, предпочтения, приоритеты."),
                placeholder=_trans(
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
                ai_subject=_trans("описанием географии поиска поставщиков и подрядчиков"),
            ),
            **COMMON_FUTURE_STEPS,
        },
    },
}


def get_flow_config(flow_type: str) -> dict[str, Any]:
    return FLOW_TYPE_CONFIG[flow_type]


def build_step_definitions(flow_type: str) -> dict[str, dict[str, Any]]:
    flow_conf = get_flow_config(flow_type)
    step_defs: dict[str, dict[str, Any]] = {}
    for key in FLOW_STEP_ORDER:
        merged = dict(BASE_STEP_DEFINITIONS.get(key, {}))
        merged.update(flow_conf["steps"].get(key, {}))
        step_defs[key] = merged
    return step_defs


def build_edit_url(flow_type: str, item_id: str, step_key: str) -> str:
    step_part = STEP_URL_PARTS.get(step_key, "product")
    route_name = f"audience:create_edit_{flow_type}_{step_part}"
    if item_id:
        return reverse(f"{route_name}_id", args=[item_id])
    return reverse(route_name)


def is_gpt_ok(resp: Any) -> bool:
    return str(getattr(resp, "status", "") or "").strip().upper() == "OK"


def mark_flow_gpt_unavailable(request) -> None:
    request.session[FLOW_GPT_UNAVAILABLE_SESSION_KEY] = FLOW_GPT_UNAVAILABLE_TEXT
    request.session.modified = True


def pop_flow_gpt_unavailable_text(request) -> str:
    if request is None:
        return ""
    value = str(request.session.pop(FLOW_GPT_UNAVAILABLE_SESSION_KEY, "") or "").strip()
    if value:
        request.session.modified = True
    return value


def clear_dialog_state(state: dict[str, Any]) -> None:
    state["conversation_id"] = ""
    state["response_id"] = ""


def flow_back_url(request, fallback_url: str) -> str:
    ref = str(request.META.get("HTTP_REFERER") or "").strip()
    if not ref:
        return fallback_url
    try:
        host = request.get_host()
    except Exception:
        return fallback_url
    if not url_has_allowed_host_and_scheme(
        ref,
        allowed_hosts={host},
        require_https=request.is_secure(),
    ):
        return fallback_url
    parsed = urlsplit(ref)
    target = parsed.path or "/"
    if parsed.query:
        target = f"{target}?{parsed.query}"
    if target == request.get_full_path():
        return fallback_url
    return target


def session_key(request, flow_type: str, item_id: str, section: str) -> str:
    flow_conf = get_flow_config(flow_type)
    return (
        f"{flow_conf['dialog_session_prefix']}:"
        f"{request.workspace_id}:{request.user.id}:{item_id or 'new'}:{section}"
    )


def prompt_instructions(request, prompt_key: str) -> str:
    lang_name = request.ui_lang_name_en
    on_gpt_error = lambda: mark_flow_gpt_unavailable(request)
    lang_prompt = get_prompt("lang_response", on_gpt_error=on_gpt_error).replace("{LANG}", lang_name).strip()
    prompt_text = get_prompt(prompt_key, on_gpt_error=on_gpt_error).strip()
    return "\n\n".join(part for part in (lang_prompt, prompt_text) if part).strip()


def title_prompt_key(task) -> str:
    return "create_buy_title" if str(task.type or "").strip() == "buy" else "create_sell_title"


def title_user_id(task) -> str:
    suffix = "buy" if str(task.type or "").strip() == "buy" else "sell"
    return f"panel.audience.create_edit_{suffix}.title"


def title_input(task) -> str:
    return (
        f"PRODUCT:\n{(task.source_product or '').strip()}\n\n"
        f"COMPANY:\n{(task.source_company or '').strip()}\n\n"
        f"GEO:\n{(task.source_geo or '').strip()}"
    )


def has_technical_title(task) -> bool:
    if not task:
        return False
    return f"#{int(task.id)}" in str(task.title or "")


def suggest_title_for_task(request, task) -> tuple[str, bool]:
    resp = GPTClient().ask(
        model=FLOW_GPT_MODEL,
        instructions=prompt_instructions(request, title_prompt_key(task)),
        input=title_input(task),
        user_id=title_user_id(task),
        service_tier=FLOW_GPT_SERVICE_TIER,
        web_search=False,
    )
    if not is_gpt_ok(resp):
        mark_flow_gpt_unavailable(request)
        return "", False
    return str(resp.content or "").strip(), True


def maybe_update_title_on_geo_enter(request, *, requested_step: str, task) -> tuple[Any, bool]:
    if request.method == "POST" or requested_step != "geo" or not task or not has_technical_title(task):
        return task, False
    try:
        title, ok = suggest_title_for_task(request, task)
    except Exception:
        mark_flow_gpt_unavailable(request)
        return task, True
    if not ok:
        return task, True
    if not title:
        return task, False
    AudienceTask.objects.filter(id=task.id).update(title=title)
    task.title = title
    return task, False


def parse_ai_json(text: str, main_key: str) -> tuple[str, str, str]:
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


def resolve_task(request, flow_type: str, item_id: str, *, include_archived: bool = False):
    if not item_id:
        return None
    try:
        pk = int(decode_id(item_id))
    except Exception:
        return None
    query = AudienceTask.objects.filter(
        id=pk,
        workspace_id=request.workspace_id,
        type=flow_type,
    )
    if not include_archived:
        query = query.filter(archived=False)
    return query.first()


def reset_section_dialog(request, *, flow_type: str, item_id: str, step_key: str):
    request.session.pop(session_key(request, flow_type, item_id, step_key), None)
    request.session.modified = True


def create_task(request, *, flow_type: str, title: str, **extra_fields):
    task = AudienceTask.objects.create(
        workspace_id=request.workspace_id,
        user=request.user,
        title=title,
        type=flow_type,
        **extra_fields,
    )
    title_value = f"{_trans('Список рассылки')} #{int(task.id)}"
    AudienceTask.objects.filter(id=task.id).update(title=title_value)
    task.title = title_value
    return task


def task_saved_values(task) -> dict[str, Any]:
    return {
        "title": (task.title or "") if task else "",
        "source_product": (task.source_product or "") if task else "",
        "source_company": (task.source_company or "") if task else "",
        "source_geo": (task.source_geo or "") if task else "",
        "ready": bool(task.ready) if task else False,
        "user_active": bool(task.user_active) if task else False,
    }


def current_contact_rating_hash(task) -> int:
    if not task:
        return 0
    task_type = str(task.type or "").strip().lower()
    return int(
        h64_text(
            task_type
            + str(task.source_product or "")
            + str(task.source_company or "")
            + str(task.source_geo or "")
        )
    )


def build_contact_rating_hash_alert_context(task) -> dict[str, Any]:
    if not task or bool(task.archived):
        return {
            "show": False,
            "rated_count": 0,
            "task_hash": 0,
            "has_hash_mismatch": False,
            "is_used_in_campaign": False,
        }

    task_hash = current_contact_rating_hash(task)
    is_used_in_campaign = Campaign.objects.filter(
        workspace_id=task.workspace_id,
        sending_list_id=int(task.id),
        archived=False,
    ).exists()
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE sl.rate IS NOT NULL)::int AS rated_count,
                BOOL_OR(sl.rate IS NOT NULL AND sl.rating_hash IS DISTINCT FROM %s) AS has_hash_mismatch
            FROM public.sending_lists sl
            WHERE sl.task_id = %s
              AND COALESCE(sl.removed, false) = false
            """,
            [int(task_hash), int(task.id)],
        )
        row = cur.fetchone() or [0, False]

    rated_count = int((row or [0])[0] or 0)
    has_hash_mismatch = bool((row or [0, False])[1])
    show = (not is_used_in_campaign) and rated_count <= 500 and has_hash_mismatch
    return {
        "show": bool(show),
        "rated_count": int(rated_count),
        "task_hash": int(task_hash),
        "has_hash_mismatch": bool(has_hash_mismatch),
        "is_used_in_campaign": bool(is_used_in_campaign),
    }


def has_insertable_company_tasks(request, current_task) -> bool:
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


def build_current_step_context(
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
    from .create_edit_flow_status import get_next_step_key

    current_step = step_definitions[current_step_key]
    next_step_key = get_next_step_key(flow_step_states, current_step_key)
    next_step = step_definitions.get(next_step_key, current_step)
    if current_step_key == "geo":
        next_step_key = "branches"
        next_step = step_definitions.get("branches", current_step)

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
        "next_url": build_edit_url(flow_type, item_id, next_step_key),
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


def build_summary_items(
    step_definitions: Mapping[str, Mapping[str, Any]],
    saved_values: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
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


def build_flow_js_config(
    step_definitions: Mapping[str, Mapping[str, Any]],
    *,
    geo_title_autogen_pending: bool,
) -> dict[str, Any]:
    return {
        "labels": {
            **{
                key: str(step_definitions[key].get("dirty_label") or step_definitions[key].get("summary_label") or key)
                for key in FLOW_STEP_ORDER
                if step_definitions.get(key)
            },
        },
        "requiredStepKeys": ["product", "company", "geo"],
        "missingTitle": str(_trans("Не заполнены обязательные разделы")),
        "missingText": str(_trans("Не заполнены или не сохранены разделы:")),
        "closeLabel": str(_trans("Отменить")),
        "geoTitleAutogenPending": bool(geo_title_autogen_pending),
    }


def build_flow_render_context(
    *,
    request,
    flow_type: str,
    item_id: str,
    task,
    saved_values: Mapping[str, Any],
    step_definitions: Mapping[str, Mapping[str, Any]],
    flow_status: Mapping[str, Any],
    current_step_key: str,
    step_template: str,
    extra_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    flow_conf = get_flow_config(flow_type)
    gpt_unavailable_popup_text = pop_flow_gpt_unavailable_text(request)
    geo_title_autogen_pending = bool(task and has_technical_title(task))
    is_archived_task = bool(task and bool(getattr(task, "archived", False)))
    close_url = reverse("audience:create_list")
    if is_archived_task:
        close_url = f"{close_url}?show=archive"
    context = {
        "type": flow_type,
        "status": current_step_key,
        "task": task,
        "task_id_token": item_id,
        "saved_title": saved_values["title"],
        "display_title": saved_values["title"] or str(_trans("Новый список рассылки")),
        "flow_mode_class": flow_conf["mode_class"],
        "flow_mode_label": flow_conf["mode_label"],
        "page_title": flow_conf["page_title"],
        "flow_close_url": close_url,
        "flow_step_states": flow_status["step_states"],
        "flow_current_step_key": current_step_key,
        "summary_items": build_summary_items(step_definitions, saved_values),
        "flow_js_config": build_flow_js_config(
            step_definitions,
            geo_title_autogen_pending=geo_title_autogen_pending,
        ),
        "flow_gpt_unavailable_popup_text": gpt_unavailable_popup_text,
        "step_template": step_template,
    }
    if extra_context:
        context.update(extra_context)
    return context
