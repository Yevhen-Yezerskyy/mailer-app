# FILE: web/panel/aap_audience/views/create_edit_buy.py
# DATE: 2026-03-15
# PURPOSE: Create/edit buy page with independent title/product/company/geo flows and separate GPT dialogs.

import json

from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils.translation import get_language

from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt
from mailer_web.access import decode_id, encode_id
from panel.aap_audience.models import AudienceTask


FORM_CONFIG = {
    "product": {
        "prompt_key": "create_buy_product",
        "json_key": "product",
        "input_label": "ПРОДУКТ",
        "user_id": "panel.audience.create_edit_buy.product",
    },
    "company": {
        "prompt_key": "create_buy_company",
        "json_key": "company",
        "input_label": "COMPANY",
        "user_id": "panel.audience.create_edit_buy.company",
    },
    "geo": {
        "prompt_key": "create_buy_geo",
        "json_key": "geo",
        "input_label": "GEO",
        "user_id": "panel.audience.create_edit_buy.geo",
    },
}


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


def _build_edit_url(item_id: str, status: str) -> str:
    return f"{reverse('audience:create_edit_buy_id', args=[item_id])}?status={status}"


def _session_key(request, item_id: str, section: str) -> str:
    return f"create_edit_buy_dialog:{request.workspace_id}:{request.user.id}:{item_id or 'new'}:{section}"


def _fallback_title_from_texts(*values: str) -> str:
    text = " ".join(" ".join((value or "").split()).strip() for value in values if (value or "").strip()).strip()
    if not text:
        return "Новая аудитория"
    return text[:120].strip() or "Новая аудитория"


def _prompt_instructions(request, prompt_key: str) -> str:
    lang_code = (getattr(request, "LANGUAGE_CODE", "") or get_language() or "en").lower()
    lang_key = lang_code.split("-")[0].split("_")[0]
    lang_name = LANG_RESPONSE_NAMES.get(lang_key, "English")
    lang_prompt = get_prompt("lang_response").replace("{LANG}", lang_name).strip()
    prompt_text = get_prompt(prompt_key).strip()
    return "\n\n".join(part for part in (lang_prompt, prompt_text) if part).strip()


def _generate_task_title_from_db(
    *,
    request,
    product_text: str,
    company_text: str,
    geo_text: str,
) -> str:
    payload = (
        f"PRODUCT:\n{(product_text or '').strip()}\n\n"
        f"COMPANY:\n{(company_text or '').strip()}\n\n"
        f"GEO:\n{(geo_text or '').strip()}"
    )
    try:
        resp = GPTClient().ask(
            model="gpt-5.4",
            instructions=_prompt_instructions(request, "create_buy_title"),
            input=payload,
            user_id="panel.audience.create_edit_buy.title",
            service_tier="flex",
            use_cache=False,
        )
        title = " ".join((resp.content or "").split()).strip()
        return title[:255] if title else _fallback_title_from_texts(product_text, company_text, geo_text)
    except Exception:
        return _fallback_title_from_texts(product_text, company_text, geo_text)


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


def _resolve_task(request, item_id: str):
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
            user=request.user,
            archived=False,
            type="buy",
        ).first()
    )


def _run_section_dialog(request, *, section: str, item_id: str, value: str, command: str):
    conf = FORM_CONFIG[section]
    state_key = _session_key(request, item_id, section)
    state = request.session.get(state_key, {}) or {}
    if section == "geo":
        task = _resolve_task(request, item_id)
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
        payload = f"{conf['input_label']}:\n{value}\n\nКОМАНДА:\n{command}"

    resp = GPTClient().ask_dialog(
        model="gpt-5.4",
        instructions=_prompt_instructions(request, conf["prompt_key"]),
        input=payload,
        conversation=str(state.get("conversation_id") or ""),
        previous_response_id=str(state.get("response_id") or ""),
        user_id=conf["user_id"],
        service_tier="flex",
    )
    new_value, new_advice, new_question = _parse_ai_json(resp.content or "", conf["json_key"])

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


def _reset_section_dialog(request, *, item_id: str, section: str):
    request.session.pop(_session_key(request, item_id, section), None)
    request.session.modified = True


def create_edit_buy_view(request, item_id: str = ""):
    status = (request.GET.get("status") or request.POST.get("status") or "product").strip().lower()
    if status not in FORM_CONFIG:
        status = "product"

    task = _resolve_task(request, item_id)
    saved_title = (task.title or "") if task else ""
    saved_product = (task.source_product or "") if task else ""
    saved_company = (task.source_company or "") if task else ""
    saved_geo = (task.source_geo or "") if task else ""

    title_text = saved_title
    product_text = saved_product
    company_text = saved_company
    geo_text = saved_geo

    ai_command_display_map = {"product": "", "company": "", "geo": ""}
    ai_advice_map = {"product": "", "company": "", "geo": ""}
    ai_question_map = {"product": "", "company": "", "geo": ""}
    last_action = ""

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        last_action = action
        title_text = (request.POST.get("audience_title") or saved_title).strip()
        product_text = (request.POST.get("source_product") or saved_product).strip()
        company_text = (request.POST.get("source_company") or saved_company).strip()
        geo_text = (request.POST.get("source_geo") or saved_geo).strip()
        posted_product_command = (request.POST.get("product_ai_command") or "").strip()
        posted_company_command = (request.POST.get("company_ai_command") or "").strip()
        posted_geo_command = (request.POST.get("geo_ai_command") or "").strip()
        ai_command_display_map["product"] = posted_product_command
        ai_command_display_map["company"] = posted_company_command
        ai_command_display_map["geo"] = posted_geo_command
        if action == "save_title" and title_text:
            if task:
                task.title = title_text
                task.save(update_fields=["title", "updated_at"])
                saved_title = task.title
            else:
                task = AudienceTask.objects.create(
                    workspace_id=request.workspace_id,
                    user=request.user,
                    task="",
                    title=title_text,
                    task_branches="",
                    task_geo="",
                    type="buy",
                )
                return redirect(_build_edit_url(encode_id(int(task.id)), status))

        elif action == "suggest_title" and task and (saved_product or saved_company or saved_geo):
            title_text = _generate_task_title_from_db(
                request=request,
                product_text=saved_product,
                company_text=saved_company,
                geo_text=saved_geo,
            )

        elif action == "process_product":
            try:
                new_value, new_advice, new_question = _run_section_dialog(
                    request,
                    section="product",
                    item_id=item_id,
                    value=product_text,
                    command=posted_product_command,
                )
                if new_value:
                    product_text = new_value
                ai_advice_map["product"] = new_advice
                ai_question_map["product"] = new_question
            except Exception:
                pass

        elif action == "save_product" and product_text:
            if task:
                task.source_product = product_text
                task.save(update_fields=["source_product", "updated_at"])
                saved_product = task.source_product
                ai_command_display_map["product"] = ""
                ai_advice_map["product"] = "__saved__"
                ai_question_map["product"] = ""
            else:
                title_generated = _generate_task_title_from_db(
                    request=request,
                    product_text=product_text,
                    company_text="",
                    geo_text="",
                )
                task = AudienceTask.objects.create(
                    workspace_id=request.workspace_id,
                    user=request.user,
                    task="",
                    title=title_generated,
                    task_branches="",
                    task_geo="",
                    type="buy",
                    source_product=product_text,
                )
                return redirect(_build_edit_url(encode_id(int(task.id)), "product"))

        elif action == "reset_product_context":
            _reset_section_dialog(request, item_id=item_id, section="product")
            product_text = ""
            ai_command_display_map["product"] = ""
            ai_advice_map["product"] = ""
            ai_question_map["product"] = ""

        elif action == "process_company":
            try:
                new_value, new_advice, new_question = _run_section_dialog(
                    request,
                    section="company",
                    item_id=item_id,
                    value=company_text,
                    command=posted_company_command,
                )
                if new_value:
                    company_text = new_value
                ai_advice_map["company"] = new_advice
                ai_question_map["company"] = new_question
            except Exception:
                pass

        elif action == "save_company" and company_text:
            if task:
                task.source_company = company_text
                task.save(update_fields=["source_company", "updated_at"])
                saved_company = task.source_company
                ai_command_display_map["company"] = ""
                ai_advice_map["company"] = "__saved__"
                ai_question_map["company"] = ""
            else:
                title_generated = _generate_task_title_from_db(
                    request=request,
                    product_text="",
                    company_text=company_text,
                    geo_text="",
                )
                task = AudienceTask.objects.create(
                    workspace_id=request.workspace_id,
                    user=request.user,
                    task="",
                    title=title_generated,
                    task_branches="",
                    task_geo="",
                    type="buy",
                    source_company=company_text,
                )
                return redirect(_build_edit_url(encode_id(int(task.id)), "company"))

        elif action == "reset_company_context":
            _reset_section_dialog(request, item_id=item_id, section="company")
            company_text = ""
            ai_command_display_map["company"] = ""
            ai_advice_map["company"] = ""
            ai_question_map["company"] = ""

        elif action == "process_geo":
            try:
                new_value, new_advice, new_question = _run_section_dialog(
                    request,
                    section="geo",
                    item_id=item_id,
                    value=geo_text,
                    command=posted_geo_command,
                )
                if new_value:
                    geo_text = new_value
                ai_advice_map["geo"] = new_advice
                ai_question_map["geo"] = new_question
            except Exception:
                pass

        elif action == "save_geo" and geo_text:
            if task:
                task.source_geo = geo_text
                task.save(update_fields=["source_geo", "updated_at"])
                saved_geo = task.source_geo
                ai_command_display_map["geo"] = ""
                ai_advice_map["geo"] = "__saved__"
                ai_question_map["geo"] = ""
            else:
                title_generated = _generate_task_title_from_db(
                    request=request,
                    product_text="",
                    company_text="",
                    geo_text=geo_text,
                )
                task = AudienceTask.objects.create(
                    workspace_id=request.workspace_id,
                    user=request.user,
                    task="",
                    title=title_generated,
                    task_branches="",
                    task_geo="",
                    type="buy",
                    source_geo=geo_text,
                )
                return redirect(_build_edit_url(encode_id(int(task.id)), "geo"))

        elif action == "reset_geo_context":
            _reset_section_dialog(request, item_id=item_id, section="geo")
            geo_text = ""
            ai_command_display_map["geo"] = ""
            ai_advice_map["geo"] = ""
            ai_question_map["geo"] = ""

        elif action == "close":
            return redirect("audience:create_list")

        task = _resolve_task(request, item_id) if item_id else task
        saved_title = (task.title or "") if task else saved_title
        saved_product = (task.source_product or "") if task else saved_product
        saved_company = (task.source_company or "") if task else saved_company
        saved_geo = (task.source_geo or "") if task else saved_geo

    audience_title = title_text if last_action in {"suggest_title", "save_title"} else saved_title

    return render(
        request,
        "panels/aap_audience/create_edit_buy.html",
        {
            "type": "buy",
            "is_placeholder": False,
            "status": status,
            "task": task,
            "task_id_token": item_id,
            "audience_title": audience_title,
            "source_product": product_text,
            "source_company": company_text,
            "source_geo": geo_text,
            "product_ai_command": "",
            "product_ai_command_display": ai_command_display_map["product"],
            "product_ai_advice": ai_advice_map["product"],
            "product_ai_question": ai_question_map["product"],
            "company_ai_command": "",
            "company_ai_command_display": ai_command_display_map["company"],
            "company_ai_advice": ai_advice_map["company"],
            "company_ai_question": ai_question_map["company"],
            "geo_ai_command": "",
            "geo_ai_command_display": ai_command_display_map["geo"],
            "geo_ai_advice": ai_advice_map["geo"],
            "geo_ai_question": ai_question_map["geo"],
            "saved_title": saved_title,
            "saved_source_product": saved_product,
            "saved_source_company": saved_company,
            "saved_source_geo": saved_geo,
        },
    )
