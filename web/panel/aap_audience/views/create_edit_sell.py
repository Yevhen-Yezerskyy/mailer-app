# FILE: web/panel/aap_audience/views/create_edit_sell.py
# DATE: 2026-03-08
# PURPOSE: Create/edit sell page with GPT dialog for product refinement.

import json
from pathlib import Path

from django.shortcuts import redirect, render

from engine.common.gpt import GPTClient
from mailer_web.access import decode_id
from panel.aap_audience.models import AudienceTask


def _prompt_text() -> str:
    p = Path(__file__).resolve().parents[4] / "engine" / "common" / "prompts" / "create_sell_product_01.txt"
    try:
        return p.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _session_key(request, form_ref: str) -> str:
    return f"create_sell_product_dialog:{request.workspace_id}:{request.user.id}:{form_ref}"


def _parse_ai_json(text: str) -> tuple[str, str]:
    raw = (text or "").strip()
    if not raw:
        return "", ""
    try:
        data = json.loads(raw)
    except Exception:
        s = raw.find("{")
        e = raw.rfind("}")
        if s == -1 or e == -1 or e <= s:
            return "", raw
        try:
            data = json.loads(raw[s : e + 1])
        except Exception:
            return "", raw

    if not isinstance(data, dict):
        return "", raw

    product = str(data.get("product") or "").strip()
    advice = str(data.get("advice") or "").strip()
    return product, advice


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
            user=request.user,
            archived=False,
            type="sell",
        ).first()
    )


def create_edit_sell_view(request):
    token = (request.GET.get("id") or request.POST.get("id") or "").strip()
    task = _resolve_task(request, token)
    form_ref = token or "new"

    title_text = (task.title or "") if task else ""
    product_text = (task.source_product or "") if task else ""
    instruction_text = ""
    ai_advice_text = ""
    status_text = ""

    state_key = _session_key(request, form_ref)
    state = request.session.get(state_key, {}) or {}

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        title_text = (request.POST.get("audience_title") or "").strip()
        product_text = (request.POST.get("source_product") or "").strip()
        instruction_text = (request.POST.get("ai_instruction") or "").strip()
        ai_advice_text = (request.POST.get("ai_advice") or "").strip()

        if action in {"run_ai", "process_product"}:
            try:
                prompt = _prompt_text()
                payload = (
                    f"ПРОДУКТ:\n{product_text}\n\n"
                    f"ИНСТРУКЦИЯ:\n{instruction_text}"
                )
                resp = GPTClient().ask_dialog(
                    model="gpt-5.1",
                    instructions=prompt,
                    input=payload,
                    conversation=str(state.get("conversation_id") or ""),
                    previous_response_id=str(state.get("response_id") or ""),
                    user_id="panel.audience.create_edit_sell.product",
                    service_tier="flex",
                )
                new_product, new_advice = _parse_ai_json(resp.content or "")
                if new_product:
                    product_text = new_product
                ai_advice_text = new_advice or ai_advice_text

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
                status_text = "Обработка выполнена."
            except Exception as exc:
                status_text = f"Ошибка: {exc}"

        elif action == "save_title":
            if task:
                task.title = title_text
                task.save(update_fields=["title", "updated_at"])
                status_text = "Название сохранено."
            else:
                status_text = "Название не сохранено: новая форма без записи."

        elif action == "save_product":
            if task:
                task.source_product = product_text
                task.save(update_fields=["source_product", "updated_at"])
                status_text = "Продукт/услуга сохранены."
            else:
                status_text = "Продукт/услуга не сохранены: новая форма без записи."

        elif action == "close":
            return redirect("audience:create_list")

    return render(
        request,
        "panels/aap_audience/create_edit_sell.html",
        {
            "type": "sell",
            "is_placeholder": False,
            "task": task,
            "task_id_token": token,
            "audience_title": title_text,
            "source_product": product_text,
            "ai_instruction": instruction_text,
            "ai_advice": ai_advice_text,
            "status_text": status_text,
        },
    )
