# FILE: web/panel/aap_audience/views/modal_pause_info.py
# DATE: 2026-04-06
# PURPOSE: Info modal shown from paused contacts/rating statuses with workspace-specific message.

from __future__ import annotations

import json

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from engine.common.cache.client import CLIENT
from engine.core_status.is_active import CACHE_TTL_SEC, is_more_needed, start_full_continue_window
from engine.core_status.status import is_active as core_status_is_active
from mailer_web.access import decode_id
from panel.aap_campaigns.models import Campaign
from panel.aap_audience.models import AudienceTask

CONTINUE_STATE = "Continue"
STATE_PLACEHOLDER = "—"


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


def _state_cache_key(task_id: int) -> str:
    return f"core_status:is_more_needed:state:{int(task_id)}"


def _read_state_value(task_id: int) -> str:
    raw = CLIENT.get(_state_cache_key(int(task_id)), ttl_sec=CACHE_TTL_SEC)
    if raw is None:
        return ""
    try:
        return bytes(raw).decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def _write_continue_state(task_id: int) -> None:
    start_full_continue_window(int(task_id))


def _safe_int_or_none(value):
    try:
        return int(value)
    except Exception:
        return None


def _safe_text_or_placeholder(value) -> str:
    num = _safe_int_or_none(value)
    if num is None:
        return STATE_PLACEHOLDER
    return str(num)


def _safe_percent_or_placeholder(good_value, total_value) -> str:
    good = _safe_int_or_none(good_value)
    total = _safe_int_or_none(total_value)
    if good is None or total is None or total <= 0:
        return STATE_PLACEHOLDER
    return str(int(round((float(good) * 100.0) / float(total))))


def _parse_state_payload(state_value: str) -> dict[str, object]:
    if not state_value:
        return {}
    try:
        payload = json.loads(state_value)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _refresh_task_active(task) -> None:
    try:
        is_more_needed(int(task.id), update=True)
    except Exception:
        pass

    try:
        next_active = bool(
            core_status_is_active(
                {
                    "id": int(task.id),
                    "ready": bool(task.ready),
                    "archived": bool(task.archived),
                    "user_active": bool(task.user_active),
                }
            )
        )
        if bool(task.active) != next_active:
            task.active = next_active
            task.save(update_fields=["active", "updated_at"])
    except Exception:
        pass


def _is_task_used_in_campaigns(task) -> bool:
    if not task:
        return False
    return Campaign.objects.filter(
        workspace_id=task.workspace_id,
        sending_list_id=int(task.id),
    ).exists()


@csrf_exempt
def modal_pause_info_view(request):
    token = (request.POST.get("id") or request.GET.get("id") or "").strip()
    task = _resolve_task(request, token)

    if request.method == "POST":
        if not task:
            return JsonResponse({"ok": False, "error": "task_not_found"}, status=404)

        action = str(request.POST.get("action") or "").strip()
        if action != "continue_collection":
            return JsonResponse({"ok": False, "error": "unknown_action"}, status=400)

        try:
            _write_continue_state(int(task.id))
        except Exception:
            return JsonResponse({"ok": False, "error": "cache_write_failed"}, status=500)

        _refresh_task_active(task)
        return JsonResponse({"ok": True})

    if not task:
        return render(
            request,
            "panels/aap_audience/modal_pause_info.html",
            {
                "status": "empty",
            },
        )

    ws = getattr(request.user, "workspace", None)
    ws_access_type = str(getattr(ws, "access_type", "") or "").strip().lower()
    is_test_workspace = ws_access_type == "test"
    intro_line = ""
    limit_lines: list[str] = []
    outro_lines: list[str] = []
    show_continue_button = False

    if is_test_workspace:
        title = "Ограничение тестового доступа"
        intro_line = "В тестовом доступе действуют ограничения:"
        limit_lines = [
            "сбор контактов с успешным рейтингом (рейтинг меньше или равен рейтингу отсечения) — не более 20 контактов;",
            "количество контактов для рейтингования — не более 60 контактов.",
        ]
        outro_lines = [
            "Для нормальной работы, пожалуйста, свяжитесь с нами и приобретите подписку.",
            "Пишите нам: sales@serenity-mail.de",
        ]
    elif ws_access_type == "full" and not _is_task_used_in_campaigns(task):
        title = "Сбор контактов тестируется"
        outro_lines = [
            "Этот список рассылки не задействован ни в одной кампании. "
            "Пока список рассылки не задействован, рейтингование контактов ограничено.",
            "Ограничение на рейтингование для незадействованного списка — до 100 контактов.",
            "Пока список не задействован, вы можете пересчитывать рейтинги и начинать сбор контактов заново. "
            "Для этого необходимо изменить задачу (продукт, компания, география).",
            "После изменения задачи вам будет предложено начать сбор контактов заново или произвести пересчёт "
            "рейтинга для уже собранных контактов по изменённой задаче.",
            "После того как список рассылки задействован в кампании и прорейтинговано значительно более 100 "
            "контактов, а также этим контактам отправлены письма, повторное рейтингование для собранных "
            "контактов не проводится. Однако, если вы изменили задачу, новые собранные контакты будут "
            "рейтинговаться по новой задаче.",
        ]
    elif not bool(task.user_active):
        title = "Обработка списка отключена"
        outro_lines = [
            "Сбор контактов по списку приостановлен, рейтингование контактов не производится.",
            "Для продолжения сбора контактов и рейтингования включите обработку списка наверху страницы.",
        ]
    else:
        state_value = _read_state_value(int(task.id))
        payload = _parse_state_payload(state_value)
        mode = str(payload.get("mode") or "")
        if (not state_value) or (state_value == CONTINUE_STATE) or (mode == "continue_window"):
            title = "Собрано и отрейтинговано достаточно контактов"
            outro_lines = [
                "В настоящий момент собрано достаточно контактов с успешным рейтингом для обеспечения рассылок.",
                "Сбор контактов и их рейтингование производится автоматически по мере осуществления рассылок.",
                "Только контакты с рейтингом лучше (меньше), чем рейтинг отсечения, попадают в рассылку.",
                "Это сделано для возможности оперативного управления рейтингованием.",
                "Действует ограничение на рейтингование контактов — не более 50 000 контактов в месяц, вне зависимости от успешности рейтингования. Также действует ограничение на отправку через один почтовый сервер — не более 1 000 писем в день.",
            ]
        else:
            total_cnt = _safe_text_or_placeholder(payload.get("total_cnt"))
            good_cnt = _safe_text_or_placeholder(payload.get("good_cnt"))
            bad_cnt = _safe_text_or_placeholder(payload.get("bad_cnt"))
            rate_limit = _safe_text_or_placeholder(payload.get("rate_limit"))
            good_percent = _safe_percent_or_placeholder(payload.get("good_cnt"), payload.get("total_cnt"))

            next_rate_from = STATE_PLACEHOLDER
            parsed_rate_limit = _safe_int_or_none(payload.get("rate_limit"))
            if parsed_rate_limit is not None:
                next_rate_from = str(parsed_rate_limit + 1)

            title = "Сбор контактов приостановлен из-за низкого качества контактов."
            outro_lines = [
                "Мы приостановили сбор контактов и их рейтингование для этого списка рассылки.",
                f"Из последних {total_cnt} контактов с успешным рейтингом — не более {good_percent}%.",
                f"Рейтинг отсечения — {rate_limit}.",
                f"С рейтингом 1–{rate_limit} — {good_cnt} контактов.",
                f"С рейтингом {next_rate_from}–100 — {bad_cnt} контактов.",
                "Напоминаем, что действует ограничение на рейтингование контактов — не более 50 000 контактов в месяц, вне зависимости от успешности рейтингования.",
                "Вы можете создать новый список рассылки и в нём по-другому задать параметры сбора контактов.",
                "Вы также можете исправить задачу для этого списка рассылки — убрать ненужные категории, изменить описание продукта или компании.",
                "Если вы хотите продолжать сбор и рейтингование контактов в этой задаче, пожалуйста, подтвердите это. В этом случае мы рекомендуем следить за дальнейшим сбором контактов, и, если контактов с успешным рейтингом по-прежнему будет очень мало, отключить обработку этого списка.",
            ]
            show_continue_button = True

    return render(
        request,
        "panels/aap_audience/modal_pause_info.html",
        {
            "status": "ok",
            "type": str(task.type or "").strip(),
            "title": title,
            "intro_line": intro_line,
            "limit_lines": limit_lines,
            "outro_lines": outro_lines,
            "show_continue_button": show_continue_button,
            "task_id_token": token,
        },
    )
