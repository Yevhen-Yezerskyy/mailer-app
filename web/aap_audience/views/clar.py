# FILE: web/aap_audience/views/clar.py  (новое) 2025-12-12

import json

from django.db import connection
from django.shortcuts import render, redirect, get_object_or_404

from aap_audience.models import AudienceTask
from aap_audience.forms import AudienceClarForm
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt


_gpt_client = GPTClient()

SYSTEM_PROMPT_CITY = """
Ты — модуль генерации городов для B2B-поиска клиентов.

По описанию продукта и географии сгенерируй ДО 20 городов, которые логично использовать
для поиска потенциальных клиентов. Чем ниже rate (1–100), тем лучше город подходит.

Формат ответа — ТОЛЬКО JSON-массив, без комментариев и текста вокруг:
[
  {"value": "Berlin", "rate": 5},
  {"value": "Hamburg", "rate": 12}
]
""".strip()

SYSTEM_PROMPT_BRANCH = """
Ты — модуль генерации бизнес-отраслей (branchen) для B2B-поиска клиентов.

По описанию продукта и задач сгенерируй ДО 20 узких, практичных отраслей (branchen),
которые подходят под задачу. Чем ниже rate (1–100), тем лучше отрасль подходит.

Формат ответа — ТОЛЬКО JSON-массив, без комментариев и текста вокруг:
[
  {"value": "Metallbau", "rate": 4},
  {"value": "Fensterbau", "rate": 7}
]
""".strip()


def _load_crawl_items(workspace_id, user_id, task_id, type_):
    if not task_id:
        return []
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT value, rate
            FROM crawl_tasks
            WHERE workspace_id = %s
              AND user_id      = %s
              AND task_id      = %s
              AND type         = %s
            ORDER BY rate ASC, value ASC
            """,
            [str(workspace_id), int(user_id), int(task_id), type_],
        )
        rows = cur.fetchall()
    return [{"value": r[0], "rate": r[1]} for r in rows]


def _delete_crawl_items(workspace_id, user_id, task_id, type_):
    if not task_id:
        return
    with connection.cursor() as cur:
        cur.execute(
            """
            DELETE FROM crawl_tasks
            WHERE workspace_id = %s
              AND user_id      = %s
              AND task_id      = %s
              AND type         = %s
            """,
            [str(workspace_id), int(user_id), int(task_id), type_],
        )


def _insert_crawl_items(workspace_id, user_id, task_id, type_, items):
    if not items:
        return
    params = [
        (str(workspace_id), int(user_id), int(task_id), type_, it["value"], int(it["rate"]))
        for it in items
    ]
    with connection.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO crawl_tasks (workspace_id, user_id, task_id, type, value, rate)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            params,
        )


def _parse_json_list(raw_content: str):
    """
    Пытаемся аккуратно вытащить JSON-массив из ответа модели.
    Ожидаем список объектов {"value": str, "rate": int}.
    """
    if not raw_content:
        return []

    s = raw_content.strip()

    # срежем возможные ```json ... ```
    if s.startswith("```"):
        # убираем обертку ```...```
        s = s.strip("`")
    idx = s.find("[")
    if idx > 0:
        s = s[idx:]

    try:
        data = json.loads(s)
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    result = []
    for item in data:
        if not isinstance(item, dict):
            continue
        value = str(item.get("value", "")).strip()
        if not value:
            continue
        try:
            rate = int(item.get("rate", 100))
        except (TypeError, ValueError):
            rate = 100
        if rate < 1:
            rate = 1
        if rate > 100:
            rate = 100
        result.append({"value": value, "rate": rate})
    return result[:20]


def _generate_items_for_task(*, tier, workspace_id, user_id, task, type_):
    """
    Генерация  до 20 элементов для конкретного task:
    type_: 'city' | 'branch'
    """
    if type_ == "city":
        system_prompt = get_prompt("audience_clar_city"),
        user_prompt = f"Основная задача:\n{task.task}\n\nГеография:\n{task.task_geo}"
    else:
        system_prompt = get_prompt("audience_clar_branch")
        user_prompt = f"Основная задача:\n{task.task}\n\nОтрасли (branchen):\n{task.task_branches}"

    # уже существующие значения
    existing = _load_crawl_items(workspace_id, user_id, task.id, type_)
    existing_vals = {row["value"].strip().lower() for row in existing if row["value"]}

    if existing_vals:
        user_prompt += (
            "\n\nЭти значения уже были сгенерированы ранее, не повторяй их "
            "(и похожие по смыслу тоже не нужно):\n"
            + ", ".join(sorted(existing_vals))
        )

    # maxi + web, без max_output_tokens, с явным JSON-форматом
    resp = _gpt_client.ask(
        tier=tier,
        workspace_id=str(workspace_id),
        user_id=str(user_id),
        system=system_prompt,
        user=user_prompt,
        with_web=True if tier == "maxi" else None,
        endpoint=f"aap_audience_clar_{type_}",
        use_cache=True,
    )

    raw_items = _parse_json_list(resp.content)
    result = []
    seen = set(existing_vals)

    for it in raw_items:
        v_norm = it["value"].strip().lower()
        if not v_norm or v_norm in seen:
            continue
        seen.add(v_norm)
        result.append(it)
        if len(result) >= 20:
            break

    return result


def clar_view(request):
    ws_id = request.workspace_id
    user = request.user

    tasks = AudienceTask.objects.filter(workspace_id=ws_id, user=user)
    form = None
    current_task = None

    mode = request.POST.get("mode") if request.method == "POST" else None

    # DELETE задачи HOW
    if request.method == "POST" and mode == "delete":
        delete_id = request.POST.get("delete_id")
        if delete_id:
            AudienceTask.objects.filter(
                id=delete_id,
                workspace_id=ws_id,
                user=user,
            ).delete()
        return redirect(request.path)

    # Генерация / удаление городов/бранчей
    if request.method == "POST" and mode in {"gen_city", "gen_branch", "clear_city", "clear_branch"}:
        task_id = request.POST.get("task_id")
        if task_id:
            obj = get_object_or_404(
                AudienceTask,
                id=task_id,
                workspace_id=ws_id,
                user=user,
            )
            current_task = obj

            if mode == "clear_city":
                _delete_crawl_items(ws_id, user.id, obj.id, "city")
            elif mode == "clear_branch":
                _delete_crawl_items(ws_id, user.id, obj.id, "branch")
            elif mode == "gen_city":
                new_items = _generate_items_for_task(
                    tier="maxi",
                    workspace_id=ws_id,
                    user_id=user.id,
                    task=obj,
                    type_="city",
                )
                _insert_crawl_items(ws_id, user.id, obj.id, "city", new_items)
            elif mode == "gen_branch":
                new_items = _generate_items_for_task(
                    tier="maxi",
                    workspace_id=ws_id,
                    user_id=user.id,
                    task=obj,
                    type_="branch",
                )
                _insert_crawl_items(ws_id, user.id, obj.id, "branch", new_items)
            form = AudienceClarForm(initial={
                "edit_id": obj.id,
                "title": obj.title,
                "task": obj.task,
                "task_branches": obj.task_branches,
                "task_geo": obj.task_geo,
                "task_client": obj.task_client,
            })
            

        # после генерации/очистки просто падаем ниже и рендерим ту же страницу

    # SAVE (редактирование)
    elif request.method == "POST":
        form = AudienceClarForm(request.POST)
        if form.is_valid():
            edit_id = form.cleaned_data.get("edit_id")
            obj = get_object_or_404(
                AudienceTask,
                id=edit_id,
                workspace_id=ws_id,
                user=user,
            )

            obj.title = form.cleaned_data["title"]
            obj.task = form.cleaned_data["task"]
            obj.task_branches = form.cleaned_data["task_branches"]
            obj.task_geo = form.cleaned_data["task_geo"]
            obj.task_client = form.cleaned_data["task_client"]
            obj.save()

            current_task = obj

            # остаёмся в режиме редактирования
            form = AudienceClarForm(
                initial={
                    "edit_id": obj.id,
                    "title": obj.title,
                    "task": obj.task,
                    "task_branches": obj.task_branches,
                    "task_geo": obj.task_geo,
                    "task_client": obj.task_client,
                }
            )

    # GET + режим "edit"
    if request.method == "GET":
        edit_id = request.GET.get("edit")
        if edit_id:
            obj = get_object_or_404(
                AudienceTask,
                id=edit_id,
                workspace_id=ws_id,
                user=user,
            )
            current_task = obj
            form = AudienceClarForm(
                initial={
                    "edit_id": obj.id,
                    "title": obj.title,
                    "task": obj.task,
                    "task_branches": obj.task_branches,
                    "task_geo": obj.task_geo,
                    "task_client": obj.task_client,
                }
            )

    # данные по городам/бранчам для правых колонок
    clar_city_items = []
    clar_branch_items = []
    if current_task is not None:
        clar_city_items = _load_crawl_items(ws_id, user.id, current_task.id, "city")
        clar_branch_items = _load_crawl_items(ws_id, user.id, current_task.id, "branch")

    # 🔹 ДОБАВЛЕНО: заполняем списки для каждой строки таблицы
    for t in tasks:
        t.clar_city_items = _load_crawl_items(ws_id, user.id, t.id, "city")
        t.clar_branch_items = _load_crawl_items(ws_id, user.id, t.id, "branch")

    return render(
        request,
        "panels/aap_audience/clar.html",
        {
            "form": form,
            "tasks": tasks,
            "current_task_id": current_task.id if current_task else None,
            "clar_city_items": clar_city_items,
            "clar_city_count": len(clar_city_items),
            "clar_branch_items": clar_branch_items,
            "clar_branch_count": len(clar_branch_items),
        },
    )
