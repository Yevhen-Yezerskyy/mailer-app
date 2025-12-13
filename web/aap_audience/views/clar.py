# FILE: web/aap_audience/views/clar.py  (обновлено) 2025-12-13
# Смысл: сохраняем/показываем run_processing и subscribers_limit в Clar.

import json

from django.db import connection
from django.shortcuts import render, redirect, get_object_or_404

from aap_audience.models import AudienceTask
from aap_audience.forms import AudienceClarForm
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt


_gpt_client = GPTClient()


def _load_crawl_items(workspace_id, user_id, task_id, type_):
    if not task_id:
        return []
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT value_id, value_text, rate
            FROM crawl_tasks_labeled
            WHERE workspace_id = %s
              AND user_id      = %s
              AND task_id      = %s
              AND type         = %s
            ORDER BY rate ASC, value_text ASC
            """,
            [str(workspace_id), int(user_id), int(task_id), type_],
        )
        rows = cur.fetchall()
    return [{"value_id": r[0], "value_text": r[1], "rate": r[2]} for r in rows]


def _load_all_crawl_items_for_tasks(workspace_id, user_id, task_ids):
    task_ids = [int(x) for x in task_ids if x]
    if not task_ids:
        return {}

    out = {tid: {"city": [], "branch": []} for tid in task_ids}

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT task_id, type, value_id, value_text, rate
            FROM crawl_tasks_labeled
            WHERE workspace_id = %s
              AND user_id      = %s
              AND task_id = ANY(%s)
            ORDER BY task_id ASC, type ASC, rate ASC, value_text ASC
            """,
            [str(workspace_id), int(user_id), task_ids],
        )
        rows = cur.fetchall()

    for task_id, type_, value_id, value_text, rate in rows:
        if task_id in out and type_ in ("city", "branch"):
            out[task_id][type_].append(
                {"value_id": value_id, "value_text": value_text, "rate": rate}
            )
    return out


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
        (str(workspace_id), int(user_id), int(task_id), type_, int(it["value_id"]), int(it["rate"]))
        for it in items
    ]
    with connection.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO crawl_tasks (workspace_id, user_id, task_id, type, value_id, rate)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (workspace_id, user_id, task_id, type, value_id) DO NOTHING
            """,
            params,
        )


def _pick_random_candidates(workspace_id, user_id, task_id, type_, limit=25):
    if type_ == "branch":
        sql = """
            SELECT b.id, b.name
            FROM gb_branches b
            WHERE NOT EXISTS (
                SELECT 1
                FROM crawl_tasks ct
                WHERE ct.workspace_id = %s
                  AND ct.user_id      = %s
                  AND ct.task_id      = %s
                  AND ct.type         = 'branch'
                  AND ct.value_id     = b.id
            )
            ORDER BY random()
            LIMIT %s
        """
        with connection.cursor() as cur:
            cur.execute(sql, [str(workspace_id), int(user_id), int(task_id), int(limit)])
            rows = cur.fetchall()
        return [{"id": int(r[0]), "name": str(r[1])} for r in rows]

    sql = """
        SELECT
            c.id,
            c.name,
            c.state_name      AS land,
            c.urban_code,
            c.urban_name,
            c.travel_code,
            c.travel_name,
            c.pop_total,
            c.area_km2,
            c.pop_density
        FROM cities_sys c
        WHERE NOT EXISTS (
            SELECT 1
            FROM crawl_tasks ct
            WHERE ct.workspace_id = %s
              AND ct.user_id      = %s
              AND ct.task_id      = %s
              AND ct.type         = 'city'
              AND ct.value_id     = c.id
        )
        ORDER BY random()
        LIMIT %s
    """
    with connection.cursor() as cur:
        cur.execute(sql, [str(workspace_id), int(user_id), int(task_id), int(limit)])
        rows = cur.fetchall()

    return [
        {
            "id": int(r[0]),
            "name": str(r[1]),
            "land": r[2],
            "urban_code": r[3],
            "urban_name": r[4],
            "travel_code": r[5],
            "travel_name": r[6],
            "pop_total": r[7],
            "area_km2": r[8],
            "pop_density": r[9],
        }
        for r in rows
    ]


def _parse_strict_ranked_list(raw_content: str):
    if not raw_content:
        return None

    s = raw_content.strip()
    if "```" in s or not (s.startswith("[") and s.endswith("]")):
        return None

    try:
        data = json.loads(s)
    except Exception:
        return None

    if not isinstance(data, list):
        return None

    out = []
    for item in data:
        if not isinstance(item, dict) or set(item.keys()) != {"id", "name", "rate"}:
            return None
        try:
            _id = int(item["id"])
            rate = int(item["rate"])
        except Exception:
            return None
        if rate < 1 or rate > 100:
            return None
        name = item["name"]
        if not isinstance(name, str) or not name.strip():
            return None
        out.append({"id": _id, "name": name.strip(), "rate": rate})
    return out


def _generate_items_for_task(*, tier, workspace_id, user_id, task, type_):
    candidates = _pick_random_candidates(workspace_id, user_id, task.id, type_, limit=25)
    if not candidates:
        return []

    cand_map = {c["id"]: c["name"] for c in candidates}

    if type_ == "city":
        system_prompt = get_prompt("audience_clar_city")
        user_prompt = (
            f"Основная задача:\n{task.task}\n\n"
            f"Geo task:\n{task.task_geo}\n\n"
            f"Кандидаты (оценить ВСЕ):\n"
            f"{json.dumps(candidates, ensure_ascii=False)}"
        )
    else:
        system_prompt = get_prompt("audience_clar_branch")
        user_prompt = (
            f"Основная задача:\n{task.task}\n\n"
            f"Branches task:\n{task.task_branches}\n\n"
            f"Кандидаты (оценить ВСЕ):\n"
            f"{json.dumps(candidates, ensure_ascii=False)}"
        )

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

    parsed = _parse_strict_ranked_list(resp.content)
    if parsed is None:
        return []

    seen = set()
    result = []
    for it in parsed:
        _id = it["id"]
        if _id not in cand_map or it["name"] != cand_map[_id] or _id in seen:
            return []
        seen.add(_id)
        result.append({"value_id": _id, "rate": it["rate"]})
    return result


def clar_view(request):
    ws_id = request.workspace_id
    user = request.user

    tasks = AudienceTask.objects.filter(workspace_id=ws_id, user=user)
    form = None
    current_task = None

    mode = request.POST.get("mode") if request.method == "POST" else None

    if request.method == "POST" and mode == "delete":
        delete_id = request.POST.get("delete_id")
        if delete_id:
            AudienceTask.objects.filter(id=delete_id, workspace_id=ws_id, user=user).delete()
        return redirect(request.path)

    if request.method == "POST" and mode in {"gen_city", "gen_branch", "clear_city", "clear_branch"}:
        task_id = request.POST.get("task_id")
        if task_id:
            obj = get_object_or_404(AudienceTask, id=task_id, workspace_id=ws_id, user=user)
            current_task = obj

            if mode == "clear_city":
                _delete_crawl_items(ws_id, user.id, obj.id, "city")
            elif mode == "clear_branch":
                _delete_crawl_items(ws_id, user.id, obj.id, "branch")
            elif mode == "gen_city":
                items = _generate_items_for_task(
                    tier="maxi", workspace_id=ws_id, user_id=user.id, task=obj, type_="city"
                )
                _insert_crawl_items(ws_id, user.id, obj.id, "city", items)
            elif mode == "gen_branch":
                items = _generate_items_for_task(
                    tier="maxi", workspace_id=ws_id, user_id=user.id, task=obj, type_="branch"
                )
                _insert_crawl_items(ws_id, user.id, obj.id, "branch", items)

            form = AudienceClarForm(initial={
                "edit_id": obj.id,
                "title": obj.title,
                "task": obj.task,
                "task_branches": obj.task_branches,
                "task_geo": obj.task_geo,
                "task_client": obj.task_client,
                "run_processing": obj.run_processing,
                "subscribers_limit": obj.subscribers_limit,
            })

    elif request.method == "POST":
        form = AudienceClarForm(request.POST)
        if form.is_valid():
            edit_id = form.cleaned_data.get("edit_id")
            obj = get_object_or_404(AudienceTask, id=edit_id, workspace_id=ws_id, user=user)

            obj.title = form.cleaned_data["title"]
            obj.task = form.cleaned_data["task"]
            obj.task_branches = form.cleaned_data["task_branches"]
            obj.task_geo = form.cleaned_data["task_geo"]
            obj.task_client = form.cleaned_data["task_client"]
            obj.run_processing = form.cleaned_data["run_processing"]
            obj.subscribers_limit = form.cleaned_data["subscribers_limit"]
            obj.save()

            current_task = obj
            form = AudienceClarForm(initial={
                "edit_id": obj.id,
                "title": obj.title,
                "task": obj.task,
                "task_branches": obj.task_branches,
                "task_geo": obj.task_geo,
                "task_client": obj.task_client,
                "run_processing": obj.run_processing,
                "subscribers_limit": obj.subscribers_limit,
            })

    if request.method == "GET":
        edit_id = request.GET.get("edit")
        if edit_id:
            obj = get_object_or_404(AudienceTask, id=edit_id, workspace_id=ws_id, user=user)
            current_task = obj
            form = AudienceClarForm(initial={
                "edit_id": obj.id,
                "title": obj.title,
                "task": obj.task,
                "task_branches": obj.task_branches,
                "task_geo": obj.task_geo,
                "task_client": obj.task_client,
                "run_processing": obj.run_processing,
                "subscribers_limit": obj.subscribers_limit,
            })

    clar_city_items, clar_branch_items = [], []
    if current_task is not None:
        clar_city_items = _load_crawl_items(ws_id, user.id, current_task.id, "city")
        clar_branch_items = _load_crawl_items(ws_id, user.id, current_task.id, "branch")

    task_ids = [t.id for t in tasks]
    all_items = _load_all_crawl_items_for_tasks(ws_id, user.id, task_ids)
    for t in tasks:
        bucket = all_items.get(t.id, {"city": [], "branch": []})
        t.clar_city_items = bucket["city"]
        t.clar_branch_items = bucket["branch"]

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
