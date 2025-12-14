# FILE: engine/common/fill.py  (обновлено) 2025-12-14
# Смысл: только GPT-часть для ранжирования кандидатов (city/branch): запрос → строгая валидация → [{value_id, rate}] или [].

from __future__ import annotations

import json
from typing import Any, Dict, List, Literal, Optional

from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt

TypeName = Literal["city", "branch"]


def parse_strict_ranked_list(raw_content: str) -> Optional[List[Dict[str, Any]]]:
    """
    Ожидаем СТРОГО:
      [
        {"id": <int>, "name": <str>, "rate": <int 1..100>},
        ...
      ]
    Без ``` и без мусора вокруг JSON.
    """
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

    out: List[Dict[str, Any]] = []
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


def gpt_rank_candidates(
    *,
    gpt: GPTClient,
    tier: Literal["nano", "mini", "maxi"],
    workspace_id: str,
    user_id: int,
    main_task: str,
    sub_task_text: str,
    candidates: List[Dict[str, Any]],
    type_: TypeName,
    endpoint: str,
) -> List[Dict[str, int]]:
    """
    candidates: список словарей как в clar (для city расширенный, для branch {id,name}).
    Возвращает: [{value_id, rate}] в том же наборе id, только если всё строго валидно.
    Иначе: [] (fail-closed).
    """
    if not candidates:
        return []

    cand_map = {int(c["id"]): str(c["name"]) for c in candidates if "id" in c and "name" in c}
    if len(cand_map) != len(candidates):
        return []

    if type_ == "city":
        system_prompt = get_prompt("audience_clar_city")
        user_prompt = (
            f"Основная задача:\n{main_task}\n\n"
            f"Geo task:\n{sub_task_text}\n\n"
            f"Кандидаты (оценить ВСЕ):\n"
            f"{json.dumps(candidates, ensure_ascii=False)}"
        )
    else:
        system_prompt = get_prompt("audience_clar_branch")
        user_prompt = (
            f"Основная задача:\n{main_task}\n\n"
            f"Branches task:\n{sub_task_text}\n\n"
            f"Кандидаты (оценить ВСЕ):\n"
            f"{json.dumps(candidates, ensure_ascii=False)}"
        )

    resp = gpt.ask(
        tier=tier,
        workspace_id=str(workspace_id),
        user_id=str(user_id),
        system=system_prompt,
        user=user_prompt,
        with_web=True if tier == "maxi" else None,
        endpoint=endpoint,
        use_cache=True,
    )

    parsed = parse_strict_ranked_list(resp.content)
    if parsed is None:
        return []

    seen = set()
    out: List[Dict[str, int]] = []
    for it in parsed:
        _id = int(it["id"])
        if _id not in cand_map:
            return []
        if str(it["name"]) != cand_map[_id]:
            return []
        if _id in seen:
            return []
        seen.add(_id)
        out.append({"value_id": _id, "rate": int(it["rate"])})

    return out
