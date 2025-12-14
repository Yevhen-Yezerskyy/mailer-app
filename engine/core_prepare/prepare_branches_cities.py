# FILE: engine/core_prepare/prepare_branches_cities.py  (обновлено) 2025-12-14
# Смысл: dev-скрипт автозаполнения crawl_tasks (city/branch) через GPT до 200/200 для задач run_processing=true.
# Всё кроме GPT-части живёт здесь: выбор кандидатов, лимиты, циклы, insert/commit, идемпотентность.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from engine.common.db import get_connection
from engine.common.gpt import GPTClient
from engine.common.fill import gpt_rank_candidates

TypeName = Literal["city", "branch"]


@dataclass(frozen=True)
class TaskRow:
    id: int
    workspace_id: str
    user_id: int
    task: str
    task_geo: str
    task_branches: str


SQL_TASKS = """
SELECT id, workspace_id, user_id, task, task_geo, task_branches
FROM aap_audience_audiencetask
WHERE run_processing = true
ORDER BY id ASC
"""


def fetch_processing_tasks(*, conn, limit: Optional[int] = None) -> List[TaskRow]:
    sql = SQL_TASKS
    params: tuple[Any, ...] = ()
    if isinstance(limit, int) and limit > 0:
        sql += "\nLIMIT %s"
        params = (int(limit),)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    out: List[TaskRow] = []
    for r in rows:
        out.append(
            TaskRow(
                id=int(r[0]),
                workspace_id=str(r[1]),
                user_id=int(r[2]),
                task=str(r[3] or ""),
                task_geo=str(r[4] or ""),
                task_branches=str(r[5] or ""),
            )
        )
    return out


def count_items(*, conn, t: TaskRow, type_: TypeName) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM crawl_tasks
            WHERE workspace_id = %s
              AND user_id      = %s
              AND task_id      = %s
              AND type         = %s
            """,
            (t.workspace_id, t.user_id, t.id, type_),
        )
        row = cur.fetchone()
    return int(row[0] or 0)


def pick_random_candidates(*, conn, t: TaskRow, type_: TypeName, limit: int = 25) -> List[Dict[str, Any]]:
    if type_ == "branch":
        with conn.cursor() as cur:
            cur.execute(
                """
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
                """,
                (t.workspace_id, t.user_id, t.id, int(limit)),
            )
            rows = cur.fetchall()
        return [{"id": int(r[0]), "name": str(r[1])} for r in rows]

    with conn.cursor() as cur:
        cur.execute(
            """
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
            """,
            (t.workspace_id, t.user_id, t.id, int(limit)),
        )
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


def insert_items(*, conn, t: TaskRow, type_: TypeName, items: List[Dict[str, int]]) -> int:
    if not items:
        return 0

    params = [(t.workspace_id, t.user_id, t.id, type_, int(it["value_id"]), int(it["rate"])) for it in items]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO crawl_tasks (workspace_id, user_id, task_id, type, value_id, rate)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (workspace_id, user_id, task_id, type, value_id) DO NOTHING
            """,
            params,
        )
        # rowcount на executemany у psycopg может быть не тем, поэтому не полагаемся.
    return len(items)


def fill_one_task(
    *,
    conn,
    gpt: GPTClient,
    t: TaskRow,
    cities_limit: int = 200,
    branches_limit: int = 200,
    batch_size: int = 25,
    max_rounds: int = 200,
) -> Dict[str, int]:
    city_count = count_items(conn=conn, t=t, type_="city")
    branch_count = count_items(conn=conn, t=t, type_="branch")

    rounds = 0
    while rounds < max_rounds and (city_count < cities_limit or branch_count < branches_limit):
        rounds += 1

        if city_count < cities_limit:
            candidates = pick_random_candidates(conn=conn, t=t, type_="city", limit=batch_size)
            ranked = gpt_rank_candidates(
                gpt=gpt,
                tier="maxi",
                workspace_id=t.workspace_id,
                user_id=t.user_id,
                main_task=t.task,
                sub_task_text=t.task_geo,
                candidates=candidates,
                type_="city",
                endpoint="core_prepare_city",
            )
            insert_items(conn=conn, t=t, type_="city", items=ranked)
            conn.commit()
            city_count = count_items(conn=conn, t=t, type_="city")

        if branch_count < branches_limit:
            candidates = pick_random_candidates(conn=conn, t=t, type_="branch", limit=batch_size)
            ranked = gpt_rank_candidates(
                gpt=gpt,
                tier="maxi",
                workspace_id=t.workspace_id,
                user_id=t.user_id,
                main_task=t.task,
                sub_task_text=t.task_branches,
                candidates=candidates,
                type_="branch",
                endpoint="core_prepare_branch",
            )
            insert_items(conn=conn, t=t, type_="branch", items=ranked)
            conn.commit()
            branch_count = count_items(conn=conn, t=t, type_="branch")

        # если кандидатов больше нет — дальше бессмысленно
        if city_count < cities_limit:
            if not pick_random_candidates(conn=conn, t=t, type_="city", limit=1):
                break
        if branch_count < branches_limit:
            if not pick_random_candidates(conn=conn, t=t, type_="branch", limit=1):
                break

    return {"city": city_count, "branch": branch_count, "rounds": rounds}


def main(limit_tasks: Optional[int] = None) -> None:
    gpt = GPTClient()
    with get_connection() as conn:
        tasks = fetch_processing_tasks(conn=conn, limit=limit_tasks)
        for t in tasks:
            res = fill_one_task(conn=conn, gpt=gpt, t=t)
            print(
                f"[task_id={t.id}] ws={t.workspace_id} user={t.user_id} "
                f"city={res['city']}/200 branch={res['branch']}/200 rounds={res['rounds']}"
            )


if __name__ == "__main__":
    main()
