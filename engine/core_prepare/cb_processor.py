# FILE: engine/core_prepare/cb_processor.py  (обновлено — 2025-12-14)
# Смысл: CB_PROCESSOR — лёгкие воркеры (worker.py) + queue_filler (heavy).
#   1) cb_status_updater (каждые 5с): ставит CB_COLLECTING/CB_EXTRACT по наполненности crawl_tasks
#   2) cb_city_adder (каждые 10с): доливает города (4*25=100) для задач в CB_COLLECTING
#   3) cb_branch_adder (каждые 10с, стартовый сдвиг +5с): доливает бранчи (4*25=100) для задач в CB_COLLECTING
#   4) cb_queue_filler (каждые 10с, heavy): наполняет queue_sys для одного task в CB_EXTRACT “тупым” окном 10%
#      - если queue_sys(task) уже == cb_crawler (gen) -> noop
#      - если в queue_sys(task) осталось >=1000 status!=done -> noop
#      - иначе: большой селект (все city×plz × все branch) -> rate=city_rate*branch_rate -> join cb_crawler -> ORDER BY rate -> LIMIT 10%
#      - upsert: rate улучшаем; done/error -> pending; processing не трогаем
#      - после успешной заливки -> статус task = CB_CRAWL
#   5) cb_integrity_check (раз в час, heavy): expected vs COUNT(cb_crawler) (справочник пар)
#
# Важно:
# - cb_crawler = справочник пар (PLZ×branch), без rate/очереди
# - GPT_DEBUG — единая точка управления: True = всегда nano, без web и кеша

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from engine.common.db import get_connection
from engine.common.fill import gpt_rank_candidates
from engine.common.gpt import GPTClient
from engine.common.worker import Worker


# -------------------- GLOBAL FLAGS --------------------

GPT_DEBUG = True  # <-- ЕДИНАЯ ТОЧКА: True = всегда nano, без web и кеша

# ----------------------------------------------------

TypeName = Literal["city", "branch"]

STATUS_COLLECTING = "CB_COLLECTING"
STATUS_EXTRACT = "CB_EXTRACT"
STATUS_CRAWL = "CB_CRAWL"
STATUS_FINISHED = "CB_FINISHED"

UPDATER = "CB_PROCESSOR"

TASK_TIMEOUT_SEC = 600  # 10 минут
BATCH_SIZE = 25
ROUNDS_PER_RUN = 4  # 4 * 25 = 100

QUEUE_WINDOW_SHARE = 0.10
QUEUE_MIN_LEFT = 1000


@dataclass(frozen=True)
class TaskRow:
    id: int
    workspace_id: str
    user_id: int
    task: str
    task_geo: str
    task_branches: str
    sys_status: Optional[str]


def _fetch_processing_tasks(conn) -> List[TaskRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, workspace_id, user_id, task, task_geo, task_branches, _sys_status
            FROM aap_audience_audiencetask
            WHERE run_processing = true
            ORDER BY id ASC
            """
        )
        rows = cur.fetchall()

    return [
        TaskRow(
            id=int(r[0]),
            workspace_id=str(r[1]),
            user_id=int(r[2]),
            task=str(r[3] or ""),
            task_geo=str(r[4] or ""),
            task_branches=str(r[5] or ""),
            sys_status=str(r[6]) if r[6] is not None else None,
        )
        for r in rows
    ]


def _fetch_tasks_by_status(conn, status: str) -> List[TaskRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, workspace_id, user_id, task, task_geo, task_branches, _sys_status
            FROM aap_audience_audiencetask
            WHERE run_processing = true
              AND _sys_status = %s
            ORDER BY id ASC
            """,
            (status,),
        )
        rows = cur.fetchall()

    return [
        TaskRow(
            id=int(r[0]),
            workspace_id=str(r[1]),
            user_id=int(r[2]),
            task=str(r[3] or ""),
            task_geo=str(r[4] or ""),
            task_branches=str(r[5] or ""),
            sys_status=str(r[6]) if r[6] is not None else None,
        )
        for r in rows
    ]


def _fetch_one_task_by_status(conn, status: str) -> Optional[TaskRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, workspace_id, user_id, task, task_geo, task_branches, _sys_status
            FROM aap_audience_audiencetask
            WHERE run_processing = true
              AND _sys_status = %s
            ORDER BY id ASC
            LIMIT 1
            """,
            (status,),
        )
        r = cur.fetchone()
    if not r:
        return None
    return TaskRow(
        id=int(r[0]),
        workspace_id=str(r[1]),
        user_id=int(r[2]),
        task=str(r[3] or ""),
        task_geo=str(r[4] or ""),
        task_branches=str(r[5] or ""),
        sys_status=str(r[6]) if r[6] is not None else None,
    )


def _count_total(conn, type_: TypeName) -> int:
    with conn.cursor() as cur:
        if type_ == "branch":
            cur.execute("SELECT COUNT(*) FROM gb_branches")
        else:
            cur.execute("SELECT COUNT(*) FROM cities_sys")
        row = cur.fetchone()
    return int(row[0] or 0)


def _count_done(conn, t: TaskRow, type_: TypeName) -> int:
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


def _update_task_status(conn, task_id: int, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE aap_audience_audiencetask
            SET _sys_status = %s,
                _sys_updater = %s,
                _sys_last_updated = now()
            WHERE id = %s
            """,
            (status, UPDATER, int(task_id)),
        )


def _pick_candidates(conn, t: TaskRow, type_: TypeName, limit: int) -> List[Dict[str, Any]]:
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
                c.state_name,
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


def _insert_ranked(conn, t: TaskRow, type_: TypeName, items: List[Dict[str, int]]) -> int:
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
    return len(items)


def _add_for_one_task(
    *,
    conn,
    gpt: GPTClient,
    t: TaskRow,
    type_: TypeName,
    rounds: int,
    batch_size: int,
) -> Dict[str, int]:
    added_total = 0
    rounds_done = 0

    for _ in range(rounds):
        rounds_done += 1
        candidates = _pick_candidates(conn, t, type_, limit=batch_size)
        if not candidates:
            break

        ranked = gpt_rank_candidates(
            gpt=gpt,
            tier="maxi",  # debug=True → фактически nano
            workspace_id=t.workspace_id,
            user_id=t.user_id,
            main_task=t.task,
            sub_task_text=(t.task_geo if type_ == "city" else t.task_branches),
            candidates=candidates,
            type_=type_,
            endpoint=("cb_city_adder" if type_ == "city" else "cb_branch_adder"),
        )

        added_total += _insert_ranked(conn, t, type_, ranked)
        conn.commit()

    return {"task_id": t.id, "rounds": rounds_done, "added": added_total}


# -------------------- Worker tasks --------------------

def task_cb_status_updater() -> Dict[str, Any]:
    with get_connection() as conn:
        tasks = _fetch_processing_tasks(conn)

        cities_total = _count_total(conn, "city")
        branches_total = _count_total(conn, "branch")

        forced_collecting = 0
        set_extract = 0
        untouched = 0

        for t in tasks:
            cities_done = _count_done(conn, t, "city")
            branches_done = _count_done(conn, t, "branch")

            lacks = (cities_done < cities_total) or (branches_done < branches_total)
            if lacks:
                _update_task_status(conn, t.id, STATUS_COLLECTING)
                forced_collecting += 1
                continue

            desired = STATUS_EXTRACT

            if t.sys_status == STATUS_FINISHED:
                untouched += 1
                continue

            # менять можно только из NULL/CB_COLLECTING
            if t.sys_status != desired and (t.sys_status is None or t.sys_status == STATUS_COLLECTING):
                _update_task_status(conn, t.id, desired)
                set_extract += 1
            else:
                untouched += 1

        conn.commit()

    return {
        "tasks": len(tasks),
        "cities_total": cities_total,
        "branches_total": branches_total,
        "forced_collecting": forced_collecting,
        "set_extract": set_extract,
        "untouched": untouched,
    }


def task_cb_city_adder() -> Dict[str, Any]:
    gpt = GPTClient(debug=GPT_DEBUG)

    with get_connection() as conn:
        tasks = _fetch_tasks_by_status(conn, STATUS_COLLECTING)
        if not tasks:
            return {"tasks": 0, "processed": 0, "added": 0}

        t = tasks[0]
        res = _add_for_one_task(
            conn=conn,
            gpt=gpt,
            t=t,
            type_="city",
            rounds=ROUNDS_PER_RUN,
            batch_size=BATCH_SIZE,
        )

    return {"tasks": len(tasks), "processed": 1, "added": res["added"]}


def task_cb_branch_adder() -> Dict[str, Any]:
    gpt = GPTClient(debug=GPT_DEBUG)

    with get_connection() as conn:
        tasks = _fetch_tasks_by_status(conn, STATUS_COLLECTING)
        if not tasks:
            return {"tasks": 0, "processed": 0, "added": 0}

        t = tasks[0]
        res = _add_for_one_task(
            conn=conn,
            gpt=gpt,
            t=t,
            type_="branch",
            rounds=ROUNDS_PER_RUN,
            batch_size=BATCH_SIZE,
        )

    return {"tasks": len(tasks), "processed": 1, "added": res["added"]}


def task_cb_queue_filler() -> Dict[str, Any]:
    with get_connection() as conn, conn.cursor() as cur:
        t = _fetch_one_task_by_status(conn, STATUS_EXTRACT)
        if not t:
            return {"mode": "noop", "reason": "no_tasks_in_extract"}

        cur.execute("SELECT COUNT(*)::bigint FROM cb_crawler")
        total_gen = int(cur.fetchone()[0] or 0)
        window_lim = max(1, int(total_gen * QUEUE_WINDOW_SHARE))

        cur.execute("SELECT COUNT(*)::bigint FROM queue_sys WHERE task_id = %s", (t.id,))
        total_in_queue = int(cur.fetchone()[0] or 0)
        if total_in_queue >= total_gen:
            return {
                "mode": "noop",
                "task_id": t.id,
                "reason": "queue_full_for_task",
                "total_in_queue": total_in_queue,
                "total_gen": total_gen,
            }

        cur.execute(
            "SELECT COUNT(*)::bigint FROM queue_sys WHERE task_id = %s AND status != 'done'",
            (t.id,),
        )
        left_cnt = int(cur.fetchone()[0] or 0)
        if left_cnt >= QUEUE_MIN_LEFT:
            return {"mode": "noop", "task_id": t.id, "reason": "queue_has_enough_left", "left": left_cnt}

        # большой тупой селект (без top'ов) + upsert
        cur.execute(
            """
            WITH
            cities AS (
              SELECT ct.value_id AS city_id, ct.rate AS city_rate
              FROM crawl_tasks ct
              WHERE ct.workspace_id = %s
                AND ct.user_id      = %s
                AND ct.task_id      = %s
                AND ct.type         = 'city'
            ),
            city_plz AS (
              SELECT unnest(c.plz_list) AS plz, ci.city_rate
              FROM cities ci
              JOIN cities_sys c ON c.id = ci.city_id
            ),
            branches AS (
              SELECT ct.value_id AS branch_id, ct.rate AS branch_rate
              FROM crawl_tasks ct
              WHERE ct.workspace_id = %s
                AND ct.user_id      = %s
                AND ct.task_id      = %s
                AND ct.type         = 'branch'
            ),
            cand AS (
              SELECT
                cc.id AS cb_crawler_id,
                (%s)::int AS task_id,
                (cp.city_rate * br.branch_rate) AS rate
              FROM city_plz cp
              CROSS JOIN branches br
              JOIN cb_crawler cc
                ON cc.plz = cp.plz
               AND cc.branch_id = br.branch_id
            )
            INSERT INTO queue_sys (cb_crawler_id, task_id, rate, status, time)
            SELECT
              c.cb_crawler_id,
              c.task_id,
              c.rate,
              'pending'::text,
              NULL::timestamptz
            FROM cand c
            ORDER BY c.rate ASC, c.cb_crawler_id ASC
            LIMIT %s
            ON CONFLICT (task_id, cb_crawler_id) DO UPDATE
            SET
              rate = LEAST(queue_sys.rate, EXCLUDED.rate),
              status = CASE
                        WHEN queue_sys.status = 'processing' THEN queue_sys.status
                        WHEN queue_sys.status IN ('done','error') THEN 'pending'
                        ELSE queue_sys.status
                       END,
              time = CASE
                      WHEN queue_sys.status = 'processing' THEN queue_sys.time
                      WHEN queue_sys.status IN ('done','error') THEN NULL
                      ELSE queue_sys.time
                     END
            WHERE queue_sys.status <> 'processing'
            """,
            (
                t.workspace_id,
                t.user_id,
                t.id,
                t.workspace_id,
                t.user_id,
                t.id,
                t.id,
                window_lim,
            ),
        )
        # rowcount тут не идеален, но хотя бы ориентир
        inserted_or_updated = int(cur.rowcount or 0)

        _update_task_status(conn, t.id, STATUS_CRAWL)
        conn.commit()

        return {
            "mode": "filled",
            "task_id": t.id,
            "window_lim": window_lim,
            "queue_before": total_in_queue,
            "left_before": left_cnt,
            "affected": inserted_or_updated,
            "status_set": STATUS_CRAWL,
        }


def task_cb_integrity_check() -> Dict[str, Any]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT COALESCE(SUM(cardinality(plz_list)), 0)::bigint FROM cities_sys")
        plz_total = int(cur.fetchone()[0] or 0)

        cur.execute("SELECT COUNT(*)::bigint FROM gb_branches")
        branches_total = int(cur.fetchone()[0] or 0)

        expected = plz_total * branches_total

        cur.execute("SELECT COUNT(*)::bigint FROM cb_crawler")
        actual = int(cur.fetchone()[0] or 0)

    ok = (expected == actual)
    if not ok:
        print(
            f"[{UPDATER}] MISMATCH: expected={expected} (plz_total={plz_total} * branches_total={branches_total}) "
            f"!= cb_crawler={actual}. сделайте что-то!",
            flush=True,
        )

    return {"ok": ok, "expected": expected, "actual": actual, "plz_total": plz_total, "branches_total": branches_total}


def main() -> None:
    w = Worker(name="cb_processor", tick_sec=0.5)

    w.register(
        "cb_status_updater",
        task_cb_status_updater,
        every_sec=5,
        timeout_sec=TASK_TIMEOUT_SEC,
        priority=10,
    )
    w.register(
        "cb_city_adder",
        task_cb_city_adder,
        every_sec=10,
        timeout_sec=TASK_TIMEOUT_SEC,
        priority=20,
    )
    w.register(
        "cb_branch_adder",
        task_cb_branch_adder,
        every_sec=10,
        timeout_sec=TASK_TIMEOUT_SEC,
        priority=30,
    )

    # 5-й воркер: queue filler (heavy) каждые 10 секунд
    w.register(
        "cb_queue_filler",
        task_cb_queue_filler,
        every_sec=10,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=True,
        priority=35,
    )

    w.register(
        "cb_integrity_check",
        task_cb_integrity_check,
        every_sec=3600,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=True,
        priority=5,
    )

    # сдвиг +5 секунд для branch
    try:
        w._next_run_at["cb_branch_adder"] += 5.0  # type: ignore[attr-defined]
    except Exception:
        pass

    w.run_forever()


if __name__ == "__main__":
    main()
