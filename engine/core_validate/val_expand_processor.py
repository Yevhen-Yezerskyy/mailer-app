# FILE: engine/core_validate/val_expand_processor.py  (обновлено — 2025-12-28)
# Смысл:
# - Один процессор развёртывания rate_contacts (1 процесс, max_parallel=1)
# - Каждую секунду выбирает случайный task: run_processing=true AND inserted_50k=false
# - Внутри task:
#   * окно 300×300 (cities × branches), сортировка по K=(rate_cb, city_id, branch_id)
#   * CUT-OFF: берём последнюю пару из rate_contacts (по max rate_cb + city/branch) и СТАРТУЕМ С НЕЁ
#     (потому что она может быть дыркой)
#   * пара готова только если ВСЕ plz (cb_crawler) collected=true
#   * если у пары есть collected=false → дырка → стоп
#   * BULK INSERT: на одну пару (city,branch) — один INSERT..SELECT
#   * лимит 50k: если rate_contacts(task_id) >= 50_000 → inserted_50k=true и стоп
# - Печать ключевых шагов

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from engine.common.worker import Worker
from engine.common.db import fetch_all, fetch_one, get_connection

MAX_CONTACTS = 50_000
WINDOW_LIMIT = 300


def _p(msg: str) -> None:
    print(f"[val_expand] {msg}")


def _count_task(task_id: int) -> int:
    row = fetch_one("SELECT count(*) FROM rate_contacts WHERE task_id = %s", (task_id,))
    return int(row[0]) if row else 0


def _set_inserted_50k(task_id: int) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            cur.execute(
                """
                UPDATE aap_audience_audiencetask
                SET inserted_50k = true
                WHERE id = %s
                """,
                (task_id,),
            )
            cur.execute("COMMIT")


def _get_cutoff_pair(task_id: int) -> Optional[Tuple[int, int, int]]:
    """
    Возвращает K=(rate_cb, city_id, branch_id) для последней уже развёрнутой записи.
    Важно: стартуем С ЭТОЙ ЖЕ пары, т.к. она может быть дыркой.
    """
    row = fetch_one(
        """
        SELECT rc.rate_cb, c.city_id, c.branch_id
        FROM rate_contacts rc
        JOIN cb_crawler c ON c.id = rc.cb_crawler_id
        WHERE rc.task_id = %s
          AND rc.rate_cb IS NOT NULL
        ORDER BY rc.rate_cb DESC, c.city_id DESC, c.branch_id DESC
        LIMIT 1
        """,
        (task_id,),
    )
    if not row:
        return None
    return (int(row[0]), int(row[1]), int(row[2]))


def run_once() -> None:
    # 1) случайный task
    row = fetch_one(
        """
        SELECT id
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND inserted_50k = false
        ORDER BY random()
        LIMIT 1
        """
    )
    if not row:
        return

    task_id = int(row[0])
    _p(f"picked task_id={task_id}")

    # 2) быстрый стоп по флагу
    row = fetch_one(
        "SELECT inserted_50k FROM aap_audience_audiencetask WHERE id = %s",
        (task_id,),
    )
    if row and bool(row[0]) is True:
        _p(f"task_id={task_id} inserted_50k=true -> skip")
        return

    # 3) лимит
    current_count = _count_task(task_id)
    _p(f"task_id={task_id} current rate_contacts={current_count}")
    if current_count >= MAX_CONTACTS:
        _set_inserted_50k(task_id)
        _p(f"task_id={task_id} already >= {MAX_CONTACTS} -> inserted_50k=true")
        return

    # 4) окно 300×300
    cities = fetch_all(
        """
        SELECT value_id, rate
        FROM crawl_tasks
        WHERE task_id = %s AND type = 'city'
        ORDER BY rate ASC, value_id ASC
        LIMIT %s
        """,
        (task_id, WINDOW_LIMIT),
    )
    if not cities:
        _p(f"task_id={task_id} no cities -> stop")
        return

    branches = fetch_all(
        """
        SELECT value_id, rate
        FROM crawl_tasks
        WHERE task_id = %s AND type = 'branch'
        ORDER BY rate ASC, value_id ASC
        LIMIT %s
        """,
        (task_id, WINDOW_LIMIT),
    )
    if not branches:
        _p(f"task_id={task_id} no branches -> stop")
        return

    city_rate: Dict[int, int] = {int(cid): int(rate) for cid, rate in cities}
    branch_rate: Dict[int, int] = {int(bid): int(rate) for bid, rate in branches}
    _p(f"task_id={task_id} window cities={len(city_rate)} branches={len(branch_rate)}")

    # 5) пары (city, branch) с ключом K=(rate_cb, city_id, branch_id)
    pairs: List[Tuple[int, int, int]] = []
    for city_id, cr in city_rate.items():
        for branch_id, br in branch_rate.items():
            pairs.append((cr * br, city_id, branch_id))
    pairs.sort(key=lambda x: (x[0], x[1], x[2]))

    cutoff = _get_cutoff_pair(task_id)
    if cutoff:
        _p(f"task_id={task_id} cutoff K=(rate_cb={cutoff[0]}, city_id={cutoff[1]}, branch_id={cutoff[2]})")
    else:
        _p(f"task_id={task_id} cutoff: none (start from beginning)")

    # 6) идём по парам: пропускаем только то, что строго МЕНЬШЕ cutoff.
    #    Ровно cutoff-пару проверяем (она может быть дыркой).
    started = cutoff is None

    for rate_cb, city_id, branch_id in pairs:
        if not started:
            if (rate_cb, city_id, branch_id) < cutoff:
                continue
            started = True  # дошли до cutoff-пары или дальше

        if current_count >= MAX_CONTACTS:
            _set_inserted_50k(task_id)
            _p(f"task_id={task_id} reached {MAX_CONTACTS} -> inserted_50k=true and stop")
            return

        # дырка? (есть хоть один uncollected plz) -> стоп
        row = fetch_one(
            """
            SELECT 1
            FROM cb_crawler
            WHERE city_id = %s
              AND branch_id = %s
              AND collected = false
            LIMIT 1
            """,
            (city_id, branch_id),
        )
        if row:
            _p(
                f"task_id={task_id} HOLE at city_id={city_id} branch_id={branch_id} rate_cb={rate_cb} -> stop"
            )
            return

        remaining = MAX_CONTACTS - current_count
        if remaining <= 0:
            _set_inserted_50k(task_id)
            _p(f"task_id={task_id} remaining<=0 -> inserted_50k=true and stop")
            return

        _p(
            f"task_id={task_id} expand pair city_id={city_id} branch_id={branch_id} rate_cb={rate_cb} remaining={remaining}"
        )

        # BULK INSERT: на пару — один запрос
        affected = 0
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                try:
                    cur.execute(
                        """
                        WITH cb AS (
                            SELECT id
                            FROM cb_crawler
                            WHERE city_id = %s AND branch_id = %s
                        ),
                        cb_arr AS (
                            SELECT array_agg(id) AS ids FROM cb
                        ),
                        picked AS (
                            SELECT
                                r.id AS contact_id,
                                (
                                    SELECT min(cb.id)
                                    FROM cb
                                    WHERE cb.id = ANY(r.cb_crawler_ids)
                                ) AS cb_crawler_id
                            FROM raw_contacts_aggr r, cb_arr a
                            WHERE r.cb_crawler_ids && a.ids
                            ORDER BY r.id ASC
                            LIMIT %s
                        )
                        INSERT INTO rate_contacts
                            (task_id, contact_id, cb_crawler_id, rate_cb)
                        SELECT
                            %s, p.contact_id, p.cb_crawler_id, %s
                        FROM picked p
                        WHERE p.cb_crawler_id IS NOT NULL
                        ON CONFLICT (task_id, contact_id)
                        DO UPDATE SET
                            cb_crawler_id = EXCLUDED.cb_crawler_id,
                            rate_cb = EXCLUDED.rate_cb,
                            updated_at = now()
                        """,
                        (city_id, branch_id, remaining, task_id, rate_cb),
                    )
                    affected = int(cur.rowcount or 0)
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise

        if affected <= 0:
            _p(f"task_id={task_id} pair city_id={city_id} branch_id={branch_id} -> 0 rows affected")
            # важно: даже если 0 строк, пара всё равно "готова" (все plz collected),
            # значит можно идти дальше.
            continue

        # пересчёт (rowcount может включать updates)
        current_count = _count_task(task_id)
        _p(f"task_id={task_id} pair done: affected={affected}, total_now={current_count}")

        if current_count >= MAX_CONTACTS:
            _set_inserted_50k(task_id)
            _p(f"task_id={task_id} reached {MAX_CONTACTS} -> inserted_50k=true and stop")
            return


def main() -> None:
    w = Worker(
        name="val_expand_processor",
        tick_sec=1,
        max_parallel=1,
    )

    w.register(
        name="expand_rate_contacts",
        fn=run_once,
        every_sec=2,
        timeout_sec=900,
        singleton=True,
        heavy=True,
        priority=5,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
