# FILE: engine/crawler/cr_processor.py  (обновлено — 2025-12-15)
# Fix: добавлены 2 DB-воркера (seed/promote) без функций в Postgres — только прямые SQL INSERT/UPDATE.
# Seed: раз в минуту добавляет task_id (run_processing=true) в ___crawler_priority с prio=1 (если ещё нет).
# Promote: раз в час переводит prio=1 -> prio=2, если для task_id найдена 1001-я строка (>=1001) в raw_contacts_gb
#         через связь queue_sys(task_id, cb_crawler_id). Обратно в 1 никогда.

from __future__ import annotations

from engine.common.db import execute, fetch_one
from engine.common.worker import Worker
from engine.crawler.fetch_gs_cb import main as run_gs_cb_spider

TASK_TIMEOUT_SEC = 900  # 15 минут

PRIO_SEED_EVERY_SEC = 60
PRIO_PROMOTE_EVERY_SEC = 60 * 60
PRIO_PROMOTE_BATCH_LIMIT = 200


def task_prio_seed() -> None:
    execute(
        """
        INSERT INTO ___crawler_priority (task_id, prio)
        SELECT t.id, 1
        FROM aap_audience_audiencetask t
        WHERE t.run_processing = true
        ON CONFLICT (task_id) DO NOTHING
        """
    )


def task_prio_promote() -> None:
    row = fetch_one(
        """
        WITH cand AS (
            SELECT task_id
            FROM ___crawler_priority
            WHERE prio = 1
            ORDER BY task_id
            LIMIT %s
        ),
        hit AS (
            SELECT c.task_id
            FROM cand c
            WHERE EXISTS (
                SELECT 1
                FROM raw_contacts_gb r
                WHERE EXISTS (
                    SELECT 1
                    FROM queue_sys q
                    WHERE q.task_id = c.task_id
                      AND q.cb_crawler_id = r.cb_crawler_id
                )
                LIMIT 1 OFFSET 1000
            )
        ),
        upd AS (
            UPDATE ___crawler_priority p
            SET prio = 2,
                updated_at = now()
            WHERE p.prio = 1
              AND p.task_id IN (SELECT task_id FROM hit)
            RETURNING 1
        )
        SELECT count(*)::int FROM upd
        """,
        (PRIO_PROMOTE_BATCH_LIMIT,),
    )
    updated = int(row[0]) if row and row[0] is not None else 0
    print(f"DEBUG: ___crawler_priority promote updated={updated} batch_limit={PRIO_PROMOTE_BATCH_LIMIT}")


def main() -> None:
    w = Worker(
        name="cb_processor",
        tick_sec=0.5,
        max_parallel=5,
    )

    # основной паук
    w.register(
        name="gs_cb_spider",
        fn=run_gs_cb_spider,
        every_sec=1,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=False,
        heavy=False,
        priority=40,
    )

    # db: seed новых task_id (раз в минуту)
    w.register(
        name="db_prio_seed",
        fn=task_prio_seed,
        every_sec=PRIO_SEED_EVERY_SEC,
        timeout_sec=30,
        singleton=True,
        heavy=False,
        priority=5,
    )

    # db: promote prio=1 -> prio=2 (раз в час)
    w.register(
        name="db_prio_promote",
        fn=task_prio_promote,
        every_sec=PRIO_PROMOTE_EVERY_SEC,
        timeout_sec=600,
        singleton=True,
        heavy=False,
        priority=6,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
