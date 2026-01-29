# FILE: engine/core_validate/val_expand_processor.py
# DATE: 2026-01-29
# PURPOSE:
# - Expander + hash-guard as one logical block with queue_builder.
# - Insert-only into rate_contacts: ON CONFLICT (task_id, contact_id) DO NOTHING.
# - full: get_expand_full(task_id) -> cb_id batches -> insert contacts not yet present.
# - light:
#     * if task has NO rate_contacts yet -> call full for this task and exit.
#     * get crawler list (cb_id, rate, collected flag)
#     * recheck cb_crawler.collected for these cb_ids
#     * insert ONLY for cb_ids that became collected=true since last cached flag
#     * update local flags and persist back via queue_builder.put_crawler
# - hash-guard (every 10 min):
#     * compare queue_builder.kt_hash(crawl_tasks) vs __task__kt_hash.kt_hash
#     * if mismatch: TOUCH one crawl_tasks.updated_at, recompute hash via SAME cursor, DELETE rate_contacts, store new hash

from __future__ import annotations

from typing import Dict, List, Tuple

from engine.common.db import execute, fetch_all, fetch_one, get_connection
from engine.common.worker import Worker
from engine.core_validate import queue_builder

Val = Tuple[int, int, bool]  # (cb_id, rate_cb, collected)

FULL_CB_BATCH = 10
LIGHT_CB_BATCH = 50
MAX_RATE_CONTACTS_PER_TASK = 50_000


def _p(msg: str) -> None:
    print(f"[val_expand] {msg}")


def _pick_task_id() -> int | None:
    row = fetch_one(
        """
        SELECT id
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND collected = false
        ORDER BY random()
        LIMIT 1
        """
    )
    return int(row[0]) if row else None


def _has_any_rate_contacts(task_id: int) -> bool:
    row = fetch_one("SELECT 1 FROM rate_contacts WHERE task_id = %s LIMIT 1", (int(task_id),))
    return bool(row)


def _get_stored_task_hash(task_id: int) -> str | None:
    row = fetch_one("SELECT kt_hash FROM __task__kt_hash WHERE task_id = %s", (int(task_id),))
    return str(row[0]) if row and row[0] else None


def _is_task_hash_ok(task_id: int) -> bool:
    stored = _get_stored_task_hash(int(task_id))
    if stored is None:
        return False
    return str(stored) == str(queue_builder.kt_hash(int(task_id)))


def _insert_for_cb_batch(task_id: int, cb_batch: List[Tuple[int, int]]) -> int:
    """
    cb_batch: [(cb_id, rate_cb)].
    Insert-only; ON CONFLICT DO NOTHING.
    Deterministic: for contacts matching multiple cb_ids in batch:
      pick min(rate_cb), and if tie pick min(cb_id).
    """
    if not cb_batch:
        return 0

    cb_ids = [int(cb_id) for (cb_id, _rate) in cb_batch]
    rates = [int(rate) for (_cb_id, rate) in cb_batch]

    row = fetch_one(
        """
        WITH inp(cb_id, rate_cb) AS (
            SELECT * FROM unnest(%s::bigint[], %s::int[])
        ),
        cand_raw AS (
            SELECT r.id AS contact_id, r.cb_crawler_ids
            FROM raw_contacts_aggr r
            WHERE r.cb_crawler_ids && %s::bigint[]
        ),
        cand AS (
            SELECT
                cr.contact_id,
                min(inp.rate_cb) AS best_rate_cb,
                (array_agg(inp.cb_id ORDER BY inp.rate_cb ASC, inp.cb_id ASC))[1] AS best_cb_id
            FROM cand_raw cr
            JOIN LATERAL unnest(cr.cb_crawler_ids) AS u(cb_id) ON true
            JOIN inp ON inp.cb_id = u.cb_id
            GROUP BY cr.contact_id
        ),
        ins AS (
            INSERT INTO rate_contacts (task_id, contact_id, cb_id, rate_cb, updated_at)
            SELECT %s, c.contact_id, c.best_cb_id, c.best_rate_cb, now()
            FROM cand c
            ON CONFLICT (task_id, contact_id) DO NOTHING
            RETURNING 1
        )
        SELECT count(*) FROM ins
        """,
        (cb_ids, rates, cb_ids, int(task_id)),
    )
    return int(row[0]) if row else 0


def _cb_collected_map(cb_ids: List[int]) -> Dict[int, bool]:
    if not cb_ids:
        return {}
    rows = fetch_all(
        """
        SELECT id, collected
        FROM cb_crawler
        WHERE id = ANY(%s::bigint[])
        """,
        (list(map(int, cb_ids)),),
    )
    out: Dict[int, bool] = {}
    for cb_id, collected in rows:
        out[int(cb_id)] = bool(collected)
    return out


def mark_collected_once() -> None:
    rows = fetch_all(
        """
        SELECT id
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND collected = false
        ORDER BY id ASC
        """
    )
    if not rows:
        return

    for (task_id_raw,) in rows:
        task_id = int(task_id_raw)

        row_lim = fetch_one(
            """
            SELECT 1
            FROM rate_contacts
            WHERE task_id = %s
            ORDER BY id ASC
            OFFSET %s
            LIMIT 1
            """,
            (int(task_id), int(MAX_RATE_CONTACTS_PER_TASK - 1)),
        )
        if row_lim:
            execute(
                """
                UPDATE aap_audience_audiencetask
                SET collected = true
                WHERE id = %s
                """,
                (int(task_id),),
            )
            _p(f"MARK task_id={task_id} -> collected=true (limit={MAX_RATE_CONTACTS_PER_TASK})")
            continue

        row_rc = fetch_one("SELECT max(updated_at) FROM rate_contacts WHERE task_id = %s", (int(task_id),))
        rc_max = row_rc[0] if row_rc else None
        if rc_max is None:
            continue

        row_old = fetch_one("SELECT 1 WHERE %s < (now() - interval '24 hours')", (rc_max,))
        if row_old:
            execute(
                """
                UPDATE aap_audience_audiencetask
                SET collected = true
                WHERE id = %s
                """,
                (int(task_id),),
            )
            _p(f"MARK task_id={task_id} -> collected=true (stale >24h)")


def hash_guard_once() -> None:
    rows = fetch_all(
        """
        SELECT id
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND collected = false
        ORDER BY id ASC
        """
    )
    if not rows:
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            for (task_id_raw,) in rows:
                task_id = int(task_id_raw)

                stored = _get_stored_task_hash(task_id)
                current = str(queue_builder.kt_hash(task_id, cur=cur))

                if stored is None:
                    cur.execute(
                        """
                        INSERT INTO __task__kt_hash (task_id, kt_hash)
                        VALUES (%s, %s)
                        ON CONFLICT (task_id)
                        DO UPDATE SET kt_hash = EXCLUDED.kt_hash
                        """,
                        (task_id, current),
                    )
                    continue

                if str(stored) == current:
                    continue

                # atomic switch: touch -> new hash (same cursor) -> delete -> store
                cur.execute(
                    """
                    UPDATE crawl_tasks
                    SET updated_at = now()
                    WHERE task_id = %s
                      AND id = (
                          SELECT id
                          FROM crawl_tasks
                          WHERE task_id = %s
                          ORDER BY id ASC
                          LIMIT 1
                      )
                    """,
                    (task_id, task_id),
                )

                new_hash = str(queue_builder.kt_hash(task_id, cur=cur))

                cur.execute("DELETE FROM rate_contacts WHERE task_id = %s", (task_id,))
                cur.execute(
                    """
                    INSERT INTO __task__kt_hash (task_id, kt_hash)
                    VALUES (%s, %s)
                    ON CONFLICT (task_id)
                    DO UPDATE SET kt_hash = EXCLUDED.kt_hash
                    """,
                    (task_id, new_hash),
                )

                _p(f"HASH-GUARD task_id={task_id} mismatch -> touch + delete + set_hash")


def light_run_once() -> None:
    task_id = _pick_task_id()
    if not task_id:
        return

    # if guard hasn't initialized hash yet or mismatch is in progress -> do nothing (avoid "floating")
    if not _is_task_hash_ok(int(task_id)):
        return

    # if deleted by guard (or new task) -> do full immediately for THIS task
    if not _has_any_rate_contacts(int(task_id)):
        full_reconcile_task(int(task_id))
        return

    crawler = list(queue_builder.get_crawler(int(task_id)))
    if not crawler:
        return

    cb_ids = [int(cb_id) for (cb_id, _rate, _col) in crawler]
    cmap = _cb_collected_map(cb_ids)

    newly_collected: List[Tuple[int, int]] = []
    updated: List[Val] = []

    for cb_id, rate_cb, old_col in crawler:
        now_col = bool(cmap.get(int(cb_id), bool(old_col)))
        if (not bool(old_col)) and now_col:
            newly_collected.append((int(cb_id), int(rate_cb)))
        updated.append((int(cb_id), int(rate_cb), bool(now_col)))

    if newly_collected:
        total = 0
        for i in range(0, len(newly_collected), int(LIGHT_CB_BATCH)):
            total += _insert_for_cb_batch(int(task_id), newly_collected[i : i + int(LIGHT_CB_BATCH)])
        _p(f"LIGHT task_id={task_id} newly_collected={len(newly_collected)} inserted={total}")

    queue_builder.put_crawler(int(task_id), updated)


def full_reconcile_task(task_id: int) -> None:
    if not _is_task_hash_ok(int(task_id)):
        return

    values = list(queue_builder.get_expand_full(int(task_id)))
    if not values:
        return

    cb_batch: List[Tuple[int, int]] = []
    total = 0

    for cb_id, rate_cb, _collected in values:
        cb_batch.append((int(cb_id), int(rate_cb)))
        if len(cb_batch) >= int(FULL_CB_BATCH):
            total += _insert_for_cb_batch(int(task_id), cb_batch)
            cb_batch = []

    if cb_batch:
        total += _insert_for_cb_batch(int(task_id), cb_batch)

    _p(f"FULL task_id={task_id} values={len(values)} inserted={total}")


def full_reconcile_once() -> None:
    task_id = _pick_task_id()
    if not task_id:
        return
    full_reconcile_task(int(task_id))


def main() -> None:
    w = Worker(
        name="val_expand_processor",
        tick_sec=1,
        max_parallel=1,
    )

    w.register(
        name="light_expand_rate_contacts",
        fn=light_run_once,
        every_sec=2,
        timeout_sec=120,
        singleton=True,
        heavy=False,
        priority=5,
    )

    w.register(
        name="full_reconcile_rate_contacts",
        fn=full_reconcile_once,
        every_sec=600,
        timeout_sec=1800,
        singleton=True,
        heavy=True,
        priority=3,
    )

    w.register(
        name="hash_guard_tasks",
        fn=hash_guard_once,
        every_sec=600,
        timeout_sec=600,
        singleton=True,
        heavy=False,
        priority=2,
    )

    w.register(
        name="mark_tasks_collected",
        fn=mark_collected_once,
        every_sec=120,
        timeout_sec=900,
        singleton=True,
        heavy=False,
        priority=1,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
