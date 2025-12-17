# FILE: engine/core_validate/enrich_priority_updater.py  (обновлено) 2025-12-16
# Смысл: раз в час синхронизирует public.__enrich_priority с aap_audience_audiencetask:
# - для run_processing=true: upsert task_id, en_needed=subscribers_limit//2 (en_done не трогаем)
# - для run_processing=false: удалить из __enrich_priority
# - прогресс считаем ТОЛЬКО если en_done=0 (то самое "пусто"):
#   count(*) из raw_contacts_aggr, где sources содержит 'GPT' и запись относится к task_id
#   (через пересечение cb_crawler_ids с queue_sys.cb_crawler_id по этому task_id)

from __future__ import annotations

from typing import List

from engine.common.db import get_connection


def run_batch() -> None:
    print("[enrich_updater] start")

    sql_upsert_active = """
        INSERT INTO public.__enrich_priority (task_id, en_needed, en_done, created_at, updated_at)
        SELECT
            t.id AS task_id,
            GREATEST(0, (t.subscribers_limit / 2))::int AS en_needed,
            0::int AS en_done,
            now(),
            now()
        FROM public.aap_audience_audiencetask t
        WHERE t.run_processing = TRUE
        ON CONFLICT (task_id) DO UPDATE
        SET
            en_needed = EXCLUDED.en_needed,
            updated_at = now()
        -- en_done не трогаем вообще (чтобы не сбрасывать прогресс)
    """

    sql_delete_inactive = """
        DELETE FROM public.__enrich_priority ep
        WHERE NOT EXISTS (
            SELECT 1
            FROM public.aap_audience_audiencetask t
            WHERE t.id = ep.task_id
              AND t.run_processing = TRUE
        )
    """

    # "пусто" == 0
    sql_tasks_need_count = """
        SELECT task_id
        FROM public.__enrich_priority
        WHERE en_done = 0
        ORDER BY task_id
    """

    # считаем "энричед" из raw_contacts_aggr:
    # - запись получала GPT (sources содержит 'GPT')
    # - и относится к task_id: есть cb_crawler_id из queue_sys (по task_id),
    #   который присутствует в aggr.cb_crawler_ids
    sql_count_done_aggr = """
        SELECT COUNT(*)::int
        FROM public.raw_contacts_aggr a
        WHERE a.sources @> ARRAY['GPT']::text[]
          AND EXISTS (
              SELECT 1
              FROM public.queue_sys q
              WHERE q.task_id = %s
                AND q.cb_crawler_id = ANY(a.cb_crawler_ids)
          )
    """

    sql_set_done = """
        UPDATE public.__enrich_priority
        SET en_done = %s,
            updated_at = now()
        WHERE task_id = %s
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_upsert_active)
            print("[enrich_updater] upsert active")

            cur.execute(sql_delete_inactive)
            deleted = cur.rowcount if cur.rowcount is not None else 0
            if deleted:
                print(f"[enrich_updater] deleted inactive: rows={deleted}")

            cur.execute(sql_tasks_need_count)
            task_ids: List[int] = [int(x[0]) for x in (cur.fetchall() or [])]

            if not task_ids:
                print("[enrich_updater] no tasks with en_done=0")
                conn.commit()
                print("[enrich_updater] committed")
                return

            print(f"[enrich_updater] need count (en_done=0): tasks={len(task_ids)}")
            for task_id in task_ids:
                cur.execute(sql_count_done_aggr, (task_id,))
                done = int(cur.fetchone()[0])
                cur.execute(sql_set_done, (done, task_id))
                print(f"[enrich_updater] task_id={task_id} en_done={done}")

        conn.commit()

    print("[enrich_updater] committed")


def main() -> None:
    run_batch()


if __name__ == "__main__":
    main()
