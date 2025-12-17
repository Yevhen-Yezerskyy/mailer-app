# FILE: engine/core_rate/rate_contacts.py  (новое) 2025-12-17
# Смысл: батч ранжирования компаний (raw_contacts_aggr) для task_id.
# - task_id выбирается функцией public.__pick_rate_task_id()
# - кандидаты: queue_sys(task_id) -> raw_contacts_aggr по cb_crawler_id ∈ cb_crawler_ids
# - исключаем уже оцененные (rate_contacts по task_id+contact_id)
# - делим на 2 группы по status_data: YES WEB / NOT YES WEB
# - для каждой группы до 50 контактов -> GPT (2 запроса)
# - результат -> upsert в rate_contacts
# - __rate_priority.rt_done не трогаем (апдейтер раз в 10 минут считает count(*) из rate_contacts)

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from engine.common.db import get_connection
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt

BATCH_SIZE_PER_GROUP = 50

STATUS_YES_WEB = "YES WEB"


def _trim(s: Any) -> Optional[str]:
    if s is None:
        return None
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    return s or None


def _drop_empty(v: Any) -> Any:
    """
    Рекурсивно убираем пустышки:
    - None
    - ""
    - []
    - {}
    """
    if v is None:
        return None

    if isinstance(v, str):
        s = v.strip()
        return s or None

    if isinstance(v, list):
        out = []
        for x in v:
            x2 = _drop_empty(x)
            if x2 is None:
                continue
            out.append(x2)
        return out or None

    if isinstance(v, dict):
        out = {}
        for k, x in v.items():
            x2 = _drop_empty(x)
            if x2 is None:
                continue
            out[k] = x2
        return out or None

    return v


def _clean_norm(norm: Dict[str, Any]) -> Dict[str, Any]:
    """
    Всегда выкидываем:
      source_urls, city, plz, email, fax
    Потом чистим пустышки рекурсивно.
    """
    if not isinstance(norm, dict):
        return {}

    norm2 = dict(norm)

    for k in ("source_urls", "city", "plz", "email", "fax"):
        norm2.pop(k, None)

    cleaned = _drop_empty(norm2)
    return cleaned if isinstance(cleaned, dict) else {}


def _gpt_base_system_prompt() -> str:
    # файл промпта вы добавите отдельно; если его нет — вернется ""
    return get_prompt("engine_core_rate_contacts")


def _gpt_system_prompt_for_task(task: str, task_client: str) -> str:
    base = _gpt_base_system_prompt()
    # task/task_client — неизменяемая часть в рамках таска: вклеиваем в system
    t = _trim(task) or ""
    c = _trim(task_client) or ""
    return (
        f"{base}\n\n"
        f"=== TASK ===\n{t}\n\n"
        f"=== CLIENT ===\n{c}\n"
    ).strip() + "\n"


def _gpt_user_payload(items: List[Dict[str, Any]]) -> str:
    return json.dumps({"items": items}, ensure_ascii=False)


def _parse_gpt_json(content: str) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(content)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _items_by_id(gpt_data: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    items = gpt_data.get("items")
    if not isinstance(items, list):
        return out
    for it in items:
        if not isinstance(it, dict):
            continue
        try:
            rid = int(it.get("id"))
        except Exception:
            continue
        out[rid] = it
    return out


def run_priority_updater() -> None:
    """
    Синхронизация __rate_priority:
    - upsert активных task_id (rt_needed=subscribers_limit), rt_done по умолчанию 0
    - delete неактивных
    - rt_done = LEAST(count(rate_contacts), rt_needed) для активных
    """
    print("[rate_updater] start")

    sql_upsert_active = """
        INSERT INTO public.__rate_priority (task_id, rt_needed, rt_done, created_at, updated_at)
        SELECT
            t.id AS task_id,
            GREATEST(0, t.subscribers_limit)::int AS rt_needed,
            0::int AS rt_done,
            now(),
            now()
        FROM public.aap_audience_audiencetask t
        WHERE t.run_processing = TRUE
        ON CONFLICT (task_id) DO UPDATE
        SET
            rt_needed = EXCLUDED.rt_needed,
            updated_at = now()
        -- rt_done обновим отдельно
    """

    sql_delete_inactive = """
        DELETE FROM public.__rate_priority rp
        WHERE NOT EXISTS (
            SELECT 1
            FROM public.aap_audience_audiencetask t
            WHERE t.id = rp.task_id
              AND t.run_processing = TRUE
        )
    """

    sql_update_done = """
        UPDATE public.__rate_priority rp
        SET
            rt_done = COALESCE((
                SELECT COUNT(*)
                FROM public.rate_contacts rc
                WHERE rc.task_id = rp.task_id
            ), 0)::int,
            updated_at = now()
        WHERE EXISTS (
            SELECT 1
            FROM public.aap_audience_audiencetask t
            WHERE t.id = rp.task_id
            AND t.run_processing = TRUE
        )
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_upsert_active)
            cur.execute(sql_delete_inactive)
            cur.execute(sql_update_done)
        conn.commit()

    print("[rate_updater] committed")


def run_batch() -> None:
    sql_pick_task = "SELECT public.__pick_rate_task_id()"

    sql_task_meta = """
        SELECT workspace_id::text, user_id::text, task::text, task_client::text
        FROM public.aap_audience_audiencetask
        WHERE id = %s
    """

    # NOTE:
    # - status queue_sys не фильтруем (как договорились)
    # - джойн: q.cb_crawler_id = ANY(a.cb_crawler_ids)
    # - исключаем уже оцененных по (task_id, contact_id)
    # - lock: FOR UPDATE OF a SKIP LOCKED (на случай параллелизма в будущем)
    sql_pick_group = """
        SELECT
            a.id AS contact_id,
            a.company_data
        FROM public.queue_sys q
        JOIN public.raw_contacts_aggr a
          ON q.cb_crawler_id = ANY(a.cb_crawler_ids)
        LEFT JOIN public.rate_contacts rc
          ON rc.task_id = q.task_id
         AND rc.contact_id = a.id
        WHERE q.task_id = %s
          AND rc.contact_id IS NULL
          AND (
              (%s = TRUE  AND a.status_data = 'YES WEB') OR
              (%s = FALSE AND a.status_data <> 'YES WEB')
          )
        ORDER BY q.rate ASC
        LIMIT %s
        FOR UPDATE OF a SKIP LOCKED
    """

    sql_upsert_rate = """
        INSERT INTO public.rate_contacts (task_id, contact_id, rate, created_at, updated_at)
        VALUES (%s, %s, %s, now(), now())
        ON CONFLICT (task_id, contact_id) DO UPDATE
        SET
            rate = EXCLUDED.rate,
            updated_at = now()
    """

    print("[rate] batch start")

    with get_connection() as conn:
        with conn.cursor() as cur:
            # 1) pick task_id
            cur.execute(sql_pick_task)
            row = cur.fetchone()
            task_id = row[0] if row else None
            if task_id is None:
                print("[rate] no task_id -> exit")
                conn.commit()
                return
            task_id = int(task_id)
            print(f"[rate] task_id={task_id}")

            # 2) task meta for GPT logging + system context
            cur.execute(sql_task_meta, (task_id,))
            meta = cur.fetchone()
            if not meta:
                print(f"[rate] task_id={task_id} not found -> exit")
                conn.commit()
                return
            workspace_id_str, user_id_str, task_text, task_client_text = meta

            # 3) pick groups
            groups: List[Tuple[bool, str]] = [
                (True, "YES_WEB"),
                (False, "NOT_YES_WEB"),
            ]

            gpt = GPTClient()

            total_sent = 0
            total_written = 0

            for is_yes_web, label in groups:
                cur.execute(sql_pick_group, (task_id, is_yes_web, is_yes_web, BATCH_SIZE_PER_GROUP))
                rows = cur.fetchall() or []
                if not rows:
                    print(f"[rate] {label}: no candidates")
                    continue

                items: List[Dict[str, Any]] = []
                for contact_id, company_data in rows:
                    company_data = company_data or {}
                    if not isinstance(company_data, dict):
                        company_data = {}
                    norm = company_data.get("norm") or {}
                    if not isinstance(norm, dict):
                        norm = {}
                    norm_clean = _clean_norm(norm)
                    items.append({"id": int(contact_id), "norm": norm_clean})

                # если после чистки ничего не осталось (очень редко) — всё равно отправим, но отметим
                total_sent += len(items)
                print(f"[rate] {label}: send={len(items)}")

                system = _gpt_system_prompt_for_task(task_text, task_client_text)
                user = _gpt_user_payload(items)

                resp = gpt.ask(
                    tier="maxi-51",
                    with_web=False,
                    workspace_id=workspace_id_str,
                    user_id=user_id_str,
                    system=system,
                    user=user,
                    endpoint="rate_contacts",
                    use_cache=False,
                )
                print(
                    f"[rate] {label}: gpt ok tokens_in={resp.usage.prompt_tokens} tokens_out={resp.usage.completion_tokens}"
                )

                gpt_data = _parse_gpt_json(resp.content)
                if not gpt_data:
                    print(f"[rate] {label}: gpt JSON parse failed -> skip group")
                    continue

                by_id = _items_by_id(gpt_data)

                write_rows: List[Tuple[int, int, int]] = []
                bad = 0
                for it in items:
                    cid = int(it["id"])
                    g = by_id.get(cid)
                    if not g:
                        bad += 1
                        continue
                    try:
                        rate = int(g.get("rate"))
                    except Exception:
                        bad += 1
                        continue
                    if rate < 1 or rate > 100:
                        bad += 1
                        continue
                    write_rows.append((task_id, cid, rate))

                if bad:
                    print(f"[rate] {label}: bad_items={bad}")

                if write_rows:
                    cur.executemany(sql_upsert_rate, write_rows)
                    total_written += len(write_rows)
                    print(f"[rate] {label}: written={len(write_rows)}")
                else:
                    print(f"[rate] {label}: nothing to write")

        conn.commit()

    print(f"[rate] batch committed sent={total_sent} written={total_written}")

# FILE: engine/core_rate/rate_contacts.py  (дополнение) 2025-12-17
# Смысл: позволить запускать rate_contacts напрямую (updater → batch), как в validate.

def main() -> None:
    run_priority_updater()
    run_batch()


if __name__ == "__main__":
    main()
