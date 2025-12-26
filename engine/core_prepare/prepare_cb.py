# FILE: engine/core_prepare/prepare_cb.py  (обновлено — 2025-12-26)

from __future__ import annotations

import json
from typing import Any, Dict, List

from engine.common.db import get_connection
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt, translate_text
from engine.common.utils import h64_text

BATCH_SIZE = 50
MODEL = "maxi"
SERVICE_TIER = "flex"


def task_prepare_geo() -> Dict[str, Any]:
    tag = "[prepare_geo]"

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_id, hash_task
            FROM __tasks_rating
            WHERE done=false
              AND type='geo'
              AND hash_task IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        )
        rr = cur.fetchone()
        if not rr:
            return {"mode": "noop", "reason": "no_tasks"}

        rating_id = int(rr[0])
        task_id = int(rr[1])
        target_hash = int(rr[2])

        cur.execute(
            """
            SELECT workspace_id, user_id, type, task, task_geo
            FROM aap_audience_audiencetask
            WHERE id=%s
            LIMIT 1
            """,
            (task_id,),
        )
        t = cur.fetchone()
        if not t:
            cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (rating_id,))
            conn.commit()
            print(f"{tag} close rating_id={rating_id} task_id={task_id} reason=task_missing")
            return {"mode": "closed", "reason": "task_missing", "task_id": task_id}

        ws_id = str(t[0])
        user_id = int(t[1])
        task_mode = str(t[2] or "")
        main_task = str(t[3] or "")
        geo_task = str(t[4] or "")

        prompt_name = "prepare_geo_buy" if task_mode.strip().lower() == "buy" else "prepare_geo_sell"
        base_instructions = (get_prompt(prompt_name) or "").strip()
        if not base_instructions:
            print(f"{tag} error task_id={task_id} reason=prompt_empty prompt={prompt_name}")
            return {"mode": "error", "reason": "prompt_empty", "prompt": prompt_name, "task_id": task_id}

        # переводим task + geo_task на DE и кладём в instructions (а НЕ в input)
        task_de = translate_text(main_task, "de")
        geo_de = translate_text(geo_task, "de")
        instructions = (
            base_instructions
            + "\n\n"
            + "TASK (DE):\n"
            + (task_de or "")
            + "\n\n"
            + "GEO TASK (DE):\n"
            + (geo_de or "")
            + "\n"
        )

        # 1) missing
        cur.execute(
            """
            SELECT c.id, c.name
            FROM cities_sys c
            WHERE NOT EXISTS (
                SELECT 1
                FROM crawl_tasks ct
                WHERE ct.task_id=%s
                  AND ct.type='city'
                  AND ct.value_id=c.id
            )
            ORDER BY c.id ASC
            LIMIT %s
            """,
            (task_id, BATCH_SIZE),
        )
        candidates = [{"id": int(r[0]), "name": str(r[1])} for r in cur.fetchall()]
        step = "missing"

        # 2) stale
        if not candidates:
            step = "stale"
            cur.execute(
                """
                SELECT value_id
                FROM crawl_tasks
                WHERE task_id=%s
                  AND type='city'
                  AND hash_task IS DISTINCT FROM %s
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
                """,
                (task_id, target_hash, BATCH_SIZE),
            )
            ids = [int(r[0]) for r in cur.fetchall()]
            if not ids:
                return {"mode": "noop", "reason": "nothing_to_do", "task_id": task_id, "target_hash": target_hash}

            cur.execute(
                """
                SELECT id, name
                FROM cities_sys
                WHERE id = ANY(%s)
                ORDER BY id ASC
                """,
                (ids,),
            )
            candidates = [{"id": int(r[0]), "name": str(r[1])} for r in cur.fetchall()]
            if not candidates:
                return {"mode": "noop", "reason": "stale_ids_not_found", "task_id": task_id, "target_hash": target_hash}

        print(
            f"{tag} rating_id={rating_id} task_id={task_id} step={step} "
            f"cand={len(candidates)} target_hash={target_hash}"
        )

        # input: ТОЛЬКО кандидаты
        payload = json.dumps(candidates, ensure_ascii=False, separators=(",", ":"))

        try:
            out = (
                GPTClient()
                .ask(
                    model=MODEL,
                    service_tier=SERVICE_TIER,
                    user_id=str(user_id),
                    instructions=instructions,
                    input=payload,
                    use_cache=True,
                )
                .content
                or ""
            ).strip()

            data = json.loads(out)
            if not isinstance(data, list):
                raise ValueError("GPT output is not a JSON list.")
            for it in data:
                if not isinstance(it, dict) or set(it.keys()) != {"id", "name", "rate"}:
                    raise ValueError("GPT output item must be object with keys: id, name, rate.")
        except Exception as exc:
            print(f"{tag} ERROR task_id={task_id} step={step} err={exc}")
            return {"mode": "error", "step": step, "task_id": task_id, "error": str(exc)}

        cand_ids = {int(c["id"]) for c in candidates}
        rows: List[tuple] = []
        for it in data:
            try:
                vid = int(it["id"])
                if vid not in cand_ids:
                    continue
                rate = int(it["rate"])
            except Exception:
                continue
            rows.append((ws_id, user_id, task_id, "city", vid, rate, target_hash))

        if not rows:
            print(f"{tag} noop task_id={task_id} step={step} reason=gpt_filtered_to_empty")
            return {"mode": "noop", "step": step, "task_id": task_id, "reason": "gpt_filtered_to_empty"}

        cur.executemany(
            """
            INSERT INTO crawl_tasks (workspace_id, user_id, task_id, type, value_id, rate, hash_task)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workspace_id, user_id, task_id, type, value_id)
            DO UPDATE SET
              rate = EXCLUDED.rate,
              hash_task = EXCLUDED.hash_task,
              updated_at = now()
            """,
            rows,
        )
        conn.commit()

        print(f"{tag} ok task_id={task_id} step={step} written={len(rows)}")
        return {"mode": "ok", "step": step, "task_id": task_id, "target_hash": target_hash, "written": len(rows)}


def task_prepare_branches() -> Dict[str, Any]:
    tag = "[prepare_branches]"

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_id, hash_task
            FROM __tasks_rating
            WHERE done=false
              AND type='branches'
              AND hash_task IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        )
        rr = cur.fetchone()
        if not rr:
            return {"mode": "noop", "reason": "no_tasks"}

        rating_id = int(rr[0])
        task_id = int(rr[1])
        target_hash = int(rr[2])

        cur.execute(
            """
            SELECT workspace_id, user_id, type, task, task_branches
            FROM aap_audience_audiencetask
            WHERE id=%s
            LIMIT 1
            """,
            (task_id,),
        )
        t = cur.fetchone()
        if not t:
            cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (rating_id,))
            conn.commit()
            print(f"{tag} close rating_id={rating_id} task_id={task_id} reason=task_missing")
            return {"mode": "closed", "reason": "task_missing", "task_id": task_id}

        ws_id = str(t[0])
        user_id = int(t[1])
        task_mode = str(t[2] or "")
        main_task = str(t[3] or "")
        branches_task = str(t[4] or "")

        prompt_name = "prepare_branches_buy" if task_mode.strip().lower() == "buy" else "prepare_branches_sell"
        base_instructions = (get_prompt(prompt_name) or "").strip()
        if not base_instructions:
            print(f"{tag} error task_id={task_id} reason=prompt_empty prompt={prompt_name}")
            return {"mode": "error", "reason": "prompt_empty", "prompt": prompt_name, "task_id": task_id}

        # переводим task + branches на DE и кладём в instructions (а НЕ в input)
        task_de = translate_text(main_task, "de")
        branches_de = translate_text(branches_task, "de")
        instructions = (
            base_instructions
            + "\n\n"
            + "TASK (DE):\n"
            + (task_de or "")
            + "\n\n"
            + "BRANCHES TASK (DE):\n"
            + (branches_de or "")
            + "\n"
        )

        # 1) missing
        cur.execute(
            """
            SELECT b.id, b.name
            FROM gb_branches b
            WHERE NOT EXISTS (
                SELECT 1
                FROM crawl_tasks ct
                WHERE ct.task_id=%s
                  AND ct.type='branch'
                  AND ct.value_id=b.id
            )
            ORDER BY b.id ASC
            LIMIT %s
            """,
            (task_id, BATCH_SIZE),
        )
        candidates = [{"id": int(r[0]), "name": str(r[1])} for r in cur.fetchall()]
        step = "missing"

        # 2) stale
        if not candidates:
            step = "stale"
            cur.execute(
                """
                SELECT value_id
                FROM crawl_tasks
                WHERE task_id=%s
                  AND type='branch'
                  AND hash_task IS DISTINCT FROM %s
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
                """,
                (task_id, target_hash, BATCH_SIZE),
            )
            ids = [int(r[0]) for r in cur.fetchall()]
            if not ids:
                return {"mode": "noop", "reason": "nothing_to_do", "task_id": task_id, "target_hash": target_hash}

            cur.execute(
                """
                SELECT id, name
                FROM gb_branches
                WHERE id = ANY(%s)
                ORDER BY id ASC
                """,
                (ids,),
            )
            candidates = [{"id": int(r[0]), "name": str(r[1])} for r in cur.fetchall()]
            if not candidates:
                return {"mode": "noop", "reason": "stale_ids_not_found", "task_id": task_id, "target_hash": target_hash}

        print(
            f"{tag} rating_id={rating_id} task_id={task_id} step={step} "
            f"cand={len(candidates)} target_hash={target_hash}"
        )

        # input: ТОЛЬКО кандидаты
        payload = json.dumps(candidates, ensure_ascii=False, separators=(",", ":"))

        try:
            out = (
                GPTClient()
                .ask(
                    model=MODEL,
                    service_tier=SERVICE_TIER,
                    user_id=str(user_id),
                    instructions=instructions,
                    input=payload,
                    use_cache=True,
                )
                .content
                or ""
            ).strip()

            data = json.loads(out)
            if not isinstance(data, list):
                raise ValueError("GPT output is not a JSON list.")
            for it in data:
                if not isinstance(it, dict) or set(it.keys()) != {"id", "name", "rate"}:
                    raise ValueError("GPT output item must be object with keys: id, name, rate.")
        except Exception as exc:
            print(f"{tag} ERROR task_id={task_id} step={step} err={exc}")
            return {"mode": "error", "step": step, "task_id": task_id, "error": str(exc)}

        cand_ids = {int(c["id"]) for c in candidates}
        rows: List[tuple] = []
        for it in data:
            try:
                vid = int(it["id"])
                if vid not in cand_ids:
                    continue
                rate = int(it["rate"])
            except Exception:
                continue
            rows.append((ws_id, user_id, task_id, "branch", vid, rate, target_hash))

        if not rows:
            print(f"{tag} noop task_id={task_id} step={step} reason=gpt_filtered_to_empty")
            return {"mode": "noop", "step": step, "task_id": task_id, "reason": "gpt_filtered_to_empty"}

        cur.executemany(
            """
            INSERT INTO crawl_tasks (workspace_id, user_id, task_id, type, value_id, rate, hash_task)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (workspace_id, user_id, task_id, type, value_id)
            DO UPDATE SET
              rate = EXCLUDED.rate,
              hash_task = EXCLUDED.hash_task,
              updated_at = now()
            """,
            rows,
        )
        conn.commit()

        print(f"{tag} ok task_id={task_id} step={step} written={len(rows)}")
        return {"mode": "ok", "step": step, "task_id": task_id, "target_hash": target_hash, "written": len(rows)}


def task_prepare_done() -> Dict[str, Any]:
    tag = "[prepare_done]"
    processed = 0
    closed_stale = 0
    closed_ready = 0

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_id, type, hash_task
            FROM __tasks_rating
            WHERE done=false
              AND type IN ('geo','branches')
              AND hash_task IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT 50
            """
        )
        rows = cur.fetchall()

        for r in rows:
            processed += 1
            rating_id = int(r[0])
            task_id = int(r[1])
            kind = str(r[2] or "")
            target_hash = int(r[3])

            if kind == "geo":
                cur.execute(
                    "SELECT task, task_geo FROM aap_audience_audiencetask WHERE id=%s LIMIT 1",
                    (task_id,),
                )
                t = cur.fetchone()
                if not t:
                    cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (rating_id,))
                    closed_stale += 1
                    continue

                real_hash = h64_text(str(t[0] or "") + str(t[1] or ""))
                if int(real_hash) != int(target_hash):
                    cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (rating_id,))
                    closed_stale += 1
                    continue

                cur.execute(
                    """
                    SELECT COUNT(*)::bigint
                    FROM cities_sys c
                    WHERE NOT EXISTS (
                        SELECT 1 FROM crawl_tasks ct
                        WHERE ct.task_id=%s AND ct.type='city' AND ct.value_id=c.id
                    )
                    """,
                    (task_id,),
                )
                missing_cnt = int(cur.fetchone()[0] or 0)

                cur.execute(
                    """
                    SELECT COUNT(*)::bigint
                    FROM crawl_tasks
                    WHERE task_id=%s AND type='city' AND hash_task IS DISTINCT FROM %s
                    """,
                    (task_id, target_hash),
                )
                stale_cnt = int(cur.fetchone()[0] or 0)

                if missing_cnt == 0 and stale_cnt == 0:
                    cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (rating_id,))
                    closed_ready += 1

            else:  # branches
                cur.execute(
                    "SELECT task, task_branches FROM aap_audience_audiencetask WHERE id=%s LIMIT 1",
                    (task_id,),
                )
                t = cur.fetchone()
                if not t:
                    cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (rating_id,))
                    closed_stale += 1
                    continue

                real_hash = h64_text(str(t[0] or "") + str(t[1] or ""))
                if int(real_hash) != int(target_hash):
                    cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (rating_id,))
                    closed_stale += 1
                    continue

                cur.execute(
                    """
                    SELECT COUNT(*)::bigint
                    FROM gb_branches b
                    WHERE NOT EXISTS (
                        SELECT 1 FROM crawl_tasks ct
                        WHERE ct.task_id=%s AND ct.type='branch' AND ct.value_id=b.id
                    )
                    """,
                    (task_id,),
                )
                missing_cnt = int(cur.fetchone()[0] or 0)

                cur.execute(
                    """
                    SELECT COUNT(*)::bigint
                    FROM crawl_tasks
                    WHERE task_id=%s AND type='branch' AND hash_task IS DISTINCT FROM %s
                    """,
                    (task_id, target_hash),
                )
                stale_cnt = int(cur.fetchone()[0] or 0)

                if missing_cnt == 0 and stale_cnt == 0:
                    cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (rating_id,))
                    closed_ready += 1

        conn.commit()

    print(f"{tag} processed={processed} closed_stale={closed_stale} closed_ready={closed_ready}")
    return {"processed": processed, "closed_stale": closed_stale, "closed_ready": closed_ready}


def main() -> None:
    task_prepare_geo()
    task_prepare_branches()
    task_prepare_done()


if __name__ == "__main__":
    main()
