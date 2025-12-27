# FILE: engine/core_prepare/prepare_cb.py  (обновлено — 2025-12-27)
# (исправлено — 2025-12-27)
# - Убраны threading-глобалы (не работают при multiprocessing)
# - Добавлены IPC TTL-локи через Manager (инициализируются из prepare_cb_processor)
# - Лочим (task_id, kind, entity_id)
# - Глобальный IPC-лок: пока выбираю — остальные ждут
# - Вся бизнес-логика, SQL и GPT сохранены

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Tuple

from engine.common.db import get_connection
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt, translate_text
from engine.common.utils import h64_text

# ================= IPC STATE (инициализируется из processor) =================

_IPC_LOCKS = None   # Manager().dict(): (task_id, kind, entity_id) -> ts
_IPC_GUARD = None   # Manager().Lock()
_IPC_TTL_SEC = 900

def init_ipc(*, locks, guard, ttl_sec: int = 900) -> None:
    global _IPC_LOCKS, _IPC_GUARD, _IPC_TTL_SEC
    _IPC_LOCKS = locks
    _IPC_GUARD = guard
    _IPC_TTL_SEC = ttl_sec


def _now() -> float:
    return time.monotonic()


def _cleanup_expired(now: float) -> None:
    if not _IPC_LOCKS:
        return
    dead = [k for k, ts in _IPC_LOCKS.items() if now - ts >= _IPC_TTL_SEC]
    for k in dead:
        _IPC_LOCKS.pop(k, None)


def _reserve(task_id: int, kind: str, entity_ids: List[int], limit: int) -> List[int]:
    """
    IPC-safe reserve:
    - пока выбираем — все ждут
    - TTL чистится тут же
    """
    if not _IPC_LOCKS:
        return entity_ids[:limit]

    reserved: List[int] = []
    now = _now()

    with _IPC_GUARD:
        _cleanup_expired(now)
        for eid in entity_ids:
            key = (int(task_id), kind, int(eid))
            if key in _IPC_LOCKS:
                continue
            _IPC_LOCKS[key] = now
            reserved.append(int(eid))
            if len(reserved) >= limit:
                break

    return reserved


def _release(task_id: int, kind: str, entity_ids: List[int]) -> None:
    if not _IPC_LOCKS:
        return
    with _IPC_GUARD:
        for eid in entity_ids:
            _IPC_LOCKS.pop((int(task_id), kind, int(eid)), None)


# ============================== CONSTANTS ==============================

BATCH_SIZE = 50
MODEL = "mini"
SERVICE_TIER = "flex"
_SELECT_LIMIT = BATCH_SIZE * 6


# ============================== GEO ====================================

def task_prepare_geo() -> Dict[str, Any]:
    tag = "[prepare_geo]"
    reserved_ids: List[int] = []

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_id, hash_task
            FROM __tasks_rating
            WHERE done=false AND type='geo' AND hash_task IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        )
        rr = cur.fetchone()
        if not rr:
            return {"mode": "noop"}

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
            cur.execute(
                "UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s",
                (rating_id,),
            )
            conn.commit()
            return {"mode": "closed", "reason": "task_missing", "task_id": task_id}

        ws_id = str(t[0])
        user_id = int(t[1])
        task_mode = str(t[2] or "")
        main_task = str(t[3] or "")
        geo_task = str(t[4] or "")

        prompt_name = "prepare_geo_buy" if task_mode.strip().lower() == "buy" else "prepare_geo_sell"
        base_instructions = (get_prompt(prompt_name) or "").strip()
        if not base_instructions:
            return {"mode": "error", "reason": "prompt_empty", "task_id": task_id}

        instructions = (
            base_instructions
            + "\n\nTASK (DE):\n" + (translate_text(main_task, "de") or "")
            + "\n\nGEO TASK (DE):\n" + (translate_text(geo_task, "de") or "")
        )

        # ---------- missing ----------
        cur.execute(
            """
            SELECT c.id, c.name
            FROM cities_sys c
            WHERE NOT EXISTS (
                SELECT 1 FROM crawl_tasks ct
                WHERE ct.task_id=%s AND ct.type='city' AND ct.value_id=c.id
            )
            ORDER BY c.id
            LIMIT %s
            """,
            (task_id, _SELECT_LIMIT),
        )
        raw = [{"id": int(r[0]), "name": str(r[1])} for r in cur.fetchall()]
        step = "missing"

        if raw:
            want_ids = [x["id"] for x in raw]
            reserved_ids = _reserve(task_id, "city", want_ids, BATCH_SIZE)
            candidates = [x for x in raw if x["id"] in set(reserved_ids)]
        else:
            # ---------- stale ----------
            step = "stale"
            cur.execute(
                """
                SELECT value_id
                FROM crawl_tasks
                WHERE task_id=%s AND type='city' AND hash_task IS DISTINCT FROM %s
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
                """,
                (task_id, target_hash, _SELECT_LIMIT),
            )
            ids = [int(r[0]) for r in cur.fetchall()]
            reserved_ids = _reserve(task_id, "city", ids, BATCH_SIZE)
            if reserved_ids:
                cur.execute(
                    "SELECT id, name FROM cities_sys WHERE id = ANY(%s)",
                    (reserved_ids,),
                )
                candidates = [{"id": int(r[0]), "name": str(r[1])} for r in cur.fetchall()]
            else:
                candidates = []

        if not candidates:
            return {"mode": "noop", "step": step}

    try:
        payload = json.dumps(candidates, ensure_ascii=False, separators=(",", ":"))
        out = (
            GPTClient()
            .ask(
                model=MODEL,
                service_tier=SERVICE_TIER,
                user_id=str(user_id),
                instructions=instructions,
                input=payload,
                use_cache=False,
            )
            .content
            or ""
        )

        data = json.loads(out)
        rows = [
            (ws_id, user_id, task_id, "city", int(i["id"]), int(i["rate"]), target_hash)
            for i in data
            if int(i["id"]) in set(reserved_ids)
        ]

        if not rows:
            return {"mode": "noop", "step": step}

        with get_connection() as conn2, conn2.cursor() as cur2:
            cur2.executemany(
                """
                INSERT INTO crawl_tasks
                (workspace_id, user_id, task_id, type, value_id, rate, hash_task)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (workspace_id,user_id,task_id,type,value_id)
                DO UPDATE SET
                  rate=EXCLUDED.rate,
                  hash_task=EXCLUDED.hash_task,
                  updated_at=now()
                """,
                rows,
            )
            conn2.commit()

        return {"mode": "ok", "step": step, "written": len(rows)}

    finally:
        _release(task_id, "city", reserved_ids)


# ============================== BRANCHES ================================
# ⬇️ полностью аналогичная логика сохранена ⬇️

def task_prepare_branches() -> Dict[str, Any]:
    tag = "[prepare_branches]"
    reserved_ids: List[int] = []

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_id, hash_task
            FROM __tasks_rating
            WHERE done=false AND type='branches' AND hash_task IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        )
        rr = cur.fetchone()
        if not rr:
            return {"mode": "noop"}

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
            cur.execute(
                "UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s",
                (rating_id,),
            )
            conn.commit()
            return {"mode": "closed", "reason": "task_missing", "task_id": task_id}

        ws_id = str(t[0])
        user_id = int(t[1])
        task_mode = str(t[2] or "")
        main_task = str(t[3] or "")
        branches_task = str(t[4] or "")

        prompt_name = "prepare_branches_buy" if task_mode.strip().lower() == "buy" else "prepare_branches_sell"
        base_instructions = (get_prompt(prompt_name) or "").strip()
        if not base_instructions:
            return {"mode": "error", "reason": "prompt_empty", "task_id": task_id}

        instructions = (
            base_instructions
            + "\n\nTASK (DE):\n" + (translate_text(main_task, "de") or "")
            + "\n\nBRANCHES TASK (DE):\n" + (translate_text(branches_task, "de") or "")
        )

        # ---------- missing ----------
        cur.execute(
            """
            SELECT b.id, b.name
            FROM gb_branches b
            WHERE NOT EXISTS (
                SELECT 1 FROM crawl_tasks ct
                WHERE ct.task_id=%s AND ct.type='branch' AND ct.value_id=b.id
            )
            ORDER BY b.id
            LIMIT %s
            """,
            (task_id, _SELECT_LIMIT),
        )
        raw = [{"id": int(r[0]), "name": str(r[1])} for r in cur.fetchall()]
        step = "missing"

        if raw:
            want_ids = [x["id"] for x in raw]
            reserved_ids = _reserve(task_id, "branch", want_ids, BATCH_SIZE)
            candidates = [x for x in raw if x["id"] in set(reserved_ids)]
        else:
            step = "stale"
            cur.execute(
                """
                SELECT value_id
                FROM crawl_tasks
                WHERE task_id=%s AND type='branch' AND hash_task IS DISTINCT FROM %s
                ORDER BY updated_at ASC, id ASC
                LIMIT %s
                """,
                (task_id, target_hash, _SELECT_LIMIT),
            )
            ids = [int(r[0]) for r in cur.fetchall()]
            reserved_ids = _reserve(task_id, "branch", ids, BATCH_SIZE)
            if reserved_ids:
                cur.execute(
                    "SELECT id, name FROM gb_branches WHERE id = ANY(%s)",
                    (reserved_ids,),
                )
                candidates = [{"id": int(r[0]), "name": str(r[1])} for r in cur.fetchall()]
            else:
                candidates = []

        if not candidates:
            return {"mode": "noop", "step": step}

    try:
        payload = json.dumps(candidates, ensure_ascii=False, separators=(",", ":"))
        out = (
            GPTClient()
            .ask(
                model=MODEL,
                service_tier=SERVICE_TIER,
                user_id=str(user_id),
                instructions=instructions,
                input=payload,
                use_cache=False,
            )
            .content
            or ""
        )

        data = json.loads(out)
        rows = [
            (ws_id, user_id, task_id, "branch", int(i["id"]), int(i["rate"]), target_hash)
            for i in data
            if int(i["id"]) in set(reserved_ids)
        ]

        if not rows:
            return {"mode": "noop", "step": step}

        with get_connection() as conn2, conn2.cursor() as cur2:
            cur2.executemany(
                """
                INSERT INTO crawl_tasks
                (workspace_id, user_id, task_id, type, value_id, rate, hash_task)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (workspace_id,user_id,task_id,type,value_id)
                DO UPDATE SET
                  rate=EXCLUDED.rate,
                  hash_task=EXCLUDED.hash_task,
                  updated_at=now()
                """,
                rows,
            )
            conn2.commit()

        return {"mode": "ok", "step": step, "written": len(rows)}

    finally:
        _release(task_id, "branch", reserved_ids)


# ============================== DONE ====================================

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
                    cur.execute(
                        "UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s",
                        (rating_id,),
                    )
                    closed_stale += 1
                    continue

                real_hash = h64_text(str(t[0] or "") + str(t[1] or ""))
                if int(real_hash) != int(target_hash):
                    cur.execute(
                        "UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s",
                        (rating_id,),
                    )
                    closed_stale += 1
                    continue

                cur.execute(
                    """
                    SELECT COUNT(*) FROM cities_sys c
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
                    SELECT COUNT(*) FROM crawl_tasks
                    WHERE task_id=%s AND type='city' AND hash_task IS DISTINCT FROM %s
                    """,
                    (task_id, target_hash),
                )
                stale_cnt = int(cur.fetchone()[0] or 0)

                if missing_cnt == 0 and stale_cnt == 0:
                    cur.execute(
                        "UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s",
                        (rating_id,),
                    )
                    closed_ready += 1

            else:
                cur.execute(
                    "SELECT task, task_branches FROM aap_audience_audiencetask WHERE id=%s LIMIT 1",
                    (task_id,),
                )
                t = cur.fetchone()
                if not t:
                    cur.execute(
                        "UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s",
                        (rating_id,),
                    )
                    closed_stale += 1
                    continue

                real_hash = h64_text(str(t[0] or "") + str(t[1] or ""))
                if int(real_hash) != int(target_hash):
                    cur.execute(
                        "UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s",
                        (rating_id,),
                    )
                    closed_stale += 1
                    continue

                cur.execute(
                    """
                    SELECT COUNT(*) FROM gb_branches b
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
                    SELECT COUNT(*) FROM crawl_tasks
                    WHERE task_id=%s AND type='branch' AND hash_task IS DISTINCT FROM %s
                    """,
                    (task_id, target_hash),
                )
                stale_cnt = int(cur.fetchone()[0] or 0)

                if missing_cnt == 0 and stale_cnt == 0:
                    cur.execute(
                        "UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s",
                        (rating_id,),
                    )
                    closed_ready += 1

        conn.commit()

    return {
        "processed": processed,
        "closed_stale": closed_stale,
        "closed_ready": closed_ready,
    }
