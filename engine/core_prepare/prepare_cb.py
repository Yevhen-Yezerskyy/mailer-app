# FILE: engine/core_prepare/prepare_cb.py  (обновлено — 2025-12-31)
# Смысл: расширили данные города, которые кормятся в GPT (state/area/pop/urban/travel); остальная логика файла не тронута.

from __future__ import annotations

import json
import pickle
import random
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from engine.common.cache.client import CLIENT
from engine.common.db import get_connection
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt, translate_text, denormalize_branches_prompt
from engine.common.utils import h64_text


# ------------------------------
BATCH_SIZE = 20
MODEL = "maxi"
SERVICE_TIER = "flex"

MAX_CANDIDATES = 2000           # если кандидатов больше — считаем аномалией и валимся
TASKS_QUEUE_LIMIT = 500         # сколько rating_id держим в очереди (newest-first)
DONE_SCAN_LIMIT = 200           # сколько rating записей прогоняем в task_prepare_done за тик

QUEUE_TTL_SEC = 60 * 60         # best-effort
LOCK_TTL_SEC = 60.0
LOCK_RETRY_SLEEP_SEC = 0.10
ENTITY_LOCK_TTL_SEC = 900

DO_PROB = 0.70                  # если entity-очередь не пустая: 70% берём батч, 30% rotate на следующий task
# ------------------------------


def _ts() -> str:
    return time.strftime("%H:%M:%S")


def _p(kind: str, msg: str) -> None:
    print(f"{_ts()} [prepare:{kind}] {msg}")


def _k_q_tasks(kind: str) -> str:
    return f"prep:{kind}:tasks:q"


def _k_lock(kind: str) -> str:
    return f"prep:{kind}:lock"


def _k_q_entities(kind: str, rating_id: int) -> str:
    return f"prep:{kind}:entities:q:{int(rating_id)}"


def _k_entity_lock(kind: str, task_id: int, entity_id: int) -> str:
    return f"prep:{kind}:eid:{int(task_id)}:{int(entity_id)}"


def _cache_get_list(key: str) -> List[Any]:
    payload = CLIENT.get(key, ttl_sec=QUEUE_TTL_SEC)
    if not payload:
        return []
    try:
        obj = pickle.loads(payload)
        return obj if isinstance(obj, list) else []
    except Exception:
        return []


def _cache_set_list(key: str, items: Sequence[Any]) -> None:
    payload = pickle.dumps(list(items), protocol=pickle.HIGHEST_PROTOCOL)
    CLIENT.set(key, payload, ttl_sec=QUEUE_TTL_SEC)


def _lock_acquire(kind: str, owner: str) -> str:
    key = _k_lock(kind)
    while True:
        resp = CLIENT.lock_try(key, ttl_sec=LOCK_TTL_SEC, owner=owner)
        if resp and resp.get("acquired") is True and isinstance(resp.get("token"), str):
            return resp["token"]
        time.sleep(LOCK_RETRY_SLEEP_SEC)


def _lock_release(kind: str, token: str) -> None:
    CLIENT.lock_release(_k_lock(kind), token=token)


def _rotate_tasks(q: List[int]) -> List[int]:
    if len(q) <= 1:
        return q
    return q[1:] + [q[0]]


def _db_build_tasks_queue(kind: str, limit: int = TASKS_QUEUE_LIMIT) -> List[int]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM __tasks_rating
            WHERE done=false
              AND type=%s
              AND hash_task IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            (kind, int(limit)),
        )
        return [int(x[0]) for x in cur.fetchall()]


def _ensure_tasks_queue(kind: str) -> List[int]:
    qk = _k_q_tasks(kind)
    q = _cache_get_list(qk)
    q = [int(x) for x in q if isinstance(x, int)]
    if q:
        return q
    q = _db_build_tasks_queue(kind)
    _cache_set_list(qk, q)
    return q


def _db_rating_is_alive(rating_id: int, kind: str) -> Optional[Tuple[int, int, int]]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_id, hash_task
            FROM __tasks_rating
            WHERE id=%s
              AND type=%s
              AND done=false
              AND hash_task IS NOT NULL
            LIMIT 1
            """,
            (int(rating_id), kind),
        )
        r = cur.fetchone()
        if not r:
            return None
        return int(r[0]), int(r[1]), int(r[2])


def _reserve_entities(
    kind: str,
    task_id: int,
    ids: List[int],
    limit: int,
) -> Tuple[List[int], List[Tuple[str, str]]]:
    reserved: List[int] = []
    tokens: List[Tuple[str, str]] = []
    owner = f"prep:{kind}:{int(time.time())}"

    for eid in ids:
        if len(reserved) >= limit:
            break
        lock_key = _k_entity_lock(kind, task_id, int(eid))
        resp = CLIENT.lock_try(lock_key, ttl_sec=ENTITY_LOCK_TTL_SEC, owner=owner)
        if not resp or resp.get("acquired") is not True or not isinstance(resp.get("token"), str):
            continue
        reserved.append(int(eid))
        tokens.append((lock_key, resp["token"]))

    return reserved, tokens


def _release_entity_tokens(tokens: List[Tuple[str, str]]) -> None:
    for lock_key, token in tokens:
        try:
            CLIENT.lock_release(lock_key, token=token)
        except Exception:
            pass


def _rotate_head(kind: str) -> None:
    owner = f"prep:{kind}:rot:{int(time.time())}"
    token = _lock_acquire(kind, owner=owner)
    try:
        qk = _k_q_tasks(kind)
        q = _cache_get_list(qk)
        q = [int(x) for x in q if isinstance(x, int)]
        q = _rotate_tasks(q)
        _cache_set_list(qk, q)
    finally:
        _lock_release(kind, token=token)


def _fill_entities(kind: str, rating_id: int, ids: List[int]) -> None:
    owner = f"prep:{kind}:fill:{int(time.time())}"
    token = _lock_acquire(kind, owner=owner)
    try:
        ek = _k_q_entities(kind, rating_id)
        _cache_set_list(ek, [int(x) for x in ids])
    finally:
        _lock_release(kind, token=token)


def _guard_all(rows: List[Any], what: str) -> None:
    if len(rows) > MAX_CANDIDATES:
        raise RuntimeError(f"too_many_{what}: {len(rows)} > {MAX_CANDIDATES}")


def _select_candidates_geo(task_id: int, target_hash: int) -> Tuple[str, List[Dict[str, Any]]]:
    with get_connection() as conn, conn.cursor() as cur:
        # missing: ВСЕ (guard > 2000)
        cur.execute(
            """
            SELECT c.id, c.name
            FROM cities_sys c
            WHERE NOT EXISTS (
                SELECT 1
                FROM crawl_tasks ct
                WHERE ct.task_id=%s AND ct.type='city' AND ct.value_id=c.id
            )
            ORDER BY random()
            LIMIT %s
            """,
            (task_id, MAX_CANDIDATES + 1),
        )
        rows = cur.fetchall()
        _guard_all(rows, "geo_missing")
        raw = [{"id": int(r[0]), "name": str(r[1])} for r in rows]
        if raw:
            return "missing", raw

        # stale: ВСЕ (guard > 2000)
        cur.execute(
            """
            SELECT value_id
            FROM crawl_tasks
            WHERE task_id=%s AND type='city' AND hash_task IS DISTINCT FROM %s
            ORDER BY updated_at ASC, id ASC
            LIMIT %s
            """,
            (task_id, target_hash, MAX_CANDIDATES + 1),
        )
        ids_rows = cur.fetchall()
        _guard_all(ids_rows, "geo_stale")
        ids = [int(r[0]) for r in ids_rows]
        if not ids:
            return "stale", []
        cur.execute("SELECT id, name FROM cities_sys WHERE id = ANY(%s)", (ids,))
        rows2 = cur.fetchall()
        raw2 = [{"id": int(r[0]), "name": str(r[1])} for r in rows2]
        return "stale", raw2


def _select_candidates_branches(task_id: int, target_hash: int) -> Tuple[str, List[Dict[str, Any]]]:
    with get_connection() as conn, conn.cursor() as cur:
        # missing: ВСЕ (guard > 2000)
        cur.execute(
            """
            SELECT b.id, b.name
            FROM gb_branches b
            WHERE NOT EXISTS (
                SELECT 1
                FROM crawl_tasks ct
                WHERE ct.task_id=%s AND ct.type='branch' AND ct.value_id=b.id
            )
            ORDER BY random()
            LIMIT %s
            """,
            (task_id, MAX_CANDIDATES + 1),
        )
        rows = cur.fetchall()
        _guard_all(rows, "branches_missing")
        raw = [{"id": int(r[0]), "name": str(r[1])} for r in rows]
        if raw:
            return "missing", raw

        # stale: ВСЕ (guard > 2000)
        cur.execute(
            """
            SELECT value_id
            FROM crawl_tasks
            WHERE task_id=%s AND type='branch' AND hash_task IS DISTINCT FROM %s
            ORDER BY updated_at ASC, id ASC
            LIMIT %s
            """,
            (task_id, target_hash, MAX_CANDIDATES + 1),
        )
        ids_rows = cur.fetchall()
        _guard_all(ids_rows, "branches_stale")
        ids = [int(r[0]) for r in ids_rows]
        if not ids:
            return "stale", []
        cur.execute("SELECT id, name FROM gb_branches WHERE id = ANY(%s)", (ids,))
        rows2 = cur.fetchall()
        raw2 = [{"id": int(r[0]), "name": str(r[1])} for r in rows2]
        return "stale", raw2


def _pop_batch(kind: str) -> Dict[str, Any]:
    """
    Возвращает:
    - work: {"mode":"work", rating_id, task_id, target_hash, ids}
    - need_fill: {"mode":"need_fill", rating_id, task_id, target_hash, eq_len}
    - noop
    """
    owner = f"prep:{kind}:proc:{int(time.time())}"
    token = _lock_acquire(kind, owner=owner)
    try:
        qk = _k_q_tasks(kind)
        q = _ensure_tasks_queue(kind)
        if not q:
            return {"mode": "noop"}

        tries = len(q)

        while tries > 0 and q:
            rating_id = int(q[0])

            alive = _db_rating_is_alive(rating_id, kind)
            if not alive:
                _p(kind, f"DROP rating_id={rating_id} (dead/done) -> rotate")
                q = q[1:]
                _cache_set_list(qk, q)
                tries -= 1
                continue

            _, task_id, target_hash = alive

            ek = _k_q_entities(kind, rating_id)
            eq = _cache_get_list(ek)
            eq = [int(x) for x in eq if isinstance(x, int)]

            if eq:
                if random.random() <= DO_PROB:
                    take = eq[:BATCH_SIZE]
                    rest = eq[BATCH_SIZE:]
                    _cache_set_list(ek, rest)
                    _p(kind, f"HEAD rating_id={rating_id} task_id={task_id} eq={len(eq)} -> DO take={len(take)} rest={len(rest)}")
                    return {
                        "mode": "work",
                        "rating_id": rating_id,
                        "task_id": task_id,
                        "target_hash": target_hash,
                        "ids": take,
                    }

                _p(kind, f"HEAD rating_id={rating_id} task_id={task_id} eq={len(eq)} -> JUMP")
                q = _rotate_tasks(q)
                _cache_set_list(qk, q)
                tries -= 1
                continue

            _p(kind, f"HEAD rating_id={rating_id} task_id={task_id} eq=0 -> NEED_FILL")
            return {
                "mode": "need_fill",
                "rating_id": rating_id,
                "task_id": task_id,
                "target_hash": target_hash,
                "eq_len": 0,
            }

        return {"mode": "noop"}

    finally:
        _lock_release(kind, token=token)


def _load_audience_task_for_geo(task_id: int) -> Optional[Tuple[str, int, str, str, str]]:
    with get_connection() as conn, conn.cursor() as cur:
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
            return None
        return str(t[0]), int(t[1]), str(t[2] or ""), str(t[3] or ""), str(t[4] or "")


def _load_audience_task_for_branches(task_id: int) -> Optional[Tuple[str, int, str, str, str]]:
    with get_connection() as conn, conn.cursor() as cur:
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
            return None
        return str(t[0]), int(t[1]), str(t[2] or ""), str(t[3] or ""), str(t[4] or "")


def _close_rating_done(rating_id: int) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (int(rating_id),))
        conn.commit()


# ============================== GEO ====================================

def task_prepare_geo() -> Dict[str, Any]:
    kind = "geo"

    try:
        st = _pop_batch(kind)
        if st.get("mode") == "noop":
            return {"mode": "noop"}

        if st.get("mode") == "need_fill":
            rating_id = int(st["rating_id"])
            task_id = int(st["task_id"])
            target_hash = int(st["target_hash"])

            step, raw = _select_candidates_geo(task_id, target_hash)
            if not raw:
                _p(kind, f"FILL rating_id={rating_id} task_id={task_id} step={step} -> EMPTY -> ROTATE")
                _rotate_head(kind)
                return {"mode": "noop", "step": step}

            ids = [int(x["id"]) for x in raw]
            _p(kind, f"FILL rating_id={rating_id} task_id={task_id} step={step} -> PUT ids={len(ids)}")
            _fill_entities(kind, rating_id, ids)
            return {"mode": "filled", "step": step, "queued": len(ids)}

        # work
        rating_id = int(st["rating_id"])
        task_id = int(st["task_id"])
        target_hash = int(st["target_hash"])
        ids = [int(x) for x in st["ids"]]

        t = _load_audience_task_for_geo(task_id)
        if not t:
            _p(kind, f"CLOSE rating_id={rating_id} task_id={task_id} reason=task_missing")
            _close_rating_done(rating_id)
            return {"mode": "closed", "reason": "task_missing", "task_id": task_id}

        ws_id, user_id, task_mode, main_task, geo_task = t

        prompt_name = "prepare_geo_buy" if task_mode.strip().lower() == "buy" else "prepare_geo_sell"
        base_instructions = (get_prompt(prompt_name) or "").strip()
        if not base_instructions:
            _p(kind, f"ERROR task_id={task_id} reason=prompt_empty")
            return {"mode": "error", "reason": "prompt_empty", "task_id": task_id}

        instructions = (
            base_instructions
            + "\n\nTASK (DE):\n" + (translate_text(main_task, "de") or "")
            + "\n\nGEO TASK (DE):\n" + (translate_text(geo_task, "de") or "")
        )

        reserved_ids, lock_tokens = _reserve_entities(kind, task_id, ids, BATCH_SIZE)
        try:
            _p(kind, f"RESERVE rating_id={rating_id} task_id={task_id} want={len(ids)} got={len(reserved_ids)}")
            if not reserved_ids:
                _p(kind, f"NOOP rating_id={rating_id} task_id={task_id} reason=locked_out")
                return {"mode": "noop", "step": "locked_out"}

            # === единственное изменение: расширили выборку и формат cities_sys -> candidates для GPT ===
            with get_connection() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      id,
                      state_name,
                      name,
                      area_km2,
                      pop_total,
                      urban_code,
                      urban_name,
                      travel_code,
                      travel_name
                    FROM cities_sys
                    WHERE id = ANY(%s)
                    """,
                    (reserved_ids,),
                )
                candidates: List[Dict[str, Any]] = []
                for r in cur.fetchall():
                    candidates.append(
                        {
                            "id": int(r[0]),
                            "state_name": str(r[1] or ""),
                            "name": str(r[2] or ""),
                            "area_km2": (float(r[3]) if r[3] is not None else None),
                            "pop_total": (int(r[4]) if r[4] is not None else None),
                            "urban_code": str(r[5] or ""),
                            "urban_name": str(r[6] or ""),
                            "travel_code": str(r[7] or ""),
                            "travel_name": str(r[8] or ""),
                        }
                    )
            # === /изменение ===

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

            allowed = set(reserved_ids)
            rows = [
                (ws_id, user_id, task_id, "city", int(i["id"]), int(i["rate"]), target_hash)
                for i in data
                if int(i["id"]) in allowed
            ]

            if not rows:
                _p(kind, f"NOOP rating_id={rating_id} task_id={task_id} reason=gpt_empty")
                return {"mode": "noop", "step": "gpt_empty"}

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

            _p(kind, f"DB_OK rating_id={rating_id} task_id={task_id} written={len(rows)}")
            return {"mode": "ok", "written": len(rows)}

        except Exception as e:
            _p(kind, f"FAIL rating_id={rating_id} task_id={task_id} err={e!r}")
            return {"mode": "error", "reason": "exception", "err": repr(e)}

        finally:
            _release_entity_tokens(lock_tokens)

    except Exception as e:
        _p(kind, f"FATAL err={e!r}")
        return {"mode": "error", "reason": "fatal", "err": repr(e)}


# ============================== BRANCHES ================================

def task_prepare_branches() -> Dict[str, Any]:
    kind = "branches"

    try:
        st = _pop_batch(kind)
        if st.get("mode") == "noop":
            return {"mode": "noop"}

        if st.get("mode") == "need_fill":
            rating_id = int(st["rating_id"])
            task_id = int(st["task_id"])
            target_hash = int(st["target_hash"])

            step, raw = _select_candidates_branches(task_id, target_hash)
            if not raw:
                _p(kind, f"FILL rating_id={rating_id} task_id={task_id} step={step} -> EMPTY -> ROTATE")
                _rotate_head(kind)
                return {"mode": "noop", "step": step}

            ids = [int(x["id"]) for x in raw]
            _p(kind, f"FILL rating_id={rating_id} task_id={task_id} step={step} -> PUT ids={len(ids)}")
            _fill_entities(kind, rating_id, ids)
            return {"mode": "filled", "step": step, "queued": len(ids)}

        # work
        rating_id = int(st["rating_id"])
        task_id = int(st["task_id"])
        target_hash = int(st["target_hash"])
        ids = [int(x) for x in st["ids"]]

        t = _load_audience_task_for_branches(task_id)
        if not t:
            _p(kind, f"CLOSE rating_id={rating_id} task_id={task_id} reason=task_missing")
            _close_rating_done(rating_id)
            return {"mode": "closed", "reason": "task_missing", "task_id": task_id}

        ws_id, user_id, task_mode, main_task, branches_task = t

        prompt_name = "prepare_branches_buy" if task_mode.strip().lower() == "buy" else "prepare_branches_sell"
        base_instructions = (get_prompt(prompt_name) or "").strip()
        if not base_instructions:
            _p(kind, f"ERROR task_id={task_id} reason=prompt_empty")
            return {"mode": "error", "reason": "prompt_empty", "task_id": task_id}

        instructions = (
            base_instructions
            + (translate_text(main_task, "de") or "") + (translate_text(branches_task, "de") or "")
        )


        reserved_ids, lock_tokens = _reserve_entities(kind, task_id, ids, BATCH_SIZE)
        try:
            _p(kind, f"RESERVE rating_id={rating_id} task_id={task_id} want={len(ids)} got={len(reserved_ids)}")
            if not reserved_ids:
                _p(kind, f"NOOP rating_id={rating_id} task_id={task_id} reason=locked_out")
                return {"mode": "noop", "step": "locked_out"}

            with get_connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT id, name FROM gb_branches WHERE id = ANY(%s)", (reserved_ids,))
                candidates = [{"id": int(r[0]), "name": str(r[1])} for r in cur.fetchall()]

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

            allowed = set(reserved_ids)
            rows = [
                (ws_id, user_id, task_id, "branch", int(i["id"]), int(i["rate"]), target_hash)
                for i in data
                if int(i["id"]) in allowed
            ]

            if not rows:
                _p(kind, f"NOOP rating_id={rating_id} task_id={task_id} reason=gpt_empty")
                return {"mode": "noop", "step": "gpt_empty"}

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

            _p(kind, f"DB_OK rating_id={rating_id} task_id={task_id} written={len(rows)}")
            return {"mode": "ok", "written": len(rows)}

        except Exception as e:
            _p(kind, f"FAIL rating_id={rating_id} task_id={task_id} err={e!r}")
            return {"mode": "error", "reason": "exception", "err": repr(e)}

        finally:
            _release_entity_tokens(lock_tokens)

    except Exception as e:
        _p(kind, f"FATAL err={e!r}")
        return {"mode": "error", "reason": "fatal", "err": repr(e)}


# ============================== DONE ===================================

def task_prepare_done() -> Dict[str, Any]:
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
            LIMIT %s
            """,
            (int(DONE_SCAN_LIMIT),),
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
                    cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (rating_id,))
                    closed_ready += 1

            else:
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
                    cur.execute("UPDATE __tasks_rating SET done=true, updated_at=now() WHERE id=%s", (rating_id,))
                    closed_ready += 1

        conn.commit()

    return {"processed": processed, "closed_stale": closed_stale, "closed_ready": closed_ready}


# ============================== RESET (каждые 2 минуты) ==============================

def reset_prepare_queues() -> Dict[str, Any]:
    _cache_set_list(_k_q_tasks("geo"), [])
    _cache_set_list(_k_q_tasks("branches"), [])
    _p("reset", "RESET tasks queues (geo, branches)")
    return {"mode": "reset"}
