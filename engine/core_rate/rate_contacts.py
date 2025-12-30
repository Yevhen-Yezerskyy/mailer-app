# FILE: engine/core_rate/rate_contacts.py  (обновлено — 2025-12-30)
# Смысл:
# - Рейтингование контактов (raw_contacts_aggr) для AudienceTask по двум типам __tasks_rating:
#   - contacts: добираем контакты без rate_cl или без валидного hash_task
#   - contacts_update: переоценка только тех, у кого rate_cl есть и hash_task валиден, но != target_hash
# - Очереди/локи — как в prepare_cb (lock только на POP/FILL кеш-очередей; GPT/DB без лока).
# - GPT батч: фиксированный BATCH_SIZE (как в prepare_cb), модель/тир — тоже как в prepare_cb.
# - Done:
#   - contacts: done когда rated_cnt >= subscribers_limit + BATCH_SIZE
#   - contacts_update: done когда больше нет stale по критериям contacts_update
# - Guard (NEW):
#   - contacts: перед "DO take" вероятностно ограничиваем параллельные батчи около лимита,
#     чтобы перелёт был ~1–2 батча, а не N батчей.

from __future__ import annotations

import json
import pickle
import random
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from engine.common.cache.client import CLIENT
from engine.common.db import get_connection
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt, translate_text
from engine.common.utils import h64_text

# ------------------------------
BATCH_SIZE = 20  # как в prepare_cb
MODEL = "maxi"   # как в prepare_cb
SERVICE_TIER = "flex"

TASKS_QUEUE_LIMIT = 500
DONE_SCAN_LIMIT = 200
GUARD_MAX_PARALLEL = 10  # как Worker.max_parallel в rate_contacts_processor.py

QUEUE_TTL_SEC = 60 * 60  # best-effort
LOCK_TTL_SEC = 60.0
LOCK_RETRY_SLEEP_SEC = 0.10

DO_PROB = 0.70  # если entity-очередь не пустая: 70% берём батч, 30% rotate на следующий rating
MAX_FILL = 1000
# ------------------------------


def _ts() -> str:
    return time.strftime("%H:%M:%S")


def _p(msg: str) -> None:
    print(f"{_ts()} [rate:contacts] {msg}")


def _k_q_tasks() -> str:
    return "prep:contacts:tasks:q"


def _k_lock() -> str:
    return "prep:contacts:lock"


def _k_q_entities(rating_id: int) -> str:
    return f"prep:contacts:entities:q:{int(rating_id)}"


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


def _lock_acquire(owner: str) -> str:
    key = _k_lock()
    while True:
        resp = CLIENT.lock_try(key, ttl_sec=LOCK_TTL_SEC, owner=owner)
        if resp and resp.get("acquired") is True and isinstance(resp.get("token"), str):
            return resp["token"]
        time.sleep(LOCK_RETRY_SLEEP_SEC)


def _lock_release(token: str) -> None:
    CLIENT.lock_release(_k_lock(), token=token)


def _rotate_tasks(q: List[int]) -> List[int]:
    if len(q) <= 1:
        return q
    return q[1:] + [q[0]]


def _hash_is_valid(v: Any) -> bool:
    # "мусор" считаем: None, 0, 1, -1 (остальное допускаем, включая отрицательные)
    try:
        x = int(v)
    except Exception:
        return False
    return x not in (0, 1, -1)


def _db_build_tasks_queue(limit: int = TASKS_QUEUE_LIMIT) -> List[int]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM __tasks_rating
            WHERE done=false
              AND type IN ('contacts','contacts_update')
              AND hash_task IS NOT NULL
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            (int(limit),),
        )
        return [int(x[0]) for x in cur.fetchall()]


def _ensure_tasks_queue() -> List[int]:
    qk = _k_q_tasks()
    q = _cache_get_list(qk)
    q = [int(x) for x in q if isinstance(x, int)]
    if q:
        return q
    q = _db_build_tasks_queue()
    _cache_set_list(qk, q)
    return q


def _db_rating_is_alive(rating_id: int) -> Optional[Tuple[int, int, str, int]]:
    """
    return: (rating_id, task_id, rtype, target_hash)
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_id, type, hash_task
            FROM __tasks_rating
            WHERE id=%s
              AND done=false
              AND type IN ('contacts','contacts_update')
              AND hash_task IS NOT NULL
            LIMIT 1
            """,
            (int(rating_id),),
        )
        r = cur.fetchone()
        if not r:
            return None
        return int(r[0]), int(r[1]), str(r[2]), int(r[3])


def _close_rating_done(rating_id: int) -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE __tasks_rating
            SET done=true, updated_at=now()
            WHERE id=%s
            """,
            (int(rating_id),),
        )
        conn.commit()


def _load_audience_task(task_id: int) -> Optional[Tuple[int, str, str, str, int]]:
    """
    return: (user_id, task_mode(buy/sell), task, task_client, subscribers_limit)
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT user_id::int, type::text, task::text, task_client::text, subscribers_limit::int
            FROM public.aap_audience_audiencetask
            WHERE id=%s
            LIMIT 1
            """,
            (int(task_id),),
        )
        r = cur.fetchone()
        if not r:
            return None
        return int(r[0]), str(r[1]), str(r[2]), str(r[3]), int(r[4])


def _rated_cnt(task_id: int) -> int:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM public.rate_contacts
            WHERE task_id=%s
              AND rate_cl IS NOT NULL
              AND hash_task IS NOT NULL
              AND hash_task NOT IN (-1,0,1)
            """,
            (int(task_id),),
        )
        r = cur.fetchone()
        return int(r[0] or 0)


def _guard_allow_contacts_batch(*, task_id: int, subs_limit: int) -> bool:
    """
    Предохранитель от перелёта лимита для rtype='contacts' при параллельных once.
    Правило:
    - если remaining > BATCH_SIZE * GUARD_MAX_PARALLEL -> allow
    - иначе allow с вероятностью remaining/(BATCH_SIZE*GUARD_MAX_PARALLEL)
    """
    rc = _rated_cnt(int(task_id))
    threshold = int(subs_limit) + int(BATCH_SIZE)
    remaining = int(threshold) - int(rc)
    if remaining <= 0:
        return False

    safe_window = int(BATCH_SIZE) * int(GUARD_MAX_PARALLEL)
    if remaining > safe_window:
        return True

    p = remaining / float(safe_window)
    return random.random() < p


def _has_stale_for_update(task_id: int, target_hash: int) -> bool:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM public.rate_contacts
            WHERE task_id=%s
              AND rate_cl IS NOT NULL
              AND hash_task IS NOT NULL
              AND hash_task NOT IN (-1,0,1)
              AND hash_task IS DISTINCT FROM %s
            LIMIT 1
            """,
            (int(task_id), int(target_hash)),
        )
        return cur.fetchone() is not None


def _select_candidates_contacts(task_id: int, limit: int = MAX_FILL) -> List[int]:
    # contacts: берем только тех, у кого нет рейтинга ИЛИ hash_task невалиден/пустой (NULL тоже сюда)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT contact_id
            FROM public.rate_contacts
            WHERE task_id=%s
              AND (
                   rate_cl IS NULL
                OR hash_task IS NULL
                OR hash_task IN (-1,0,1)
              )
            ORDER BY rate_cb ASC NULLS LAST, contact_id ASC
            LIMIT %s
            """,
            (int(task_id), int(limit)),
        )
        return [int(r[0]) for r in cur.fetchall()]


def _select_candidates_contacts_update(task_id: int, target_hash: int, limit: int = MAX_FILL) -> List[int]:
    # contacts_update: только те, у кого rate_cl есть и hash_task валиден, но != target_hash
    # NULL сюда НЕ попадает.
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT contact_id
            FROM public.rate_contacts
            WHERE task_id=%s
              AND rate_cl IS NOT NULL
              AND hash_task IS NOT NULL
              AND hash_task NOT IN (-1,0,1)
              AND hash_task IS DISTINCT FROM %s
            ORDER BY rate_cb ASC NULLS LAST, contact_id ASC
            LIMIT %s
            """,
            (int(task_id), int(target_hash), int(limit)),
        )
        return [int(r[0]) for r in cur.fetchall()]


def _fill_entities(rating_id: int, ids: List[int]) -> None:
    owner = f"prep:contacts:fill:{int(time.time())}"
    token = _lock_acquire(owner=owner)
    try:
        ek = _k_q_entities(rating_id)
        _cache_set_list(ek, [int(x) for x in ids])
    finally:
        _lock_release(token=token)


def _pop_batch() -> Dict[str, Any]:
    """
    Возвращает:
    - work: {"mode":"work", rating_id, task_id, rtype, target_hash, ids}
    - need_fill: {"mode":"need_fill", rating_id, task_id, rtype, target_hash}
    - noop
    """
    owner = f"prep:contacts:proc:{int(time.time())}"
    token = _lock_acquire(owner=owner)
    try:
        qk = _k_q_tasks()
        q = _ensure_tasks_queue()
        if not q:
            return {"mode": "noop"}

        tries = len(q)

        while tries > 0 and q:
            rating_id = int(q[0])

            alive = _db_rating_is_alive(rating_id)
            if not alive:
                _p(f"DROP rating_id={rating_id} (dead/done) -> rotate")
                q = q[1:]
                _cache_set_list(qk, q)
                tries -= 1
                continue

            _, task_id, rtype, target_hash = alive

            ek = _k_q_entities(rating_id)
            eq = _cache_get_list(ek)
            eq = [int(x) for x in eq if isinstance(x, int)]

            if eq:
                # --- GUARD: contacts (вероятностный предохранитель около лимита) ---
                if str(rtype) == "contacts":
                    t = _load_audience_task(int(task_id))
                    if t:
                        _, _, _, _, subs_limit = t
                        if not _guard_allow_contacts_batch(task_id=int(task_id), subs_limit=int(subs_limit)):
                            rc = _rated_cnt(int(task_id))
                            threshold = int(subs_limit) + int(BATCH_SIZE)
                            _p(
                                f"GUARD_SKIP rating_id={rating_id} task_id={task_id} "
                                f"type=contacts rated_cnt={rc} threshold={threshold} eq={len(eq)} -> JUMP"
                            )
                            q = _rotate_tasks(q)
                            _cache_set_list(qk, q)
                            tries -= 1
                            continue

                if random.random() <= DO_PROB:
                    take = eq[:BATCH_SIZE]
                    rest = eq[BATCH_SIZE:]
                    _cache_set_list(ek, rest)
                    _p(f"HEAD rating_id={rating_id} task_id={task_id} type={rtype} eq={len(eq)} -> DO take={len(take)} rest={len(rest)}")
                    return {
                        "mode": "work",
                        "rating_id": rating_id,
                        "task_id": task_id,
                        "rtype": rtype,
                        "target_hash": target_hash,
                        "ids": take,
                    }

                _p(f"HEAD rating_id={rating_id} task_id={task_id} type={rtype} eq={len(eq)} -> JUMP")
                q = _rotate_tasks(q)
                _cache_set_list(qk, q)
                tries -= 1
                continue

            _p(f"HEAD rating_id={rating_id} task_id={task_id} type={rtype} eq=0 -> NEED_FILL")
            return {
                "mode": "need_fill",
                "rating_id": rating_id,
                "task_id": task_id,
                "rtype": rtype,
                "target_hash": target_hash,
            }

        return {"mode": "noop"}

    finally:
        _lock_release(token=token)


def _trim(s: Any) -> Optional[str]:
    if s is None:
        return None
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    return s or None


def _drop_empty(v: Any) -> Any:
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
    # как в старом core_rate/rate_contacts.py: выкидываем поля которые нам не нужны
    if not isinstance(norm, dict):
        return {}
    norm2 = dict(norm)
    for k in ("source_urls", "city", "plz", "email", "fax"):
        norm2.pop(k, None)
    cleaned = _drop_empty(norm2)
    return cleaned if isinstance(cleaned, dict) else {}


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


def _build_instructions(task_mode: str, task: str, task_client: str) -> Optional[str]:
    prompt_name = "rate_contacts_buy" if task_mode.strip().lower() == "buy" else "rate_contacts_sell"
    base = (get_prompt(prompt_name) or "").strip()
    if not base:
        return None

    # как договорились в prepare_cb: task/task_client переводим на DE и кладём в instructions (НЕ в input)
    task_de = translate_text(task, "de") or ""
    client_de = translate_text(task_client, "de") or ""

    return (
        base
        + "\n\nTASK (DE):\n" + task_de.strip()
        + "\n\nCLIENT (DE):\n" + client_de.strip()
    ).strip()


def _fetch_contacts_payload(ids: List[int]) -> List[Dict[str, Any]]:
    if not ids:
        return []

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, company_data
            FROM public.raw_contacts_aggr
            WHERE id = ANY(%s)
            """,
            (ids,),
        )
        rows = cur.fetchall() or []

    by_id: Dict[int, Dict[str, Any]] = {}
    for cid, company_data in rows:
        cid_i = int(cid)
        cd = company_data or {}
        if not isinstance(cd, dict):
            cd = {}
        norm = cd.get("norm") or {}
        if not isinstance(norm, dict):
            norm = {}
        by_id[cid_i] = {"id": cid_i, "norm": _clean_norm(norm)}

    # сохранить порядок ids (чтобы "allowed" совпадало и для дебага было понятно)
    out: List[Dict[str, Any]] = []
    for cid in ids:
        item = by_id.get(int(cid))
        if not item:
            # если в raw_contacts_aggr нет строки — пропускаем (это ок)
            continue
        out.append(item)

    return out


def _write_rates(task_id: int, target_hash: int, items_sent: List[Dict[str, Any]], gpt_data: Dict[str, Any]) -> Tuple[int, int]:
    """
    return: (written, bad)
    """
    by_id = _items_by_id(gpt_data)
    allowed = {int(it["id"]) for it in items_sent if isinstance(it, dict) and "id" in it}

    write_rows: List[Tuple[int, int, int, int]] = []
    bad = 0

    for cid in allowed:
        g = by_id.get(int(cid))
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
        write_rows.append((int(task_id), int(cid), int(rate), int(target_hash)))

    if not write_rows:
        return 0, bad

    with get_connection() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO public.rate_contacts (task_id, contact_id, rate_cl, hash_task, created_at, updated_at)
            VALUES (%s, %s, %s, %s, now(), now())
            ON CONFLICT (task_id, contact_id) DO UPDATE
            SET
                rate_cl = EXCLUDED.rate_cl,
                hash_task = EXCLUDED.hash_task,
                updated_at = now()
            """,
            write_rows,
        )
        conn.commit()

    return len(write_rows), bad


def task_rate_contacts_once() -> Dict[str, Any]:
    """
    Один тик:
    - под lock: pop batch / decide need_fill
    - если need_fill: под lock заполнить entities:q:{rating_id} (до 1000)
    - если work: GPT + upsert rate_contacts (без lock)
    """
    st = _pop_batch()
    if st.get("mode") == "noop":
        return {"mode": "noop"}

    rating_id = int(st["rating_id"])
    task_id = int(st["task_id"])
    rtype = str(st["rtype"])
    target_hash = int(st["target_hash"])

    # sanity: target_hash должен быть валидный (и не пустышка)
    if not _hash_is_valid(target_hash):
        _p(f"CLOSE rating_id={rating_id} task_id={task_id} type={rtype} reason=bad_target_hash target_hash={target_hash}")
        _close_rating_done(rating_id)
        return {"mode": "closed", "reason": "bad_target_hash"}

    if st.get("mode") == "need_fill":
        # ВАЖНО: fill делаем здесь (как шаг 1), и только под коротким lock.
        owner = f"prep:contacts:fillgate:{int(time.time())}"
        token = _lock_acquire(owner=owner)
        try:
            ek = _k_q_entities(rating_id)
            eq = _cache_get_list(ek)
            eq = [int(x) for x in eq if isinstance(x, int)]
            if eq:
                _p(f"FILL_SKIP rating_id={rating_id} task_id={task_id} type={rtype} reason=already_filled eq={len(eq)}")
                return {"mode": "noop", "step": "already_filled"}

            # close-by-state (done) решаем тут же, чтобы не плодить лишние candidates
            t = _load_audience_task(task_id)
            if not t:
                _p(f"CLOSE rating_id={rating_id} task_id={task_id} type={rtype} reason=task_missing")
                _close_rating_done(rating_id)
                return {"mode": "closed", "reason": "task_missing"}

            user_id, task_mode, main_task, task_client, subs_limit = t

            if rtype == "contacts":
                rc = _rated_cnt(task_id)
                threshold = int(subs_limit) + int(BATCH_SIZE)
                if rc >= threshold:
                    _p(f"DONE rating_id={rating_id} task_id={task_id} type=contacts rated_cnt={rc} threshold={threshold}")
                    _close_rating_done(rating_id)
                    _cache_set_list(ek, [])
                    return {"mode": "closed", "reason": "enough"}

                ids = _select_candidates_contacts(task_id, limit=MAX_FILL)
                _p(f"FILL rating_id={rating_id} task_id={task_id} type=contacts rated_cnt={rc}/{threshold} put={len(ids)}")
                _cache_set_list(ek, ids)
                return {"mode": "filled", "queued": len(ids)}

            if rtype == "contacts_update":
                if not _has_stale_for_update(task_id, target_hash):
                    _p(f"DONE rating_id={rating_id} task_id={task_id} type=contacts_update reason=no_stale")
                    _close_rating_done(rating_id)
                    _cache_set_list(ek, [])
                    return {"mode": "closed", "reason": "no_stale"}

                ids = _select_candidates_contacts_update(task_id, target_hash, limit=MAX_FILL)
                _p(f"FILL rating_id={rating_id} task_id={task_id} type=contacts_update put={len(ids)}")
                _cache_set_list(ek, ids)
                return {"mode": "filled", "queued": len(ids)}

            _p(f"CLOSE rating_id={rating_id} task_id={task_id} type={rtype} reason=unknown_type")
            _close_rating_done(rating_id)
            _cache_set_list(ek, [])
            return {"mode": "closed", "reason": "unknown_type"}

        finally:
            _lock_release(token=token)

    # work
    ids = [int(x) for x in st["ids"]]
    t = _load_audience_task(task_id)
    if not t:
        _p(f"CLOSE rating_id={rating_id} task_id={task_id} type={rtype} reason=task_missing")
        _close_rating_done(rating_id)
        return {"mode": "closed", "reason": "task_missing"}

    user_id, task_mode, main_task, task_client, subs_limit = t

    instructions = _build_instructions(task_mode, main_task, task_client)
    if not instructions:
        _p(f"ERROR rating_id={rating_id} task_id={task_id} type={rtype} reason=prompt_empty mode={task_mode}")
        return {"mode": "error", "reason": "prompt_empty"}

    items = _fetch_contacts_payload(ids)
    if not items:
        _p(f"NOOP rating_id={rating_id} task_id={task_id} type={rtype} reason=no_contacts_in_db take={len(ids)}")
        return {"mode": "noop", "step": "no_contacts_in_db"}

    payload = _gpt_user_payload(items)

    try:
        _p(f"BATCH rating_id={rating_id} task_id={task_id} type={rtype} send={len(items)}")
        resp = (
            GPTClient()
            .ask(
                model=MODEL,
                service_tier=SERVICE_TIER,
                user_id=str(user_id),
                instructions=instructions,
                input=payload,
                use_cache=False,
            )
        )
        out = resp.content or ""
    except Exception as e:
        _p(f"FAIL rating_id={rating_id} task_id={task_id} type={rtype} step=gpt err={e!r}")
        return {"mode": "error", "reason": "gpt_exception", "err": repr(e)}

    gpt_data = _parse_gpt_json(out)
    if not gpt_data:
        _p(f"FAIL rating_id={rating_id} task_id={task_id} type={rtype} step=parse_json")
        return {"mode": "error", "reason": "bad_json"}

    written, bad = _write_rates(task_id, target_hash, items, gpt_data)
    _p(f"DB_OK rating_id={rating_id} task_id={task_id} type={rtype} written={written} bad={bad}")
    return {"mode": "ok", "written": written, "bad": bad}


def task_rate_contacts_done_scan() -> Dict[str, Any]:
    """
    Периодически закрывает __tasks_rating.done по правилам:
    - contacts: rated_cnt >= subscribers_limit + BATCH_SIZE
    - contacts_update: нет stale (rate_cl есть, hash валиден, hash != target_hash)
    """
    closed = 0
    scanned = 0

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, task_id, type, hash_task
            FROM __tasks_rating
            WHERE done=false
              AND type IN ('contacts','contacts_update')
              AND hash_task IS NOT NULL
            ORDER BY updated_at ASC, id ASC
            LIMIT %s
            """,
            (int(DONE_SCAN_LIMIT),),
        )
        rows = cur.fetchall() or []

    for rid, task_id, rtype, target_hash in rows:
        scanned += 1
        rid_i = int(rid)
        task_id_i = int(task_id)
        rtype_s = str(rtype)
        th = int(target_hash)

        if not _hash_is_valid(th):
            _p(f"DONE_SCAN close rating_id={rid_i} task_id={task_id_i} type={rtype_s} reason=bad_target_hash")
            _close_rating_done(rid_i)
            closed += 1
            continue

        t = _load_audience_task(task_id_i)
        if not t:
            _p(f"DONE_SCAN close rating_id={rid_i} task_id={task_id_i} type={rtype_s} reason=task_missing")
            _close_rating_done(rid_i)
            closed += 1
            continue

        user_id, task_mode, main_task, task_client, subs_limit = t

        if rtype_s == "contacts":
            rc = _rated_cnt(task_id_i)
            threshold = int(subs_limit) + int(BATCH_SIZE)
            if rc >= threshold:
                _p(f"DONE_SCAN close rating_id={rid_i} task_id={task_id_i} type=contacts rated_cnt={rc} threshold={threshold}")
                _close_rating_done(rid_i)
                closed += 1
            continue

        if rtype_s == "contacts_update":
            if not _has_stale_for_update(task_id_i, th):
                _p(f"DONE_SCAN close rating_id={rid_i} task_id={task_id_i} type=contacts_update reason=no_stale")
                _close_rating_done(rid_i)
                closed += 1
            continue

        _p(f"DONE_SCAN close rating_id={rid_i} task_id={task_id_i} type={rtype_s} reason=unknown_type")
        _close_rating_done(rid_i)
        closed += 1

    return {"mode": "ok", "scanned": scanned, "closed": closed}


def task_rate_contacts_reset_cache() -> Dict[str, Any]:
    # delete нет — просто затираем tasks queue; entities сами протухнут по TTL
    _cache_set_list(_k_q_tasks(), [])
    _p("RESET_CACHE tasks_q cleared (entities expire by TTL)")
    return {"mode": "ok"}


def main() -> None:
    # удобный прямой запуск: один тик + done_scan
    task_rate_contacts_once()
    task_rate_contacts_done_scan()


if __name__ == "__main__":
    main()
