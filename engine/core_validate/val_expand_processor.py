# FILE: engine/core_validate/val_expand_processor.py  (обновлено — 2025-12-29)
# Смысл:
# - expand_rate_contacts: инкрементально добирает rate_contacts из raw_contacts_aggr по приоритетам (score-группам)
# - check_inserted_50k: раз в 10 минут ставит inserted_50k=true
# - reset_cache: раз в час сбрасывает кеш (через epoch)
# - Батчирование pairs по 200, чтобы не раздувать SQL.
# - Один проход по cb_crawler на батч: получаем collected_ids и факт наличия collected=false.
# - В rate_contacts пишем ТОЛЬКО (task_id, contact_id, rate_cb). Никаких cb_crawler_id / hash_task / update.

from __future__ import annotations

import pickle
import time
from typing import Dict, List, Optional, Tuple

from engine.common.cache.client import CLIENT as CACHE
from engine.common.db import execute, fetch_all, fetch_one
from engine.common.worker import Worker

TOP_LIMIT = 300
SOFT_LIMIT_INSERTED = 1000
BATCH_PAIRS = 200

CACHE_TTL_SEC = 24 * 60 * 60  # сутки; сброс делает reset_cache_once()
CACHE_EPOCH_KEY = "val_expand:epoch"


def _p(msg: str) -> None:
    print(f"[val_expand] {msg}")


def _cache_get_obj(key: str) -> Optional[object]:
    payload = CACHE.get(key, ttl_sec=CACHE_TTL_SEC)
    if payload is None:
        return None
    try:
        return pickle.loads(payload)
    except Exception:
        return None


def _cache_set_obj(key: str, obj: object) -> bool:
    try:
        payload = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        return False
    return CACHE.set(key, payload, ttl_sec=CACHE_TTL_SEC)


def _get_epoch() -> int:
    v = _cache_get_obj(CACHE_EPOCH_KEY)
    if isinstance(v, int) and v > 0:
        return v
    epoch = int(time.time())
    _cache_set_obj(CACHE_EPOCH_KEY, epoch)
    return epoch


def _key(epoch: int, suffix: str) -> str:
    return f"val_expand:{epoch}:{suffix}"


def _get_ref_counts(epoch: int) -> Tuple[int, int]:
    # кешируем ТОЛЬКО эталонные counts (cities_sys, gb_branches)
    k = _key(epoch, "ref_counts")
    v = _cache_get_obj(k)
    if isinstance(v, tuple) and len(v) == 2:
        a, b = v
        if isinstance(a, int) and isinstance(b, int):
            return a, b

    row1 = fetch_one("SELECT count(*) FROM cities_sys")
    row2 = fetch_one("SELECT count(*) FROM gb_branches")
    cities_ref = int(row1[0]) if row1 else 0
    branches_ref = int(row2[0]) if row2 else 0
    _cache_set_obj(k, (cities_ref, branches_ref))
    return cities_ref, branches_ref


def _build_score_groups(task_id: int) -> List[Tuple[int, List[Tuple[int, int]]]]:
    cities = fetch_all(
        """
        SELECT value_id, rate
        FROM crawl_tasks
        WHERE task_id = %s AND type = 'city'
        ORDER BY rate ASC, value_id ASC
        LIMIT %s
        """,
        (task_id, TOP_LIMIT),
    )
    branches = fetch_all(
        """
        SELECT value_id, rate
        FROM crawl_tasks
        WHERE task_id = %s AND type = 'branch'
        ORDER BY rate ASC, value_id ASC
        LIMIT %s
        """,
        (task_id, TOP_LIMIT),
    )

    city_rate: Dict[int, int] = {int(cid): int(rate) for (cid, rate) in cities}
    branch_rate: Dict[int, int] = {int(bid): int(rate) for (bid, rate) in branches}

    groups: Dict[int, List[Tuple[int, int]]] = {}
    for city_id, cr in city_rate.items():
        for branch_id, br in branch_rate.items():
            score = cr * br
            groups.setdefault(score, []).append((city_id, branch_id))

    out: List[Tuple[int, List[Tuple[int, int]]]] = []
    for score in sorted(groups.keys()):
        pairs = groups[score]
        pairs.sort(key=lambda x: (x[0], x[1]))
        out.append((score, pairs))
    return out


def _is_skippable(epoch: int, task_id: int, score: int) -> bool:
    k = _key(epoch, f"skip:{task_id}:{score}")
    v = _cache_get_obj(k)
    return bool(v is True)


def _set_skippable(epoch: int, task_id: int, score: int) -> None:
    k = _key(epoch, f"skip:{task_id}:{score}")
    _cache_set_obj(k, True)


def _inc_full_hits(epoch: int, task_id: int, score: int) -> int:
    k = _key(epoch, f"hits_full:{task_id}:{score}")
    v = _cache_get_obj(k)
    n = int(v) if isinstance(v, int) else 0
    n += 1
    _cache_set_obj(k, n)
    return n


def run_once() -> None:
    epoch = _get_epoch()

    _p("tick mode=INCREMENTAL")

    # 0) выбираем одну задачу случайно
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

    # 1) валидация counts
    cities_ref, branches_ref = _get_ref_counts(epoch)

    row_c = fetch_one(
        "SELECT count(*) FROM crawl_tasks WHERE task_id = %s AND type = 'city'",
        (task_id,),
    )
    row_b = fetch_one(
        "SELECT count(*) FROM crawl_tasks WHERE task_id = %s AND type = 'branch'",
        (task_id,),
    )
    cnt_city_task = int(row_c[0]) if row_c else 0
    cnt_branch_task = int(row_b[0]) if row_b else 0

    if cnt_city_task != cities_ref or cnt_branch_task != branches_ref:
        _p(
            f"task_id={task_id} invalid counts: "
            f"task cities={cnt_city_task} ref={cities_ref}, "
            f"task branches={cnt_branch_task} ref={branches_ref} -> skip"
        )
        return

    # 2) score-группы
    groups = _build_score_groups(task_id)
    if not groups:
        _p(f"task_id={task_id} no groups -> stop")
        return

    inserted_total = 0

    # 3) итерации: много групп подряд
    for score, pairs in groups:
        if _is_skippable(epoch, task_id, score):
            continue

        inserted_this_group = 0
        fully_collected = True

        _p(f"task_id={task_id} score={score} pairs={len(pairs)}")

        # батчи по 200 пар
        i = 0
        while i < len(pairs):
            batch = pairs[i : i + BATCH_PAIRS]
            i += BATCH_PAIRS

            values_sql = ", ".join(["(%s,%s)"] * len(batch))
            params: List[int] = []
            for city_id, branch_id in batch:
                params.append(int(city_id))
                params.append(int(branch_id))

            # один проход по cb_crawler на батч:
            # - collected_ids: все plz -> id (только collected=true)
            # - has_not_collected: exists(collected=false) внутри батча
            row_cb = fetch_one(
                f"""
                WITH pairs(city_id, branch_id) AS (VALUES {values_sql})
                SELECT
                    array_agg(c.id) FILTER (WHERE c.collected = true)::bigint[] AS collected_ids,
                    bool_or(c.collected = false) AS has_not_collected
                FROM cb_crawler c
                JOIN pairs p ON p.city_id = c.city_id AND p.branch_id = c.branch_id
                """,
                tuple(params),
            )

            collected_ids = None
            has_not_collected = False
            if row_cb:
                collected_ids = row_cb[0]
                has_not_collected = bool(row_cb[1])

            if has_not_collected:
                fully_collected = False

            if not collected_ids:
                continue

            # insert-only в rate_contacts (без cb_crawler_id)
            row_ins = fetch_one(
                """
                WITH cb_arr AS (
                    SELECT %s::bigint[] AS ids
                ),
                ins AS (
                    INSERT INTO rate_contacts (task_id, contact_id, rate_cb)
                    SELECT %s, r.id, %s
                    FROM raw_contacts_aggr r, cb_arr a
                    WHERE r.cb_crawler_ids && a.ids
                    ON CONFLICT (task_id, contact_id) DO NOTHING
                    RETURNING 1
                )
                SELECT count(*) FROM ins
                """,
                (collected_ids, task_id, score),
            )

            inserted_batch = int(row_ins[0]) if row_ins else 0
            inserted_this_group += inserted_batch

        inserted_total += inserted_this_group

        _p(
            f"task_id={task_id} score={score} "
            f"fully_collected={fully_collected} inserted={inserted_this_group} total={inserted_total}"
        )

        # fully-collected + 0 вставок => hit++ ; на 3 => skippable
        if fully_collected and inserted_this_group == 0:
            hits = _inc_full_hits(epoch, task_id, score)
            _p(f"task_id={task_id} score={score} full+0 -> hits_full={hits}")
            if hits >= 3:
                _set_skippable(epoch, task_id, score)
                _p(f"task_id={task_id} score={score} marked skippable")

        # стоп по 0 вставок (именно вставок)
        if inserted_this_group == 0:
            return

        # мягкий лимит 1000: группу прошли целиком, дальше не идём
        if inserted_total >= SOFT_LIMIT_INSERTED:
            return


def check_inserted_50k_once() -> None:
    rows = fetch_all(
        """
        SELECT id
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND inserted_50k = false
        """
    )
    if not rows:
        return

    for (task_id_raw,) in rows:
        task_id = int(task_id_raw)
        row = fetch_one("SELECT count(*) FROM rate_contacts WHERE task_id = %s", (task_id,))
        cnt = int(row[0]) if row else 0
        if cnt >= 50_000:
            execute(
                """
                UPDATE aap_audience_audiencetask
                SET inserted_50k = true
                WHERE id = %s
                """,
                (task_id,),
            )
            _p(f"task_id={task_id} rate_contacts={cnt} -> inserted_50k=true")


def reset_cache_once() -> None:
    epoch = int(time.time())
    _cache_set_obj(CACHE_EPOCH_KEY, epoch)
    _p(f"cache reset: epoch={epoch}")


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

    w.register(
        name="check_inserted_50k",
        fn=check_inserted_50k_once,
        every_sec=600,  # 10 минут
        timeout_sec=600,
        singleton=True,
        heavy=True,
        priority=3,
    )

    w.register(
        name="expand_rate_contacts_reset",
        fn=reset_cache_once,
        every_sec=3600,
        timeout_sec=3600,
        singleton=True,
        heavy=True,
        priority=1,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
