# FILE: engine/crawler/fetch_gs_cb.py  (обновлено — 2025-12-30)
# PATH: engine/crawler/fetch_gs_cb.py
# Смысл (переписано полностью):
# - rebuild=500: 50 random head + 450 main (RR по активным tasks) + random tail.
# - Приоритет сохранён: режим A (есть task с <1000 в rate_contacts) → RR только по ним; иначе режим B → RR по всем.
# - Работа с score-группами БЕЗ "батч-логики" по смыслу:
#   * Для текущей score-группы делаем выборку uncollected (cb_crawler.collected=false) по ВСЕМ pairs группы (один логический селект).
#   * Если список пустой -> группа fully -> курсор на следующую группу -> повторяем.
#   * Пока в группе есть хоть кто-то uncollected — группа НЕ может быть пропущена.
# - Курсор cbq отдельный (не val_expand) и сбрасывается там же, где сбрасывается кеш очереди: через epoch (cbq_reset_cache()).
# - Печать "разумная": epoch/mode/task/score/group_i/pairs/picked.

from __future__ import annotations

import os
import pickle
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Sequence, Tuple

from scrapy.crawler import CrawlerProcess

from engine.common.cache.client import CLIENT
from engine.common.db import fetch_all, fetch_one
from engine.crawler.spiders.spider_gs_cb import GelbeSeitenCBSpider

# источник истины для score-групп (как в val_expand)
from engine.core_validate.val_expand_processor import TOP_LIMIT, _build_score_groups

# -------------------------
# cache keys
# -------------------------
QUEUE_LOCK_KEY = "cbq:lock"
QUEUE_DATA_KEY = "cbq:list"

CBQ_EPOCH_KEY = "cbq:epoch"               # epoch для cbq-курсоров (сброс в cbq_reset_cache)
CURSOR_PREFIX = "cbq:cursor:"             # + <epoch>:<task_id> => int(score)
CB2TASK_PREFIX = "cbq:cb2task:"           # + <cb_crawler_id> => int(task_id)

LOCK_TTL_SEC = 60.0
QUEUE_TTL_SEC = 60 * 60                   # 1 hour (best-effort)
EPOCH_TTL_SEC = 60 * 60 * 24              # 24h (epoch живёт долго, но обновляем reset'ом)

REBUILD_SIZE = 500
RATE_CONTACTS_PRIORITY_OFFSET = 50

# технический лимит для VALUES, чтобы не взрывать SQL строкой на десятки тысяч пар
PAIRS_SQL_CHUNK = 2000

# чтобы одна задача не забрала весь main, оставим верхний потолок
MAX_FROM_ONE_TASK_PER_REBUILD = 200

LOCK_RETRY_SLEEP_SEC = 0.10
LOCK_RENEW_EVERY_SEC = 7.0


# -------------------------
# models
# -------------------------
@dataclass(frozen=True)
class QueueItem:
    cb_crawler_id: int
    plz: str
    branch_slug: str


@dataclass
class _TaskState:
    task_id: int
    groups: List[Tuple[int, List[Tuple[int, int]]]]  # [(score, [(city_id, branch_id), ...]), ...]
    group_i: int                                     # текущая группа
    score: int                                       # score текущей группы


# -------------------------
# small helpers
# -------------------------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _cache_get_queue() -> List[QueueItem]:
    payload = CLIENT.get(QUEUE_DATA_KEY, ttl_sec=QUEUE_TTL_SEC)
    if not payload:
        return []
    try:
        obj = pickle.loads(payload)
        if not isinstance(obj, list):
            return []
        out: List[QueueItem] = []
        for it in obj:
            if (
                isinstance(it, (tuple, list))
                and len(it) == 3
                and isinstance(it[0], int)
                and isinstance(it[1], str)
                and isinstance(it[2], str)
            ):
                out.append(QueueItem(int(it[0]), str(it[1]), str(it[2])))
        return out
    except Exception:
        return []


def _cache_set_queue(items: Sequence[QueueItem]) -> None:
    raw = [(it.cb_crawler_id, it.plz, it.branch_slug) for it in items]
    CLIENT.set(QUEUE_DATA_KEY, pickle.dumps(raw, protocol=pickle.HIGHEST_PROTOCOL), ttl_sec=QUEUE_TTL_SEC)


def _lock_acquire(owner: str) -> Tuple[str, float]:
    while True:
        resp = CLIENT.lock_try(QUEUE_LOCK_KEY, ttl_sec=LOCK_TTL_SEC, owner=owner)
        if resp and resp.get("acquired") is True and isinstance(resp.get("token"), str):
            return resp["token"], time.monotonic()
        time.sleep(LOCK_RETRY_SLEEP_SEC)


def _lock_renew_if_needed(token: str, last_renew_monotonic: float) -> float:
    now_m = time.monotonic()
    if (now_m - last_renew_monotonic) < LOCK_RENEW_EVERY_SEC:
        return last_renew_monotonic
    ok = CLIENT.lock_renew(QUEUE_LOCK_KEY, ttl_sec=LOCK_TTL_SEC, token=token)
    return now_m if ok else last_renew_monotonic


def _lock_release(token: str) -> None:
    CLIENT.lock_release(QUEUE_LOCK_KEY, token=token)


def _run_spider(cb_crawler_id: int, plz: str, branch_slug: str) -> None:
    process = CrawlerProcess(
        settings={
            "LOG_LEVEL": "ERROR",
            "TELNETCONSOLE_ENABLED": False,
            "DOWNLOAD_DELAY": 2.0,
            "RANDOMIZE_DOWNLOAD_DELAY": True,
            "CONCURRENT_REQUESTS": 1,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
            "AUTOTHROTTLE_ENABLED": True,
            "AUTOTHROTTLE_START_DELAY": 2.0,
            "AUTOTHROTTLE_MAX_DELAY": 30.0,
            "AUTOTHROTTLE_TARGET_CONCURRENCY": 0.5,
            "COOKIES_ENABLED": True,
            "ITEM_PIPELINES": {
                "engine.crawler.pipelines.pipeline_gs_cb.GSCBPipeline": 300,
            },
        }
    )
    process.crawl(
        GelbeSeitenCBSpider,
        plz=plz,
        branch_slug=branch_slug,
        cb_crawler_id=cb_crawler_id,
    )
    process.start()


# -------------------------
# epoch + cursor (cbq-only)
# -------------------------
def _get_epoch() -> int:
    payload = CLIENT.get(CBQ_EPOCH_KEY, ttl_sec=EPOCH_TTL_SEC)
    if payload:
        try:
            v = pickle.loads(payload)
            if isinstance(v, int) and v > 0:
                return int(v)
        except Exception:
            pass
    epoch = int(time.time())
    CLIENT.set(CBQ_EPOCH_KEY, pickle.dumps(epoch, protocol=pickle.HIGHEST_PROTOCOL), ttl_sec=EPOCH_TTL_SEC)
    return epoch


def _set_epoch(epoch: int) -> None:
    CLIENT.set(CBQ_EPOCH_KEY, pickle.dumps(int(epoch), protocol=pickle.HIGHEST_PROTOCOL), ttl_sec=EPOCH_TTL_SEC)


def _cursor_key(epoch: int, task_id: int) -> str:
    return f"{CURSOR_PREFIX}{int(epoch)}:{int(task_id)}"


def _cursor_get_score(epoch: int, task_id: int) -> int:
    payload = CLIENT.get(_cursor_key(epoch, task_id), ttl_sec=QUEUE_TTL_SEC)
    if not payload:
        return 0
    try:
        v = pickle.loads(payload)
        return int(v) if isinstance(v, int) else 0
    except Exception:
        return 0


def _cursor_set_score(epoch: int, task_id: int, score: int) -> None:
    CLIENT.set(_cursor_key(epoch, task_id), pickle.dumps(int(score), protocol=pickle.HIGHEST_PROTOCOL), ttl_sec=QUEUE_TTL_SEC)


def cbq_reset_cache() -> None:
    """
    Сброс очереди + сброс cbq-курсоров (через epoch).
    """
    new_epoch = int(time.time())
    _set_epoch(new_epoch)

    CLIENT.set(QUEUE_DATA_KEY, pickle.dumps([], protocol=pickle.HIGHEST_PROTOCOL), ttl_sec=QUEUE_TTL_SEC)
    print(f"[cbq] RESET: epoch={new_epoch} cbq:list=[] (cursors reset by epoch)")


# -------------------------
# rate_contacts <1000 priority
# -------------------------
def _task_has_priority_row(task_id: int) -> bool:
    row = fetch_one(
        """
        SELECT 1
        FROM rate_contacts
        WHERE task_id = %s
        ORDER BY contact_id ASC
        OFFSET %s
        LIMIT 1
        """,
        (task_id, RATE_CONTACTS_PRIORITY_OFFSET),
    )
    return bool(row)


# -------------------------
# group helpers
# -------------------------
def _pairs_chunks(pairs: List[Tuple[int, int]]) -> List[List[Tuple[int, int]]]:
    if not pairs:
        return []
    return [pairs[i:i + PAIRS_SQL_CHUNK] for i in range(0, len(pairs), PAIRS_SQL_CHUNK)]


def _fetch_group_uncollected(pairs: List[Tuple[int, int]], limit: int, *, task_id: int) -> List[QueueItem]:
    """
    Это и есть "выбрать ВСЕХ из группы" по смыслу:
    - один логический селект по группе (pairs -> cb_crawler JOIN, collected=false),
    - но технически режем VALUES на чанки, чтобы SQL не взрывался.
    """
    if limit <= 0 or not pairs:
        return []

    out: List[QueueItem] = []
    left = int(limit)

    for chunk in _pairs_chunks(pairs):
        if left <= 0:
            break

        values_sql = ", ".join(["(%s,%s)"] * len(chunk))
        params: List[int] = []
        for city_id, branch_id in chunk:
            params.extend((int(city_id), int(branch_id)))

        rows = fetch_all(
            f"""
            WITH pairs(city_id, branch_id) AS (VALUES {values_sql})
            SELECT c.id, c.plz, c.branch_slug
            FROM cb_crawler c
            JOIN pairs p
              ON p.city_id = c.city_id
             AND p.branch_id = c.branch_id
            WHERE c.collected = false
            ORDER BY c.id ASC
            LIMIT %s
            """,
            tuple(params + [left]),
        )

        for cb_id, plz, branch_slug in rows:
            CLIENT.set(f"{CB2TASK_PREFIX}{int(cb_id)}", pickle.dumps(int(task_id), protocol=pickle.HIGHEST_PROTOCOL), ttl_sec=QUEUE_TTL_SEC)
            out.append(QueueItem(int(cb_id), str(plz), str(branch_slug)))
            left -= 1
            if left <= 0:
                break

    return out


def _start_index_by_score(groups: List[Tuple[int, List[Tuple[int, int]]]], cursor_score: int) -> int:
    for i, (score, _pairs) in enumerate(groups):
        if int(score) >= int(cursor_score):
            return i
    return len(groups)


def _build_task_state(epoch: int, task_id: int) -> Optional[_TaskState]:
    groups = _build_score_groups(task_id)
    if not groups:
        return None

    cursor_score = _cursor_get_score(epoch, task_id)
    start_i = _start_index_by_score(groups, cursor_score)
    if start_i >= len(groups):
        # курсор в конец — стартуем заново (чтобы не залипнуть)
        start_i = 0
        cursor_score = 0
        _cursor_set_score(epoch, task_id, 0)

    score, _pairs = groups[start_i]
    _cursor_set_score(epoch, task_id, int(score))

    return _TaskState(task_id=int(task_id), groups=groups, group_i=int(start_i), score=int(score))


def _advance_to_next_group(epoch: int, st: _TaskState) -> bool:
    """
    Двигаем state на следующую группу. True если удалось, False если групп больше нет.
    """
    st.group_i += 1
    if st.group_i >= len(st.groups):
        return False
    st.score = int(st.groups[st.group_i][0])
    _cursor_set_score(epoch, st.task_id, st.score)
    return True


# -------------------------
# RR fill main
# -------------------------
def _round_robin_fill(
    *,
    epoch: int,
    states: List[_TaskState],
    need: int,
    token: str,
    last_renew: float,
) -> Tuple[List[QueueItem], float]:
    out: List[QueueItem] = []
    if need <= 0 or not states:
        return out, last_renew

    # базовый fair-share
    per_task_cap = max(1, (need // max(1, len(states))) + 1)
    per_task_cap = min(per_task_cap, MAX_FROM_ONE_TASK_PER_REBUILD)

    # несколько кругов, чтобы задачи могли "перескочить пустые группы" и всё же дать элементы
    # (но без фанатизма)
    max_rounds = 5

    for _round in range(max_rounds):
        if len(out) >= need or not states:
            break

        added_this_round = 0

        for st in list(states):
            if len(out) >= need:
                break

            last_renew = _lock_renew_if_needed(token, last_renew)

            take = min(per_task_cap, need - len(out))
            # гарантируем: пока в группе есть хоть кто-то — берём; если пусто — группа fully -> next
            while True:
                score, pairs = st.groups[st.group_i]
                items = _fetch_group_uncollected(pairs, limit=take, task_id=st.task_id)

                print(
                    f"[cbq] task={st.task_id} group_i={st.group_i} score={score} "
                    f"pairs={len(pairs)} picked={len(items)}"
                )

                if items:
                    out.extend(items)
                    added_this_round += len(items)
                    break

                # пусто -> группа fully -> следующий score
                if not _advance_to_next_group(epoch, st):
                    # задачa исчерпана (все группы пустые)
                    states.remove(st)
                    break

        if added_this_round == 0:
            break

    return out[:need], last_renew


# -------------------------
# rebuild=500
# -------------------------
def _pick_random_cb(*, need: int) -> List[QueueItem]:
    if need <= 0:
        return []

    row = fetch_one("SELECT max(id) FROM cb_crawler")
    max_id = int(row[0]) if row and row[0] is not None else 0
    if max_id <= 0:
        return []

    start = random.randint(1, max_id)

    rows = fetch_all(
        """
        SELECT id, plz, branch_slug
        FROM cb_crawler
        WHERE collected = false
          AND id >= %s
        ORDER BY id ASC
        LIMIT %s
        """,
        (start, need * 3),
    )

    out: List[QueueItem] = []
    for cb_id, plz, branch_slug in rows:
        out.append(QueueItem(int(cb_id), str(plz), str(branch_slug)))
        if len(out) >= need:
            return out

    if len(out) < need:
        rows2 = fetch_all(
            """
            SELECT id, plz, branch_slug
            FROM cb_crawler
            WHERE collected = false
              AND id < %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (start, (need - len(out)) * 3),
        )
        for cb_id, plz, branch_slug in rows2:
            out.append(QueueItem(int(cb_id), str(plz), str(branch_slug)))
            if len(out) >= need:
                break

    return out[:need]


def _rebuild_queue_500(token: str, last_renew: float) -> Tuple[List[QueueItem], float]:
    last_renew = _lock_renew_if_needed(token, last_renew)
    epoch = _get_epoch()

    print(f"[cbq] rebuild: epoch={epoch} size={REBUILD_SIZE}")

    out: List[QueueItem] = []

    # 1) active tasks
    tasks = fetch_all(
        """
        SELECT id
        FROM aap_audience_audiencetask
        WHERE run_processing = true
          AND collected = false
        ORDER BY id ASC
        """
    )
    active_task_ids = [int(t[0]) for t in tasks] if tasks else []
    print(f"[cbq] rebuild: active_tasks={len(active_task_ids)}")

    # 2) НЕТ активных тасков → ТОЛЬКО random
    if not active_task_ids:
        out = _pick_random_cb(need=REBUILD_SIZE)
        print(f"[cbq] rebuild done: mode=RANDOM_ONLY out={len(out)}")
        return out[:REBUILD_SIZE], last_renew

    # 3) underdone (<1000)
    underdone_ids: List[int] = []
    for task_id in active_task_ids:
        last_renew = _lock_renew_if_needed(token, last_renew)
        if not _task_has_priority_row(task_id):
            underdone_ids.append(task_id)

    # 4) mode
    if underdone_ids:
        mode = f"A_UNDERDONE_LT_{RATE_CONTACTS_PRIORITY_OFFSET+1}"
        target_ids = underdone_ids
    else:
        mode = f"B_FAIR_ALL_GE_{RATE_CONTACTS_PRIORITY_OFFSET+1}"
        target_ids = active_task_ids

    print(f"[cbq] rebuild: mode={mode} target_tasks={len(target_ids)}")

    # 5) states
    states: List[_TaskState] = []
    for task_id in target_ids:
        last_renew = _lock_renew_if_needed(token, last_renew)
        st = _build_task_state(epoch, task_id)
        if st:
            states.append(st)

    print(f"[cbq] rebuild: states_ready={len(states)}")

    # 6) RR fill до REBUILD_SIZE
    if states:
        picked, last_renew = _round_robin_fill(
            epoch=epoch,
            states=states,
            need=REBUILD_SIZE,
            token=token,
            last_renew=last_renew,
        )
        out.extend(picked)
    random.shuffle(out)
    print(f"[cbq] rebuild done: out={len(out)}")
    return out[:REBUILD_SIZE], last_renew


# -------------------------
# worker entry
# -------------------------
def worker_run_once() -> None:
    owner = f"cb_processor:{os.getpid()}:{uuid.uuid4().hex[:10]}"
    token, last_renew = _lock_acquire(owner=owner)
    item: Optional[QueueItem] = None

    try:
        q = _cache_get_queue()
        if not q:
            q, last_renew = _rebuild_queue_500(token=token, last_renew=last_renew)
            _cache_set_queue(q)

        if q:
            item = q[0]
            _cache_set_queue(q[1:])

    finally:
        _lock_release(token=token)

    if not item:
        print("[cbq] queue empty; nothing to do")
        return

    print(f"[cbq] pop cb_crawler_id={item.cb_crawler_id} plz={item.plz} branch={item.branch_slug}")
    _run_spider(cb_crawler_id=item.cb_crawler_id, plz=item.plz, branch_slug=item.branch_slug)



if __name__ == "__main__":
    row = fetch_one(
        """
        SELECT id, plz, branch_slug
        FROM cb_crawler
        WHERE collected = false
        ORDER BY random()
        LIMIT 1
        """
    )
    if not row:
        print("DEBUG: no uncollected cb_crawler rows")
    else:
        cb_crawler_id, plz, branch_slug = row
        print(f"DEBUG: picked cb_crawler_id={cb_crawler_id} plz={plz} branch={branch_slug}")
        _run_spider(cb_crawler_id=int(cb_crawler_id), plz=str(plz), branch_slug=str(branch_slug))
