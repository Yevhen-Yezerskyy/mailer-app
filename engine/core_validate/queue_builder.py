# FILE: engine/core_validate/queue_builder.py  (обновлено — 2026-01-29)
# PURPOSE:
# - Build TOP-K window values for task_id as sorted list: (cb_id, rate, collected).
# - get_expand(task_id): cached slice around first uncollected (±CB_DIFF).
# - get_expand_full(task_id): cached prefix up to (and incl.) first uncollected (may fail to cache if >5MB -> ok).
# - get_crawler(task_id): cached 2*CB_DIFF uncollected items starting from first uncollected.
# - Cache key version = kt_hash(task_id) from crawl_tasks; TTL random 2–4 hours. No debug prints.

from __future__ import annotations

import heapq
import random
from typing import Dict, List, Optional, Tuple

from engine.common.cache.client import memo
from engine.common.db import fetch_all, fetch_one, get_connection

CB_WINDOW = 100_000
CB_BATCH = 1_000
CB_DIFF = 1_000

# -----------------------------
# types
# -----------------------------

PlzRate = Tuple[int, str]        # (city_rate, plz)
BranchRate = Tuple[int, int]     # (branch_rate, branch_id)
Pair = Tuple[str, int, int]      # (plz, branch_id, score)

Key = Tuple[str, int]            # (plz, branch_id)
Val = Tuple[int, int, bool]      # (cb_id, rate, collected)


# -----------------------------
# hash & ttl
# -----------------------------

def _ttl_2_4h_sec() -> int:
    return int(random.randint(2 * 60 * 60, 4 * 60 * 60))


def _kt_hash(task_id: int) -> str:
    row = fetch_one(
        """
        SELECT md5(
            string_agg(
                type || ':' || value_id::text || '=' || rate::text,
                '|'
                ORDER BY type, value_id
            )
        )
        FROM crawl_tasks
        WHERE task_id = %s
        """,
        (task_id,),
    )
    return str(row[0]) if row and row[0] else ""


# -----------------------------
# load rates
# -----------------------------

def _load_plz_rates(task_id: int) -> List[PlzRate]:
    rows = fetch_all(
        """
        SELECT m.plz, ct.rate
        FROM __city__plz_map m
        JOIN crawl_tasks ct
          ON ct.task_id = %s
         AND ct.type = 'city'
         AND ct.value_id = m.city_id
        ORDER BY ct.rate ASC, m.plz ASC
        """,
        (task_id,),
    )
    return [(int(rate), str(plz)) for (plz, rate) in rows]


def _load_branch_rates(task_id: int) -> List[BranchRate]:
    rows = fetch_all(
        """
        SELECT value_id, rate
        FROM crawl_tasks
        WHERE task_id = %s AND type = 'branch'
        ORDER BY rate ASC, value_id ASC
        """,
        (task_id,),
    )
    return [(int(rate), int(bid)) for (bid, rate) in rows]


# -----------------------------
# TOP-K via heap (k-way merge)
# -----------------------------

def _top_k_pairs(plz_rates: List[PlzRate], branch_rates: List[BranchRate], k: int) -> List[Pair]:
    if not plz_rates or not branch_rates or k <= 0:
        return []

    outer_is_branch = len(branch_rates) <= len(plz_rates)
    outer = branch_rates if outer_is_branch else plz_rates
    inner = plz_rates if outer_is_branch else branch_rates

    # heap item: (score, plz, branch_id, i, j)
    h: List[Tuple[int, str, int, int, int]] = []
    for i, (orate, oid) in enumerate(outer):
        irate0, iid0 = inner[0]
        if outer_is_branch:
            plz = str(iid0)
            branch_id = int(oid)
        else:
            plz = str(oid)
            branch_id = int(iid0)
        score = int(orate) * int(irate0)
        heapq.heappush(h, (score, plz, branch_id, i, 0))

    out: List[Pair] = []
    while h and len(out) < k:
        score, plz, branch_id, i, j = heapq.heappop(h)
        out.append((str(plz), int(branch_id), int(score)))

        j2 = j + 1
        if j2 < len(inner):
            orate, oid = outer[i]
            irate2, iid2 = inner[j2]
            if outer_is_branch:
                plz2 = str(iid2)
                branch_id2 = int(oid)
            else:
                plz2 = str(oid)
                branch_id2 = int(iid2)
            score2 = int(orate) * int(irate2)
            heapq.heappush(h, (score2, plz2, branch_id2, i, j2))

    return out


# -----------------------------
# build full sorted values (no cache)
# -----------------------------

def build_cb_window_values(task_id: int, k: int = CB_WINDOW) -> List[Val]:
    plz_rates = _load_plz_rates(task_id)
    branch_rates = _load_branch_rates(task_id)

    pairs = _top_k_pairs(plz_rates, branch_rates, int(k))
    rate_dict: Dict[Key, int] = {(plz, int(bid)): int(score) for (plz, bid, score) in pairs}

    keys = list(rate_dict.keys())
    enriched: Dict[Key, Val] = {}

    with get_connection() as conn:
        with conn.cursor() as cur:
            for off in range(0, len(keys), CB_BATCH):
                chunk = keys[off: off + CB_BATCH]
                plz_arr = [p for (p, _b) in chunk]
                bid_arr = [int(b) for (_p, b) in chunk]

                cur.execute(
                    """
                    SELECT cb.plz, cb.branch_id, cb.id, cb.collected
                    FROM unnest(%s::text[], %s::int[]) AS u(plz, branch_id)
                    JOIN cb_crawler cb
                      ON cb.plz = u.plz
                     AND cb.branch_id = u.branch_id
                    """,
                    (plz_arr, bid_arr),
                )
                for plz, branch_id, cb_id, collected in cur.fetchall():
                    kk: Key = (str(plz), int(branch_id))
                    enriched[kk] = (int(cb_id), int(rate_dict[kk]), bool(collected))

    values = list(enriched.values())
    values.sort(key=lambda x: (int(x[1]), int(x[0])))
    return values


def _first_uncollected_idx(values: List[Val]) -> Optional[int]:
    for i, (_cb_id, _rate, collected) in enumerate(values):
        if not bool(collected):
            return int(i)
    return None


# -----------------------------
# API
# -----------------------------

def get_expand(task_id: int) -> List[Val]:
    kt = _kt_hash(task_id)
    ttl = _ttl_2_4h_sec()

    def _compute(q: Tuple[str, int]) -> List[Val]:
        _tag, _task_id = q
        values = build_cb_window_values(int(_task_id), int(CB_WINDOW))
        i = _first_uncollected_idx(values)
        if i is None:
            return []
        lo = max(0, i - CB_DIFF)
        hi = min(len(values), i + CB_DIFF)
        return values[lo:hi]

    return list(memo(("expand", int(task_id)), _compute, ttl=ttl, version=str(kt), update=False))  # type: ignore[arg-type]


def get_expand_full(task_id: int) -> List[Val]:
    kt = _kt_hash(task_id)
    ttl = _ttl_2_4h_sec()

    def _compute(q: Tuple[str, int]) -> List[Val]:
        _tag, _task_id = q
        values = build_cb_window_values(int(_task_id), int(CB_WINDOW))
        i = _first_uncollected_idx(values)
        if i is None:
            return values
        return values[: i + 1]

    return list(memo(("expand_full", int(task_id)), _compute, ttl=ttl, version=str(kt), update=False))  # type: ignore[arg-type]


def get_crawler(task_id: int) -> List[Val]:
    kt = _kt_hash(task_id)
    ttl = _ttl_2_4h_sec()
    need = int(2 * CB_DIFF)

    def _compute(q: Tuple[str, int]) -> List[Val]:
        _tag, _task_id = q
        values = build_cb_window_values(int(_task_id), int(CB_WINDOW))
        i = _first_uncollected_idx(values)
        if i is None:
            return []
        out: List[Val] = []
        for cb_id, rate, collected in values[i:]:
            if not bool(collected):
                out.append((int(cb_id), int(rate), bool(collected)))
                if len(out) >= need:
                    break
        return out

    return list(memo(("crawler", int(task_id)), _compute, ttl=ttl, version=str(kt), update=False))  # type: ignore[arg-type]
