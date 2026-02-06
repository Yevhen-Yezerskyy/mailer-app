# FILE: engine/work/cluster_leafs_json.py  (обновлено — 2026-02-05)
# PURPOSE: Incremental LLM clustering (~600) for 11880+GS leaf-branches (strict JSON I/O);
#          persist full state JSON into _tmp_crwl_cluster_state; print per-batch results.

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from engine.common.db import get_connection
from engine.common.gpt import GPTClient

MODEL = os.getenv("GPT_MODEL", "mini")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
MAX_BATCHES = int(os.getenv("MAX_BATCHES", "1"))
MAX_CLUSTERS = int(os.getenv("MAX_CLUSTERS", "600"))

# one-row "state" table (jsonb)
STATE_TABLE = os.getenv("CLUSTER_STATE_TABLE", "public._tmp_crwl_cluster_state")

INSTRUCTIONS = (
    "You are given JSON with current clusters and new leaf business categories in German (Deutsch).\n"
    "Goal: maintain up to MAX_CLUSTERS clusters that represent BUSINESS COUNTERPART TYPES for B2B outreach.\n"
    "\n"
    "B2B criteria:\n"
    "- Cluster must represent an organization/professional provider that can be a contracting party.\n"
    "- Doctors: office-based doctors may be grouped by specialty.\n"
    "- Hospitals/clinics (Krankenhaus/Klinik) must be separate from doctor-office clusters.\n"
    "- Public institutions (Behörde/Schule/Universität) must be separate from private providers.\n"
    "- Prefer stable business types; avoid consumer-only micro-variants.\n"
    "\n"
    "Naming:\n"
    "- Each cluster name_de must be 2–3 words in German describing the business type.\n"
    "- Provide desc_de (one sentence) and up to 5 examples.\n"
    "\n"
    "Rules:\n"
    "- You MUST assign each input item to exactly one cluster.\n"
    "- You MAY rename clusters (name_de/desc_de) to improve clarity.\n"
    "- You MUST NOT exceed MAX_CLUSTERS total clusters.\n"
    "\n"
    "Return ONLY valid JSON (no markdown, no comments, no extra text) with exactly this shape:\n"
    "{\n"
    '  "clusters": [{"name_de": "...", "desc_de": "...", "examples": ["...", "..."]}, ...],\n'
    '  "assignments": [{"id": 123, "cluster_name_de": "...", "confidence": 0.0}, ...]\n'
    "}\n"
    "Constraints:\n"
    "- clusters[].name_de must be unique (case-insensitive uniqueness preferred).\n"
    "- assignments must include all ids from input.items exactly once.\n"
    "- assignments[].cluster_name_de must match one of clusters[].name_de exactly.\n"
    "- confidence is a float in [0,1].\n"
)


@dataclass(frozen=True)
class Item:
    uid: int
    text: str


def _norm_name_key(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _validate_response(
    payload: Any,
    expected_ids: list[int],
    max_clusters: int,
) -> Tuple[list[dict[str, Any]], Dict[int, Tuple[str, float]]] | None:
    if not isinstance(payload, dict):
        return None
    if set(payload.keys()) != {"clusters", "assignments"}:
        return None

    clusters = payload["clusters"]
    assignments = payload["assignments"]

    if not isinstance(clusters, list) or not isinstance(assignments, list):
        return None
    if len(clusters) == 0 or len(clusters) > max_clusters:
        return None

    # clusters
    seen_ci = set()
    name_exact = set()
    for c in clusters:
        if not isinstance(c, dict):
            return None
        if set(c.keys()) != {"name_de", "desc_de", "examples"}:
            return None
        name = c["name_de"]
        desc = c["desc_de"]
        ex = c["examples"]

        if not isinstance(name, str) or not name.strip():
            return None
        if not isinstance(desc, str) or not desc.strip():
            return None
        if not isinstance(ex, list) or any(not isinstance(x, str) for x in ex):
            return None

        k = _norm_name_key(name)
        if k in seen_ci:
            return None
        seen_ci.add(k)
        name_exact.add(name)

    # assignments
    exp = set(expected_ids)
    got: Dict[int, Tuple[str, float]] = {}

    for a in assignments:
        if not isinstance(a, dict):
            return None
        if set(a.keys()) != {"id", "cluster_name_de", "confidence"}:
            return None

        rid = a["id"]
        cn = a["cluster_name_de"]
        conf = a["confidence"]

        if not isinstance(rid, int) or rid in got:
            return None
        if not isinstance(cn, str) or cn not in name_exact:
            return None
        if not (isinstance(conf, float) or isinstance(conf, int)):
            return None

        conf_f = float(conf)
        if conf_f < 0.0 or conf_f > 1.0:
            return None

        got[rid] = (cn, conf_f)

    if set(got.keys()) != exp:
        return None

    return clusters, got


def _ask_gpt(
    gpt: GPTClient,
    clusters: list[dict[str, Any]],
    items: list[Item],
) -> Tuple[list[dict[str, Any]], Dict[int, Tuple[str, float]]] | None:
    payload_in = {
        "MAX_CLUSTERS": MAX_CLUSTERS,
        "clusters": clusters,
        "items": [{"id": it.uid, "text": it.text} for it in items],
    }

    resp = gpt.ask(
        model=MODEL,
        instructions=INSTRUCTIONS.replace("MAX_CLUSTERS", str(MAX_CLUSTERS)),
        input=json.dumps(payload_in, ensure_ascii=False),
        use_cache=False,
        user_id="engine.work.cluster_leafs_json",
        service_tier="flex",
    )

    try:
        parsed = json.loads(resp.content)
    except Exception:
        return None

    expected_ids = [it.uid for it in items]
    return _validate_response(parsed, expected_ids, MAX_CLUSTERS)


def _read_state(cur) -> tuple[int, list[dict[str, Any]]]:
    cur.execute(f"SELECT state_json FROM {STATE_TABLE} LIMIT 1")
    row = cur.fetchone()
    if not row:
        return 0, []

    state = row[0]
    if not isinstance(state, dict):
        return 0, []

    cursor_uid = int(state.get("cursor_uid") or 0)
    clusters = state.get("clusters")
    if not isinstance(clusters, list):
        clusters = []
    return cursor_uid, clusters


def _write_state(cur, state: dict[str, Any]) -> None:
    cur.execute(f"TRUNCATE {STATE_TABLE}")
    cur.execute(f"INSERT INTO {STATE_TABLE} (state_json) VALUES (%s)", (json.dumps(state, ensure_ascii=False),))


def _select_batch(cur, cursor_uid: int, limit: int) -> list[Item]:
    # unify ids: 11880.id as-is; GS.id + offset to avoid collisions
    cur.execute(
        """
        WITH
        a AS (
          SELECT id::bigint AS uid, COALESCE(NULLIF(label,''), slug) AS text
          FROM crwl_slug_11880
        ),
        b AS (
          SELECT (id::bigint + 1000000000) AS uid, slug AS text
          FROM crwl_slug_gs
        ),
        all_items AS (
          SELECT uid, text FROM a
          UNION ALL
          SELECT uid, text FROM b
        )
        SELECT uid, text
        FROM all_items
        WHERE uid > %s
        ORDER BY uid
        LIMIT %s
        """,
        (cursor_uid, limit),
    )
    rows = cur.fetchall()
    out: list[Item] = []
    for uid, text in rows:
        t = str(text or "").strip()
        if not t:
            continue
        out.append(Item(uid=int(uid), text=t))
    return out


def main() -> None:
    gpt = GPTClient(debug=False)

    batches_done = 0
    total_selected = 0
    total_skipped = 0

    with get_connection() as conn:
        while batches_done < MAX_BATCHES:
            with conn.cursor() as cur:
                cursor_uid, clusters = _read_state(cur)
                items = _select_batch(cur, cursor_uid, BATCH_SIZE)

            if not items:
                print("done: nothing_to_do")
                break

            total_selected += len(items)

            before_n = len(clusters)
            res = _ask_gpt(gpt, clusters, items)
            if res is None:
                total_skipped += 1
                print(f"batch={batches_done+1} status=SKIP_BAD_RESPONSE selected={len(items)} cursor={cursor_uid}")
                batches_done += 1
                continue

            new_clusters, mapping = res
            after_n = len(new_clusters)
            created = after_n - before_n

            new_cursor = max(it.uid for it in items)

            # store last batch results into state (to inspect in psql)
            last_assignments = [
                {"id": uid, "cluster_name_de": mapping[uid][0], "confidence": mapping[uid][1]}
                for uid in sorted(mapping.keys())
            ]

            state = {
                "cursor_uid": new_cursor,
                "clusters": new_clusters,
                "last_batch": {
                    "batch": batches_done + 1,
                    "selected": len(items),
                    "created_clusters": created,
                    "cursor_uid_prev": cursor_uid,
                    "cursor_uid_now": new_cursor,
                    "assignments": last_assignments,
                },
            }

            with conn.cursor() as cur:
                _write_state(cur, state)
            conn.commit()

            # print per-batch result (short)
            print(
                f"batch={batches_done+1} status=OK selected={len(items)} "
                f"clusters={after_n}/{MAX_CLUSTERS} created={created} cursor={new_cursor}"
            )
            # print first 10 assignments (readable)
            for a in last_assignments[:10]:
                print(f"  id={a['id']} -> {a['cluster_name_de']} conf={a['confidence']:.2f}")

            batches_done += 1

    print(
        f"summary: batches={batches_done} selected={total_selected} skipped_bad_response={total_skipped} "
        f"model={MODEL} batch_size={BATCH_SIZE} state_table={STATE_TABLE}"
    )


if __name__ == "__main__":
    main()
