# FILE: engine/core_validate/val_enrich.py  (обновлено) 2025-12-17
# Fix:
# - добавлен run_priority_updater() (бывший enrich_priority_updater.run_batch) в этом же файле
# - run_batch() больше НЕ трогает public.__enrich_priority (убран инкремент en_done)
# - main(): при прямом запуске файла — updater → enrich

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from psycopg.types.json import Json

from engine.common.db import get_connection
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt

from engine.core_validate.val_prepare import (  # type: ignore
    SOURCE_NAME,
    _build_norm,
    _calc_status,
    _merge_norm,
    _next_gs_key,
    _trim,
    _uniq,
)

BATCH_SIZE = 7

STATUS_ENRICHED = "ENRICHED"
STATUS_ENRICH_FAILED = "ENRICH FAILED"
STATUS_ENRICH_ERROR = "ENRICH ERROR"


def _safe_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _uniq_str_list(values: Any, *, lower: bool = False) -> List[str]:
    out: List[str] = []
    for x in _safe_list(values):
        if x is None:
            continue
        s = str(x).strip()
        if not s:
            continue
        if lower:
            s = s.lower()
        if s not in out:
            out.append(s)
    return out


def _normalize_emails(v: Any) -> List[str]:
    return _uniq_str_list(v, lower=True)


def _normalize_phones(v: Any) -> List[str]:
    return _uniq_str_list(v, lower=False)


def _normalize_sources(v: Any) -> List[str]:
    return _uniq_str_list(v, lower=False)


def _extract_source_url(company_data: Dict[str, Any]) -> Optional[str]:
    u = _trim(company_data.get("source_url"))
    if u:
        return u
    urls = company_data.get("source_urls")
    if isinstance(urls, list) and urls:
        u2 = _trim(urls[0])
        if u2:
            return u2
    return None


def _extract_expected_plz(company_data: Dict[str, Any]) -> Optional[str]:
    return _trim(company_data.get("plz"))


def _gpt_system_prompt() -> str:
    return get_prompt("engine_core_validate_enrich")


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
    Синхронизация __enrich_priority:
    - upsert активных task_id (en_needed=subscribers_limit/2), en_done не трогаем
    - delete неактивных
    - "done" считаем только для (en_done IS NULL OR en_done=0) через пороговую проверку:
      en_done = 0 или en_needed+5
    """
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
        -- en_done не трогаем
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

    sql_tasks_need_probe = """
        SELECT task_id, en_needed
        FROM public.__enrich_priority
        WHERE en_done IS NULL OR en_done = 0
        ORDER BY task_id
    """

    # пороговая проверка: существует ли (en_needed+5)-я запись?
    sql_has_threshold = """
        SELECT 1
        FROM public.raw_contacts_aggr a
        WHERE a.sources @> ARRAY['GPT']::text[]
          AND EXISTS (
              SELECT 1
              FROM public.queue_sys q
              WHERE q.task_id = %s
                AND q.cb_crawler_id = ANY(a.cb_crawler_ids)
          )
        OFFSET %s
        LIMIT 1
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
            cur.execute(sql_delete_inactive)

            cur.execute(sql_tasks_need_probe)
            rows: List[Tuple[int, int]] = [(int(tid), int(need)) for (tid, need) in (cur.fetchall() or [])]

            if not rows:
                conn.commit()
                print("[enrich_updater] no tasks with en_done NULL/0; committed")
                return

            for task_id, en_needed in rows:
                threshold = int(en_needed) + 5
                offset = max(0, threshold - 1)

                cur.execute(sql_has_threshold, (task_id, offset))
                done = threshold if (cur.fetchone() is not None) else 0

                cur.execute(sql_set_done, (done, task_id))
                print(f"[enrich_updater] task_id={task_id} en_needed={en_needed} en_done={done}")

        conn.commit()

    print("[enrich_updater] committed")


def run_batch() -> None:
    sql_pick_task = "SELECT public.__pick_enrich_task_id()"

    sql_task_meta = """
        SELECT workspace_id::text, user_id::text
        FROM public.aap_audience_audiencetask
        WHERE id = %s
    """

    sql_pick_candidates = """
        SELECT
            r.id,
            r.cb_crawler_id,
            r.company_name,
            r.company_data,
            c.branch_id
        FROM public.queue_sys q
        JOIN public.raw_contacts_gb r
          ON r.cb_crawler_id = q.cb_crawler_id
        JOIN public.cb_crawler c
          ON c.id = q.cb_crawler_id
        WHERE q.task_id = %s
          AND r.processed = FALSE
          AND (r.email IS NULL OR btrim(r.email) = '')
        ORDER BY q.rate ASC
        LIMIT %s
        FOR UPDATE OF r SKIP LOCKED
    """

    sql_mark = """
        UPDATE public.raw_contacts_gb
        SET processed = TRUE,
            status = %s,
            updated_at = now()
        WHERE id = %s
    """

    sql_find_aggr = """
        SELECT id, cb_crawler_ids, sources, branches, plz_list, address_list, company_data
        FROM public.raw_contacts_aggr
        WHERE email = %s
        FOR UPDATE
    """

    sql_insert_aggr = """
        INSERT INTO public.raw_contacts_aggr
        (cb_crawler_ids, sources, company_name, branches, plz_list, address_list,
         email, company_data, status_data, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
    """

    sql_update_aggr = """
        UPDATE public.raw_contacts_aggr
        SET cb_crawler_ids=%s,
            sources=%s,
            branches=%s,
            plz_list=%s,
            address_list=%s,
            company_data=%s,
            status_data=%s,
            updated_at=now()
        WHERE id=%s
    """

    print("[val_enrich] batch start")

    with get_connection() as conn:
        with conn.cursor() as cur:
            # 1) pick task (логика в Postgres; python не трогает __enrich_priority)
            cur.execute(sql_pick_task)
            row = cur.fetchone()
            task_id = row[0] if row else None
            if task_id is None:
                print("[val_enrich] no task_id -> exit")
                conn.commit()
                return
            print(f"[val_enrich] task_id={task_id}")

            # 2) task meta for GPT logging
            cur.execute(sql_task_meta, (task_id,))
            meta = cur.fetchone()
            if not meta:
                print(f"[val_enrich] task_id={task_id} not found -> exit")
                conn.commit()
                return
            workspace_id_str, user_id_str = meta

            # 3) pick candidates
            cur.execute(sql_pick_candidates, (task_id, BATCH_SIZE))
            rows = cur.fetchall()
            if not rows:
                print(f"[val_enrich] task_id={task_id}: no candidates -> exit")
                conn.commit()
                return
            print(f"[val_enrich] picked candidates={len(rows)}")

            gpt_items: List[Dict[str, Any]] = []
            db_by_id: Dict[int, Dict[str, Any]] = {}

            pre_errors = 0
            for rid, cb_id, cname, company_data, branch_id in rows:
                company_data = company_data or {}
                if not isinstance(company_data, dict):
                    company_data = {}

                db_by_id[int(rid)] = {
                    "rid": int(rid),
                    "cb_id": int(cb_id),
                    "cname": str(cname),
                    "company_data": company_data,
                    "branch_id": int(branch_id),
                }

                source_url = _extract_source_url(company_data)
                if not source_url:
                    pre_errors += 1
                    cur.execute(sql_mark, (STATUS_ENRICH_ERROR, int(rid)))
                    continue

                gpt_items.append({"id": int(rid), "source_url": source_url})

            if pre_errors:
                print(f"[val_enrich] pre-errors(no source_url)={pre_errors}")

            if not gpt_items:
                print("[val_enrich] nothing to send to GPT -> exit")
                conn.commit()
                return

            # 4) GPT call
            print(f"[val_enrich] gpt request items={len(gpt_items)}")
            gpt = GPTClient()
            resp = gpt.ask(
                tier="maxi-51",
                with_web=True,
                workspace_id=workspace_id_str,
                user_id=user_id_str,
                system=_gpt_system_prompt(),
                user=_gpt_user_payload(gpt_items),
                endpoint="val_enrich",
                use_cache=False,
            )
            print(
                f"[val_enrich] gpt ok, tokens_in={resp.usage.prompt_tokens} tokens_out={resp.usage.completion_tokens}"
            )

            gpt_data = _parse_gpt_json(resp.content)
            if not gpt_data:
                print("[val_enrich] gpt JSON parse failed -> mark all as ENRICH ERROR")
                for it in gpt_items:
                    rid = int(it["id"])
                    cur.execute(sql_mark, (STATUS_ENRICH_ERROR, rid))
                conn.commit()
                return

            by_id = _items_by_id(gpt_data)

            # 5) process each company
            cnt_enriched = 0
            cnt_failed = 0
            cnt_error = 0

            for it in gpt_items:
                rid = int(it["id"])
                info = db_by_id.get(rid)
                if not info:
                    continue

                src_json: Dict[str, Any] = info["company_data"]
                expected_plz = _extract_expected_plz(src_json)

                g = by_id.get(rid)
                if not g:
                    cnt_error += 1
                    cur.execute(sql_mark, (STATUS_ENRICH_ERROR, rid))
                    continue

                g_plz = _trim(g.get("plz"))

                # VALIDATION 1: PLZ
                if not expected_plz or not g_plz or expected_plz != g_plz:
                    cnt_error += 1
                    cur.execute(sql_mark, (STATUS_ENRICH_ERROR, rid))
                    continue

                emails = _normalize_emails(g.get("emails"))

                # VALIDATION 2: EMAILS
                if not emails:
                    cnt_failed += 1
                    cur.execute(sql_mark, (STATUS_ENRICH_FAILED, rid))
                    continue

                # ENRICH
                phones = _normalize_phones(g.get("phones"))
                website = _trim(g.get("website"))
                descr = _trim(g.get("description"))
                sources_urls = _normalize_sources(g.get("sources"))

                norm_gs = _build_norm(info["cname"], src_json)

                gpt_src: Dict[str, Any] = {
                    "plz": g_plz,
                    "website": website,
                    "phone": phones,
                    "email": emails if len(emails) > 1 else emails[0],
                    "description": descr,
                    "source_url": (sources_urls[0] if sources_urls else None),
                }
                norm_gpt = _build_norm(info["cname"], gpt_src)
                norm = _merge_norm(norm_gs, norm_gpt)

                company_data: Dict[str, Any] = {"norm": norm, "gs-1": src_json, "gpt-1": g}
                status_data = _calc_status(company_data["norm"])

                for email_norm in emails:
                    cur.execute(sql_find_aggr, (email_norm,))
                    aggr = cur.fetchone()

                    plz_add = [_trim(src_json.get("plz"))]
                    addr_add = [_trim(src_json.get("address") or src_json.get("address_text"))]

                    if not aggr:
                        cur.execute(
                            sql_insert_aggr,
                            (
                                [info["cb_id"]],
                                _uniq([], [SOURCE_NAME, "GPT"]),
                                info["cname"],
                                [info["branch_id"]],
                                _uniq([], plz_add),
                                _uniq([], addr_add),
                                email_norm,
                                Json(company_data),
                                status_data,
                            ),
                        )
                    else:
                        aggr_id, cb_ids, sources, branches, plz_list, addr_list, aggr_data = aggr
                        aggr_data = aggr_data or {}
                        if not isinstance(aggr_data, dict):
                            aggr_data = {}

                        gs_key = _next_gs_key(aggr_data)
                        aggr_data[gs_key] = src_json

                        gpt_key = "gpt-1"
                        i = 1
                        while gpt_key in aggr_data:
                            i += 1
                            gpt_key = f"gpt-{i}"
                        aggr_data[gpt_key] = g

                        aggr_data["norm"] = _merge_norm(aggr_data.get("norm", {}), norm)
                        status_data2 = _calc_status(aggr_data["norm"])

                        cur.execute(
                            sql_update_aggr,
                            (
                                _uniq(cb_ids, [info["cb_id"]]),
                                _uniq(sources, [SOURCE_NAME, "GPT"]),
                                _uniq(branches, [info["branch_id"]]),
                                _uniq(plz_list, plz_add),
                                _uniq(addr_list, addr_add),
                                Json(aggr_data),
                                status_data2,
                                aggr_id,
                            ),
                        )

                cnt_enriched += 1
                cur.execute(sql_mark, (STATUS_ENRICHED, rid))

            print(f"[val_enrich] done: enriched={cnt_enriched} failed={cnt_failed} error={cnt_error}")

        conn.commit()
        print("[val_enrich] batch committed")


def main() -> None:
    run_priority_updater()
    run_batch()


if __name__ == "__main__":
    main()
