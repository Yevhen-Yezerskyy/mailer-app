# FILE: engine/core_validate/val_prepare.py  (обновлено — 2025-12-16)
# Fix: psycopg3 требует явной адаптации dict→json/jsonb через psycopg.types.json.Json.
# Остальная логика: перенос/агрегация raw_contacts_gb -> raw_contacts_aggr (dedup по email), как обсуждали.

from __future__ import annotations

from typing import Any, Dict, List, Optional

from psycopg.types.json import Json

from engine.common.db import get_connection

BATCH_SIZE = 100
SOURCE_NAME = "GelbeSeiten"

STATUS_YES_WEB = "YES WEB"
STATUS_NO_WEB_YES_DESCR = "NO WEB - YES DESCR"
STATUS_NO_WEB_NO_DESCR = "NO WEB - NO DESCR"


def _trim(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = v.strip()
    return v or None


def _uniq(base: List[Any], add: List[Any]) -> List[Any]:
    out = list(base or [])
    for x in add:
        if x is None:
            continue
        if x not in out:
            out.append(x)
    return out


def _next_gs_key(company_data: Dict[str, Any]) -> str:
    i = 1
    while True:
        k = f"gs-{i}"
        if k not in company_data:
            return k
        i += 1


def _calc_status(norm: Dict[str, Any]) -> str:
    if _trim(norm.get("website")):
        return STATUS_YES_WEB
    if _trim(norm.get("description")):
        return STATUS_NO_WEB_YES_DESCR
    return STATUS_NO_WEB_NO_DESCR


def _build_norm(company_name: str, src: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "company_name": _trim(company_name),
        "source_urls": [_trim(src.get("source_url"))] if _trim(src.get("source_url")) else [],
        "branches": src.get("branches") or [],
        "address": _trim(src.get("address") or src.get("address_text")),
        "city": _trim(src.get("city")),
        "plz": _trim(src.get("plz")),
        "phone": src.get("phone") or [],
        "email": src.get("email"),
        "fax": src.get("fax"),
        "website": _trim(src.get("website")),
        "socials": src.get("socials") or [],
        "description": _trim(src.get("description")),
    }


def _merge_norm(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(dst or {})

    # скаляры — только если пусто
    for k in ("company_name", "address", "city", "plz", "website", "fax", "description"):
        if not _trim(out.get(k)):
            out[k] = src.get(k)

    # массивы — уникально
    for k in ("source_urls", "phone", "socials", "branches"):
        out[k] = _uniq(out.get(k, []) or [], (src.get(k, []) or []))

    # email — str | list
    def to_list(v):
        if v is None:
            return []
        return v if isinstance(v, list) else [v]

    emails = _uniq(to_list(out.get("email")), to_list(src.get("email")))
    if not emails:
        out["email"] = None
    elif len(emails) == 1:
        out["email"] = emails[0]
    else:
        out["email"] = emails

    return out


def run_batch() -> None:
    sql_pick = """
        SELECT id, cb_crawler_id, company_name, email, company_data
        FROM raw_contacts_gb
        WHERE processed = false
          AND processed_email = true
          AND status_email = 'OK'
          AND email IS NOT NULL
          AND btrim(email) <> ''
        ORDER BY id
        LIMIT %s
        FOR UPDATE SKIP LOCKED
    """

    sql_cb = "SELECT branch_id, plz FROM cb_crawler WHERE id = %s"

    sql_find_aggr = """
        SELECT id, cb_crawler_ids, sources, branches, plz_list, address_list, company_data
        FROM raw_contacts_aggr
        WHERE email = %s
        FOR UPDATE
    """

    sql_insert = """
        INSERT INTO raw_contacts_aggr
        (cb_crawler_ids, sources, company_name, branches, plz_list, address_list,
         email, company_data, status_data, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
    """

    sql_update = """
        UPDATE raw_contacts_aggr
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

    sql_mark_done = """
        UPDATE raw_contacts_gb
        SET processed=true, updated_at=now()
        WHERE id=%s
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_pick, (BATCH_SIZE,))
            rows = cur.fetchall()

            for src_id, cb_crawler_id, cname, email, src_json in rows:
                email_norm = email.strip().lower()

                cur.execute(sql_cb, (cb_crawler_id,))
                cb = cur.fetchone()
                if not cb:
                    continue
                branch_id, cb_plz = cb

                src_json = src_json or {}
                if not isinstance(src_json, dict):
                    src_json = {}

                norm_src = _build_norm(cname, src_json)

                cur.execute(sql_find_aggr, (email_norm,))
                aggr = cur.fetchone()

                plz_add = [_trim(src_json.get("plz")), _trim(cb_plz)]
                addr_add = [_trim(src_json.get("address") or src_json.get("address_text"))]

                if not aggr:
                    company_data = {"norm": norm_src, "gs-1": src_json}
                    status = _calc_status(company_data["norm"])

                    cur.execute(
                        sql_insert,
                        (
                            [cb_crawler_id],
                            [SOURCE_NAME],
                            cname,
                            [branch_id],
                            _uniq([], plz_add),
                            _uniq([], addr_add),
                            email_norm,
                            Json(company_data),   # <-- FIX
                            status,
                        ),
                    )
                else:
                    aggr_id, cb_ids, sources, branches, plz_list, addr_list, aggr_data = aggr
                    aggr_data = aggr_data or {}
                    if not isinstance(aggr_data, dict):
                        aggr_data = {}

                    gs_key = _next_gs_key(aggr_data)
                    aggr_data[gs_key] = src_json

                    aggr_data["norm"] = _merge_norm(aggr_data.get("norm", {}), norm_src)
                    status = _calc_status(aggr_data["norm"])

                    cur.execute(
                        sql_update,
                        (
                            _uniq(cb_ids, [cb_crawler_id]),
                            _uniq(sources, [SOURCE_NAME]),
                            _uniq(branches, [branch_id]),
                            _uniq(plz_list, plz_add),
                            _uniq(addr_list, addr_add),
                            Json(aggr_data),      # <-- FIX
                            status,
                            aggr_id,
                        ),
                    )

                cur.execute(sql_mark_done, (src_id,))

        conn.commit()


def main() -> None:
    run_batch()


if __name__ == "__main__":
    main()
