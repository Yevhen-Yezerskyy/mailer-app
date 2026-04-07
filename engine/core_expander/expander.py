# FILE: engine/core_expander/expander.py
# DATE: 2026-03-31
# PURPOSE: Aggregate raw_contacts_cb into aggr_contacts_cb / cb_contacts.

from __future__ import annotations

import json
import os
import random
import time
from typing import Any, Dict, List, Optional

from psycopg.types.json import Json

from engine.common.cache.client import CLIENT
from engine.common.db import get_connection
from engine.common.logs import log
from engine.common.utils import (
    email_domain_from_email,
    email_has_mx,
    email_is_bad_syntax,
    load_email_domains_allowlist,
)
from engine.core_status.is_active import is_more_needed

RAW_BATCH_SIZE = 100
SENDING_LIST_LIMIT = 50000

STATUS_EMPTY = "EMPTY"
STATUS_INVALID = "INVALID"
STATUS_CREATED = "CREATED"
STATUS_UPDATED = "UPDATED"

_ALLOWLIST = load_email_domains_allowlist()
_MX_CACHE: Dict[str, bool] = {}
_SENDING_HASH_TTL_MIN_SEC = 24 * 60 * 60
_SENDING_HASH_TTL_MAX_SEC = 2 * 24 * 60 * 60
_RATE_NULL_ORD = 9223372036854775807
_SENDING_TASK_LOCK_TTL_SEC = 300


def _log_line(branch: str, message: str) -> None:
    line = f"[core_expander:{branch}] {message}"
    print(line, flush=True)
    log("expander.log", "crawler", line)


def _log_json(branch: str, payload: Dict[str, Any]) -> None:
    _log_line(branch, json.dumps(payload, ensure_ascii=True, default=str))


def _trim_str(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return value.strip()


def _safe_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            value = bytes(value).decode("utf-8", errors="strict")
        except Exception:
            return {}
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return {}
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _safe_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    return [value]


def _drop_empty(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    if isinstance(value, list):
        out: List[Any] = []
        for item in value:
            item_clean = _drop_empty(item)
            if item_clean is None:
                continue
            out.append(item_clean)
        return out or None
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            item_clean = _drop_empty(item)
            if item_clean is None:
                continue
            out[str(key)] = item_clean
        return out or None
    return value


def _uniq_keep_order(values: List[str]) -> List[str]:
    out: List[str] = []
    for value in values:
        if value and value not in out:
            out.append(value)
    return out


def _text_list(values: Any) -> List[str]:
    out: List[str] = []
    for item in _safe_list(values):
        text = _trim_str(item)
        if text:
            out.append(text)
    return _uniq_keep_order(out)


def _email_list(values: Any) -> List[str]:
    out: List[str] = []
    for item in _safe_list(values):
        email_value = _normalize_email_syntax_only(item)
        if email_value:
            out.append(email_value)
    return _uniq_keep_order(out)


def _normalize_lookup_city(value: Any) -> str:
    text = _trim_str(value)
    if not text:
        return ""
    if "," in text:
        text = text.split(",", 1)[0].strip()
    return text


def _normalize_email_syntax_only(value: Any) -> Optional[str]:
    text = _trim_str(value).lower()
    if not text:
        return None
    if email_is_bad_syntax(text):
        return None
    return text


def _normalize_primary_email(value: Any) -> Optional[str]:
    text = _trim_str(value).lower()
    if not text:
        return None
    if email_is_bad_syntax(text):
        return None
    domain = email_domain_from_email(text)
    if not domain:
        return None
    if domain in _ALLOWLIST:
        return text
    cached = _MX_CACHE.get(domain)
    if cached is None:
        cached = bool(email_has_mx(domain))
        _MX_CACHE[domain] = cached
    return text if cached else None


def _compose_address(address: str, street: str, plz: str, city: str) -> str:
    if address:
        return address
    tail = " ".join([part for part in [plz, city] if part]).strip()
    if street and tail:
        return f"{street}, {tail}"
    return street or tail


def _merge_text_list(norm: Dict[str, Any], key: str, values: Any) -> None:
    existing = _text_list(norm.get(key))
    merged = _uniq_keep_order(existing + _text_list(values))
    if merged:
        norm[key] = merged
    else:
        norm.pop(key, None)


def _merge_description(norm: Dict[str, Any], value: Any) -> None:
    text = _trim_str(value)
    if not text:
        return
    current = _trim_str(norm.get("description"))
    if not current:
        norm["description"] = text
        return
    parts = [part for part in current.split("\n\n") if part.strip()]
    if text in parts:
        return
    parts.append(text)
    norm["description"] = "\n\n".join(parts)


def _merge_single_plural(
    norm: Dict[str, Any],
    *,
    single_key: str,
    plural_key: str,
    values: Any,
    is_email: bool = False,
) -> None:
    normalize = _email_list if is_email else _text_list
    current_single = norm.get(single_key)
    current_plural = norm.get(plural_key)

    merged = normalize([current_single] + _safe_list(current_plural) + _safe_list(values))
    if not merged:
        norm.pop(single_key, None)
        norm.pop(plural_key, None)
        return
    norm[single_key] = merged[0]
    if len(merged) > 1:
        norm[plural_key] = merged
    else:
        norm.pop(plural_key, None)


def _lookup_cb_meta(cur, cb_id: int) -> Dict[str, str]:
    cur.execute(
        """
        SELECT
          ps.plz,
          split_part(COALESCE(cs.name, ''), ',', 1) AS city_name,
          COALESCE(cs.state_name, '') AS land,
          COALESCE(bs.branch_name, '') AS branch_name,
          COALESCE(bs.catalog, '') AS catalog
        FROM public.cb_crawl_pairs cp
        JOIN public.plz_sys ps
          ON ps.id = cp.plz_id
        JOIN public.branches_sys bs
          ON bs.id = cp.branch_id
        LEFT JOIN public.__city__plz_map m
          ON m.plz = ps.plz
        LEFT JOIN public.cities_sys cs
          ON cs.id = m.city_id
        WHERE cp.id = %s
        LIMIT 1
        """,
        (int(cb_id),),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"cb_crawl_pairs row not found for cb_id={int(cb_id)}")
    plz, city, land, branch, catalog = row
    meta = {
        "plz": _trim_str(plz),
        "city": _normalize_lookup_city(city),
        "land": _trim_str(land),
        "branch": _trim_str(branch),
        "catalog": _trim_str(catalog),
    }
    cleaned = _drop_empty(meta)
    return dict(cleaned) if isinstance(cleaned, dict) else {}


def _build_cb_entry(cb_meta: Dict[str, str]) -> Dict[str, Any]:
    cleaned = _drop_empty(
        {
            "plz": cb_meta.get("plz"),
            "city": cb_meta.get("city"),
            "branch": cb_meta.get("branch"),
            "land": cb_meta.get("land"),
            "catalog": cb_meta.get("catalog"),
        }
    )
    return dict(cleaned) if isinstance(cleaned, dict) else {}


def _build_card_entry(cb_meta: Dict[str, str], url: Any, card: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = _drop_empty(
        {
            "plz": cb_meta.get("plz"),
            "city": cb_meta.get("city"),
            "branch": cb_meta.get("branch"),
            "land": cb_meta.get("land"),
            "catalog": cb_meta.get("catalog"),
            "url": _trim_str(url),
            "card": _drop_empty(card),
        }
    )
    return dict(cleaned) if isinstance(cleaned, dict) else {}


def _apply_card_to_norm(
    norm_in: Dict[str, Any],
    *,
    primary_email: str,
    card: Dict[str, Any],
    cb_meta: Dict[str, str],
) -> Dict[str, Any]:
    norm = dict(norm_in or {})

    company_name = _trim_str(card.get("company_name"))
    city_card = _trim_str(card.get("city"))
    land_card = _trim_str(card.get("land"))
    city_value = city_card or cb_meta.get("city") or ""
    land_value = _trim_str(norm.get("land")) or land_card or cb_meta.get("land") or ""
    plz_value = _trim_str(card.get("plz")) or cb_meta.get("plz") or ""
    street_value = _trim_str(card.get("street"))
    address_value = _compose_address(
        _trim_str(card.get("address")),
        street_value,
        plz_value,
        city_value,
    )

    category_values = _text_list(card.get("categories_gs")) + _text_list(card.get("categories_11880"))
    website_values = _text_list(_safe_list(card.get("website")) + _safe_list(card.get("websites")))
    email_values = [primary_email] + _email_list(card.get("emails"))

    _merge_single_plural(norm, single_key="company_name", plural_key="company_names", values=[company_name])
    _merge_single_plural(norm, single_key="email", plural_key="emails", values=email_values, is_email=True)
    if city_value and not _trim_str(norm.get("city")):
        norm["city"] = city_value
    if land_value and not _trim_str(norm.get("land")):
        norm["land"] = land_value
    _merge_single_plural(norm, single_key="address", plural_key="addresses", values=[address_value])
    _merge_text_list(norm, "categories", category_values)
    _merge_text_list(norm, "phones", card.get("phones"))
    _merge_text_list(norm, "fax", card.get("fax"))
    _merge_text_list(norm, "websites", website_values)
    _merge_text_list(norm, "socials", card.get("socials"))
    _merge_text_list(norm, "statuses_11880", card.get("statuses_11880"))
    _merge_text_list(norm, "keywords_11880", card.get("keywords_11880"))
    _merge_description(norm, card.get("description"))

    cleaned = _drop_empty(norm)
    return dict(cleaned) if isinstance(cleaned, dict) else {}


def _mark_raw_processed(cur, raw_id: int) -> None:
    cur.execute(
        """
        UPDATE public.raw_contacts_cb
        SET processed = true
        WHERE id = %s
        """,
        (int(raw_id),),
    )


def _get_existing_aggr(cur, email: str):
    cur.execute(
        """
        SELECT id, company_name, company_data
        FROM public.aggr_contacts_cb
        WHERE lower(btrim(email)) = %s
        FOR UPDATE
        """,
        (str(email),),
    )
    return cur.fetchone()


def _insert_cb_link(cur, aggr_contact_id: int, cb_id: int) -> int:
    cur.execute(
        """
        INSERT INTO public.cb_contacts (aggr_contact_id, cb_id)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        (int(aggr_contact_id), int(cb_id)),
    )
    return 1 if cur.rowcount > 0 else 0


def _create_company_data(
    *,
    primary_email: str,
    card: Dict[str, Any],
    cb_meta: Dict[str, str],
    cb_id: int,
    url: Any,
) -> Dict[str, Any]:
    norm = _apply_card_to_norm({}, primary_email=primary_email, card=card, cb_meta=cb_meta)
    cb_key = str(int(cb_id))
    company_data = {
        "norm": norm,
        "CB": {cb_key: _build_cb_entry(cb_meta)},
        "cards": {cb_key: _build_card_entry(cb_meta, url, card)},
    }
    cleaned = _drop_empty(company_data)
    return dict(cleaned) if isinstance(cleaned, dict) else {}


def _update_company_data(
    existing_company_data: Dict[str, Any],
    *,
    primary_email: str,
    card: Dict[str, Any],
    cb_meta: Dict[str, str],
    cb_id: int,
    url: Any,
) -> Dict[str, Any]:
    company_data = dict(existing_company_data or {})
    norm = _safe_dict(company_data.get("norm"))
    cb_block = _safe_dict(company_data.get("CB"))
    cards_block = _safe_dict(company_data.get("cards"))

    norm = _apply_card_to_norm(norm, primary_email=primary_email, card=card, cb_meta=cb_meta)
    cb_block[str(int(cb_id))] = _build_cb_entry(cb_meta)
    cards_block[str(int(cb_id))] = _build_card_entry(cb_meta, url, card)

    company_data["norm"] = norm
    company_data["CB"] = cb_block
    company_data["cards"] = cards_block
    cleaned = _drop_empty(company_data)
    return dict(cleaned) if isinstance(cleaned, dict) else {}


def _create_contact(cur, *, email: str, company_data: Dict[str, Any]) -> Optional[int]:
    company_name = _trim_str(_safe_dict(company_data.get("norm")).get("company_name"))
    cur.execute(
        """
        INSERT INTO public.aggr_contacts_cb (email, company_name, company_data)
        VALUES (%s, %s, %s)
        ON CONFLICT ((lower(btrim(email)))) DO NOTHING
        RETURNING id
        """,
        (str(email), company_name, Json(company_data)),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def _update_contact(
    cur,
    *,
    aggr_contact_id: int,
    existing_company_name: str,
    existing_company_data: Dict[str, Any],
    email: str,
    card: Dict[str, Any],
    cb_meta: Dict[str, str],
    cb_id: int,
    url: Any,
) -> int:
    updated_company_data = _update_company_data(
        existing_company_data,
        primary_email=email,
        card=card,
        cb_meta=cb_meta,
        cb_id=cb_id,
        url=url,
    )
    norm = _safe_dict(updated_company_data.get("norm"))
    company_name_new = _trim_str(existing_company_name) or _trim_str(norm.get("company_name"))

    cur.execute(
        """
        UPDATE public.aggr_contacts_cb
        SET company_name = %s,
            company_data = %s,
            updated_at = now()
        WHERE id = %s
        """,
        (
            company_name_new,
            Json(updated_company_data),
            int(aggr_contact_id),
        ),
    )
    return int(aggr_contact_id)


def _process_valid_email(
    cur,
    *,
    raw_id: int,
    cb_id: int,
    email: str,
    card: Dict[str, Any],
    url: Any,
) -> tuple[str, int]:
    cb_meta = _lookup_cb_meta(cur, int(cb_id))
    existing = _get_existing_aggr(cur, email)

    if existing:
        aggr_contact_id, company_name, company_data = existing
        aggr_contact_id = _update_contact(
            cur,
            aggr_contact_id=int(aggr_contact_id),
            existing_company_name=_trim_str(company_name),
            existing_company_data=_safe_dict(company_data),
            email=email,
            card=card,
            cb_meta=cb_meta,
            cb_id=int(cb_id),
            url=url,
        )
        inserted_cb_link = _insert_cb_link(cur, aggr_contact_id, int(cb_id))
        _mark_raw_processed(cur, int(raw_id))
        return STATUS_UPDATED, inserted_cb_link

    company_data = _create_company_data(
        primary_email=email,
        card=card,
        cb_meta=cb_meta,
        cb_id=int(cb_id),
        url=url,
    )
    inserted_id = _create_contact(cur, email=email, company_data=company_data)

    if inserted_id is None:
        existing = _get_existing_aggr(cur, email)
        if not existing:
            raise RuntimeError(f"aggr_contacts_cb row not found after conflict for email={email}")
        aggr_contact_id, company_name, company_data_existing = existing
        aggr_contact_id = _update_contact(
            cur,
            aggr_contact_id=int(aggr_contact_id),
            existing_company_name=_trim_str(company_name),
            existing_company_data=_safe_dict(company_data_existing),
            email=email,
            card=card,
            cb_meta=cb_meta,
            cb_id=int(cb_id),
            url=url,
        )
        inserted_cb_link = _insert_cb_link(cur, aggr_contact_id, int(cb_id))
        _mark_raw_processed(cur, int(raw_id))
        return STATUS_UPDATED, inserted_cb_link

    inserted_cb_link = _insert_cb_link(cur, int(inserted_id), int(cb_id))
    _mark_raw_processed(cur, int(raw_id))
    return STATUS_CREATED, inserted_cb_link


def _process_one(cur, raw_id: int, cb_id: int, card: Any, url: Any) -> tuple[str, int]:
    card_data = _safe_dict(card)

    email_raw = _trim_str(card_data.get("email"))
    if not email_raw:
        _mark_raw_processed(cur, int(raw_id))
        return STATUS_EMPTY, 0

    email_norm = _normalize_primary_email(email_raw)
    if not email_norm:
        _mark_raw_processed(cur, int(raw_id))
        return STATUS_INVALID, 0

    return _process_valid_email(
        cur,
        raw_id=int(raw_id),
        cb_id=int(cb_id),
        email=email_norm,
        card=card_data,
        url=url,
    )


def run_batch() -> Dict[str, int]:
    started_at = time.time()
    counts = {
        "picked": 0,
        "inserted_aggr": 0,
        "updated_aggr": 0,
        "inserted_cb_links": 0,
        "skipped_empty_email": 0,
        "skipped_invalid_email": 0,
        "processed_raw": 0,
        "duration_ms": 0,
    }

    sql_pick = """
        SELECT id, cb_id, card, url
        FROM public.raw_contacts_cb
        WHERE processed = false
        ORDER BY id
        LIMIT %s
        FOR UPDATE SKIP LOCKED
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql_pick, (int(RAW_BATCH_SIZE),))
        rows = cur.fetchall() or []
        counts["picked"] = len(rows)

        for idx, row in enumerate(rows, start=1):
            raw_id, cb_id, card, url = row
            savepoint_name = f"sp_expander_{idx}"
            cur.execute(f"SAVEPOINT {savepoint_name}")
            try:
                status, inserted_cb_link = _process_one(cur, int(raw_id), int(cb_id), card, url)
                if status == STATUS_CREATED:
                    counts["inserted_aggr"] += 1
                    counts["inserted_cb_links"] += inserted_cb_link
                    counts["processed_raw"] += 1
                elif status == STATUS_UPDATED:
                    counts["updated_aggr"] += 1
                    counts["processed_raw"] += 1
                    counts["inserted_cb_links"] += inserted_cb_link
                elif status == STATUS_EMPTY:
                    counts["skipped_empty_email"] += 1
                    counts["processed_raw"] += 1
                elif status == STATUS_INVALID:
                    counts["skipped_invalid_email"] += 1
                    counts["processed_raw"] += 1
                cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            except Exception as exc:
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                _mark_raw_processed(cur, int(raw_id))
                cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                counts["processed_raw"] += 1
                _log_line("raw_to_aggr", f"raw_id={int(raw_id)} cb_id={int(cb_id)} error={type(exc).__name__}: {exc}")

        conn.commit()

    counts["duration_ms"] = int((time.time() - started_at) * 1000)
    _log_json("raw_to_aggr", counts)
    return counts


def _pick_sending_task(cur) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
          id,
          COALESCE(rating_city_hash::text, '') AS rating_city_hash,
          COALESCE(rating_branch_hash::text, '') AS rating_branch_hash
        FROM public.aap_audience_audiencetask
        WHERE active = true
        ORDER BY random()
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "rating_city_hash": str(row[1] or ""),
        "rating_branch_hash": str(row[2] or ""),
    }


def _sending_hash_cache_key(task_id: int) -> str:
    return f"core_expander:sending_hash:{int(task_id)}"


def _sending_task_lock_key(task_id: int) -> str:
    return f"core_expander:sending_list:task:{int(task_id)}"


def _try_lock_sending_task(task_id: int, owner: str) -> Optional[str]:
    resp = CLIENT.lock_try(
        _sending_task_lock_key(int(task_id)),
        ttl_sec=_SENDING_TASK_LOCK_TTL_SEC,
        owner=str(owner),
    )
    if not resp or resp.get("acquired") is not True or not isinstance(resp.get("token"), str):
        return None
    return str(resp["token"])


def _release_sending_task_lock(task_id: int, token: Optional[str]) -> None:
    if not token:
        return
    try:
        CLIENT.lock_release(_sending_task_lock_key(int(task_id)), token=str(token))
    except Exception:
        pass


def _get_cached_sending_hash(task_id: int) -> str:
    raw = CLIENT.get(_sending_hash_cache_key(int(task_id)), ttl_sec=1)
    if raw is None:
        return ""
    try:
        return bytes(raw).decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def _set_cached_sending_hash(task_id: int, task_hash: str) -> None:
    ttl_sec = random.randint(_SENDING_HASH_TTL_MIN_SEC, _SENDING_HASH_TTL_MAX_SEC)
    CLIENT.set(
        _sending_hash_cache_key(int(task_id)),
        str(task_hash).encode("utf-8"),
        ttl_sec=ttl_sec,
    )


def _current_prefix_candidate_top_sql() -> str:
    return """
        WITH first_hole AS MATERIALIZED (
            SELECT
                COALESCE(tcr.rate::bigint, %s::bigint) AS hole_rate_ord,
                tcr.id AS hole_id
            FROM public.task_cb_ratings tcr
            JOIN public.cb_crawl_pairs cp
              ON cp.id = tcr.cb_id
            WHERE tcr.task_id = %s
              AND cp.collected = false
            ORDER BY tcr.rate ASC NULLS LAST, tcr.id ASC
            LIMIT 1
        ),
        candidate_best AS (
            SELECT DISTINCT ON (cc.aggr_contact_id)
                tcr.task_id,
                cc.aggr_contact_id AS aggr_contact_cb_id,
                tcr.cb_id,
                tcr.rate AS rate_cb,
                tcr.id AS task_cb_rating_id
            FROM public.task_cb_ratings tcr
            JOIN public.cb_crawl_pairs cp
              ON cp.id = tcr.cb_id
            JOIN public.cb_contacts cc
              ON cc.cb_id = tcr.cb_id
            LEFT JOIN first_hole fh
              ON true
            WHERE tcr.task_id = %s
              AND cp.collected = true
              AND (
                  fh.hole_id IS NULL
                  OR COALESCE(tcr.rate::bigint, %s::bigint) < fh.hole_rate_ord
                  OR (
                      COALESCE(tcr.rate::bigint, %s::bigint) = fh.hole_rate_ord
                      AND tcr.id < fh.hole_id
                  )
              )
            ORDER BY cc.aggr_contact_id, tcr.rate ASC NULLS LAST, tcr.id ASC
        ),
        candidate_top AS (
            SELECT
                task_id,
                cb_id,
                aggr_contact_cb_id,
                rate_cb
            FROM candidate_best
            ORDER BY rate_cb ASC NULLS LAST, task_cb_rating_id ASC, aggr_contact_cb_id ASC
            LIMIT %s
        )
    """


def _current_prefix_candidate_top_params(task_id: int) -> tuple[int, int, int, int, int, int]:
    return (
        int(_RATE_NULL_ORD),
        int(task_id),
        int(task_id),
        int(_RATE_NULL_ORD),
        int(_RATE_NULL_ORD),
        int(SENDING_LIST_LIMIT),
    )


def _rebuild_upsert_sending_list(cur, task_id: int) -> int:
    cur.execute(
        _current_prefix_candidate_top_sql()
        + """
        INSERT INTO public.sending_lists (
            task_id,
            cb_id,
            aggr_contact_cb_id,
            rate_cb
        )
        SELECT
            task_id,
            cb_id,
            aggr_contact_cb_id,
            rate_cb
        FROM candidate_top
        ON CONFLICT (task_id, aggr_contact_cb_id) DO UPDATE
        SET cb_id = EXCLUDED.cb_id,
            rate_cb = EXCLUDED.rate_cb,
            updated_at = now()
        WHERE public.sending_lists.cb_id IS DISTINCT FROM EXCLUDED.cb_id
           OR public.sending_lists.rate_cb IS DISTINCT FROM EXCLUDED.rate_cb
        """,
        _current_prefix_candidate_top_params(int(task_id)),
    )
    return int(cur.rowcount or 0)


def _delete_rebuild_sending_list_missing(cur, task_id: int) -> int:
    cur.execute(
        _current_prefix_candidate_top_sql()
        + """
        DELETE FROM public.sending_lists sl
        WHERE sl.task_id = %s
          AND sl.rate IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM candidate_top ct
              WHERE ct.task_id = sl.task_id
                AND ct.aggr_contact_cb_id = sl.aggr_contact_cb_id
          )
        """,
        (
            *_current_prefix_candidate_top_params(int(task_id)),
            int(task_id),
        ),
    )
    return int(cur.rowcount or 0)


def _insert_incremental_sending_list(cur, task_id: int) -> int:
    cur.execute(
        _current_prefix_candidate_top_sql()
        + """
        INSERT INTO public.sending_lists (
            task_id,
            cb_id,
            aggr_contact_cb_id,
            rate_cb
        )
        SELECT
            task_id,
            cb_id,
            aggr_contact_cb_id,
            rate_cb
        FROM candidate_top
        ON CONFLICT (task_id, aggr_contact_cb_id) DO NOTHING
        """,
        _current_prefix_candidate_top_params(int(task_id)),
    )
    return int(cur.rowcount or 0)


def _run_rebuild_sending_list(cur, task_id: int) -> Dict[str, int]:
    upserted_rows = _rebuild_upsert_sending_list(cur, int(task_id))
    deleted_rows = _delete_rebuild_sending_list_missing(cur, int(task_id))
    return {
        "deleted_rows": int(deleted_rows),
        "upserted_rows": int(upserted_rows),
    }


def _run_incremental_sending_list(cur, task_id: int) -> Dict[str, int]:
    upserted_rows = _insert_incremental_sending_list(cur, int(task_id))
    return {
        "deleted_rows": 0,
        "upserted_rows": int(upserted_rows),
    }


def run_sending_list_batch() -> Dict[str, int]:
    started_at = time.time()
    counts = {
        "task_id": 0,
        "upserted_rows": 0,
        "deleted_rows": 0,
        "rebuilt": 0,
        "duration_ms": 0,
    }

    with get_connection() as conn, conn.cursor() as cur:
        task = _pick_sending_task(cur)
        if not task:
            counts["duration_ms"] = int((time.time() - started_at) * 1000)
            _log_json("sending_lists", {"reason": "no_active_task", **counts})
            return counts

        task_id = int(task["id"])
        counts["task_id"] = int(task_id)
        lock_owner = f"{os.getpid()}:{int(time.time())}"
        lock_token = _try_lock_sending_task(int(task_id), lock_owner)
        if not lock_token:
            counts["duration_ms"] = int((time.time() - started_at) * 1000)
            _log_json("sending_lists", {"reason": "task_locked", **counts})
            return counts

        try:
            task_hash = f"{str(task['rating_city_hash'])}:{str(task['rating_branch_hash'])}"
            cached_hash = _get_cached_sending_hash(int(task_id))
            must_rebuild = (not cached_hash) or (cached_hash != task_hash)

            if must_rebuild:
                counts["rebuilt"] = 1
                result = _run_rebuild_sending_list(cur, int(task_id))
            else:
                result = _run_incremental_sending_list(cur, int(task_id))

            counts["deleted_rows"] = int(result["deleted_rows"])
            counts["upserted_rows"] = int(result["upserted_rows"])
            conn.commit()
        finally:
            _release_sending_task_lock(int(task_id), lock_token)

    if int(counts["rebuilt"]) == 1:
        _set_cached_sending_hash(int(task_id), task_hash)

    if int(counts["upserted_rows"]) > 0 or int(counts["deleted_rows"]) > 0:
        is_more_needed(int(task_id), update=True)

    counts["duration_ms"] = int((time.time() - started_at) * 1000)
    _log_json("sending_lists", counts)
    return counts


def main() -> None:
    run_batch()


if __name__ == "__main__":
    main()
