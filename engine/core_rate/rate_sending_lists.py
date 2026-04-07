# FILE: engine/core_rate/rate_sending_lists.py
# DATE: 2026-04-05
# PURPOSE: Fill public.sending_lists.rate for one random active task.

from __future__ import annotations

import json
import re
import time
from html import unescape
from typing import Any, Dict, List
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from psycopg.types.json import Json

from engine.common.db import get_connection
from engine.common.gpt import GPTClient
from engine.common.prompts.process import get_prompt, translate_text
from engine.common.utils import h64_text, parse_json_object, parse_json_response
from engine.core_status.is_active import is_more_needed

WEBSITE_HTTP_TIMEOUT_SEC = 4
WEBSITE_CONTACT_BUDGET_SEC = 12
MAX_PAGE_TEXT_LEN = 8000
MAX_WEBSITE_INPUT_LEN = 12000
MAX_MEANINGFUL_BLOCKS = 30
MIN_WORK_BATCH_SIZE = 15
NOISY_LINE_MARKERS = (
    "cookie",
    "datenschutz",
    "agb",
    "impressum",
    "login",
    "newsletter",
    "kontakt",
)


def _uniq_text_list(values: Any) -> List[str]:
    out: List[str] = []
    if isinstance(values, list):
        raw_values = values
    elif values is None:
        raw_values = []
    else:
        raw_values = [values]

    for value in raw_values:
        text = str(value or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _fetch_text_from_url(url: str) -> str:
    url_value = str(url or "").strip()
    if not url_value:
        return ""

    try:
        request = Request(
            url_value,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                )
            },
        )
        with urlopen(request, timeout=WEBSITE_HTTP_TIMEOUT_SEC) as resp:
            body = resp.read()
    except (HTTPError, URLError, TimeoutError, ValueError):
        return ""
    except Exception:
        return ""

    try:
        html = body.decode("utf-8", errors="ignore")
    except Exception:
        return ""

    body_match = re.search(r"(?is)<body[^>]*>(.*?)</body>", html)
    if not body_match:
        return ""
    html = body_match.group(1)

    html = re.sub(r"(?is)<(header|footer|nav|aside|form|noscript|svg)[^>]*>.*?</\\1>", " ", html)
    html = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", html)
    html = re.sub(r"(?is)<!--.*?-->", " ", html)
    html = re.sub(r"(?is)</?(main|article|section|p|li|h1|h2|h3|h4)[^>]*>", "\n", html)
    html = re.sub(r"(?is)<[^>]+>", " ", html)
    html = unescape(html)
    html = re.sub(r"\r", "\n", html)
    html = re.sub(r"[ \t]+", " ", html)
    html = re.sub(r"\n\s*\n+", "\n\n", html)
    raw_lines = [line.strip() for line in html.splitlines()]
    lines: List[str] = []
    seen_lines: set[str] = set()

    for raw_line in raw_lines:
        line = re.sub(r"\s+", " ", raw_line).strip()
        if not line:
            continue
        line_l = line.lower()
        if any(marker in line_l for marker in NOISY_LINE_MARKERS) and len(line) <= 160:
            continue
        if sum(1 for ch in line if ch in "|>•") >= 3:
            continue
        if len(line.split()) <= 2 and len(line) <= 24:
            continue
        if line in seen_lines:
            continue
        seen_lines.add(line)
        lines.append(line)
        if len(lines) >= MAX_MEANINGFUL_BLOCKS:
            break

    text = "\n\n".join(lines).strip()
    if not text:
        return ""

    if len(text) > MAX_PAGE_TEXT_LEN:
        return text[:MAX_PAGE_TEXT_LEN]
    return text


def _build_contact_text(company_data: Dict[str, Any]) -> str:
    norm = parse_json_object(company_data.get("norm"), field_name="company_data.norm")
    cards = parse_json_object(company_data.get("cards"), field_name="company_data.cards")

    company_name = str(norm.get("company_name") or "").strip()
    company_names = _uniq_text_list(norm.get("company_names"))
    if company_name and company_name in company_names:
        company_names = [value for value in company_names if value != company_name]

    address = str(norm.get("address") or "").strip()
    land = str(norm.get("land") or "").strip()
    if address and land and land not in address:
        address = f"{address}, {land}"

    addresses = _uniq_text_list(norm.get("addresses"))
    if address and address in addresses:
        addresses = [value for value in addresses if value != address]

    categories = _uniq_text_list(norm.get("categories"))
    keywords = _uniq_text_list(norm.get("keywords_11880"))
    statuses = _uniq_text_list(norm.get("statuses_11880"))
    phones = _uniq_text_list(norm.get("phones"))
    socials = _uniq_text_list(norm.get("socials"))
    description_web = str(norm.get("description_web") or "").strip()
    description = str(norm.get("description") or "").strip()

    parts: List[str] = []

    if company_name:
        parts.append(company_name)
    if address:
        parts.append(address)
    if categories:
        parts.append("Geschaeftskategorien:\n" + "\n".join(categories))
    if phones:
        parts.append("\n".join(phones))
    if description_web:
        parts.append(description_web)
    if description:
        parts.append(description)
    if keywords:
        parts.append("Suchanfragen fuer Kataloge:\n" + "\n".join(keywords))
    if statuses:
        parts.append("Statusangaben:\n" + "\n".join(statuses))
    if socials:
        parts.append("Soziale Netzwerke:\n" + "\n".join(socials))
    if company_names:
        parts.append("Weitere Unternehmensnamen:\n" + "\n".join(company_names))
    if addresses:
        parts.append("Weitere Adressen:\n" + "\n".join(addresses))

    return "\n\n".join(part for part in parts if part.strip()).strip()


def _build_rating_instructions(task_type: str, source_product: str, source_company: str, source_geo: str) -> str:
    prompt_name = "rate_contacts_buy" if task_type == "buy" else "rate_contacts_sell"
    prompt_text = (get_prompt(prompt_name) or "").strip()

    product_de = (translate_text(source_product, "de") or "").strip() or source_product
    company_de = (translate_text(source_company, "de") or "").strip() or source_company
    geo_de = (translate_text(source_geo, "de") or "").strip() or source_geo

    if task_type == "buy":
        tail = (
            "Produkt, den das Unternehmen einkauft:\n"
            + product_de.strip()
            + "\n\nEinkaufendes Unternehmen:\n"
            + company_de.strip()
            + "\n\nGeografische Einschraenkungen und Prioritaeten fuer die Suche nach Lieferanten und Auftragnehmern:\n"
            + geo_de.strip()
        )
    else:
        tail = (
            "Produkt, den das Unternehmen verkauft:\n"
            + product_de.strip()
            + "\n\nVerkaufendes Unternehmen:\n"
            + company_de.strip()
            + "\n\nGeografische Einschraenkungen und Prioritaeten fuer die Suche nach Kaeufern:\n"
            + geo_de.strip()
        )

    if prompt_text:
        return (prompt_text + "\n\n" + tail).strip()
    return tail.strip()


def run_once() -> Dict[str, Any]:
    started_at = time.time()
    result: Dict[str, Any] = {
        "task_id": 0,
        "picked_cnt": 0,
        "website_processed_cnt": 0,
        "website_description_cnt": 0,
        "items_cnt": 0,
        "written_cnt": 0,
        "status": "noop",
        "duration_ms": 0,
    }

    lock_conn = get_connection(autocommit=True)
    lock_cur = lock_conn.cursor()
    task_id = 0
    task_type = ""
    user_id = 0
    access_type = ""
    source_product = ""
    source_company = ""
    source_geo = ""

    try:
        lock_cur.execute(
            """
            SELECT
                t.id,
                t.user_id,
                t.type,
                COALESCE(t.source_product, ''),
                COALESCE(t.source_company, ''),
                COALESCE(t.source_geo, ''),
                COALESCE(w.access_type, '')
            FROM public.aap_audience_audiencetask t
            JOIN public.accounts_workspaces w
              ON w.id = t.workspace_id
            WHERE t.active = true
            ORDER BY random()
            LIMIT 1
            """
        )
        row = lock_cur.fetchone()
        if not row:
            result["status"] = "no_task"
            return result

        task_id = int(row[0])
        user_id = int(row[1])
        task_type = str(row[2] or "").strip().lower()
        source_product = str(row[3] or "").strip()
        source_company = str(row[4] or "").strip()
        source_geo = str(row[5] or "").strip()
        access_type = str(row[6] or "").strip().lower()

        lock_cur.execute("SELECT pg_try_advisory_lock(%s)", (int(task_id),))
        lock_row = lock_cur.fetchone()
        if not lock_row or lock_row[0] is not True:
            result["status"] = "task_locked"
            return result

        result["task_id"] = int(task_id)

        batch_size = 20 if access_type == "test" else 50
        task_hash = h64_text(task_type + source_product + source_company + source_geo)

        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT rate
                FROM public.sending_lists
                WHERE task_id = %s
                ORDER BY rate ASC NULLS LAST
                OFFSET 1000
                LIMIT 1
                """,
                (int(task_id),),
            )
            threshold_row = cur.fetchone()
            allow_rehash = threshold_row is None or threshold_row[0] is None

            if allow_rehash:
                cur.execute(
                    """
                    SELECT
                        sl.aggr_contact_cb_id,
                        ac.company_data,
                        ac.website_processed
                    FROM public.sending_lists sl
                    JOIN public.aggr_contacts_cb ac
                      ON ac.id = sl.aggr_contact_cb_id
                    WHERE sl.task_id = %s
                      AND (
                          sl.rate IS NULL
                          OR sl.rating_hash IS DISTINCT FROM %s
                      )
                    ORDER BY sl.rate_cb ASC NULLS LAST
                    LIMIT %s
                    """,
                    (int(task_id), int(task_hash), int(batch_size)),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        sl.aggr_contact_cb_id,
                        ac.company_data,
                        ac.website_processed
                    FROM public.sending_lists sl
                    JOIN public.aggr_contacts_cb ac
                      ON ac.id = sl.aggr_contact_cb_id
                    WHERE sl.task_id = %s
                      AND sl.rate IS NULL
                    ORDER BY sl.rate_cb ASC NULLS LAST
                    LIMIT %s
                    """,
                    (int(task_id), int(batch_size)),
                )

            rows = cur.fetchall() or []
            result["picked_cnt"] = len(rows)
            if not rows:
                result["status"] = "no_batch"
                conn.rollback()
                return result
            if len(rows) < MIN_WORK_BATCH_SIZE:
                result["status"] = "small_batch"
                conn.rollback()
                return result

            items: List[Dict[str, Any]] = []

            for contact_id_raw, company_data_raw, website_processed_raw in rows:
                contact_id = int(contact_id_raw)
                company_data = parse_json_object(company_data_raw, field_name="aggr_contacts_cb.company_data")
                website_processed = bool(website_processed_raw)

                if not website_processed:
                    norm = parse_json_object(company_data.get("norm"), field_name="company_data.norm")
                    description = str(norm.get("description") or "").strip()
                    website_urls = _uniq_text_list(norm.get("websites"))
                    website_text_parts: List[str] = []
                    if len(description) < 100:
                        website_started_at = time.monotonic()

                        for website_url in website_urls[:1]:
                            if (time.monotonic() - website_started_at) >= WEBSITE_CONTACT_BUDGET_SEC:
                                break

                            main_page_text = _fetch_text_from_url(website_url)
                            if main_page_text:
                                website_text_parts.append(main_page_text)

                    description_web = ""
                    website_input = "\n\n".join(part for part in website_text_parts if part.strip()).strip()
                    if website_input:
                        if len(website_input) > MAX_WEBSITE_INPUT_LEN:
                            website_input = website_input[:MAX_WEBSITE_INPUT_LEN]
                        try:
                            website_resp = GPTClient().ask(
                                model="gpt-5-nano",
                                service_tier="flex",
                                user_id=str(user_id),
                                instructions=get_prompt("process_website"),
                                input=website_input,
                                use_cache=False,
                                web_search=False,
                            )
                            description_web = str(website_resp.content or "").strip()
                        except Exception:
                            description_web = ""

                    if description_web:
                        norm["description_web"] = description_web
                        result["website_description_cnt"] = int(result["website_description_cnt"]) + 1

                    company_data["norm"] = norm
                    cur.execute(
                        """
                        UPDATE public.aggr_contacts_cb
                        SET company_data = %s,
                            website_processed = true,
                            updated_at = now()
                        WHERE id = %s
                        """,
                        (Json(company_data), int(contact_id)),
                    )
                    conn.commit()
                    website_processed = True
                    result["website_processed_cnt"] = int(result["website_processed_cnt"]) + 1

                items.append(
                    {
                        "id": int(contact_id),
                        "text": _build_contact_text(company_data),
                    }
                )

            conn.commit()

        result["items_cnt"] = len(items)
        if not items:
            result["status"] = "no_items"
            return result

        instructions = _build_rating_instructions(task_type, source_product, source_company, source_geo)
        payload = json.dumps({"items": items}, ensure_ascii=False)

        try:
            resp = GPTClient().ask(
                model="gpt-5.4",
                service_tier="flex",
                user_id=str(user_id),
                instructions=instructions,
                input=payload,
                use_cache=False,
                web_search=False,
            )
        except Exception:
            result["status"] = "gpt_error"
            return result

        parsed = parse_json_response(resp.content or "")
        if not isinstance(parsed, dict):
            result["status"] = "bad_json"
            return result

        response_items = parsed.get("items")
        if not isinstance(response_items, list):
            result["status"] = "bad_items"
            return result

        allowed_ids = [int(item["id"]) for item in items]
        allowed_set = set(allowed_ids)
        seen_ids: set[int] = set()
        write_rows: List[tuple[int, int, int, int]] = []
        batch_invalid = False

        for response_item in response_items:
            if not isinstance(response_item, dict):
                batch_invalid = True
                break
            try:
                response_id = int(response_item.get("id"))
                response_rate = int(response_item.get("rate"))
            except Exception:
                batch_invalid = True
                break

            if response_id not in allowed_set:
                batch_invalid = True
                break
            if response_id in seen_ids:
                batch_invalid = True
                break
            if response_rate < 1 or response_rate > 100:
                batch_invalid = True
                break

            seen_ids.add(response_id)
            write_rows.append((response_rate, int(task_hash), int(task_id), response_id))

        if batch_invalid or seen_ids != allowed_set:
            result["status"] = "invalid_batch"
            return result

        with get_connection() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE public.sending_lists
                SET rate = %s,
                    rating_hash = %s,
                    updated_at = now()
                WHERE task_id = %s
                  AND aggr_contact_cb_id = %s
                """,
                write_rows,
            )
            conn.commit()

        is_more_needed(int(task_id), update=True)
        result["written_cnt"] = len(write_rows)
        result["status"] = "ok"
        return result

    finally:
        if task_id:
            try:
                lock_cur.execute("SELECT pg_advisory_unlock(%s)", (int(task_id),))
            except Exception:
                pass
        try:
            lock_cur.close()
        except Exception:
            pass
        try:
            lock_conn.close()
        except Exception:
            pass
        result["duration_ms"] = int((time.time() - started_at) * 1000)


def main() -> None:
    print(json.dumps(run_once(), ensure_ascii=False))


if __name__ == "__main__":
    main()
