# FILE: web/mailer_web/format_data.py
# DATE: 2026-01-05
# (новое) 2026-01-05
# PURPOSE:
#   format_data v8:
#   - UI зовёт ТОЛЬКО публичные функции (get_contact / get_ratings / build_contact_packet).
#   - *_raw спрятаны внутри.
#   - CONTACT (печать): raw_contacts_aggr.{id,email,company_data(norm)} + город по norm.plz (через cb_crawler.plz → city_id → cities_sys).
#     Если company_data не dict ИЛИ norm отсутствует/пустой -> raise (катастрофа).
#   - RATINGS: контракт рейтингов сохранён, но city-поля приведены к новому контракту:
#       city      = cities_sys.name (просто город)
#       city_norm = normalize_city(city)
#       city_land = city_norm + " - " + state_name
#     То же самое в ratings.chosen_cb (вместо city_str).
#   - _choose_cb_id memoized на TTL_HOUR (как ratings_raw) — второй проход быстрый.
#   - Убрана мёртвая ветка (дублирующий return) в get_contact_raw.

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from django.db import connection
from django.utils.html import escape

from engine.common.cache.client import memo
import json
import re


TTL_WEEK = 7 * 24 * 60 * 60
TTL_HOUR = 60 * 60
MEMO_VERSION = "format_data:v8"


# ============================================================
# helpers (NO SQL, NO memo)
# ============================================================

def _is_de_lang(ui_lang: str) -> bool:
    s = (ui_lang or "").strip().lower()
    return (s == "de") or s.startswith("de-")


def normalize_city(name: str) -> str:
    """
    Нормализация города (пока примитивная).
    Сейчас: отрезаем ', Stadt'. Потом можно расширять.
    """
    name = (name or "").strip()
    return name[:-7].rstrip() if name.endswith(", Stadt") else name


def _html_block(text: str) -> str:
    return f'<span class="YY-BLOCK">{escape(text)}</span>' if text else ""


def _html_link(url: str) -> str:
    if not url:
        return ""

    text = url
    if len(text) > 80:
        text = text[:70] + "."

    return (
        f'<a class="YY-LINK" href="{escape(url)}" '
        f'target="_blank" rel="noopener noreferrer">{escape(text)}</a>'
    )


def _html_mailto(email: str) -> str:
    if not email:
        return ""
    return (
        f'<a class="YY-LINK" href="mailto:{escape(email)}" '
        f'target="_blank" rel="noopener noreferrer">{escape(email)}</a>'
    )


def _must_company_data(v: Any) -> Dict[str, Any]:
    if not isinstance(v, dict):
        raise ValueError("CONTACT_COMPANY_DATA_NOT_JSON")
    return v


def _must_norm(company_data: Dict[str, Any]) -> Dict[str, Any]:
    n = company_data.get("norm")
    if not isinstance(n, dict):
        raise ValueError("CONTACT_NORM_MISSING")
    if not n:
        raise ValueError("CONTACT_NORM_EMPTY")
    return n


def _norm_str(n: Dict[str, Any], key: str) -> str:
    v = n.get(key)
    return v.strip() if isinstance(v, str) and v.strip() else ""


def _norm_list_str(n: Dict[str, Any], key: str) -> List[str]:
    v = n.get(key)
    if isinstance(v, str) and v.strip():
        return [v.strip()]
    if not isinstance(v, list):
        return []
    out: List[str] = []
    for x in v:
        if isinstance(x, str) and x.strip():
            out.append(x.strip())
    return out


def _norm_email_candidates(n: Dict[str, Any]) -> List[str]:
    # из norm может прилетать email или emails
    out: List[str] = []
    for k in ("email", "emails"):
        vv = n.get(k)
        if isinstance(vv, str) and vv.strip():
            out.append(vv.strip())
        elif isinstance(vv, list):
            for x in vv:
                if isinstance(x, str) and x.strip():
                    out.append(x.strip())
    # дедуп (case-insensitive)
    seen: set[str] = set()
    res: List[str] = []
    for e in out:
        k = e.lower()
        if k in seen:
            continue
        seen.add(k)
        res.append(e)
    return res


def _emails_add(*, aggr_email: str, norm_emails: List[str]) -> List[str]:
    main = (aggr_email or "").strip().lower()
    out: List[str] = []
    seen: set[str] = set()
    for e in norm_emails:
        ee = e.strip()
        if not ee:
            continue
        k = ee.lower()
        if main and k == main:
            continue
        if k in seen:
            continue
        seen.add(k)
        out.append(ee)
    return out


def _rate_cl_bg(rate_cl: Any) -> str:
    try:
        v = int(rate_cl)
    except Exception:
        return ""
    if v <= 0:
        return "bg-10"
    if v > 100:
        return "bg-100"
    bucket = ((v - 1) // 10 + 1) * 10
    if bucket < 10:
        bucket = 10
    if bucket > 100:
        bucket = 100
    return f"bg-{bucket}"


def _rate_cb_1_100(rate_cb: Any) -> Optional[int]:
    if rate_cb is None:
        return None
    try:
        v = int(rate_cb)
    except Exception:
        return None
    if v <= 0:
        return 1
    x = (v + 99) // 100  # ceil(v/100)
    if x < 1:
        return 1
    if x > 100:
        return 100
    return int(x)


def _safe_int_list(v: Any) -> List[int]:
    if not isinstance(v, list):
        return []
    out: List[int] = []
    for x in v:
        try:
            out.append(int(x))
        except Exception:
            pass
    return out


# ============================================================
# CITY (row → derived) + by PLZ
# ============================================================

def get_city_row(city_id: int) -> Optional[Dict[str, str]]:
    def _load(_: Tuple[str, int]):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT name, state_name
                FROM public.cities_sys
                WHERE id = %s
                """,
                [int(city_id)],
            )
            row = cur.fetchone()
            if not row:
                return None
            return {"name": (row[0] or "").strip(), "state": (row[1] or "").strip()}

    return memo(("city_row", int(city_id)), _load, ttl=TTL_WEEK, version=MEMO_VERSION)


def get_city_name(city_id: int) -> str:
    row = get_city_row(city_id)
    return row["name"] if row else ""


def get_city_norm(city_id: int) -> str:
    return normalize_city(get_city_name(city_id))


def get_city_land(city_id: int) -> str:
    row = get_city_row(city_id)
    if not row:
        return ""
    city = normalize_city(row["name"])
    state = row["state"]
    if city and state:
        return f"{city} - {state}"
    return city or state or ""


def get_city_id_by_plz(plz: str) -> Optional[int]:
    plz_s = (plz or "").strip()
    if not plz_s:
        return None

    def _load(_: Tuple[str, str]):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT city_id
                FROM public.cb_crawler
                WHERE plz = %s
                LIMIT 1
                """,
                [plz_s],
            )
            row = cur.fetchone()
            return int(row[0]) if row else None

    return memo(("city_id_by_plz", plz_s), _load, ttl=TTL_WEEK, version=MEMO_VERSION)


def get_city_payload_by_plz(plz: str) -> Dict[str, Any]:
    city_id = get_city_id_by_plz(plz)
    if not city_id:
        return {"city_id": None, "city": "", "city_norm": "", "city_land": ""}

    cid = int(city_id)
    city = get_city_name(cid)
    city_norm = normalize_city(city)
    city_land = get_city_land(cid)

    return {
        "city_id": cid,
        "city": city,
        "city_norm": city_norm,
        "city_land": city_land,
    }


# ============================================================
# BRANCH (ratings contract depends on it)
# ============================================================

def get_branch_row(branch_id: int, ui_lang: str) -> Optional[Dict[str, str]]:
    lang = (ui_lang or "ru").strip().lower()

    def _load(_: Tuple[str, int, str]):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT b.name, i.name_trans
                FROM public.gb_branches b
                LEFT JOIN public.gb_branch_i18n i
                  ON i.branch_id = b.id AND i.lang = %s
                WHERE b.id = %s
                """,
                [lang, int(branch_id)],
            )
            row = cur.fetchone()
            if not row:
                return None
            return {"de": (row[0] or "").strip(), "tr": (row[1] or "").strip()}

    return memo(("branch_row", int(branch_id), lang), _load, ttl=TTL_WEEK, version=MEMO_VERSION)


def get_branch_str(branch_id: int, ui_lang: str) -> str:
    row = get_branch_row(branch_id, ui_lang)
    if not row:
        return ""
    de = row["de"]
    tr = row["tr"]
    if _is_de_lang(ui_lang):
        return de
    return f"{de} - {tr}" if tr else de


def format_branches_html(branch_ids: List[int], ui_lang: str) -> str:
    parts: List[str] = []
    for bid in branch_ids:
        s = get_branch_str(bid, ui_lang)
        if s:
            parts.append(_html_block(s))
    return "".join(parts)


def normalize_phones_for_print(phones: List[str]) -> List[str]:
    """
    1) Убираем все пробелы/whitespace и дефисы.
    2) Дедуп по “хвосту”: сравниваем номера БЕЗ префикса +49/0049/49 и БЕЗ первого нуля (если он первый).
       При точном совпадении хвоста выбрасываем вариант "с нулём" (если есть вариант без нуля).
    3) Возвращаем список (в исходном порядке, но может произойти замена "0..." на "..." если позже встретили лучше).
    """

    def _nospaces(s: str) -> str:
        return re.sub(r"[\s\-]+", "", s or "")

    def _strip_country(s: str) -> str:
        s = _nospaces(s)
        if s.startswith("+49"):
            return s[3:]
        if s.startswith("0049"):
            return s[4:]
        if s.startswith("49"):
            return s[2:]
        return s

    def _tail_key(s: str) -> tuple[str, bool]:
        core = _strip_country(s)
        if core.startswith("0"):
            return core[1:], True
        return core, False

    out: List[str] = []
    pos: dict[str, int] = {}
    has0: dict[str, bool] = {}

    for raw in phones or []:
        s = _nospaces(raw)
        if not s:
            continue

        key, with0 = _tail_key(s)
        if not key:
            continue

        if key not in pos:
            pos[key] = len(out)
            has0[key] = with0
            out.append(s)
            continue

        if (has0.get(key) is True) and (with0 is False):
            out[pos[key]] = s
            has0[key] = False

    return out


# ============================================================
# CB (ratings contract depends on it)
# ============================================================

def get_cb_row(cb_id: int) -> Optional[Dict[str, int]]:
    def _load(_: Tuple[str, int]):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT city_id, branch_id
                FROM public.cb_crawler
                WHERE id = %s
                """,
                [int(cb_id)],
            )
            row = cur.fetchone()
            if not row:
                return None
            return {"city_id": int(row[0]), "branch_id": int(row[1])}

    return memo(("cb_row", int(cb_id)), _load, ttl=TTL_WEEK, version=MEMO_VERSION)


# ============================================================
# CONTACT RAW (PRIVATE)
# ============================================================

def get_contact_raw(aggr_id: int) -> Optional[Dict[str, Any]]:
    """
    DB → memo (неделя)
    Возвращаем: aggr_id, email, company_data(dict+norm), branches_ids (int[] из aggr).
    company_data в cursor может прилетать как dict ИЛИ как JSON-строка/bytes — парсим.
    Если norm отсутствует/пустой — raise (катастрофа).
    """

    def _parse_company_data(v: Any) -> Dict[str, Any]:
        if isinstance(v, dict):
            return v
        if isinstance(v, (bytes, bytearray, memoryview)):
            try:
                v = bytes(v).decode("utf-8", errors="strict")
            except Exception as e:
                raise ValueError(
                    f"CONTACT_COMPANY_DATA_DECODE_FAIL aggr_id={aggr_id} type={type(v).__name__}"
                ) from e
        if isinstance(v, str):
            s = v.strip()
            if not s:
                raise ValueError(f"CONTACT_COMPANY_DATA_EMPTY_STR aggr_id={aggr_id}")
            try:
                obj = json.loads(s)
            except Exception as e:
                sample = s[:200].replace("\n", "\\n")
                raise ValueError(
                    f"CONTACT_COMPANY_DATA_JSON_LOAD_FAIL aggr_id={aggr_id} sample='{sample}'"
                ) from e
            if not isinstance(obj, dict):
                raise ValueError(
                    f"CONTACT_COMPANY_DATA_JSON_NOT_OBJECT aggr_id={aggr_id} got={type(obj).__name__}"
                )
            return obj
        raise ValueError(f"CONTACT_COMPANY_DATA_BAD_TYPE aggr_id={aggr_id} type={type(v).__name__}")

    def _load(_: Tuple[str, int]):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT id, email, company_data, branches
                FROM public.raw_contacts_aggr
                WHERE id = %s
                LIMIT 1
                """,
                [int(aggr_id)],
            )
            row = cur.fetchone()
            if not row:
                return None

            cd = _parse_company_data(row[2])
            _ = _must_norm(cd)  # validate (катастрофа если нет/пустой)

            return {
                "aggr_id": int(row[0]),
                "email": (row[1] or "").strip(),
                "company_data": cd,
                "branches_ids": _safe_int_list(row[3] or []),
            }

    return memo(("contact_raw", int(aggr_id)), _load, ttl=TTL_WEEK, version=MEMO_VERSION)


# ============================================================
# CONTACT (BUILD)
# ============================================================

def build_contact(raw: Dict[str, Any], ui_lang: str) -> Dict[str, Any]:
    aggr_id = int(raw["aggr_id"])
    aggr_email = (raw.get("email") or "").strip()

    company_data = _must_company_data(raw.get("company_data"))
    n = _must_norm(company_data)

    company_name = _norm_str(n, "company_name")
    description = _norm_str(n, "details")

    # address / plz
    address = _norm_str(n, "address")
    plz = _norm_str(n, "plz")

    # emails
    norm_emails = _norm_email_candidates(n)
    emails_add = _emails_add(aggr_email=aggr_email, norm_emails=norm_emails)
    emails_add_html = "".join(_html_mailto(e) for e in emails_add if e)

    # phones
    phones = _norm_list_str(n, "phone")

    # website + socials + sources
    website = _norm_str(n, "website")
    socials = _norm_list_str(n, "socials")
    source_urls = _norm_list_str(n, "source_urls")

    # tags (старое: norm.branches)
    tags = _norm_list_str(n, "branches")
    tags_html = "".join(_html_block(t) for t in tags if t)

    # branches (canonical) = из aggr.branches_ids
    branches_ids: List[int] = raw.get("branches_ids") or []
    branches_ids = [int(x) for x in branches_ids if isinstance(x, int) or str(x).isdigit()]
    branches = [get_branch_str(bid, ui_lang) for bid in branches_ids]
    branches = [s for s in branches if s]
    branches_html = "".join(_html_block(s) for s in branches)

    # city payload (new contract)
    city_payload = get_city_payload_by_plz(plz)

    return {
        "cache_ttl_sec": TTL_WEEK,
        "aggr_id": aggr_id,

        "email": aggr_email,
        "email_html": _html_mailto(aggr_email),

        "emails_add": emails_add,
        "emails_add_html": emails_add_html,

        "company_name": company_name,
        "description": description,

        "address": address,
        "plz": plz,

        "city_id": city_payload["city_id"],
        "city": city_payload["city"],
        "city_norm": city_payload["city_norm"],
        "city_land": city_payload["city_land"],

        "phone": phones,
        "phones_html": "".join(_html_block(p) for p in normalize_phones_for_print(phones) if p),

        "website": website,
        "website_html": _html_link(website),

        "socials": socials,
        "socials_html": "".join(_html_link(u) for u in socials if u),

        "branches_ids": branches_ids,
        "branches": branches,
        "branches_html": branches_html,

        "tags": tags,
        "tags_html": tags_html,

        "source_urls": source_urls,
        "source_urls_html": "".join(_html_link(u) for u in source_urls if u),

        "norm": n,
    }


# ============================================================
# CONTACT (PUBLIC)
# ============================================================

def get_contact(aggr_id: Optional[int], ui_lang: str) -> Optional[Dict[str, Any]]:
    if not aggr_id:
        return None
    raw = get_contact_raw(int(aggr_id))
    if not raw:
        return None
    return build_contact(raw, ui_lang)


# ============================================================
# RATINGS RAW (PRIVATE)
# ============================================================

def get_ratings_raw(rate_contact_id: int) -> Optional[Dict[str, Any]]:
    def _load(_: Tuple[str, int]):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT id, task_id, contact_id, rate_cl, rate_cb
                FROM public.rate_contacts
                WHERE id = %s
                LIMIT 1
                """,
                [int(rate_contact_id)],
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "rate_contact_id": int(row[0]),
                "task_id": int(row[1]),
                "aggr_id": int(row[2]),
                "rate_cl": row[3],
                "rate_cb": row[4],
            }

    return memo(("ratings_raw", int(rate_contact_id)), _load, ttl=TTL_HOUR, version=MEMO_VERSION)


def get_aggr_cb_ids(aggr_id: int) -> List[int]:
    def _load(_: Tuple[str, int]):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT cb_crawler_ids
                FROM public.raw_contacts_aggr
                WHERE id = %s
                LIMIT 1
                """,
                [int(aggr_id)],
            )
            row = cur.fetchone()
            if not row:
                return []
            return _safe_int_list(row[0] or [])

    return memo(("aggr_cb_ids", int(aggr_id)), _load, ttl=TTL_WEEK, version=MEMO_VERSION) or []


def get_aggr_id_by_rate_contact(rate_contact_id: Optional[int]) -> Optional[int]:
    if not rate_contact_id:
        return None

    def _load(_: Tuple[str, int]):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT contact_id
                FROM public.rate_contacts
                WHERE id = %s
                LIMIT 1
                """,
                [int(rate_contact_id)],
            )
            row = cur.fetchone()
            return int(row[0]) if row else None

    return memo(("aggr_id_by_rate_contact", int(rate_contact_id)), _load, ttl=TTL_HOUR, version=MEMO_VERSION)


def _choose_cb_id(task_id: int, rate_cb: Any, cb_ids: List[int]) -> Optional[int]:
    if not cb_ids:
        return None

    target: Optional[int] = None
    try:
        target = int(rate_cb) if rate_cb is not None else None
    except Exception:
        target = None

    import hashlib

    h = hashlib.sha1()
    h.update(str(int(task_id)).encode("utf-8"))
    h.update(b"|")
    h.update(str(int(target)).encode("utf-8") if target is not None else b"none")
    h.update(b"|")
    h.update(",".join(str(int(x)) for x in cb_ids).encode("utf-8"))
    digest = h.hexdigest()[:16]

    def _load(_: Tuple[str, int, str]):
        with connection.cursor() as cur:
            cur.execute(
                """
                WITH cb AS (
                  SELECT id::bigint AS cb_id, city_id::int, branch_id::int
                  FROM public.cb_crawler
                  WHERE id = ANY(%s)
                ),
                city_r AS (
                  SELECT value_id::int AS city_id, MIN(rate)::int AS rate_city
                  FROM public.crawl_tasks
                  WHERE task_id = %s AND type = 'city'
                  GROUP BY value_id
                ),
                branch_r AS (
                  SELECT value_id::int AS branch_id, MIN(rate)::int AS rate_branch
                  FROM public.crawl_tasks
                  WHERE task_id = %s AND type = 'branch'
                  GROUP BY value_id
                )
                SELECT
                  cb.cb_id,
                  (cr.rate_city * br.rate_branch) AS product
                FROM cb
                LEFT JOIN city_r cr ON cr.city_id = cb.city_id
                LEFT JOIN branch_r br ON br.branch_id = cb.branch_id
                """,
                [cb_ids, int(task_id), int(task_id)],
            )
            rows = cur.fetchall()

        if target is not None:
            for cb_id, product in rows:
                if product is not None and int(product) == target:
                    return int(cb_id)

        best_cb: Optional[int] = None
        best_prod: Optional[int] = None
        for cb_id, product in rows:
            if product is None:
                continue
            p = int(product)
            if best_prod is None or p < best_prod:
                best_prod = p
                best_cb = int(cb_id)

        return best_cb if best_cb is not None else int(cb_ids[0])

    return memo(("choose_cb_id", int(task_id), digest), _load, ttl=TTL_HOUR, version=MEMO_VERSION)


def build_ratings(r: Dict[str, Any], cb_ids: List[int], ui_lang: str) -> Dict[str, Any]:
    task_id = int(r["task_id"])
    rate_contact_id = int(r["rate_contact_id"])
    aggr_id = int(r["aggr_id"])
    rate_cl = r.get("rate_cl")
    rate_cb = r.get("rate_cb")

    chosen_cb_id = _choose_cb_id(task_id, rate_cb, cb_ids)

    city_id: Optional[int] = None
    branch_id: Optional[int] = None

    city = ""
    city_norm = ""
    city_land = ""
    branch_str = ""

    if chosen_cb_id:
        cb = get_cb_row(chosen_cb_id)
        if cb:
            city_id = cb["city_id"]
            branch_id = cb["branch_id"]

            if city_id is not None:
                city = get_city_name(int(city_id))
                city_norm = normalize_city(city)
                city_land = get_city_land(int(city_id))

            branch_str = get_branch_str(branch_id, ui_lang) if branch_id is not None else ""

    return {
        "cache_ttl_sec": TTL_HOUR,
        "task_id": task_id,
        "rate_contact_id": rate_contact_id,
        "aggr_id": aggr_id,
        "rate_cl": rate_cl,
        "rate_cb": rate_cb,
        "rate_cb_100": _rate_cb_1_100(rate_cb),
        "rate_cl_bg": _rate_cl_bg(rate_cl),
        "chosen_cb": {
            "cb_id": int(chosen_cb_id) if chosen_cb_id else None,
            "city_id": city_id,
            "city": city,
            "city_norm": city_norm,
            "city_land": city_land,
            "branch_id": branch_id,
            "branch_str": branch_str,
        },
    }


# ============================================================
# RATINGS (PUBLIC)
# ============================================================

def get_ratings(rate_contact_id: Optional[int], ui_lang: str) -> Optional[Dict[str, Any]]:
    if not rate_contact_id:
        return None
    rr = get_ratings_raw(int(rate_contact_id))
    if not rr:
        return None
    cb_ids = get_aggr_cb_ids(int(rr["aggr_id"]))
    return build_ratings(rr, cb_ids, ui_lang)


# ============================================================
# PACKET (PUBLIC)
# ============================================================

def build_contact_packet(rate_contact_id: int, ui_lang: str) -> Dict[str, Any]:
    ratings = get_ratings(int(rate_contact_id), ui_lang)
    contact = get_contact(get_aggr_id_by_rate_contact(int(rate_contact_id)), ui_lang)
    return {"contact": contact, "ratings": ratings}
