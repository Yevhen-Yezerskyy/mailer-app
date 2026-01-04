# FILE: web/panel/aap_audience/views/modal_status_task.py
# DATE: 2026-01-04
# CHANGE:
# - modal_status_task теперь отдаёт поля для текущего шаблона (address_line/phones_list/email/website/sources/socials/description + chosen_city_str/chosen_branch_str + all_branches)
# - company_data может прилетать как dict ИЛИ как JSON-строка -> безопасно парсим
# - выбранный cb_crawler_id ищем по rate_cb (product=min(city)*min(branch) из crawl_tasks), иначе берём лучший (min product), иначе первый
# - город+земля: "City - State" из cities_sys
# - category (chosen_branch_str) + all_branches: de-name, а если язык не de -> "de - trans" из gb_branch_i18n

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from django.db import connection
from django.shortcuts import render

from mailer_web.access import decode_id


def _is_de_lang(ui_lang: str) -> bool:
    s = (ui_lang or "").strip().lower()
    return (s == "de") or s.startswith("de-")


def _rate_bg_10_100(rate_cl: Any) -> str:
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


def _cb_norm_1_100(rate_cb: Any) -> Optional[int]:
    if rate_cb is None:
        return None
    try:
        v = int(rate_cb)
    except Exception:
        try:
            v = int(float(rate_cb))
        except Exception:
            return None
    if v <= 0:
        return 1
    x = (v + 99) // 100  # ceil(v/100) for 1..10000
    if x < 1:
        return 1
    if x > 100:
        return 100
    return int(x)


def _company_data_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return {}
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _norm_dict(company_data: Dict[str, Any]) -> Dict[str, Any]:
    n = company_data.get("norm")
    return n if isinstance(n, dict) else {}


def _norm_str(n: Dict[str, Any], key: str) -> str:
    v = n.get(key)
    return v.strip() if isinstance(v, str) and v.strip() else ""


def _norm_list_str(n: Dict[str, Any], key: str) -> List[str]:
    v = n.get(key)
    if not isinstance(v, list):
        return []
    out: List[str] = []
    for x in v:
        if isinstance(x, str) and x.strip():
            out.append(x.strip())
    return out


def _norm_email(n: Dict[str, Any]) -> str:
    v = n.get("email")
    if isinstance(v, str) and v.strip():
        return v.strip()
    if isinstance(v, list):
        for x in v:
            if isinstance(x, str) and x.strip():
                return x.strip()
    return ""


def _branches_i18n(branch_ids: List[int], *, ui_lang: str) -> List[str]:
    if not branch_ids:
        return []
    want_de_only = _is_de_lang(ui_lang)
    lang = (ui_lang or "ru").strip().lower()

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              b.id::int,
              b.name::text AS de_name,
              i.name_trans::text AS tr_name
            FROM public.gb_branches b
            LEFT JOIN public.gb_branch_i18n i
              ON i.branch_id = b.id
             AND i.lang = %s
            WHERE b.id = ANY(%s)
            """,
            [lang, branch_ids],
        )
        rows = cur.fetchall()

    out: List[str] = []
    for _bid, de_name, tr_name in rows:
        de_name = (de_name or "").strip()
        tr_name = (tr_name or "").strip()
        if want_de_only:
            out.append(de_name)
        else:
            out.append(f"{de_name} - {tr_name}" if tr_name else de_name)
    return out


def _choose_cb_id(*, task_id: int, rc_rate_cb: Optional[int], cb_ids: List[int]) -> Optional[int]:
    if not cb_ids:
        return None

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

    chosen: Optional[int] = None

    if rc_rate_cb is not None:
        for cb_id, product in rows:
            if product is not None and int(product) == int(rc_rate_cb):
                chosen = int(cb_id)
                break

    if chosen is None:
        best_prod: Optional[int] = None
        best_cb: Optional[int] = None
        for cb_id, product in rows:
            if product is None:
                continue
            p = int(product)
            if best_prod is None or p < best_prod:
                best_prod = p
                best_cb = int(cb_id)
        chosen = best_cb

    if chosen is None:
        chosen = int(cb_ids[0])

    return chosen


def _city_state_by_cb(cb_id: int) -> str:
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT cs.name, cs.state_name
            FROM public.cb_crawler c
            JOIN public.cities_sys cs ON cs.id = c.city_id
            WHERE c.id = %s
            LIMIT 1
            """,
            [int(cb_id)],
        )
        row = cur.fetchone()

    if not row:
        return ""
    name, state = row
    name = (name or "").strip()
    state = (state or "").strip()
    if name and state:
        return f"{name} - {state}"
    return name or state


def _branch_str_by_cb(cb_id: int, *, ui_lang: str) -> str:
    with connection.cursor() as cur:
        cur.execute("SELECT branch_id FROM public.cb_crawler WHERE id = %s LIMIT 1", [int(cb_id)])
        row = cur.fetchone()
    if not row:
        return ""
    bid = int(row[0])
    names = _branches_i18n([bid], ui_lang=ui_lang)
    return names[0] if names else ""


def modal_status_task_view(request):
    token = (request.GET.get("id") or "").strip()
    if not token:
        return render(request, "panels/aap_audience/modal_status_task.html", {"status": "empty"})

    try:
        rc_id = int(decode_id(token))
    except Exception:
        rc_id = 0

    if rc_id <= 0:
        return render(request, "panels/aap_audience/modal_status_task.html", {"status": "empty"})

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT
              rc.id,
              rc.task_id,
              rc.contact_id,
              rc.rate_cl,
              rc.rate_cb,
              rca.company_name,
              rca.company_data,
              rca.branches,
              rca.cb_crawler_ids
            FROM public.rate_contacts rc
            LEFT JOIN public.raw_contacts_aggr rca
              ON rca.id = rc.contact_id
            WHERE rc.id = %s
            LIMIT 1
            """,
            [int(rc_id)],
        )
        row = cur.fetchone()

    if not row:
        return render(request, "panels/aap_audience/modal_status_task.html", {"status": "empty"})

    _rc_id, task_id, _contact_id, rate_cl, rate_cb, company_name, company_data_raw, branches_arr, cb_ids_arr = row
    ui_lang = getattr(request, "LANGUAGE_CODE", "") or "ru"

    company_data = _company_data_dict(company_data_raw)
    n = _norm_dict(company_data)

    address_line = _norm_str(n, "address")
    phones_list = _norm_list_str(n, "phone")
    email = _norm_email(n)
    website = _norm_str(n, "website")
    sources = _norm_list_str(n, "source_urls")
    socials = _norm_list_str(n, "socials")
    description = _norm_str(n, "description")

    branch_ids: List[int] = []
    if isinstance(branches_arr, list):
        for x in branches_arr:
            try:
                branch_ids.append(int(x))
            except Exception:
                pass
    all_branches = _branches_i18n(branch_ids, ui_lang=ui_lang)

    cb_ids: List[int] = []
    if isinstance(cb_ids_arr, list):
        for x in cb_ids_arr:
            try:
                cb_ids.append(int(x))
            except Exception:
                pass

    chosen_cb_id = _choose_cb_id(
        task_id=int(task_id),
        rc_rate_cb=(int(rate_cb) if rate_cb is not None else None),
        cb_ids=cb_ids,
    )

    chosen_city_str = _city_state_by_cb(int(chosen_cb_id)) if chosen_cb_id else ""
    chosen_branch_str = _branch_str_by_cb(int(chosen_cb_id), ui_lang=ui_lang) if chosen_cb_id else ""

    return render(
        request,
        "panels/aap_audience/modal_status_task.html",
        {
            "status": "done",
            "company_name": (company_name or "").strip(),
            "address_line": address_line,
            "rate_cl": rate_cl,
            "rate_cl_bg": _rate_bg_10_100(rate_cl),
            "rate_cb_norm": _cb_norm_1_100(rate_cb),
            "chosen_city_str": chosen_city_str,
            "chosen_branch_str": chosen_branch_str,
            "all_branches": all_branches,
            "phones_list": phones_list,
            "email": email,
            "website": website,
            "sources": sources,
            "socials": socials,
            "description": description,
        },
    )
