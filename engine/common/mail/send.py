# FILE: engine/common/mail/send.py  (новое — 2026-01-27)
# PURPOSE: Подстановка переменных в готовый HTML письма (ready_content) для рассылки.
#          - rate_contact_id=None -> всегда дефолты.
#          - rate_contact_id задан -> только реальные значения; если чего-то нет -> fatal.
#          - apply_vars(): {{ var }} -> value
#          - unapply_vars(): value -> {{ var }} (негарантированно)

from __future__ import annotations

import random
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

from engine.common.db import fetch_all, fetch_one


# ============================================================
# defaults (ONLY when rate_contact_id is None)
# ============================================================

DEFAULT_VARS: Dict[str, str] = {
    "company_name": "Unternehmen Adressat GmbH",
    "company_address": "Adressatenstraße 12, 40213 Düsseldorf, Nordrhein-Westfalen",
    "city": "Düsseldorf",
    "land": "Nordrhein-Westfalen",
    "city_land": "Düsseldorf, Nordrhein-Westfalen",
    "branch": "Adressat Geschäftskategorie",
    "date_time": "12:00 21.01.2028",
    "date": "21.01.2028",
    "date_plus_1m": "21.02.2028",
    "date_plus_3m": "21.04.2028",
    "company_email": "adressat@unternehmen.de",
    "UTM": "smrel=132246897659",
}

_VAR_KEYS = [
    "company_name",
    "company_address",
    "city",
    "land",
    "city_land",
    "branch",
    "date",
    "date_time",
    "date_plus_1m",
    "date_plus_3m",
    "company_email",
    "UTM",
]


# ============================================================
# fatal placeholder (later: log + raise)
# ============================================================

def fatal_missing_data(code: str, payload: Optional[Dict[str, Any]] = None) -> None:
    # TODO: write to send-log table; include campaign_id/rate_contact_id/etc.
    pass


def _die(code: str, payload: Dict[str, Any]) -> "NoReturn":
    fatal_missing_data(code, payload)
    raise RuntimeError(code)


# ============================================================
# helpers
# ============================================================

_TZ = ZoneInfo("Europe/Berlin")
_RE_VAR = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")


def _normalize_city(name: str) -> str:
    name = (name or "").strip()
    return name[:-7].rstrip() if name.endswith(", Stadt") else name


def _fmt_date(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y")


def _fmt_datetime(dt: datetime) -> str:
    return dt.strftime("%H:%M %d.%m.%Y")


def _utm_random() -> str:
    # placeholder until nextval(...) is implemented
    return f"smrel={random.randint(10**11, 10**12 - 1)}"


def _min_rate_map(task_id: int, typ: str) -> Dict[int, int]:
    rows = fetch_all(
        """
        SELECT value_id::int, MIN(rate)::int
        FROM public.crawl_tasks
        WHERE task_id = %s AND type = %s
        GROUP BY value_id
        """,
        [int(task_id), str(typ)],
    )
    out: Dict[int, int] = {}
    for vid, r in rows:
        if vid is None or r is None:
            continue
        out[int(vid)] = int(r)
    return out


# ============================================================
# context (compute once, reuse)
# ============================================================

@dataclass
class VarsContext:
    rate_contact_id: Optional[int] = None
    data: Dict[str, str] = field(default_factory=dict)

    def vars(self) -> Dict[str, str]:
        if self.data:
            return self.data

        if not self.rate_contact_id:
            self.data = dict(DEFAULT_VARS)
            return self.data

        rcid = int(self.rate_contact_id)

        # 1) rate_contacts -> (task_id, aggr_id, rate_cb)
        row = fetch_one(
            """
            SELECT task_id, contact_id, rate_cb
            FROM public.rate_contacts
            WHERE id = %s
            LIMIT 1
            """,
            [rcid],
        )
        if not row:
            _die("RATE_CONTACT_NOT_FOUND", {"rate_contact_id": rcid})
        task_id, aggr_id, rate_cb = int(row[0]), int(row[1]), row[2]
        if not aggr_id:
            _die("AGGR_ID_MISSING", {"rate_contact_id": rcid, "task_id": task_id})

        # 2) aggr -> company_name/email/address_list/plz_list/cb_ids
        row = fetch_one(
            """
            SELECT company_name, email, address_list, plz_list, cb_crawler_ids
            FROM public.raw_contacts_aggr
            WHERE id = %s
            LIMIT 1
            """,
            [int(aggr_id)],
        )
        if not row:
            _die("AGGR_NOT_FOUND", {"rate_contact_id": rcid, "aggr_id": aggr_id})

        company_name = (row[0] or "").strip()
        company_email = (row[1] or "").strip()
        address_list = row[2] or []
        plz_list = row[3] or []
        cb_ids = row[4] or []

        if not isinstance(address_list, list):
            address_list = []
        if not isinstance(plz_list, list):
            plz_list = []
        if not isinstance(cb_ids, list):
            cb_ids = []

        cb_ids = [int(x) for x in cb_ids if isinstance(x, int) or str(x).isdigit()]
        if not cb_ids:
            _die("CB_IDS_EMPTY", {"rate_contact_id": rcid, "aggr_id": aggr_id})

        if not company_email:
            _die("COMPANY_EMAIL_EMPTY", {"rate_contact_id": rcid, "aggr_id": aggr_id})

        # 3) choose cb_id (simple)
        if len(cb_ids) == 1:
            chosen_cb_id = int(cb_ids[0])
            chosen_score = None
        else:
            cb_rows = fetch_all(
                """
                SELECT id::bigint, city_id::int, branch_id::int, plz
                FROM public.cb_crawler
                WHERE id = ANY(%s)
                """,
                [cb_ids],
            )
            if not cb_rows:
                _die("CB_ROWS_EMPTY", {"rate_contact_id": rcid, "aggr_id": aggr_id})

            city_rate = _min_rate_map(task_id, "city")
            branch_rate = _min_rate_map(task_id, "branch")

            target = None
            try:
                target = int(rate_cb) if rate_cb is not None else None
            except Exception:
                target = None

            best = None  # (score, cb_id, cb_plz, city_id, branch_id)
            for cb_id, city_id, branch_id, cb_plz in cb_rows:
                if city_id is None or branch_id is None:
                    continue
                rc = city_rate.get(int(city_id))
                rb = branch_rate.get(int(branch_id))
                if rc is None or rb is None:
                    continue
                score = int(rc) * int(rb)

                if target is not None and score == target:
                    best = (score, int(cb_id), str(cb_plz or ""), int(city_id), int(branch_id))
                    break

                if best is None or score < best[0]:
                    best = (score, int(cb_id), str(cb_plz or ""), int(city_id), int(branch_id))

            if best is None:
                _die(
                    "CHOSEN_CB_NOT_FOUND",
                    {"rate_contact_id": rcid, "aggr_id": aggr_id, "task_id": task_id, "cb_ids_len": len(cb_ids)},
                )

            chosen_score, chosen_cb_id, cb_plz, chosen_city_id, chosen_branch_id = best

        # 4) load chosen cb (city_id/branch_id/plz) if we didn't carry them
        cb_row = fetch_one(
            """
            SELECT city_id::int, branch_id::int, plz
            FROM public.cb_crawler
            WHERE id = %s
            LIMIT 1
            """,
            [int(chosen_cb_id)],
        )
        if not cb_row:
            _die("CHOSEN_CB_ROW_MISSING", {"rate_contact_id": rcid, "cb_id": chosen_cb_id})

        city_id, branch_id, cb_plz = cb_row
        if city_id is None or branch_id is None:
            _die("CHOSEN_CB_CITY_BRANCH_MISSING", {"rate_contact_id": rcid, "cb_id": chosen_cb_id})

        cb_plz = str(cb_plz or "").strip()
        if not cb_plz:
            _die("CHOSEN_CB_PLZ_EMPTY", {"rate_contact_id": rcid, "cb_id": chosen_cb_id})

        # 5) city + land
        row = fetch_one(
            """
            SELECT name, state_name
            FROM public.cities_sys
            WHERE id = %s
            LIMIT 1
            """,
            [int(city_id)],
        )
        if not row:
            _die("CITY_NOT_FOUND", {"rate_contact_id": rcid, "city_id": int(city_id)})
        city = _normalize_city((row[0] or "").strip())
        land = (row[1] or "").strip()
        if not city or not land:
            _die("CITY_LAND_EMPTY", {"rate_contact_id": rcid, "city_id": int(city_id)})

        # 6) branch (DE name)
        row = fetch_one(
            """
            SELECT name
            FROM public.gb_branches
            WHERE id = %s
            LIMIT 1
            """,
            [int(branch_id)],
        )
        if not row:
            _die("BRANCH_NOT_FOUND", {"rate_contact_id": rcid, "branch_id": int(branch_id)})
        branch = (row[0] or "").strip()
        if not branch:
            _die("BRANCH_EMPTY", {"rate_contact_id": rcid, "branch_id": int(branch_id)})

        # 7) address: pick from address_list + PLZ by chosen cb logic
        addr = ""
        # если длины совпадают — считаем что пары по индексу
        if address_list and plz_list and len(address_list) == len(plz_list):
            for a, p in zip(address_list, plz_list):
                aa = (a or "").strip() if isinstance(a, str) else ""
                pp = (p or "").strip() if isinstance(p, str) else ""
                if aa and pp == cb_plz:
                    addr = aa
                    break
        # иначе — берём первый валидный адрес
        if not addr and address_list:
            for a in address_list:
                aa = (a or "").strip() if isinstance(a, str) else ""
                if aa:
                    addr = aa
                    break
        if not addr:
            _die("ADDRESS_LIST_EMPTY", {"rate_contact_id": rcid, "aggr_id": aggr_id})

        # 8) dates
        now = datetime.now(tz=_TZ)
        d0 = now.date()

        self.data = {
            "company_name": company_name,
            "company_email": company_email,
            "city": city,
            "land": land,
            "city_land": f"{city}, {land}",
            "branch": branch,
            "company_address": f"{addr}, {cb_plz} {city}, {land}",
            "date_time": _fmt_datetime(now),
            "date": _fmt_date(now),
            "date_plus_1m": (d0 + relativedelta(months=+1)).strftime("%d.%m.%Y"),
            "date_plus_3m": (d0 + relativedelta(months=+3)).strftime("%d.%m.%Y"),
            "UTM": _utm_random(),
        }
        return self.data


# ============================================================
# apply / unapply
# ============================================================

def apply_vars(html: str, rate_contact_id: Optional[int] = None) -> str:
    s = str(html or "")
    vars_map = VarsContext(rate_contact_id=rate_contact_id).vars()

    def _rep(m: re.Match) -> str:
        k = m.group(1) or ""
        if k in vars_map:
            return str(vars_map.get(k, ""))
        return m.group(0)

    return _RE_VAR.sub(_rep, s)


def unapply_vars(html: str, rate_contact_id: Optional[int] = None) -> str:
    s = str(html or "")
    vars_map = VarsContext(rate_contact_id=rate_contact_id).vars()

    pairs = []
    for k, v in vars_map.items():
        vv = str(v or "")
        if vv:
            pairs.append((k, vv))
    pairs.sort(key=lambda kv: len(kv[1]), reverse=True)

    for k, v in pairs:
        s = s.replace(v, "{{ " + k + " }}")
    return s
