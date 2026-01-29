# FILE: engine/common/mail/send.py
# DATE: 01-29
# PURPOSE:
# - Отправка ОДНОГО письма по (campaign_id, list_id, rate_contact_id).
# - smrel: reserve id (nextval) -> подставить в {{ UTM }} и дописать в <a href>.
# - mailbox_sent пишем ТОЛЬКО на SMTP 2xx или 5xx (это “терминатор”: больше не слать никогда).
# - mailbox_events: gate + логирование SMTP_AUTH_CHECK / SMTP_SEND_CHECK по ТЗ (включая SUCCESS recovery).

from __future__ import annotations

import html as _html
import random
import re
import textwrap
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

from engine.common import db
from engine.common.mail.logs import log_mail_event
from engine.common.mail.smtp import SMTPConn


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

_RE_A_HREF = re.compile(r'(<a\b[^>]*\bhref\s*=\s*)(["\'])([^"\']*)(\2)', re.IGNORECASE)
_RE_HAS_SMREL = re.compile(r"(^|[?&])smrel=", re.IGNORECASE)

_RE_TAG = re.compile(r"<[^>]+>")
_RE_BR = re.compile(r"<\s*br\s*/?\s*>", re.IGNORECASE)
_RE_BLOCK_END = re.compile(
    r"</\s*(p|div|tr|table|ul|ol|li|h1|h2|h3|h4|h5|h6)\s*>",
    re.IGNORECASE,
)

_RE_A_TAG = re.compile(
    r'<a\b[^>]*\bhref\s*=\s*(["\'])([^"\']*)\1[^>]*>(.*?)</a\s*>',
    re.IGNORECASE | re.DOTALL,
)

_RE_DETAIL_CODE = re.compile(r"\((\d{3}),")
_RE_WS = re.compile(r"[ \t]+")


def _normalize_city(name: str) -> str:
    name = (name or "").strip()
    return name[:-7].rstrip() if name.endswith(", Stadt") else name


def _fmt_date(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y")


def _fmt_datetime(dt: datetime) -> str:
    return dt.strftime("%H:%M %d.%m.%Y")


def _min_rate_map(task_id: int, typ: str) -> Dict[int, int]:
    rows = db.fetch_all(
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
    utm_value: Optional[str] = None
    data: Dict[str, str] = field(default_factory=dict)

    def vars(self) -> Dict[str, str]:
        if self.data:
            return self.data

        if not self.rate_contact_id:
            self.data = dict(DEFAULT_VARS)
            if self.utm_value is not None:
                self.data["UTM"] = str(self.utm_value)
            return self.data

        rcid = int(self.rate_contact_id)

        # 1) rate_contacts -> (task_id, aggr_id, rate_cb)
        row = db.fetch_one(
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
        row = db.fetch_one(
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
        else:
            cb_rows = db.fetch_all(
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

            best = None  # (score, cb_id)
            for cb_id, city_id, branch_id, _cb_plz in cb_rows:
                if city_id is None or branch_id is None:
                    continue
                rc = city_rate.get(int(city_id))
                rb = branch_rate.get(int(branch_id))
                if rc is None or rb is None:
                    continue
                score = int(rc) * int(rb)

                if target is not None and score == target:
                    best = (score, int(cb_id))
                    break

                if best is None or score < best[0]:
                    best = (score, int(cb_id))

            if best is None:
                _die(
                    "CHOSEN_CB_NOT_FOUND",
                    {"rate_contact_id": rcid, "aggr_id": aggr_id, "task_id": task_id, "cb_ids_len": len(cb_ids)},
                )
            chosen_cb_id = int(best[1])

        # 4) chosen cb (city_id/branch_id/plz)
        cb_row = db.fetch_one(
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
        row = db.fetch_one(
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
        row = db.fetch_one(
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

        # 7) address
        addr = ""
        address_list = address_list or []
        plz_list = plz_list or []

        if address_list and plz_list and len(address_list) == len(plz_list):
            for a, p in zip(address_list, plz_list):
                aa = (a or "").strip() if isinstance(a, str) else ""
                pp = (p or "").strip() if isinstance(p, str) else ""
                if aa and pp == cb_plz:
                    addr = aa
                    break

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
            "UTM": str(self.utm_value) if self.utm_value is not None else "smrel=0",
        }
        return self.data


# ============================================================
# apply
# ============================================================

def apply_vars(html: str, rate_contact_id: Optional[int] = None, *, utm_value: Optional[str] = None) -> str:
    s = str(html or "")
    vars_map = VarsContext(rate_contact_id=rate_contact_id, utm_value=utm_value).vars()

    def _rep(m: re.Match) -> str:
        k = m.group(1) or ""
        if k in vars_map:
            return str(vars_map.get(k, ""))
        return m.group(0)

    return _RE_VAR.sub(_rep, s)

def unapply_vars(html: str, vars_map: Dict[str, str]) -> str:
    """
    Best-effort reverse of apply_vars():
    - Replaces exact occurrences of values with {{ KEY }}.
    - Longest values first to avoid partial overlaps.
    - Skips empty values.
    - NOT guaranteed (values may repeat naturally in template).
    """
    s = str(html or "")

    items = []
    for k, v in (vars_map or {}).items():
        kk = str(k or "").strip()
        vv = str(v or "")
        if not kk or not vv:
            continue
        items.append((kk, vv))

    # longest values first to reduce overlap issues
    items.sort(key=lambda x: len(x[1]), reverse=True)

    for key, val in items:
        # exact literal replace (case-sensitive), safe for regex chars
        s = re.sub(re.escape(val), f"{{{{ {key} }}}}", s)

    return s

# ============================================================
# send_one
# ============================================================

def send_one(campaign_id: int, list_id: int, rate_contact_id: int) -> None:
    # 1) load campaign+letter
    row = db.fetch_one(
        """
        SELECT c.mailbox_id, l.ready_content, l.subjects, l.headers
        FROM campaigns_campaigns c
        JOIN campaigns_letters  l ON l.campaign_id = c.id
        WHERE c.id = %s
        LIMIT 1
        """,
        [int(campaign_id)],
    )
    if not row:
        _die("CAMPAIGN_OR_LETTER_NOT_FOUND", {"campaign_id": int(campaign_id)})

    mailbox_id, ready_html, subjects, letter_headers = row
    mailbox_id = int(mailbox_id)

    html0 = str(ready_html or "")
    if not html0.strip():
        _die("READY_CONTENT_EMPTY", {"campaign_id": int(campaign_id)})

    subj = _pick_subject(subjects)
    headers = _normalize_headers(letter_headers)

    # 2) gate (mailbox_events)
    now = datetime.now(tz=_TZ)
    if _is_blocked_by_events(mailbox_id, now):
        return

    # 3) reserve smrel id (NO INSERT)
    smrel_id = _reserve_mailbox_sent_id()

    # 4) vars + html rewrite
    utm = f"smrel={smrel_id}"
    vars_map = VarsContext(rate_contact_id=int(rate_contact_id), utm_value=utm).vars()

    to_email = (vars_map.get("company_email") or "").strip()
    if not to_email:
        _die("TO_EMAIL_EMPTY", {"rate_contact_id": int(rate_contact_id)})

    html1 = apply_vars(html0, rate_contact_id=int(rate_contact_id), utm_value=utm)
    html2 = _append_smrel_to_a_hrefs(html1, smrel_id)

    # 5) text/plain
    body_text = _html_to_text(html2)

    # 6) SMTP send
    smtp = SMTPConn(mailbox_id)
    ok = smtp.send_mail(
        to_email,
        subj,
        body_text=body_text,
        body_html=html2,
        headers=headers,
    )

    # 7) handle outcomes (by smtp.trace / smtp.log)
    if ok:
        _smtp_success_recovery_if_needed(mailbox_id)
        _insert_mailbox_sent(
            smrel_id=smrel_id,
            campaign_id=int(campaign_id),
            list_id=int(list_id),
            rate_contact_id=int(rate_contact_id),
            status="SEND",
            payload_json={
                "mailbox_id": mailbox_id,
                "to": to_email,
                "subject": subj,
                "utm": utm,
                "smtp_trace": smtp.trace,
            },
        )
        return

    # failed: classify
    fail_kind = _classify_smtp_failure(smtp, to_email)

    if fail_kind["type"] == "AUTH_FAIL":
        log_mail_event(
            mailbox_id=mailbox_id,
            action="SMTP_AUTH_CHECK",
            status="FAIL",
            payload_json={"smtp_trace": smtp.trace, "smtp_last": smtp.log},
        )
        return

    if fail_kind["type"] == "SEND_4XX":
        log_mail_event(
            mailbox_id=mailbox_id,
            action="SMTP_SEND_CHECK",
            status="FAIL_TMP",
            payload_json={"smtp_trace": smtp.trace, "smtp_last": smtp.log, "code": fail_kind.get("code")},
        )
        return

    if fail_kind["type"] == "SEND_UNKNOWN_REPLIED":
        log_mail_event(
            mailbox_id=mailbox_id,
            action="SMTP_SEND_CHECK",
            status="FAIL",
            payload_json={"smtp_trace": smtp.trace, "smtp_last": smtp.log},
        )
        return

    if fail_kind["type"] == "SEND_NO_REPLY":
        return

    if fail_kind["type"] == "SEND_5XX":
        code = int(fail_kind["code"])
        status = _mailbox_sent_status_from_5xx(code)
        _insert_mailbox_sent(
            smrel_id=smrel_id,
            campaign_id=int(campaign_id),
            list_id=int(list_id),
            rate_contact_id=int(rate_contact_id),
            status=status,
            payload_json={
                "mailbox_id": mailbox_id,
                "to": to_email,
                "subject": subj,
                "utm": utm,
                "smtp_trace": smtp.trace,
                "smtp_last": smtp.log,
                "smtp_code": code,
            },
        )
        return

    return


# ============================================================
# subject / headers
# ============================================================

def _pick_subject(subjects_raw: Any) -> str:
    if not isinstance(subjects_raw, list):
        _die("SUBJECTS_BAD_FORMAT", {"subjects_type": str(type(subjects_raw))})

    items = [str(x or "").strip() for x in subjects_raw]
    items = [x for x in items if x]
    if len(items) < 3:
        _die("SUBJECTS_NEED_3", {"subjects_len": len(items)})

    return random.choice(items[:3])


def _normalize_headers(headers_raw: Any) -> Dict[str, str]:
    if headers_raw is None:
        return {}
    if not isinstance(headers_raw, dict):
        _die("LETTER_HEADERS_BAD_FORMAT", {"headers_type": str(type(headers_raw))})
    out: Dict[str, str] = {}
    for k, v in headers_raw.items():
        kk = str(k or "").strip()
        vv = str(v or "").strip()
        if kk and vv:
            out[kk] = vv
    return out


# ============================================================
# mailbox_sent (reserve id + insert)
# ============================================================

def _reserve_mailbox_sent_id() -> int:
    r = db.fetch_one("SELECT nextval('mailbox_sent_id_seq')", [])
    if not r or r[0] is None:
        raise RuntimeError("mailbox_sent_nextval_failed")
    return int(r[0])


def _insert_mailbox_sent(
    *,
    smrel_id: int,
    campaign_id: int,
    list_id: int,
    rate_contact_id: int,
    status: str,
    payload_json: Dict[str, Any],
) -> None:
    if status not in ("SEND", "BAD_ADDRESS", "REPUTATION", "OTHER"):
        raise ValueError(f"mail_bad_sent_status:{status}")
    if not isinstance(payload_json, dict):
        raise ValueError("mail_bad_payload:payload_json_must_be_dict")

    db.execute(
        """
        INSERT INTO mailbox_sent (
            id,
            campaign_id,
            list_id,
            rate_contact_id,
            processed,
            status,
            data,
            processed_at
        )
        VALUES (%s, %s, %s, %s, true, %s, %s, now())
        """,
        (
            int(smrel_id),
            int(campaign_id),
            int(list_id),
            int(rate_contact_id),
            str(status),
            payload_json,
        ),
    )


def _mailbox_sent_status_from_5xx(code: int) -> str:
    if code in (550, 551, 553):
        return "BAD_ADDRESS"
    if code == 554:
        return "REPUTATION"
    return "OTHER"


# ============================================================
# html rewrite: append smrel to <a href>
# ============================================================

def _append_smrel_to_a_hrefs(html: str, smrel_id: int) -> str:
    s = str(html or "")
    add_q = f"smrel={int(smrel_id)}"

    def _rep(m: re.Match) -> str:
        prefix, q, url, _q2 = m.group(1), m.group(2), m.group(3), m.group(4)

        u = str(url or "")
        if not u:
            return m.group(0)

        if not (u.startswith("http://") or u.startswith("https://")):
            return m.group(0)

        if _RE_HAS_SMREL.search(u):
            return m.group(0)

        sep = "?"
        if "?" in u:
            sep = "&"
        if u.endswith("?") or u.endswith("&"):
            sep = ""

        return f"{prefix}{q}{u}{sep}{add_q}{q}"

    return _RE_A_HREF.sub(_rep, s)


# ============================================================
# html -> text/plain
# ============================================================

def _html_to_text(html_in: str, *, width: int = 78) -> str:
    s = str(html_in or "")

    # links: TEXT (URL)
    def _a_rep(m: re.Match) -> str:
        url = (m.group(2) or "").strip()
        inner = (m.group(3) or "")
        inner = _RE_TAG.sub("", inner)
        inner = _html.unescape(inner)
        inner = _RE_WS.sub(" ", inner).strip()
        url = _html.unescape(url)
        if not inner and url:
            return url
        if inner and url:
            return f"{inner} ({url})"
        return inner

    s = _RE_A_TAG.sub(_a_rep, s)

    # br -> \n
    s = _RE_BR.sub("\n", s)

    # block ends -> \n\n
    s = _RE_BLOCK_END.sub("\n\n", s)

    # strip other tags
    s = _RE_TAG.sub("", s)

    # entities + cleanup
    s = _html.unescape(s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = _RE_WS.sub(" ", s)

    # normalize blank lines
    lines = [ln.rstrip() for ln in s.split("\n")]
    s2 = "\n".join(lines)
    s2 = re.sub(r"\n{3,}", "\n\n", s2).strip()

    # wrap paragraphs
    out_parts: List[str] = []
    for para in s2.split("\n\n"):
        p = para.strip()
        if not p:
            continue
        out_parts.append(textwrap.fill(p, width=width))
    return "\n\n".join(out_parts).strip()


# ============================================================
# mailbox_events gate (3 подряд за 5 минут)
# ============================================================

def _is_blocked_by_events(mailbox_id: int, now: datetime) -> bool:
    rows = db.fetch_all(
        """
        SELECT action, status, created_at
        FROM mailbox_events
        WHERE mailbox_id = %s
          AND action IN ('SMTP_AUTH_CHECK', 'SMTP_SEND_CHECK')
        ORDER BY created_at DESC
        LIMIT 50
        """,
        [int(mailbox_id)],
    )

    by_action: Dict[str, List[Tuple[str, datetime]]] = {"SMTP_AUTH_CHECK": [], "SMTP_SEND_CHECK": []}
    for action, status, created_at in rows:
        if action not in by_action:
            continue
        if not isinstance(created_at, datetime):
            continue
        by_action[action].append((str(status or ""), created_at))

    # rule 1: AUTH FAIL x3 подряд за 5 мин -> 30 мин
    if _blocked_for_series(by_action["SMTP_AUTH_CHECK"], need_status="FAIL", max_span=timedelta(minutes=5), cooldown=timedelta(minutes=30), now=now):
        return True

    # rule 2: SEND FAIL_TMP x3 подряд за 5 мин -> 30 мин
    if _blocked_for_series(by_action["SMTP_SEND_CHECK"], need_status="FAIL_TMP", max_span=timedelta(minutes=5), cooldown=timedelta(minutes=30), now=now):
        return True

    # rule 3: SEND FAIL x3 подряд за 5 мин -> 60 мин
    if _blocked_for_series(by_action["SMTP_SEND_CHECK"], need_status="FAIL", max_span=timedelta(minutes=5), cooldown=timedelta(minutes=60), now=now):
        return True

    return False


def _blocked_for_series(
    events_desc: List[Tuple[str, datetime]],
    *,
    need_status: str,
    max_span: timedelta,
    cooldown: timedelta,
    now: datetime,
) -> bool:
    # events_desc: [(status, created_at)] sorted DESC (newest first)
    series: List[datetime] = []
    for st, ts in events_desc:
        if st == need_status:
            series.append(ts)
            if len(series) >= 3:
                newest = series[0]
                third = series[2]
                if newest - third <= max_span:
                    blocked_until = newest + cooldown
                    return now < blocked_until
                return False
        else:
            break
    return False


# ============================================================
# smtp failure classify (by smtp.trace/log)
# ============================================================

def _classify_smtp_failure(smtp: SMTPConn, to_email: str) -> Dict[str, Any]:
    # 1) auth fail
    if _has_trace(smtp, action="AUTH", status="FAILED"):
        return {"type": "AUTH_FAIL"}

    # 2) send failed
    send_rec = _last_trace(smtp, action="SEND")
    if not send_rec:
        # connect fail or other (treat as AUTH_FAIL-ish? but TZ says only auth/4xx/unknown/no-reply; connect is auth-fail bucket)
        if _has_trace(smtp, action="CONNECT", status="FAILED"):
            return {"type": "AUTH_FAIL"}
        return {"type": "SEND_NO_REPLY"}

    data = (send_rec.get("data") or {}) if isinstance(send_rec.get("data"), dict) else {}
    refused = data.get("refused") if isinstance(data.get("refused"), dict) else None

    # refused -> server replied
    if refused:
        rr = refused.get(str(to_email)) or next(iter(refused.values()), None)
        if isinstance(rr, dict):
            code = rr.get("code")
            try:
                code_i = int(code)
            except Exception:
                code_i = None

            if code_i is not None:
                if 400 <= code_i <= 499:
                    return {"type": "SEND_4XX", "code": code_i}
                if 500 <= code_i <= 599:
                    return {"type": "SEND_5XX", "code": code_i}
                return {"type": "SEND_UNKNOWN_REPLIED", "code": code_i}

        return {"type": "SEND_UNKNOWN_REPLIED"}

    # exception detail: try extract (NNN, ...)
    detail = str(data.get("detail") or "")
    m = _RE_DETAIL_CODE.search(detail)
    if m:
        try:
            code_i = int(m.group(1))
        except Exception:
            code_i = None
        if code_i is not None:
            if 400 <= code_i <= 499:
                return {"type": "SEND_4XX", "code": code_i}
            if 500 <= code_i <= 599:
                return {"type": "SEND_5XX", "code": code_i}
            return {"type": "SEND_UNKNOWN_REPLIED", "code": code_i}

        return {"type": "SEND_UNKNOWN_REPLIED"}

    # no refused + no code => "server ничего не ответил"
    return {"type": "SEND_NO_REPLY"}


def _has_trace(smtp: SMTPConn, *, action: str, status: str) -> bool:
    for rec in reversed(smtp.trace or []):
        if rec.get("action") == action and rec.get("status") == status:
            return True
    return False


def _last_trace(smtp: SMTPConn, *, action: str) -> Optional[Dict[str, Any]]:
    for rec in reversed(smtp.trace or []):
        if rec.get("action") == action:
            return rec
    return None


# ============================================================
# success recovery (per-action last status)
# ============================================================

def _smtp_success_recovery_if_needed(mailbox_id: int) -> None:
    last_auth = db.fetch_one(
        """
        SELECT status
        FROM mailbox_events
        WHERE mailbox_id=%s AND action='SMTP_AUTH_CHECK'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [int(mailbox_id)],
    )
    last_send = db.fetch_one(
        """
        SELECT status
        FROM mailbox_events
        WHERE mailbox_id=%s AND action='SMTP_SEND_CHECK'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [int(mailbox_id)],
    )

    if last_auth and str(last_auth[0] or "") == "FAIL":
        log_mail_event(
            mailbox_id=int(mailbox_id),
            action="SMTP_AUTH_CHECK",
            status="SUCCESS",
            payload_json={"note": "recovered_by_send_one"},
        )

    if last_send and str(last_send[0] or "") in ("FAIL", "FAIL_TMP"):
        log_mail_event(
            mailbox_id=int(mailbox_id),
            action="SMTP_SEND_CHECK",
            status="SUCCESS",
            payload_json={"note": "recovered_by_send_one"},
        )
