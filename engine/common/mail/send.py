# FILE: engine/common/mail/send.py
# PATH: engine/common/mail/send.py
# DATE: 2026-01-30
# SUMMARY:
# - list_contact_id = lists_contacts.id
# - lists_contacts.rate_contact_id = rate_contacts.id (FK)
# - mailbox_sent.rate_contact_id хранит rate_contacts.id (а НЕ raw_contacts_aggr.id)
# - VarsContext(rate_contact_id) трактуется как rate_contacts.id (правильно)

from __future__ import annotations

import html as _html
import json  # <-- ДОБАВЬ ВВЕРХУ ФАЙЛА
import random
import re
import textwrap
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, List, Tuple

from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

from engine.common import db
from engine.common.mail.logs import log_mail_event
from engine.common.mail.smtp import SMTPConn

# ============================================================
# const / regex
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
_RE_WS = re.compile(r"[ \t]+")

# ============================================================
# defaults (ONLY for UI preview when rate_contact_id is None)
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
    "UTM": "smrel=0",
}


# ============================================================
# VarsContext (shared for UI + send)
# ============================================================
@dataclass
class VarsContext:
    # rate_contact_id == rate_contacts.id
    rate_contact_id: Optional[int]
    utm_value: Optional[str]

    def build(self) -> Dict[str, str]:
        # --- UI preview path ---
        if self.rate_contact_id is None:
            out = dict(DEFAULT_VARS)
            if self.utm_value is not None:
                out["UTM"] = str(self.utm_value)
            return out

        # --- send path ---
        rcid = int(self.rate_contact_id)

        row = db.fetch_one(
            """
            SELECT
              rc.task_id,
              rc.contact_id,
              rc.rate_cb,
              ag.company_name,
              ag.email,
              ag.address_list,
              ag.plz_list,
              ag.cb_crawler_ids
            FROM public.rate_contacts rc
            JOIN public.raw_contacts_aggr ag
              ON ag.id = rc.contact_id
            WHERE rc.id = %s
            LIMIT 1
            """,
            [rcid],
        )
        if not row:
            raise RuntimeError("RATE_CONTACT_NOT_FOUND")

        task_id, aggr_id, rate_cb, company_name, company_email, address_list, plz_list, cb_ids = row

        if not company_email:
            raise RuntimeError("COMPANY_EMAIL_EMPTY")
        if not cb_ids:
            raise RuntimeError("CB_IDS_EMPTY")

        cb_rows = db.fetch_all(
            """
            SELECT id, city_id, branch_id, plz
            FROM public.cb_crawler
            WHERE id = ANY(%s)
            """,
            [cb_ids],
        )
        if not cb_rows:
            raise RuntimeError("CB_ROWS_EMPTY")

        city_rate = {
            int(vid): int(r)
            for vid, r in db.fetch_all(
                """
                SELECT value_id::int, MIN(rate)::int
                FROM public.crawl_tasks
                WHERE task_id=%s AND type='city'
                GROUP BY value_id
                """,
                [task_id],
            )
        }
        branch_rate = {
            int(vid): int(r)
            for vid, r in db.fetch_all(
                """
                SELECT value_id::int, MIN(rate)::int
                FROM public.crawl_tasks
                WHERE task_id=%s AND type='branch'
                GROUP BY value_id
                """,
                [task_id],
            )
        }

        best: Optional[Tuple[int, int]] = None  # (score, cb_id)
        for cb_id, city_id, branch_id, _plz in cb_rows:
            city_id = int(city_id)
            branch_id = int(branch_id)
            if city_id not in city_rate or branch_id not in branch_rate:
                continue
            score = int(city_rate[city_id]) * int(branch_rate[branch_id])
            if rate_cb is not None and int(score) == int(rate_cb):
                best = (score, int(cb_id))
                break
            if best is None or score < best[0]:
                best = (score, int(cb_id))

        if not best:
            raise RuntimeError("CHOSEN_CB_NOT_FOUND")

        chosen_cb_id = int(best[1])

        row = db.fetch_one(
            "SELECT city_id, branch_id, plz FROM public.cb_crawler WHERE id=%s LIMIT 1",
            [chosen_cb_id],
        )
        if not row:
            raise RuntimeError("CB_ROW_MISSING")
        city_id, branch_id, cb_plz = row

        row = db.fetch_one(
            "SELECT name, state_name FROM public.cities_sys WHERE id=%s LIMIT 1",
            [int(city_id)],
        )
        if not row:
            raise RuntimeError("CITY_NOT_FOUND")
        city, land = row

        row = db.fetch_one(
            "SELECT name FROM public.gb_branches WHERE id=%s LIMIT 1",
            [int(branch_id)],
        )
        if not row:
            raise RuntimeError("BRANCH_NOT_FOUND")
        branch = row[0]

        addr = ""
        if address_list and plz_list:
            for a, p in zip(address_list, plz_list):
                if p == cb_plz:
                    addr = a
                    break
        if not addr and address_list:
            addr = address_list[0]
        if not addr:
            raise RuntimeError("ADDRESS_EMPTY")

        now = datetime.now(tz=_TZ)
        d0 = now.date()

        return {
            "company_name": (company_name or "").strip(),
            "company_email": (company_email or "").strip(),
            "city": (city or "").strip(),
            "land": (land or "").strip(),
            "city_land": f"{(city or '').strip()}, {(land or '').strip()}",
            "branch": (branch or "").strip(),
            "company_address": f"{(addr or '').strip()}, {cb_plz} {(city or '').strip()}, {(land or '').strip()}",
            "date_time": now.strftime("%H:%M %d.%m.%Y"),
            "date": now.strftime("%d.%m.%Y"),
            "date_plus_1m": (d0 + relativedelta(months=+1)).strftime("%d.%m.%Y"),
            "date_plus_3m": (d0 + relativedelta(months=+3)).strftime("%d.%m.%Y"),
            "UTM": str(self.utm_value) if self.utm_value is not None else "smrel=0",
        }


# ============================================================
# public API (USED BY TEMPLATE-EDITOR)
# ============================================================
def apply_vars(html: str, rate_contact_id: Optional[int] = None, *, utm_value: Optional[str] = None) -> str:
    s = str(html or "")
    vars_map = VarsContext(rate_contact_id, utm_value).build()

    def rep(m: re.Match) -> str:
        k = m.group(1)
        return str(vars_map.get(k, m.group(0)))

    return _RE_VAR.sub(rep, s)


def unapply_vars(html: str, vars_map: Dict[str, str]) -> str:
    s = str(html or "")
    items = [(k, v) for k, v in (vars_map or {}).items() if k and v]
    items.sort(key=lambda x: len(str(x[1])), reverse=True)
    for k, v in items:
        s = re.sub(re.escape(str(v)), f"{{{{ {k} }}}}", s)
    return s


# ============================================================
# send_one
# ============================================================
# FILE: engine/common/mail/send.py
# PATH: engine/common/mail/send.py
# DATE: 2026-01-30
# SUMMARY:
# - send_one: add to_email_override + record_sent (default True); tests can send without consuming contact / mailbox_sent row

def send_one(
    campaign_id: int,
    list_contact_id: int,
    *,
    to_email_override: Optional[str] = None,
    record_sent: bool = True,
) -> None:
    now = datetime.now(tz=_TZ)

    row = db.fetch_one(
        """
        SELECT c.mailbox_id, l.ready_content, l.subjects, l.headers
        FROM public.campaigns_campaigns c
        JOIN public.campaigns_letters l ON l.campaign_id = c.id
        WHERE c.id = %s
        LIMIT 1
        """,
        [int(campaign_id)],
    )
    if not row:
        raise RuntimeError("CAMPAIGN_OR_LETTER_NOT_FOUND")

    mailbox_id, ready_html, subjects, letter_headers = row
    mailbox_id = int(mailbox_id)

    html_tpl = (ready_html or "").strip()
    if not html_tpl:
        raise RuntimeError("READY_CONTENT_EMPTY")

    if list_contact_id is None:
        if record_sent or not (to_email_override or "").strip():
            raise RuntimeError("LIST_CONTACT_ID_REQUIRED")
        rate_contact_id: Optional[int] = None
    else:
        row = db.fetch_one(
            """
            SELECT rate_contact_id
            FROM public.lists_contacts
            WHERE id=%s AND active=true
            LIMIT 1
            """,
            [int(list_contact_id)],
        )
        if not row:
            raise RuntimeError("LIST_CONTACT_NOT_FOUND_OR_INACTIVE")
        if row[0] is None:
            raise RuntimeError("LIST_CONTACT_RATE_CONTACT_ID_NULL")
        rate_contact_id = int(row[0])

    rows = db.fetch_all(
        """
        SELECT action, status, created_at
        FROM public.mailbox_events
        WHERE mailbox_id=%s
          AND action IN ('SMTP_AUTH_CHECK','SMTP_SEND_CHECK')
        ORDER BY created_at DESC
        LIMIT 50
        """,
        [mailbox_id],
    )

    def blocked(action: str, status: str, span: timedelta, cooldown: timedelta) -> bool:
        seq: List[datetime] = []
        for a, s, ts in rows:
            if a != action:
                continue
            if s == status:
                seq.append(ts)
                if len(seq) == 3:
                    if seq[0] - seq[2] <= span:
                        return now < seq[0] + cooldown
                    return False
            else:
                break
        return False

    if blocked("SMTP_AUTH_CHECK", "FAIL", timedelta(minutes=5), timedelta(minutes=30)):
        return
    if blocked("SMTP_SEND_CHECK", "FAIL_TMP", timedelta(minutes=5), timedelta(minutes=30)):
        return
    if blocked("SMTP_SEND_CHECK", "FAIL", timedelta(minutes=5), timedelta(minutes=60)):
        return

    r = db.fetch_one("SELECT nextval('mailbox_sent_id_seq')", [])
    if not r or r[0] is None:
        raise RuntimeError("MAILBOX_SENT_NEXTVAL_FAILED")
    smrel_id = int(r[0])
    utm = f"smrel={smrel_id}"

    vars_map = VarsContext(rate_contact_id, utm).build()

    to_email = (to_email_override or "").strip() or (vars_map.get("company_email") or "").strip()
    if not to_email:
        raise RuntimeError("TO_EMAIL_EMPTY")

    html1 = apply_vars(html_tpl, rate_contact_id, utm_value=utm)

    def add_smrel(s: str) -> str:
        def rep(m: re.Match) -> str:
            url = m.group(3)
            if not url.startswith("http"):
                return m.group(0)
            if _RE_HAS_SMREL.search(url):
                return m.group(0)
            sep = "&" if "?" in url else "?"
            return f"{m.group(1)}{m.group(2)}{url}{sep}{utm}{m.group(2)}"

        return _RE_A_HREF.sub(rep, s)

    html2 = add_smrel(html1)

    def html_to_text(s: str) -> str:
        def a_rep(m: re.Match) -> str:
            inner = _RE_TAG.sub("", m.group(3) or "")
            inner = _html.unescape(inner).strip()
            url = _html.unescape(m.group(2) or "")
            return f"{inner} ({url})" if inner and url else inner or url

        s = _RE_A_TAG.sub(a_rep, s)
        s = _RE_BR.sub("\n", s)
        s = _RE_BLOCK_END.sub("\n\n", s)
        s = _RE_TAG.sub("", s)
        s = _html.unescape(s)
        s = _RE_WS.sub(" ", s)
        return textwrap.fill(s.strip(), width=78)

    body_text = html_to_text(html2)

    if not isinstance(subjects, list) or len(subjects) < 3:
        raise RuntimeError("SUBJECTS_BAD")
    subj = random.choice([str(x).strip() for x in subjects if x][:3])

    headers: Dict[str, str] = {}
    if isinstance(letter_headers, dict):
        for k, v in letter_headers.items():
            if k and v:
                headers[str(k)] = str(v)

    smtp = SMTPConn(mailbox_id)
    ok = smtp.send_mail(
        to_email,
        subj,
        body_text=body_text,
        body_html=html2,
        headers=headers,
    )

    # NOTE: test-send path should not write mailbox_sent (and must not consume contact via uniq(campaign_id, rate_contact_id))
    if not record_sent:
        return

    if ok:
        payload = {
            "mailbox_id": mailbox_id,
            "to": to_email,
            "subject": subj,
            "utm": utm,
            "smtp_trace": smtp.trace,
        }
        db.execute(
            """
            INSERT INTO public.mailbox_sent (
              id, campaign_id, list_contact_id, rate_contact_id,
              processed, status, data, processed_at
            )
            VALUES (%s,%s,%s,%s,true,'SEND',%s::jsonb,now())
            """,
            (
                smrel_id,
                int(campaign_id),
                int(list_contact_id),
                int(rate_contact_id),
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        return

    trace = smtp.trace or []
    last = next((r for r in reversed(trace) if r.get("action") == "SEND"), None)
    if last and isinstance(last.get("data"), dict):
        refused = last["data"].get("refused")
        if isinstance(refused, dict):
            rr = refused.get(to_email) or next(iter(refused.values()), {}) or {}
            code = rr.get("code")
            if code:
                code = int(code)
                if 400 <= code <= 499:
                    log_mail_event(
                        mailbox_id=mailbox_id,
                        action="SMTP_SEND_CHECK",
                        status="FAIL_TMP",
                        payload_json={"code": code, "smtp_trace": trace},
                    )
                    return
                if 500 <= code <= 599:
                    status = "BAD_ADDRESS" if code in (550, 551, 553) else "REPUTATION" if code == 554 else "OTHER"
                    payload = {
                        "mailbox_id": mailbox_id,
                        "to": to_email,
                        "subject": subj,
                        "utm": utm,
                        "smtp_trace": trace,
                        "smtp_code": code,
                    }
                    db.execute(
                        """
                        INSERT INTO public.mailbox_sent (
                          id, campaign_id, list_contact_id, rate_contact_id,
                          processed, status, data, processed_at
                        )
                        VALUES (%s,%s,%s,%s,true,%s,%s::jsonb,now())
                        """,
                        (
                            smrel_id,
                            int(campaign_id),
                            int(list_contact_id),
                            int(rate_contact_id),
                            status,
                            json.dumps(payload, ensure_ascii=False),
                        ),
                    )
                    return

    return
