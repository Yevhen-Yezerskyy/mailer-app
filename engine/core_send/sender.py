# FILE: engine/core_send/sender.py
# DATE: 2026-04-14
# PURPOSE: Sender based on campaigns.active + campaigns.sending_interval and sending_lists/sending_log.

from __future__ import annotations

import os
import random
import signal
import time
from collections import deque
from dataclasses import dataclass
from multiprocessing import Pipe, Process
from typing import Any, Dict, List, Optional, Tuple

from engine.common.db import fetch_all
from engine.common.mail.send import send_one
from engine.common.utils import safe_dict

_SEND_FAIL_SLEEP_SEC = 1.0
_RUNTIME_TO_EMAIL_OVERRIDE = "bootsp85@gmail.com"


@dataclass
class Heartbeat:
    last_seen: float
    next_wake_at: float
    state: str
    campaign_id: Optional[int] = None
    reason: str = ""


@dataclass
class SenderRuntime:
    mailbox_id: int
    campaign_ids: List[int]
    proc: Process
    hb_conn: Any
    last_hb: Optional[Heartbeat] = None


@dataclass(frozen=True)
class SendCandidate:
    campaign_id: int
    sending_list_id: int
    aggr_contact_id: Optional[int]
    email: str
    company_name: str
    norm: Dict[str, Any]


@dataclass(frozen=True)
class CampaignLetterPayload:
    ready_html: str
    subjects: List[str]
    headers: Dict[str, str]


def _now_ts() -> float:
    return time.time()


def _fmt_wait_sec(ts: float, now: float) -> str:
    try:
        d = float(ts) - float(now)
        if d < 0:
            d = 0.0
        return f"{d:.0f}s"
    except Exception:
        return "?"


def _pick_weighted_campaign(candidates: List[Tuple[int, int]]) -> Optional[int]:
    total = sum(max(0, int(weight)) for _, weight in candidates)
    if total <= 0:
        return None
    pick = random.randint(1, total)
    acc = 0
    for cid, weight in candidates:
        acc += max(0, int(weight))
        if pick <= acc:
            return int(cid)
    return int(candidates[-1][0]) if candidates else None


def _pending_by_campaign(campaign_ids: List[int]) -> Dict[int, int]:
    ids = [int(x) for x in campaign_ids if int(x) > 0]
    if not ids:
        return {}

    rows = fetch_all(
        """
        WITH selected_campaigns AS (
          SELECT
            c.id,
            c.sending_list_id,
            c.campaign_parent_id,
            COALESCE(c.send_after_parent_days, 0)::int AS send_after_parent_days
          FROM public.campaigns_campaigns c
          WHERE c.id = ANY(%s)
        ),
        parent_send AS (
          SELECT
            lg.campaign_id AS parent_campaign_id,
            lg.sending_list_id,
            MAX(COALESCE(lg.processed_at, lg.created_at)) AS parent_sent_at
          FROM public.sending_log lg
          JOIN (
            SELECT DISTINCT sc.campaign_parent_id
            FROM selected_campaigns sc
            WHERE sc.campaign_parent_id IS NOT NULL
          ) p
            ON p.campaign_parent_id = lg.campaign_id
          WHERE lg.processed = true
            AND lg.status = 'SEND'
          GROUP BY lg.campaign_id, lg.sending_list_id
        )
        SELECT
          sc.id AS campaign_id,
          COUNT(*) FILTER (WHERE lg.id IS NULL)::int AS pending
        FROM selected_campaigns sc
        JOIN public.aap_audience_audiencetask t
          ON t.id = sc.sending_list_id
        JOIN public.sending_lists sl
          ON sl.task_id = sc.sending_list_id
        JOIN public.aggr_contacts_cb ac
          ON ac.id = sl.aggr_contact_cb_id
        LEFT JOIN parent_send ps
          ON ps.parent_campaign_id = sc.campaign_parent_id
         AND ps.sending_list_id = sl.aggr_contact_cb_id
        LEFT JOIN public.sending_log lg
          ON lg.campaign_id = sc.id
         AND lg.sending_list_id = sl.aggr_contact_cb_id
        WHERE COALESCE(sl.removed, false) = false
          AND sl.rate IS NOT NULL
          AND sl.rate <= t.rate_limit
          AND COALESCE(ac.blocked, false) = false
          AND COALESCE(ac.wrong_email, false) = false
          AND (
            sc.campaign_parent_id IS NULL
            OR (
              ps.parent_sent_at IS NOT NULL
              AND ps.parent_sent_at <= now() - (sc.send_after_parent_days * interval '1 day')
            )
          )
        GROUP BY sc.id
        """,
        [ids],
    )

    out: Dict[int, int] = {int(cid): int(cnt or 0) for cid, cnt in rows}
    for cid in ids:
        out.setdefault(int(cid), 0)
    return out


def _candidate_batch_for_campaign(campaign_id: int, *, limit: int = 200) -> List[SendCandidate]:
    rows = fetch_all(
        """
        WITH selected_campaign AS (
          SELECT
            c.id,
            c.sending_list_id,
            c.campaign_parent_id,
            COALESCE(c.send_after_parent_days, 0)::int AS send_after_parent_days
          FROM public.campaigns_campaigns c
          WHERE c.id = %s
          LIMIT 1
        ),
        parent_send AS (
          SELECT
            lg.campaign_id AS parent_campaign_id,
            lg.sending_list_id,
            MAX(COALESCE(lg.processed_at, lg.created_at)) AS parent_sent_at
          FROM public.sending_log lg
          JOIN selected_campaign sc
            ON sc.campaign_parent_id IS NOT NULL
           AND lg.campaign_id = sc.campaign_parent_id
          WHERE lg.processed = true
            AND lg.status = 'SEND'
          GROUP BY lg.campaign_id, lg.sending_list_id
        )
        SELECT
          sl.aggr_contact_cb_id AS sending_list_id,
          sl.aggr_contact_cb_id AS aggr_contact_id,
          COALESCE(lower(trim(ac.email)), '') AS email,
          COALESCE(ac.company_name, '') AS company_name,
          ac.company_data
        FROM selected_campaign sc
        JOIN public.aap_audience_audiencetask t
          ON t.id = sc.sending_list_id
        JOIN public.sending_lists sl
          ON sl.task_id = sc.sending_list_id
        JOIN public.aggr_contacts_cb ac
          ON ac.id = sl.aggr_contact_cb_id
        LEFT JOIN parent_send ps
          ON ps.parent_campaign_id = sc.campaign_parent_id
         AND ps.sending_list_id = sl.aggr_contact_cb_id
        LEFT JOIN public.sending_log lg
          ON lg.campaign_id = sc.id
         AND lg.sending_list_id = sl.aggr_contact_cb_id
        WHERE COALESCE(sl.removed, false) = false
          AND sl.rate IS NOT NULL
          AND sl.rate <= t.rate_limit
          AND COALESCE(ac.blocked, false) = false
          AND COALESCE(ac.wrong_email, false) = false
          AND lg.id IS NULL
          AND (
            sc.campaign_parent_id IS NULL
            OR (
              ps.parent_sent_at IS NOT NULL
              AND ps.parent_sent_at <= now() - (sc.send_after_parent_days * interval '1 day')
            )
          )
        ORDER BY
          sl.rate ASC NULLS LAST,
          sl.rate_cb ASC NULLS LAST,
          sl.aggr_contact_cb_id ASC
        LIMIT %s
        """,
        [int(campaign_id), int(limit)],
    )
    out: List[SendCandidate] = []
    for sending_list_id, aggr_contact_id, email, company_name, company_data in rows:
        norm = safe_dict(safe_dict(company_data).get("norm"))
        out.append(
            SendCandidate(
                campaign_id=int(campaign_id),
                sending_list_id=int(sending_list_id),
                aggr_contact_id=int(aggr_contact_id) if aggr_contact_id is not None else None,
                email=str(email or "").strip().lower(),
                company_name=str(company_name or "").strip(),
                norm=norm,
            )
        )
    return out


def _pick_send_candidate(candidates: List[Tuple[int, int]]) -> Optional[SendCandidate]:
    pool = [(int(cid), int(weight)) for cid, weight in candidates if int(weight) > 0]
    while pool:
        chosen_campaign_id = _pick_weighted_campaign(pool)
        if chosen_campaign_id is None:
            return None
        batch = _candidate_batch_for_campaign(int(chosen_campaign_id))
        for candidate in batch:
            return candidate
        pool = [(cid, weight) for cid, weight in pool if int(cid) != int(chosen_campaign_id)]
    return None


def _campaign_letter_payloads(campaign_ids: List[int]) -> Dict[int, CampaignLetterPayload]:
    ids = [int(x) for x in campaign_ids if int(x) > 0]
    if not ids:
        return {}
    rows = fetch_all(
        """
        SELECT c.id, l.ready_content, l.subjects, l.headers
        FROM public.campaigns_campaigns c
        JOIN public.campaigns_letters l
          ON l.campaign_id = c.id
        WHERE c.id = ANY(%s)
        """,
        [ids],
    )
    out: Dict[int, CampaignLetterPayload] = {}
    for cid, ready_content, subjects, headers in rows:
        html_tpl = str(ready_content or "").strip()
        subject_pool = []
        if isinstance(subjects, list):
            subject_pool = [str(item).strip() for item in subjects if str(item or "").strip()]
        hdrs: Dict[str, str] = {}
        if isinstance(headers, dict):
            for k, v in headers.items():
                kk = str(k or "").strip()
                vv = str(v or "").strip()
                if kk and vv:
                    hdrs[kk] = vv
        if html_tpl and subject_pool:
            out[int(cid)] = CampaignLetterPayload(ready_html=html_tpl, subjects=subject_pool, headers=hdrs)
    return out


def _active_campaign_intervals(mailbox_id: int, campaign_ids: List[int]) -> Dict[int, int]:
    ids = [int(x) for x in campaign_ids if int(x) > 0]
    if not ids:
        return {}
    rows = fetch_all(
        """
        SELECT id, COALESCE(sending_interval, 0)::int AS sending_interval_ms
        FROM public.campaigns_campaigns
        WHERE mailbox_id = %s
          AND id = ANY(%s)
          AND active = true
          AND sending_interval IS NOT NULL
          AND sending_interval > 0
        """,
        [int(mailbox_id), ids],
    )
    return {int(cid): int(ms or 0) for cid, ms in rows if int(ms or 0) > 0}


def _sender_process_main(mailbox_id: int, campaign_ids: List[int], child_conn) -> None:
    def hb(*, next_wake_at: float, state: str, campaign_id: Optional[int] = None, reason: str = "") -> None:
        try:
            child_conn.send(
                {
                    "type": "hb",
                    "mailbox_id": int(mailbox_id),
                    "ts": _now_ts(),
                    "next_wake_at": float(next_wake_at),
                    "state": str(state),
                    "campaign_id": int(campaign_id) if campaign_id is not None else None,
                    "reason": str(reason or ""),
                }
            )
        except Exception:
            pass

    def dead(reason: str) -> None:
        try:
            child_conn.send(
                {
                    "type": "dead",
                    "mailbox_id": int(mailbox_id),
                    "ts": _now_ts(),
                    "reason": str(reason or ""),
                }
            )
        except Exception:
            pass

    _stop = {"v": False}

    def _sigterm(_signum, _frame):
        _stop["v"] = True

    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)

    death_at = _now_ts() + random.uniform(25 * 60, 45 * 60)

    try:
        _sender_loop(
            mailbox_id=int(mailbox_id),
            campaign_ids=[int(x) for x in (campaign_ids or [])],
            hb=hb,
            dead=dead,
            stop_flag=_stop,
            death_at_ts=death_at,
        )
    except Exception as e:
        dead(f"EXCEPTION:{type(e).__name__}:{e}")
        raise
    finally:
        try:
            child_conn.close()
        except Exception:
            pass


def _sender_loop(
    *,
    mailbox_id: int,
    campaign_ids: List[int],
    hb,
    dead,
    stop_flag: Dict[str, bool],
    death_at_ts: float,
) -> None:
    camp_ids = [int(x) for x in campaign_ids if int(x) > 0]
    if not camp_ids:
        while not stop_flag.get("v") and _now_ts() < death_at_ts:
            nxt = _now_ts() + 30.0
            hb(next_wake_at=nxt, state="NO_CAMPAIGNS", reason="empty_campaign_ids")
            time.sleep(30.0)
        dead("DONE")
        return

    next_send_at: Dict[int, float] = {}
    hb(next_wake_at=_now_ts() + 0.1, state="START", reason=f"campaigns={len(camp_ids)}")

    while True:
        if stop_flag.get("v"):
            dead("STOP")
            return
        if _now_ts() >= death_at_ts:
            dead("DEATH_AT")
            return

        active_intervals = _active_campaign_intervals(int(mailbox_id), camp_ids)
        active_ids = sorted(active_intervals.keys())

        for cid in list(next_send_at.keys()):
            if cid not in active_intervals:
                next_send_at.pop(cid, None)

        now_ts = _now_ts()
        for cid in active_ids:
            next_send_at.setdefault(int(cid), float(now_ts))

        if not active_ids:
            sleep_sec = 5.0
            hb(next_wake_at=_now_ts() + sleep_sec, state="NO_ACTIVE_CAMPAIGNS", reason="sleep=5s")
            time.sleep(sleep_sec)
            continue

        letter_payloads = _campaign_letter_payloads(active_ids)
        pending = _pending_by_campaign(active_ids)
        ready_candidates: List[Tuple[int, int]] = []
        nearest_next_ts: Optional[float] = None

        now_ts = _now_ts()
        for cid in active_ids:
            pending_cnt = int(pending.get(int(cid), 0))
            if pending_cnt <= 0:
                continue
            due_ts = float(next_send_at.get(int(cid), now_ts))
            if due_ts <= now_ts:
                ready_candidates.append((int(cid), int(pending_cnt)))
            else:
                nearest_next_ts = due_ts if nearest_next_ts is None else min(nearest_next_ts, due_ts)

        if not ready_candidates:
            if nearest_next_ts is None:
                sleep_sec = 5.0
            else:
                sleep_sec = max(0.5, min(5.0, float(nearest_next_ts - now_ts)))
            hb(
                next_wake_at=_now_ts() + sleep_sec,
                state="WAIT_INTERVAL",
                reason=f"ready=0 sleep={sleep_sec:.2f}s",
            )
            time.sleep(sleep_sec)
            continue

        candidate = _pick_send_candidate(ready_candidates)
        if candidate is None:
            sleep_sec = _SEND_FAIL_SLEEP_SEC
            hb(
                next_wake_at=_now_ts() + sleep_sec,
                state="NO_CANDIDATE",
                reason=f"ready={len(ready_candidates)} sleep={sleep_sec:.1f}s",
            )
            time.sleep(sleep_sec)
            continue

        interval_ms = int(active_intervals.get(int(candidate.campaign_id), 0))
        interval_sec = max(0.001, float(interval_ms) / 1000.0) if interval_ms > 0 else 0.5
        letter_payload = letter_payloads.get(int(candidate.campaign_id))
        if letter_payload is None:
            sleep_sec = _SEND_FAIL_SLEEP_SEC
            next_send_at[int(candidate.campaign_id)] = _now_ts() + sleep_sec
            hb(
                next_wake_at=next_send_at[int(candidate.campaign_id)],
                state="NO_LETTER_PAYLOAD",
                campaign_id=candidate.campaign_id,
                reason=f"campaign_id={candidate.campaign_id} no ready letter/subjects",
            )
            time.sleep(sleep_sec)
            continue

        contact = {
            "aggr_contact_id": candidate.aggr_contact_id,
            "company_name": candidate.company_name,
            "email": candidate.email,
            "norm": candidate.norm,
            "blocked": False,
            "wrong_email": False,
        }

        hb(
            next_wake_at=_now_ts() + max(1.0, interval_sec),
            state="SENDING",
            campaign_id=candidate.campaign_id,
            reason=(
                f"campaign_id={candidate.campaign_id} sending_list_id={candidate.sending_list_id} "
                f"interval_ms={interval_ms}"
            ),
        )
        sent = False
        try:
            campaign_payload = {
                "id": int(candidate.campaign_id),
                "mailbox_id": int(mailbox_id),
                "ready_content": letter_payload.ready_html,
                "subjects": letter_payload.subjects,
                "headers": letter_payload.headers,
            }
            sent = bool(
                send_one(
                    campaign=campaign_payload,
                    contact=contact,
                    sending_list_id=int(candidate.sending_list_id),
                    to_email_override=_RUNTIME_TO_EMAIL_OVERRIDE,
                    record_sent=True,
                )
            )
        except Exception as e:
            next_send_at[int(candidate.campaign_id)] = _now_ts() + 30.0
            sleep_sec = _SEND_FAIL_SLEEP_SEC
            hb(
                next_wake_at=_now_ts() + sleep_sec,
                state="SEND_EXCEPTION",
                campaign_id=candidate.campaign_id,
                reason=f"{type(e).__name__}:{e}",
            )
            time.sleep(sleep_sec)
            continue

        next_send_at[int(candidate.campaign_id)] = _now_ts() + float(interval_sec if sent else max(interval_sec, 1.0))
        hb(
            next_wake_at=next_send_at[int(candidate.campaign_id)],
            state="SCHEDULED_NEXT",
            campaign_id=candidate.campaign_id,
            reason=f"sent={str(bool(sent)).lower()} next_in={(interval_sec if sent else max(interval_sec, 1.0)):.2f}s",
        )


class Sender:
    def __init__(self) -> None:
        self.targets: Dict[int, List[int]] = {}
        self.currently_sending: Dict[int, SenderRuntime] = {}
        self.hb: Dict[int, Heartbeat] = {}
        self._start_events: deque[float] = deque()
        self._soft_failed_once: bool = False
        self._hard_dead: bool = False
        self._tick_n: int = 0
        self._next_rebuild_at: float = 0.0

    def main_guide(self, *, tick_sec: float = 5.0, hb_grace_sec: float = 60.0, rebuild_sec: float = 300.0) -> None:
        while True:
            self._tick_n += 1
            tick_started = _now_ts()

            if self._hard_dead:
                print(f"[CORE_SEND] TICK#{self._tick_n} HARD-DEAD (sleep 60s)")
                time.sleep(60.0)
                continue

            now_ts = _now_ts()
            if now_ts >= float(self._next_rebuild_at):
                print(f"[CORE_SEND] TICK#{self._tick_n} rebuild_desired_targets...")
                try:
                    self.targets = self._build_desired_targets()
                    self._next_rebuild_at = _now_ts() + float(rebuild_sec)
                except Exception as e:
                    print(f"[CORE_SEND] TICK#{self._tick_n} desired_targets error: {type(e).__name__}: {e}")
                    time.sleep(5.0)
                    continue

            desired_targets = self.targets
            desired_mailboxes = set(desired_targets.keys())

            total_campaigns = sum(len(v) for v in desired_targets.values())
            print(
                f"[CORE_SEND] TICK#{self._tick_n} desired_mailboxes={len(desired_mailboxes)} "
                f"campaigns_total={total_campaigns}"
            )

            self._poll_heartbeats()
            now = _now_ts()

            if self.currently_sending:
                for mid in sorted(self.currently_sending.keys()):
                    rt = self.currently_sending.get(mid)
                    hb0 = self.hb.get(mid)
                    pid = rt.proc.pid if rt else None
                    alive = bool(rt.proc.is_alive()) if rt else False
                    if not hb0:
                        print(f"[CORE_SEND]   STATUS mailbox_id={mid} pid={pid} alive={alive} hb=NONE")
                        continue
                    wait = _fmt_wait_sec(hb0.next_wake_at, now)
                    cid = hb0.campaign_id
                    rsn = (hb0.reason or "").strip()
                    if len(rsn) > 220:
                        rsn = rsn[:220] + "..."
                    print(
                        f"[CORE_SEND]   STATUS mailbox_id={mid} pid={pid} alive={alive} "
                        f"state={hb0.state} campaign_id={cid} next_in={wait} reason={rsn}"
                    )

            stale_killed = 0
            for mid in list(self.currently_sending.keys()):
                hb0 = self.hb.get(mid)
                if not hb0:
                    continue
                if now > hb0.next_wake_at + float(hb_grace_sec):
                    stale_killed += 1
                    print(
                        f"[CORE_SEND] TICK#{self._tick_n} STALE mailbox_id={mid} state={hb0.state} "
                        f"next_wake_at={hb0.next_wake_at:.0f} (now={now:.0f}) -> terminate"
                    )
                    self._terminate_runtime(mid, reason="stale_kill")

            changed_killed = 0
            for mid in list(self.currently_sending.keys()):
                rt = self.currently_sending.get(mid)
                if not rt:
                    continue
                desired_ids = sorted(int(x) for x in desired_targets.get(int(mid), []))
                current_ids = sorted(int(x) for x in (rt.campaign_ids or []))
                if int(mid) not in desired_mailboxes:
                    changed_killed += 1
                    self._terminate_runtime(int(mid), reason="no_longer_desired")
                    continue
                if desired_ids != current_ids:
                    changed_killed += 1
                    self._terminate_runtime(int(mid), reason="targets_changed")

            started = 0
            for mid in sorted(desired_mailboxes):
                rt = self.currently_sending.get(mid)
                if rt and rt.proc.is_alive():
                    continue
                started += 1
                mids = self.targets.get(mid, [])
                print(f"[CORE_SEND] TICK#{self._tick_n} START_NEEDED mailbox_id={mid} campaigns={len(mids)}")
                self._start_sender(mid, mids)

            if self._crashloop_triggered(limit=10, window_sec=60):
                if not self._soft_failed_once:
                    self._soft_failed_once = True
                    print("[CORE_SEND] CRASHLOOP: >=10 START за 60с -> kill all, sleep 10 минут")
                    self._kill_all_senders()
                    self._clear_start_events()
                    time.sleep(600.0)
                    print("[CORE_SEND] WAKE after 10 minutes")
                else:
                    print("[CORE_SEND] CRASHLOOP AGAIN: HARD-DEAD")
                    self._kill_all_senders()
                    self._hard_dead = True
                    continue

            tick_took = _now_ts() - tick_started
            print(
                f"[CORE_SEND] TICK#{self._tick_n} done: started={started} stale_killed={stale_killed} "
                f"changed_killed={changed_killed} took={tick_took:.2f}s sleep={float(tick_sec):.0f}s"
            )
            time.sleep(float(tick_sec))

    def _build_desired_targets(self) -> Dict[int, List[int]]:
        rows = fetch_all(
            """
            SELECT c.id, c.mailbox_id
            FROM public.campaigns_campaigns c
            WHERE c.active = true
              AND c.sending_interval IS NOT NULL
              AND c.sending_interval > 0
              AND c.sending_list_id IS NOT NULL
            """,
            [],
        )
        if not rows:
            return {}

        camp_ids = [int(row[0]) for row in rows if row and row[0] is not None]
        pending = _pending_by_campaign(camp_ids)

        out: Dict[int, List[int]] = {}
        for camp_id, mailbox_id in rows:
            cid = int(camp_id)
            if int(pending.get(cid, 0)) <= 0:
                continue
            mid = int(mailbox_id)
            out.setdefault(mid, []).append(cid)
        return out

    def _start_sender(self, mailbox_id: int, campaign_ids: List[int]) -> None:
        mailbox_id = int(mailbox_id)
        campaign_ids = [int(x) for x in (campaign_ids or [])]

        parent_conn, child_conn = Pipe(duplex=False)
        p = Process(
            target=_sender_process_main,
            args=(mailbox_id, campaign_ids, child_conn),
            name=f"core_send_mb_{mailbox_id}",
            daemon=True,
        )
        p.start()

        rt = SenderRuntime(mailbox_id=mailbox_id, campaign_ids=campaign_ids, proc=p, hb_conn=parent_conn)
        self.currently_sending[mailbox_id] = rt

        self._start_events.append(_now_ts())
        print(f"[CORE_SEND] START mailbox_id={mailbox_id} pid={p.pid} campaigns={len(campaign_ids)}")

    def _terminate_runtime(self, mailbox_id: int, *, reason: str) -> None:
        rt = self.currently_sending.get(int(mailbox_id))
        if not rt:
            return
        p = rt.proc
        if p.is_alive():
            try:
                p.terminate()
            except Exception:
                pass
            p.join(timeout=2.0)
            if p.is_alive():
                try:
                    os.kill(p.pid, signal.SIGKILL)
                except Exception:
                    pass
                p.join(timeout=2.0)

        try:
            rt.hb_conn.close()
        except Exception:
            pass

        self.currently_sending.pop(int(mailbox_id), None)
        self.hb.pop(int(mailbox_id), None)
        print(f"[CORE_SEND] TERMINATED mailbox_id={mailbox_id} reason={reason}")

    def _kill_all_senders(self) -> None:
        for mid in list(self.currently_sending.keys()):
            self._terminate_runtime(mid, reason="kill_all")

    def _poll_heartbeats(self) -> None:
        now = _now_ts()
        for mid, rt in list(self.currently_sending.items()):
            conn = rt.hb_conn
            while True:
                try:
                    if not conn.poll(0):
                        break
                    msg = conn.recv()
                except EOFError:
                    print(f"[CORE_SEND] DEAD mailbox_id={mid} reason=EOF")
                    self._handle_child_dead(mid)
                    break
                except Exception as e:
                    print(f"[CORE_SEND] DEAD mailbox_id={mid} reason=HB_ERROR:{type(e).__name__}:{e}")
                    self._handle_child_dead(mid)
                    break

                if not isinstance(msg, dict):
                    continue
                t = msg.get("type")
                if t == "hb":
                    hb0 = Heartbeat(
                        last_seen=float(msg.get("ts") or now),
                        next_wake_at=float(msg.get("next_wake_at") or now),
                        state=str(msg.get("state") or ""),
                        campaign_id=(int(msg["campaign_id"]) if msg.get("campaign_id") is not None else None),
                        reason=str(msg.get("reason") or ""),
                    )
                    self.hb[mid] = hb0
                    rt.last_hb = hb0
                elif t == "dead":
                    reason = str(msg.get("reason") or "")
                    print(f"[CORE_SEND] DEAD mailbox_id={mid} reason={reason}")
                    self._handle_child_dead(mid)

            if not rt.proc.is_alive() and mid in self.currently_sending:
                print(f"[CORE_SEND] DEAD mailbox_id={mid} reason=EXITED")
                self._handle_child_dead(mid)

    def _handle_child_dead(self, mailbox_id: int) -> None:
        rt = self.currently_sending.get(int(mailbox_id))
        if not rt:
            return
        if rt.proc.is_alive():
            self._terminate_runtime(int(mailbox_id), reason="dead_but_alive")
            return
        try:
            rt.hb_conn.close()
        except Exception:
            pass
        self.currently_sending.pop(int(mailbox_id), None)
        self.hb.pop(int(mailbox_id), None)

    def _clear_start_events(self) -> None:
        self._start_events.clear()

    def _crashloop_triggered(self, *, limit: int, window_sec: int) -> bool:
        now = _now_ts()
        while self._start_events and now - self._start_events[0] > float(window_sec):
            self._start_events.popleft()
        return len(self._start_events) >= int(limit)


def main() -> None:
    Sender().main_guide(tick_sec=5.0, hb_grace_sec=60.0, rebuild_sec=300.0)


if __name__ == "__main__":
    main()
