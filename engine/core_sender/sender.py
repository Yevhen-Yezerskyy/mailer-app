# FILE: engine/core_sender/sender.py  (обновлено — 2026-01-30)
# PURPOSE:
# - Sender: один объект-оркестратор (main_guide) + много sender-процессов (по mailbox_id).
# - Оркестратор:
#   - строит desired_targets из активных кампаний с учётом окон
#   - запускает недостающих sender'ов по mailbox_id
#   - убивает "подозрительных на сдохшесть" (now > hb.next_wake_at + grace) и запускает нового
#   - global fail-fast: если >=10 START'ов sender-процессов за 60 секунд → kill all + sleep 10 минут;
#     если повторится после пробуждения → hard-dead (навсегда), раз в минуту печатает статус.
# - SMTP/лимиты/кулдауны — только внутри sender() и send_one().
# - Праздники (DE) берём из engine/common/email_template.py (там import holidays без try/except).

from __future__ import annotations

import os
import random
import signal
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Pipe, Process
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from engine.common.db import fetch_all, fetch_one
from engine.common.email_template import _is_de_public_holiday
from engine.common.mail.send import send_one

_TZ_BERLIN = ZoneInfo("Europe/Berlin")


# -------------------------
# Window evaluation (same formats as panel)
# -------------------------


def _parse_hhmm_to_minutes(s: str) -> Optional[int]:
    try:
        s = (s or "").strip()
        if not s or ":" not in s:
            return None
        h, m = s.split(":", 1)
        hh = int(h)
        mm = int(m)
        if hh < 0 or hh > 23 or mm < 0 or mm > 59:
            return None
        return hh * 60 + mm
    except Exception:
        return None


def _window_is_nonempty(win: object) -> bool:
    if not isinstance(win, dict):
        return False
    for v in win.values():
        if isinstance(v, list) and len(v) > 0:
            return True
    return False


def _iter_slots(slots_obj: Any) -> Iterable[Tuple[str, str]]:
    """
    Accept formats:
      A) [{"from":"09:00","to":"12:00"}, ...]
      B) [["09:00","12:00"], ...] or [("09:00","12:00"), ...]
    """
    if not isinstance(slots_obj, list):
        return []
    out: list[Tuple[str, str]] = []
    for it in slots_obj:
        if isinstance(it, dict):
            a = str(it.get("from") or "").strip()
            b = str(it.get("to") or "").strip()
            if a and b:
                out.append((a, b))
            continue
        if isinstance(it, (list, tuple)) and len(it) == 2:
            a = str(it[0] or "").strip()
            b = str(it[1] or "").strip()
            if a and b:
                out.append((a, b))
    return out


def _is_now_in_send_window(now_de: datetime, camp_window: object, global_window: object) -> bool:
    win = camp_window if _window_is_nonempty(camp_window) else (global_window if isinstance(global_window, dict) else {})
    if not isinstance(win, dict):
        return False

    today = now_de.date()
    if _is_de_public_holiday(today):
        key = "hol"
    else:
        wd = now_de.weekday()  # mon=0..sun=6
        key = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")[wd]

    cur = now_de.hour * 60 + now_de.minute
    for a_str, b_str in _iter_slots(win.get(key, [])):
        a = _parse_hhmm_to_minutes(a_str)
        b = _parse_hhmm_to_minutes(b_str)
        if a is None or b is None:
            continue
        if b <= a:
            continue
        if a <= cur < b:
            return True

    return False


# -------------------------
# Heartbeat protocol
# -------------------------


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
    hb_conn: Any  # parent side of Pipe
    last_hb: Optional[Heartbeat] = None


def _now_ts() -> float:
    return time.time()


# -------------------------
# Child process entry
# -------------------------


def _sender_process_main(mailbox_id: int, campaign_ids: List[int], child_conn) -> None:
    """
    Child process:
    - Runs one sender loop for mailbox_id.
    - Sends hb/dead messages to parent via Pipe.
    """

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

    # death jitter (avoid synchronous waves)
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


# -------------------------
# Sender loop (one mailbox)
# -------------------------


def _sender_loop(
    *,
    mailbox_id: int,
    campaign_ids: List[int],
    hb,
    dead,
    stop_flag: Dict[str, bool],
    death_at_ts: float,
) -> None:
    """
    One sender loop for one mailbox.

    NOTE:
    - SMTP/лимиты/кулдауны — внутри send_one().
    - Здесь только выбор кампании/кандидата и вызов send_one().
    """

    row = fetch_one(
        """
        SELECT limit_hour_sent
        FROM aap_settings_smtp_mailboxes
        WHERE mailbox_id = %s
        LIMIT 1
        """,
        [int(mailbox_id)],
    )
    limit_hour_sent = int(row[0]) if row and row[0] is not None else 0

    if limit_hour_sent <= 0:
        while not stop_flag.get("v") and _now_ts() < death_at_ts:
            nxt = _now_ts() + 60.0
            hb(next_wake_at=nxt, state="NO_LIMIT", reason="limit_hour_sent<=0")
            time.sleep(60)
        dead("DONE")
        return

    send_interval = 3600.0 / float(limit_hour_sent)

    camp_ids = [int(x) for x in campaign_ids if int(x) > 0]
    if not camp_ids:
        while not stop_flag.get("v") and _now_ts() < death_at_ts:
            nxt = _now_ts() + 30.0
            hb(next_wake_at=nxt, state="NO_CAMPAIGNS")
            time.sleep(30)
        dead("DONE")
        return

    while True:
        if stop_flag.get("v"):
            dead("STOP")
            return
        if _now_ts() >= death_at_ts:
            dead("DEATH_AT")
            return

        now_de = datetime.now(tz=ZoneInfo("UTC")).astimezone(_TZ_BERLIN)

        rows = fetch_all(
            """
            SELECT id, workspace_id, mailing_list_id, window
            FROM campaigns_campaigns
            WHERE mailbox_id = %s
              AND id = ANY(%s)
              AND active = true
              AND start_at <= now()
              AND (end_at IS NULL OR end_at >= now())
            """,
            [int(mailbox_id), camp_ids],
        )

        if not rows:
            nxt = _now_ts() + min(60.0, send_interval)
            hb(next_wake_at=nxt, state="NO_ACTIVE_CAMPAIGNS")
            time.sleep(min(60.0, send_interval))
            continue

        ws_ids = sorted({str(r[1]) for r in rows if r and r[1]})
        global_windows: Dict[str, object] = {}
        if ws_ids:
            wrows = fetch_all(
                """
                SELECT workspace_id::text, value_json
                FROM aap_settings_sending_settings
                WHERE workspace_id = ANY(%s)
                """,
                [ws_ids],
            )
            for ws_id, val in wrows:
                global_windows[str(ws_id)] = val

        candidates: List[Tuple[int, int]] = []  # (campaign_id, weight)
        for camp_id, ws_id, mailing_list_id, win in rows:
            gw = global_windows.get(str(ws_id), {})
            if not _is_now_in_send_window(now_de, win, gw):
                continue

            r = fetch_one(
                """
                SELECT COUNT(*)
                FROM lists_contacts lc
                LEFT JOIN mailbox_sent ms
                  ON ms.campaign_id = %s
                 AND ms.rate_contact_id = lc.contact_id
                WHERE lc.list_id = %s
                  AND lc.active = true
                  AND ms.id IS NULL
                """,
                [int(camp_id), int(mailing_list_id)],
            )
            w = int(r[0]) if r and r[0] is not None else 0
            if w > 0:
                candidates.append((int(camp_id), w))

        if not candidates:
            nxt = _now_ts() + min(60.0, send_interval)
            hb(next_wake_at=nxt, state="NO_PENDING_OR_WINDOW")
            time.sleep(min(60.0, send_interval))
            continue

        total = sum(w for _, w in candidates)
        pick = random.randint(1, total)
        camp_id = candidates[0][0]
        acc = 0
        for cid, w in candidates:
            acc += w
            if pick <= acc:
                camp_id = cid
                break

        row = fetch_one(
            """
            SELECT lc.id AS list_contact_id
            FROM campaigns_campaigns c
            JOIN lists_contacts lc
              ON lc.list_id = c.mailing_list_id
             AND lc.active = true
            LEFT JOIN mailbox_sent ms
              ON ms.campaign_id = c.id
             AND ms.rate_contact_id = lc.contact_id
            LEFT JOIN rate_contacts rc
              ON rc.contact_id = lc.contact_id
            WHERE c.id = %s
              AND ms.id IS NULL
            ORDER BY
              rc.rate_cl ASC NULLS LAST,
              rc.rate_cb ASC NULLS LAST,
              lc.id ASC
            LIMIT 1
            """,
            [int(camp_id)],
        )

        if not row:
            nxt = _now_ts() + min(30.0, send_interval)
            hb(next_wake_at=nxt, state="NO_CANDIDATE", campaign_id=camp_id)
            time.sleep(min(30.0, send_interval))
            continue

        list_contact_id = int(row[0])

        # hb заранее, чтобы оркестратор не убил во время отправки
        hb(next_wake_at=_now_ts() + (send_interval + 60.0), state="SENDING", campaign_id=camp_id)

        try:
            send_one(int(camp_id), int(list_contact_id))
        except Exception as e:
            dead(f"SEND_ONE_EXCEPTION:{type(e).__name__}:{e}")
            return

        nxt = _now_ts() + float(send_interval)
        hb(next_wake_at=nxt, state="SLEEP", campaign_id=camp_id)
        time.sleep(max(0.0, float(send_interval)))


# -------------------------
# Orchestrator
# -------------------------


class Sender:
    def __init__(self) -> None:
        self.targets: Dict[int, List[int]] = {}
        self.currently_sending: Dict[int, SenderRuntime] = {}
        self.hb: Dict[int, Heartbeat] = {}

        # считаем START'ы процессов sender
        self._start_events: deque[float] = deque()

        # fail-fast levels
        self._soft_failed_once: bool = False
        self._hard_dead: bool = False

    def main_guide(self, *, tick_sec: float = 2.0, hb_grace_sec: float = 60.0) -> None:
        while True:
            if self._hard_dead:
                print("[SENDER] HARD-DEAD: мы сдохли навсегда")
                time.sleep(60)
                continue

            try:
                desired_targets = self._build_desired_targets()
                self.targets = desired_targets
            except Exception as e:
                print(f"[SENDER] desired_targets error: {type(e).__name__}: {e}")
                time.sleep(5)
                continue

            desired_mailboxes = set(desired_targets.keys())

            self._poll_heartbeats()

            now = _now_ts()

            # kill suspicious on stale (based on sender-provided next_wake_at)
            for mid in list(self.currently_sending.keys()):
                hb = self.hb.get(mid)
                if not hb:
                    continue
                if now > hb.next_wake_at + float(hb_grace_sec):
                    print(
                        f"[SENDER] STALE mailbox_id={mid} state={hb.state} next_wake_at={hb.next_wake_at:.0f} → terminate"
                    )
                    self._terminate_runtime(mid, reason="stale_kill")

            # ensure running: start missing / dead (only if needed now)
            for mid in sorted(desired_mailboxes):
                rt = self.currently_sending.get(mid)
                if rt and rt.proc.is_alive():
                    continue
                self._start_sender(mid, self.targets.get(mid, []))

            # global fail-fast by START rate
            if self._crashloop_triggered(limit=10, window_sec=60):
                if not self._soft_failed_once:
                    self._soft_failed_once = True
                    print("[SENDER] CRASHLOOP: >=10 START за 60с → kill all, sleep 10 минут")
                    self._kill_all_senders()
                    self._clear_start_events()
                    time.sleep(600)
                    print("[SENDER] WAKE after 10 minutes")
                else:
                    print("[SENDER] CRASHLOOP AGAIN: HARD-DEAD")
                    self._kill_all_senders()
                    self._hard_dead = True
                    continue

            time.sleep(float(tick_sec))

    def _build_desired_targets(self) -> Dict[int, List[int]]:
        now_de = datetime.now(tz=ZoneInfo("UTC")).astimezone(_TZ_BERLIN)

        rows = fetch_all(
            """
            SELECT id, mailbox_id, workspace_id, window
            FROM campaigns_campaigns
            WHERE active = true
              AND start_at <= now()
              AND (end_at IS NULL OR end_at >= now())
            """,
            [],
        )
        if not rows:
            return {}

        ws_ids = sorted({str(r[2]) for r in rows if r and r[2]})
        global_windows: Dict[str, object] = {}
        if ws_ids:
            wrows = fetch_all(
                """
                SELECT workspace_id::text, value_json
                FROM aap_settings_sending_settings
                WHERE workspace_id = ANY(%s)
                """,
                [ws_ids],
            )
            for ws_id, val in wrows:
                global_windows[str(ws_id)] = val

        out: Dict[int, List[int]] = {}
        for camp_id, mailbox_id, ws_id, win in rows:
            gw = global_windows.get(str(ws_id), {})
            if not _is_now_in_send_window(now_de, win, gw):
                continue
            mid = int(mailbox_id)
            out.setdefault(mid, []).append(int(camp_id))

        return out

    def _start_sender(self, mailbox_id: int, campaign_ids: List[int]) -> None:
        mailbox_id = int(mailbox_id)
        campaign_ids = [int(x) for x in (campaign_ids or [])]

        parent_conn, child_conn = Pipe(duplex=False)
        p = Process(
            target=_sender_process_main,
            args=(mailbox_id, campaign_ids, child_conn),
            name=f"sender_mb_{mailbox_id}",
            daemon=True,
        )
        p.start()

        rt = SenderRuntime(mailbox_id=mailbox_id, campaign_ids=campaign_ids, proc=p, hb_conn=parent_conn)
        self.currently_sending[mailbox_id] = rt

        # важно: считаем только START, а не terminate/kill
        self._start_events.append(_now_ts())

        print(f"[SENDER] START mailbox_id={mailbox_id} pid={p.pid} campaigns={len(campaign_ids)}")

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

        print(f"[SENDER] TERMINATED mailbox_id={mailbox_id} reason={reason}")

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
                    print(f"[SENDER] DEAD mailbox_id={mid} reason=EOF")
                    self._handle_child_dead(mid)
                    break
                except Exception as e:
                    print(f"[SENDER] DEAD mailbox_id={mid} reason=HB_ERROR:{type(e).__name__}:{e}")
                    self._handle_child_dead(mid)
                    break

                if not isinstance(msg, dict):
                    continue

                t = msg.get("type")
                if t == "hb":
                    hb = Heartbeat(
                        last_seen=float(msg.get("ts") or now),
                        next_wake_at=float(msg.get("next_wake_at") or now),
                        state=str(msg.get("state") or ""),
                        campaign_id=(int(msg["campaign_id"]) if msg.get("campaign_id") is not None else None),
                        reason=str(msg.get("reason") or ""),
                    )
                    self.hb[mid] = hb
                    rt.last_hb = hb
                elif t == "dead":
                    reason = str(msg.get("reason") or "")
                    print(f"[SENDER] DEAD mailbox_id={mid} reason={reason}")
                    self._handle_child_dead(mid)

            if not rt.proc.is_alive() and mid in self.currently_sending:
                print(f"[SENDER] DEAD mailbox_id={mid} reason=EXITED")
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
    Sender().main_guide(tick_sec=2.0, hb_grace_sec=60.0)


if __name__ == "__main__":
    main()
