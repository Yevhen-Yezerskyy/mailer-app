# FILE: web/panel/aap_campaigns/views/campaigns.py
# PATH: web/panel/aap_campaigns/views/campaigns.py
# DATE: 2026-02-03
# SUMMARY (patch):
# - fix: keep POSTed form values when mailing list is taken (dedup error), so form doesn't reset
# - stats in bottom table: total/sent/left for each campaign (total=active sending_lists rows; sent=all mailbox_sent rows; left=max(0,total-sent))
# - POST action send_test: only when letter exists; sends test with to_email_override + record_sent=False
# - do NOT touch existing window logic/helpers (kept local); reuse shared _is_de_public_holiday() helper

from __future__ import annotations

import json
import re
from datetime import date, datetime
from types import SimpleNamespace
from typing import Any, Iterable, Optional, Tuple, Union
from urllib.parse import urlencode
from uuid import UUID
from zoneinfo import ZoneInfo

from django.db import IntegrityError, connection
from django.http import HttpResponseRedirect
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext as _

from engine.common.email_template import _is_de_public_holiday, render_html, sanitize
from engine.common.mail.send import send_one
from mailer_web.access import decode_id, encode_id, resolve_pk_or_redirect
from panel.aap_audience.models import AudienceTask
from panel.aap_campaigns.models import Campaign, Letter, Templates
from panel.aap_campaigns.template_editor import (
    editor_template_parse_html,
    find_demo_content_from_template,
    letter_editor_extract_content,
    letter_editor_render_html,
    styles_css_to_json,
    styles_json_to_css,
)
from panel.models import GlobalTemplate
from panel.aap_settings.models import (
    GlobalSendingSettings,
    Mailbox,
    SendingSettings,
    SmtpMailbox,
    default_global_global_window_json,
)

_TZ_BERLIN = ZoneInfo("Europe/Berlin")


def _guard(request) -> tuple[Optional[UUID], Optional[object]]:
    ws_id = getattr(request, "workspace_id", None)
    user = getattr(request, "user", None)
    if not ws_id or not getattr(user, "is_authenticated", False):
        return None, None
    return ws_id, user


def _qs(ws_id: UUID):
    return (
        Campaign.objects.filter(workspace_id=ws_id)
        .select_related("sending_list", "mailing_list", "mailbox", "letter", "letter__template")
        .order_by("-updated_at")
    )


def _with_ui_ids(items):
    for it in items:
        it.ui_id = encode_id(int(it.id))
    return items


def _get_state(request) -> str:
    st = (request.GET.get("state") or "").strip()
    return st if st in ("add", "edit", "letter") else ""


def _legacy_get_step(request) -> str:
    step = (request.GET.get("step") or "").strip().lower()
    if step in ("campaign", "template", "letter"):
        return step

    legacy_state = _get_state(request)
    if legacy_state in ("add", "edit"):
        return "campaign"
    if legacy_state == "letter":
        return "letter"
    return "campaign"


def _get_campaign_obj_by_ui_id(ws_id: UUID, token: str) -> Campaign | None:
    if not token:
        return None
    try:
        pk = int(decode_id(token))
    except Exception:
        return None
    return Campaign.objects.filter(id=pk, workspace_id=ws_id).first()


def _flow_url(step: str, campaign_ui_id: str = "", *, tpl_state: str = "", tpl_id: str = "", gl_tpl: str = "") -> str:
    route_name_map = {
        "campaign": ("campaigns:campaigns_flow_campaign", "campaigns:campaigns_flow_campaign_id"),
        "template": ("campaigns:campaigns_flow_template", "campaigns:campaigns_flow_template_id"),
        "letter": ("campaigns:campaigns_flow_letter", "campaigns:campaigns_flow_letter_id"),
    }
    route_pair = route_name_map.get(step, route_name_map["campaign"])
    if campaign_ui_id:
        base = reverse(route_pair[1], kwargs={"item_id": campaign_ui_id})
    else:
        base = reverse(route_pair[0])

    params: dict[str, str] = {}
    if tpl_state:
        params["tpl_state"] = tpl_state
    if tpl_id:
        params["tpl_id"] = tpl_id
    if gl_tpl:
        params["gl_tpl"] = gl_tpl
    if not params:
        return base
    return f"{base}?{urlencode(params)}"


def _get_edit_obj(request, ws_id: UUID) -> Union[None, Campaign, HttpResponseRedirect]:
    state = _get_state(request)
    if state not in ("edit", "letter"):
        return None
    if not request.GET.get("id"):
        return None

    res = resolve_pk_or_redirect(request, Campaign, param="id")
    if isinstance(res, HttpResponseRedirect):
        return res

    return Campaign.objects.filter(id=int(res), workspace_id=ws_id).first()


def _ensure_letter(ws_id: UUID, camp: Campaign) -> Letter:
    obj = Letter.objects.filter(workspace_id=ws_id, campaign=camp).first()
    if obj:
        return obj
    return Letter.objects.create(workspace_id=ws_id, campaign=camp)


def _get_tpl_state(request) -> str:
    st = (request.GET.get("tpl_state") or "").strip().lower()
    return st if st in ("add", "edit") else ""


def _extract_global_template_id_from_first_tag(template_html: str) -> int | None:
    s = (template_html or "").lstrip()
    if not s:
        return None

    m_tag = re.search(r"(?is)<\s*([a-zA-Z][a-zA-Z0-9:_-]*)([^>]*)>", s)
    if not m_tag:
        return None

    attrs = m_tag.group(2) or ""
    m_class = re.search(r"""(?is)\bclass\s*=\s*(?P<q>["'])(?P<v>.*?)(?P=q)""", attrs)
    if not m_class:
        return None

    class_value = (m_class.group("v") or "").strip()
    if not class_value:
        return None

    for token in class_value.split():
        if token.startswith("id-"):
            tail = token[3:]
            if tail.isdigit():
                return int(tail)
    return None


def _get_gl_tpl_from_query(request) -> int | None:
    raw = (request.GET.get("gl_tpl") or "").strip()
    return int(raw) if raw.isdigit() else None


def _pick_random_active_gl_tpl_id() -> int | None:
    obj = GlobalTemplate.objects.filter(is_active=True).order_by("?").first()
    return int(obj.id) if obj else None


def _global_style_keys_by_gid(gid: int | None) -> tuple[int | None, list[str], list[str]]:
    if not gid:
        return None, [], []

    gt = GlobalTemplate.objects.filter(id=int(gid), is_active=True).first()
    if not gt or not isinstance(gt.styles, dict):
        return None, [], []

    colors = gt.styles.get("colors")
    fonts = gt.styles.get("fonts")

    c_keys = sorted([k for k in (colors or {}).keys() if isinstance(k, str)]) if isinstance(colors, dict) else []
    f_keys = sorted([k for k in (fonts or {}).keys() if isinstance(k, str)]) if isinstance(fonts, dict) else []
    return int(gt.id), c_keys, f_keys


def _build_global_tpl_items(current_gid: int | None):
    out = []
    qs = GlobalTemplate.objects.filter(is_active=True).order_by("order", "template_name")
    for gt in qs:
        out.append(
            SimpleNamespace(
                id=int(gt.id),
                template_name=gt.template_name,
                is_current=bool(current_gid and int(current_gid) == int(gt.id)),
            )
        )
    return out


def _get_tpl_edit_obj(request, ws_id: UUID):
    if _get_tpl_state(request) != "edit":
        return None
    token = (request.GET.get("tpl_id") or "").strip()
    if not token:
        return None
    try:
        pk = int(decode_id(token))
    except Exception:
        return None
    obj = (
        Templates.objects
        .filter(id=pk, workspace_id=ws_id, archived=False)
        .first()
    )
    if obj:
        obj.ui_id = encode_id(int(obj.id))
    return obj


def _styles_pick_main(styles_obj):
    if not isinstance(styles_obj, dict):
        return {}
    main = styles_obj.get("main")
    return main if isinstance(main, dict) else styles_obj


def _parse_date_from_post(request, field_name: str) -> Optional[date]:
    try:
        raw = (request.POST.get(field_name) or "").strip()
        if not raw:
            return None
        return date.fromisoformat(raw)
    except Exception:
        return None


def _build_sender_labels(mailboxes: list[Mailbox]) -> dict[int, str]:
    """
    sender_label = from_email (только email)
    берём из SmtpMailbox.from_email (если есть), fallback -> Mailbox.email
    """
    if not mailboxes:
        return {}

    mb_by_id = {int(m.id): m for m in mailboxes}
    mb_ids = list(mb_by_id.keys())

    smtp_by_mb: dict[int, SmtpMailbox] = {}
    for s in (
        SmtpMailbox.objects.filter(mailbox_id__in=mb_ids, is_active=True)
        .only("id", "mailbox_id", "from_email")
        .order_by("mailbox_id")
    ):
        smtp_by_mb[int(s.mailbox_id)] = s

    out: dict[int, str] = {}
    for mid, mb in mb_by_id.items():
        s = smtp_by_mb.get(mid)
        from_email = ((getattr(s, "from_email", "") or "").strip() if s else "") or (mb.email or "").strip() or "—"
        out[mid] = from_email
    return out


def _sending_list_is_taken(ws_id: UUID, sending_list_id: int, exclude_campaign_id: Optional[int]) -> bool:
    q = Campaign.objects.filter(workspace_id=ws_id, sending_list_id=int(sending_list_id))
    if exclude_campaign_id:
        q = q.exclude(id=int(exclude_campaign_id))
    return q.exists()


def _update_campaign_title_if_needed(camp: Campaign | None, new_title: str) -> None:
    if not camp:
        return
    title = (new_title or "").strip()
    if not title or title == (camp.title or ""):
        return
    camp.title = title
    camp.save(update_fields=["title", "updated_at"])


def _now_berlin() -> datetime:
    dt = timezone.now()
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone=ZoneInfo("UTC"))
    return dt.astimezone(_TZ_BERLIN)


# -------- Window evaluation (KEEP LOCAL; UI logic) --------


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
      A) [{"from":"09:00","to":"12:00"}, ...]   (JS current)
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


# -------- Stats (total/sent/left) --------


def _stats_by_campaign_ids(campaign_ids: list[int]) -> dict[int, tuple[int, int, int]]:
    """
    {campaign_id: (total, sent, left)}
    """
    ids = [int(x) for x in (campaign_ids or []) if int(x) > 0]
    if not ids:
        return {}

    out: dict[int, tuple[int, int, int]] = {cid: (0, 0, 0) for cid in ids}

    # --- total: rows in sending list for campaign task ---
    sql_total = """
    SELECT
        c.id AS campaign_id,
        COUNT(sl.aggr_contact_cb_id) AS total_cnt
    FROM public.campaigns_campaigns c
    JOIN public.sending_lists sl
      ON sl.task_id = c.sending_list_id
     AND COALESCE(sl.removed, false) = false
    WHERE c.id = ANY(%s)
    GROUP BY c.id
    """

    # --- sent: mailbox_sent rows per campaign ---
    sql_sent = """
    SELECT
        campaign_id,
        COUNT(id) AS sent_cnt
    FROM public.mailbox_sent
    WHERE campaign_id = ANY(%s)
    GROUP BY campaign_id
    """

    totals: dict[int, int] = {}
    sents: dict[int, int] = {}

    with connection.cursor() as cur:
        cur.execute(sql_total, [ids])
        for cid, total_cnt in cur.fetchall():
            totals[int(cid)] = int(total_cnt or 0)

        cur.execute(sql_sent, [ids])
        for cid, sent_cnt in cur.fetchall():
            sents[int(cid)] = int(sent_cnt or 0)

    for cid in ids:
        total = totals.get(cid, 0)
        sent = sents.get(cid, 0)
        left = total - sent
        if left < 0:
            left = 0
        out[cid] = (total, sent, left)

    return out


def _ctx_build(
    ws_id: UUID,
    state: str,
    edit_obj,
    list_items,
    mb_items,
    tpl_items,
    parent_items,
    global_window_json,
    letter_obj,
    letter_init_html: str,
    letter_init_css: str,
    letter_init_subjects: str,
    letter_init_headers: str,
    letter_template_html: str,
    deleted_tpl_ui,
    deleted_tpl_id,
    *,
    form_error_msg: str = "",
    form_error_field: str = "",
    test_msg: str = "",
):
    items = _with_ui_ids(_qs(ws_id))

    sender_label_by_mb_id = _build_sender_labels(list(mb_items))
    now_de = _now_berlin()

    camp_ids = [int(it.id) for it in items if getattr(it, "id", None) is not None]
    stats = _stats_by_campaign_ids(camp_ids)

    for it in items:
        it.sender_label = sender_label_by_mb_id.get(int(it.mailbox_id), f"{it.mailbox.email}")

        tpl = None
        if getattr(it, "letter", None):
            tpl = it.letter.template if it.letter and it.letter.template_id else None

        if tpl and not getattr(tpl, "archived", False):
            it.letter_tpl_label = tpl.template_name
        else:
            it.letter_tpl_label = _("Шаблон удален")

        total, sent, left = stats.get(int(it.id), (0, 0, 0))
        it.stat_total = int(total)
        it.stat_sent = int(sent)
        it.stat_left = int(left)

        it.is_in_window = False
        if getattr(it, "active", False):
            it.is_in_window = _is_now_in_send_window(now_de, getattr(it, "window", None), global_window_json)

    edit_window_json_str = ""
    edit_window_nonempty = False
    if edit_obj and isinstance(edit_obj.window, dict):
        edit_window_json_str = json.dumps(edit_obj.window or {}, ensure_ascii=False)
        edit_window_nonempty = _window_is_nonempty(edit_obj.window)

    # IMPORTANT: тестовая отправка доступна только когда письмо реально есть (Letter существует)
    has_letter = bool(letter_obj and (getattr(letter_obj, "ready_content", "") or "").strip())

    return {
        "items": items,
        "state": state,
        "edit_obj": edit_obj,
        "letter_obj": letter_obj,
        "has_letter": has_letter,
        "test_msg": test_msg,
        "list_items": list_items,
        "mb_items": mb_items,
        "tpl_items": tpl_items,
        "parent_items": parent_items,
        "global_window_json_str": json.dumps(global_window_json or {}, ensure_ascii=False),
        "edit_window_json_str": edit_window_json_str,
        "edit_window_nonempty": edit_window_nonempty,
        "deleted_tpl_ui": deleted_tpl_ui,
        "deleted_tpl_id": deleted_tpl_id,
        # letter init
        "letter_init_html": letter_init_html,
        "letter_init_css": letter_init_css,
        "letter_init_subjects": letter_init_subjects,
        "letter_init_headers": letter_init_headers,
        "letter_template_html": letter_template_html,
        # form errors
        "form_error_msg": form_error_msg,
        "form_error_field": form_error_field,
    }


def _build_flow_step_states(current_step: str, campaign_ui_id: str = "", *, template_saved: bool = False):
    steps = [
        ("campaign", _("Общая информация")),
        ("template", _("Шаблон письма")),
        ("letter", _("Письмо кампании")),
    ]
    out = []
    has_campaign = bool(campaign_ui_id)
    for key, label in steps:
        clickable = True
        if key in ("template", "letter") and not has_campaign:
            clickable = False
        if key == "letter" and not template_saved:
            clickable = False
        out.append(
            {
                "key": key,
                "label": label,
                "url": _flow_url(key, campaign_ui_id),
                "is_current": key == current_step,
                "is_clickable": clickable,
            }
        )
    return out


def _build_template_step_context(request, ws_id: UUID):
    tpl_state = _get_tpl_state(request)
    tpl_edit_obj = _get_tpl_edit_obj(request, ws_id) if tpl_state == "edit" else None

    current_gid = _get_gl_tpl_from_query(request)
    if not current_gid and tpl_state == "edit" and tpl_edit_obj:
        current_gid = _extract_global_template_id_from_first_tag(tpl_edit_obj.template_html or "")

    global_style_gid, global_colors, global_fonts = _global_style_keys_by_gid(current_gid)
    global_tpl_items = _build_global_tpl_items(current_gid)

    tpl_items = _with_ui_ids(Templates.objects.filter(workspace_id=ws_id, archived=False).order_by("-updated_at"))
    return {
        "tpl_items": tpl_items,
        "tpl_state": tpl_state,
        "tpl_edit_obj": tpl_edit_obj,
        "global_style_gid": global_style_gid,
        "global_colors": global_colors,
        "global_fonts": global_fonts,
        "global_tpl_items": global_tpl_items,
    }


def _suggest_flow_template_name(request, ws_id: UUID, edit_obj, mb_items) -> str:
    ws = getattr(getattr(request, "user", None), "workspace", None)
    company_name = (getattr(ws, "company_name", "") or "").strip() or "Шаблон"

    smtp_label = ""
    mailbox_id = int(getattr(edit_obj, "mailbox_id", 0) or 0)
    if mailbox_id > 0:
        for mb in mb_items:
            try:
                if int(getattr(mb, "id", 0)) != mailbox_id:
                    continue
            except Exception:
                continue
            smtp_label = (
                str(getattr(mb, "sender_label", "") or "").strip()
                or str(getattr(mb, "email", "") or "").strip()
            )
            break
    if not smtp_label:
        smtp_label = "SMTP"

    def _build_name(company_part: str) -> str:
        return f"{company_part} - {smtp_label}"

    first = _build_name(company_name)
    if not Templates.objects.filter(workspace_id=ws_id, template_name=first).exists():
        return first

    idx = 2
    while True:
        candidate = _build_name(f"{company_name} {idx}")
        if not Templates.objects.filter(workspace_id=ws_id, template_name=candidate).exists():
            return candidate
        idx += 1


def _prepare_campaign_form_data(ws_id: UUID, edit_obj):
    list_items = AudienceTask.objects.filter(workspace_id=ws_id, archived=False).order_by("-created_at")
    mb_items = Mailbox.objects.filter(workspace_id=ws_id, is_active=True, archived=False).order_by("email")
    tpl_items = Templates.objects.filter(workspace_id=ws_id, is_active=True, archived=False).order_by("order", "template_name")

    for it in list_items:
        it.ui_id = encode_id(int(it.id))

    sender_label_by_mb_id = _build_sender_labels(list(mb_items))
    for it in mb_items:
        it.ui_id = encode_id(int(it.id))
        it.sender_label = sender_label_by_mb_id.get(int(it.id), f"{it.email}")

    for it in tpl_items:
        it.ui_id = encode_id(int(it.id))

    deleted_tpl_ui = None
    deleted_tpl_id = None
    if edit_obj and getattr(edit_obj, "letter", None) and edit_obj.letter:
        t_id = int(edit_obj.letter.template_id) if edit_obj.letter.template_id else None
        if t_id:
            t = edit_obj.letter.template
            if (not t) or getattr(t, "archived", False):
                deleted_tpl_id = int(t_id)
                deleted_tpl_ui = encode_id(int(t_id))
                tpl_items = list(tpl_items)
                tpl_items.insert(
                    0,
                    SimpleNamespace(
                        id=int(t_id),
                        ui_id=deleted_tpl_ui,
                        template_name=_("Шаблон удален"),
                        is_deleted_option=True,
                    ),
                )

    parent_items = _with_ui_ids(Campaign.objects.filter(workspace_id=ws_id))

    global_default = default_global_global_window_json()
    gss, _created_gss = GlobalSendingSettings.objects.get_or_create(
        singleton_key=1,
        defaults={"global_global_window": global_default},
    )
    global_global_window = gss.global_global_window if isinstance(gss.global_global_window, dict) else {}
    if not _window_is_nonempty(global_global_window):
        global_global_window = global_default

    ss, _created = SendingSettings.objects.get_or_create(
        workspace_id=ws_id,
        defaults={"value_json": global_global_window},
    )
    global_window_json = ss.value_json if isinstance(ss.value_json, dict) else {}
    if not _window_is_nonempty(global_window_json):
        global_window_json = global_global_window
        if ss.value_json != global_window_json:
            ss.value_json = global_window_json
            ss.save(update_fields=["value_json", "updated_at"])

    return list_items, mb_items, tpl_items, parent_items, global_window_json, deleted_tpl_ui, deleted_tpl_id


def _prepare_letter_init(ws_id: UUID, edit_obj):
    letter_obj = None
    letter_init_html = ""
    letter_init_css = ""
    letter_init_subjects = "[]"
    letter_init_headers = "{}"
    letter_template_html = ""

    if edit_obj:
        letter_obj = _ensure_letter(ws_id, edit_obj)
        letter_obj.ui_id = encode_id(int(letter_obj.id))

        tpl = letter_obj.template if letter_obj.template_id else None
        if tpl:
            letter_template_html = tpl.template_html or ""
            content_html = (letter_obj.html_content or "").strip()
            if not content_html:
                content_html = find_demo_content_from_template(letter_template_html)
            letter_init_html = letter_editor_render_html(letter_template_html, content_html)
            styles_obj = _styles_pick_main(tpl.styles or {})
            letter_init_css = styles_json_to_css(styles_obj) or ""

        try:
            letter_init_subjects = json.dumps(letter_obj.subjects or [], ensure_ascii=False)
        except Exception:
            letter_init_subjects = "[]"

        try:
            letter_init_headers = json.dumps(letter_obj.headers or {}, ensure_ascii=False, indent=2)
        except Exception:
            letter_init_headers = "{}"

    return letter_obj, letter_init_html, letter_init_css, letter_init_subjects, letter_init_headers, letter_template_html


def campaigns_view(request):
    ws_id, _user = _guard(request)
    if not ws_id:
        return redirect("/")

    if request.method == "GET":
        legacy_state = _get_state(request)
        if legacy_state or (request.GET.get("step") or "").strip():
            campaign_ui_id = (request.GET.get("id") or "").strip()
            return redirect(_flow_url(_legacy_get_step(request), campaign_ui_id))

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "toggle_user_active":
            post_id = (request.POST.get("id") or "").strip()
            try:
                pk = int(decode_id(post_id))
            except Exception:
                pk = 0
            if pk > 0:
                camp = Campaign.objects.filter(id=pk, workspace_id=ws_id).first()
                if camp:
                    camp.user_active = not bool(camp.user_active)
                    camp.save(update_fields=["user_active", "updated_at"])
            return redirect("campaigns:campaigns")

        if action in ("activate", "pause"):
            post_id = (request.POST.get("id") or "").strip()
            try:
                pk = int(decode_id(post_id))
            except Exception:
                pk = 0
            if pk > 0:
                camp = Campaign.objects.filter(id=pk, workspace_id=ws_id).first()
                if camp:
                    camp.active = action == "activate"
                    camp.save(update_fields=["active", "updated_at"])
            return redirect("campaigns:campaigns")

        if action == "delete":
            post_id = (request.POST.get("id") or "").strip()
            try:
                pk = int(decode_id(post_id))
            except Exception:
                pk = 0
            if pk > 0:
                Campaign.objects.filter(id=pk, workspace_id=ws_id).delete()
            return redirect("campaigns:campaigns")

    list_items, mb_items, tpl_items, parent_items, global_window_json, deleted_tpl_ui, deleted_tpl_id = _prepare_campaign_form_data(
        ws_id, None
    )

    ctx = _ctx_build(
        ws_id,
        "",
        None,
        list_items,
        mb_items,
        tpl_items,
        parent_items,
        global_window_json,
        None,
        "",
        "",
        "[]",
        "{}",
        "",
        deleted_tpl_ui,
        deleted_tpl_id,
    )
    return render(request, "panels/aap_campaigns/campaigns_list.html", ctx)


def campaigns_flow_view(request, *, step_key: str, item_id: str = ""):
    ws_id, _user = _guard(request)
    if not ws_id:
        return redirect("/")

    step = (step_key or "campaign").strip().lower()
    if step not in {"campaign", "template", "letter"}:
        step = "campaign"

    edit_obj = _get_campaign_obj_by_ui_id(ws_id, item_id)
    if edit_obj:
        edit_obj.ui_id = encode_id(int(edit_obj.id))
        edit_obj.letter = Letter.objects.filter(workspace_id=ws_id, campaign=edit_obj).select_related("template").first()

    campaign_ui_id = edit_obj.ui_id if edit_obj else ""
    template_saved = bool(
        edit_obj
        and getattr(edit_obj, "letter", None)
        and getattr(edit_obj.letter, "template_id", None)
        and getattr(edit_obj.letter, "template", None)
        and not bool(getattr(edit_obj.letter.template, "archived", False))
    )

    if step in ("template", "letter") and not edit_obj:
        return redirect(_flow_url("campaign"))

    if step == "letter" and edit_obj and not template_saved:
        return redirect(_flow_url("template", campaign_ui_id))

    if request.method == "GET" and step == "template" and _get_tpl_state(request) == "add" and not _get_gl_tpl_from_query(request):
        rid = _pick_random_active_gl_tpl_id()
        if rid:
            return redirect(_flow_url("template", campaign_ui_id, tpl_state="add", gl_tpl=str(rid)))

    list_items, mb_items, tpl_items, parent_items, global_window_json, deleted_tpl_ui, deleted_tpl_id = _prepare_campaign_form_data(
        ws_id, edit_obj
    )
    letter_obj, letter_init_html, letter_init_css, letter_init_subjects, letter_init_headers, letter_template_html = (
        _prepare_letter_init(ws_id, edit_obj if step == "letter" else None)
    )

    test_msg = ""
    tpl_error_msg = ""
    tpl_name_input = ""

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        posted_title = (request.POST.get("title") or "").strip()

        if action == "close":
            return redirect("campaigns:campaigns")

        if action == "tpl_close":
            return redirect(_flow_url("campaign", campaign_ui_id))

        if action == "tpl_to_letter":
            if not edit_obj:
                return redirect(_flow_url("campaign"))

            tpl_ui = (request.POST.get("id") or request.GET.get("tpl_id") or "").strip()
            try:
                tpl_pk = int(decode_id(tpl_ui))
            except Exception:
                tpl_pk = 0
            tpl_obj = (
                Templates.objects
                .filter(id=tpl_pk, workspace_id=ws_id, archived=False)
                .first()
                if tpl_pk > 0 else None
            )
            if not tpl_obj:
                return redirect(_flow_url("template", campaign_ui_id))

            let = _ensure_letter(ws_id, edit_obj)
            let.template_id = int(tpl_obj.id)

            content_html = sanitize(let.html_content or "")
            ready = render_html(
                template_html=tpl_obj.template_html or "",
                content_html=content_html,
                styles=_styles_pick_main(tpl_obj.styles or {}),
                vars_json=None,
            )
            let.ready_content = ready or ""
            let.save(update_fields=["template", "ready_content", "updated_at"])
            return redirect(_flow_url("letter", campaign_ui_id))

        if action == "rename_campaign_title":
            _update_campaign_title_if_needed(edit_obj, posted_title)
            return redirect(request.get_full_path())

        if action == "tpl_choose":
            if not edit_obj:
                return redirect(_flow_url("campaign"))
            tpl_ui = (request.POST.get("template") or "").strip()
            try:
                tpl_pk = int(decode_id(tpl_ui))
            except Exception:
                tpl_pk = 0
            tpl_obj = (
                Templates.objects
                .filter(id=tpl_pk, workspace_id=ws_id, archived=False)
                .first()
                if tpl_pk > 0 else None
            )
            if not tpl_obj:
                return redirect(_flow_url("template", campaign_ui_id))

            let = _ensure_letter(ws_id, edit_obj)
            let.template_id = int(tpl_obj.id)

            content_html = sanitize(let.html_content or "")
            ready = render_html(
                template_html=tpl_obj.template_html or "",
                content_html=content_html,
                styles=_styles_pick_main(tpl_obj.styles or {}),
                vars_json=None,
            )
            let.ready_content = ready or ""
            let.save(update_fields=["template", "ready_content", "updated_at"])
            return redirect(_flow_url("template", campaign_ui_id))

        if action == "tpl_delete":
            post_id = (request.POST.get("id") or "").strip()
            try:
                pk = int(decode_id(post_id))
            except Exception:
                pk = 0
            if pk > 0:
                Templates.objects.filter(id=pk, workspace_id=ws_id, archived=False).update(archived=True, is_active=False)
            return redirect(_flow_url("template", campaign_ui_id))

        if action in ("tpl_add", "tpl_save"):
            _update_campaign_title_if_needed(edit_obj, posted_title)

            template_name = (request.POST.get("template_name") or "").strip()
            tpl_name_input = template_name
            editor_html = request.POST.get("editor_html") or ""
            css_text = request.POST.get("css_text") or ""

            if not template_name:
                tpl_error_msg = _("Имя шаблона обязательно.")
            else:
                clean_html = sanitize(editor_template_parse_html(editor_html))
                styles_obj = styles_css_to_json(css_text)
                if action == "tpl_add":
                    try:
                        obj = Templates.objects.create(
                            workspace_id=ws_id,
                            template_name=template_name,
                            template_html=clean_html,
                            styles=styles_obj,
                        )
                    except IntegrityError:
                        tpl_error_msg = _("Шаблон с таким именем уже существует.")
                    else:
                        return redirect(_flow_url("template", campaign_ui_id, tpl_state="edit", tpl_id=encode_id(int(obj.id))))
                else:
                    post_id = (request.POST.get("id") or "").strip()
                    try:
                        pk = int(decode_id(post_id))
                    except Exception:
                        pk = 0
                    obj = Templates.objects.filter(id=pk, workspace_id=ws_id, archived=False).first() if pk > 0 else None
                    if obj:
                        obj.template_name = template_name
                        obj.template_html = clean_html
                        obj.styles = styles_obj
                        try:
                            obj.save(update_fields=["template_name", "template_html", "styles", "updated_at"])
                        except IntegrityError:
                            tpl_error_msg = _("Шаблон с таким именем уже существует.")
                        else:
                            return redirect(_flow_url("template", campaign_ui_id, tpl_state="edit", tpl_id=encode_id(int(obj.id))))

        if action == "send_test":
            post_id = (request.POST.get("id") or "").strip()
            test_email = (request.POST.get("test_email") or "").strip()
            if not (post_id and test_email):
                return redirect(request.get_full_path())

            try:
                camp_id = int(decode_id(post_id))
            except Exception:
                return redirect(request.get_full_path())

            camp = Campaign.objects.filter(id=int(camp_id), workspace_id=ws_id).only("id", "sending_list_id").first()
            if not camp:
                return redirect(request.get_full_path())

            has_letter = Letter.objects.filter(workspace_id=ws_id, campaign_id=int(camp.id)).exists()
            if not has_letter:
                test_msg = _("Письмо ещё не создано — сначала откройте редактор письма и сохраните.")
            else:
                send_one(int(camp.id), None, to_email_override=test_email, record_sent=False)
                return redirect(_flow_url("letter", encode_id(int(camp.id))))

        if action in (
            "add_campaign",
            "save_campaign",
            "add_campaign_stay",
            "save_campaign_stay",
            "add_campaign_close",
            "save_campaign_close",
        ):
            title = (request.POST.get("title") or "").strip()
            sending_list_ui = (request.POST.get("sending_list") or "").strip()
            mailbox_ui = (request.POST.get("mailbox") or "").strip()
            missing_title = not bool(title)
            missing_sending_list = not bool(sending_list_ui)
            missing_mailbox = not bool(mailbox_ui)

            if missing_title or missing_sending_list or missing_mailbox:
                ctx = _ctx_build(
                    ws_id,
                    "edit" if edit_obj else "add",
                    edit_obj,
                    list_items,
                    mb_items,
                    tpl_items,
                    parent_items,
                    global_window_json,
                    letter_obj,
                    letter_init_html,
                    letter_init_css,
                    letter_init_subjects,
                    letter_init_headers,
                    letter_template_html,
                    deleted_tpl_ui,
                    deleted_tpl_id,
                    form_error_msg="",
                    form_error_field="",
                )
                ctx.update(
                    {
                        "flow_current_step": "campaign",
                        "flow_step_states": _build_flow_step_states("campaign", campaign_ui_id, template_saved=template_saved),
                        "flow_close_url": reverse("campaigns:campaigns"),
                        "flow_title": edit_obj.title if edit_obj else _("Новая кампания"),
                        "flow_campaign_title_input": title,
                        "flow_header_form_id": "yyCampaignForm",
                        "flow_header_save_action": "save_campaign_stay" if edit_obj else "add_campaign_stay",
                        "flow_template_step_url": _flow_url("template", campaign_ui_id),
                        "campaign_form_saved": False,
                        "step_template": "panels/aap_campaigns/flow_step_campaign.html",
                        "campaign_mode": "edit" if edit_obj else "add",
                        "campaign_save_action": "save_campaign_stay" if edit_obj else "add_campaign_stay",
                        "flow_error_title": missing_title,
                        "flow_error_sending_list": missing_sending_list,
                        "flow_error_mailbox": missing_mailbox,
                    }
                )
                return render(request, "panels/aap_campaigns/campaigns_flow.html", ctx)

            try:
                sending_list_pk = int(decode_id(sending_list_ui))
                mailbox_pk = int(decode_id(mailbox_ui))
            except Exception:
                return redirect(_flow_url("campaign", campaign_ui_id))

            exclude_id = int(edit_obj.id) if (edit_obj and action in ("save_campaign", "save_campaign_stay", "save_campaign_close")) else None
            if _sending_list_is_taken(ws_id, sending_list_pk, exclude_id):
                tmp = edit_obj or SimpleNamespace()
                if not getattr(tmp, "id", None):
                    tmp.id = 0
                if not getattr(tmp, "ui_id", ""):
                    tmp.ui_id = campaign_ui_id

                tmp.title = title
                tmp.sending_list_id = int(sending_list_pk)
                tmp.mailbox_id = int(mailbox_pk)
                tmp.start_at = _parse_date_from_post(request, "start_date") or date.today()
                tmp.end_at = _parse_date_from_post(request, "end_date")
                try:
                    tmp.send_after_parent_days = max(int(request.POST.get("send_after_parent_days") or 0), 0)
                except Exception:
                    tmp.send_after_parent_days = 0

                use_global_window_tmp = True if request.POST.get("use_global_window") else False
                window_raw_tmp = (request.POST.get("window") or "").strip()
                window_obj_tmp: dict = {}
                if not use_global_window_tmp:
                    try:
                        parsed_tmp = json.loads(window_raw_tmp) if window_raw_tmp else {}
                        window_obj_tmp = parsed_tmp if isinstance(parsed_tmp, dict) else {}
                    except Exception:
                        window_obj_tmp = {}
                tmp.window = window_obj_tmp

                ctx = _ctx_build(
                    ws_id,
                    "edit" if edit_obj else "add",
                    tmp,
                    list_items,
                    mb_items,
                    tpl_items,
                    parent_items,
                    global_window_json,
                    letter_obj,
                    letter_init_html,
                    letter_init_css,
                    letter_init_subjects,
                    letter_init_headers,
                    letter_template_html,
                    deleted_tpl_ui,
                    deleted_tpl_id,
                    form_error_msg="",
                    form_error_field="sending_list",
                )
                ctx.update(
                    {
                        "flow_current_step": "campaign",
                        "flow_step_states": _build_flow_step_states("campaign", campaign_ui_id, template_saved=template_saved),
                        "flow_close_url": reverse("campaigns:campaigns"),
                        "flow_title": edit_obj.title if edit_obj else _("Новая кампания"),
                        "flow_campaign_title_input": title,
                        "flow_header_form_id": "yyCampaignForm",
                        "flow_header_save_action": "save_campaign_stay" if edit_obj else "add_campaign_stay",
                        "flow_template_step_url": _flow_url("template", campaign_ui_id),
                        "campaign_form_saved": False,
                        "step_template": "panels/aap_campaigns/flow_step_campaign.html",
                        "campaign_mode": "edit" if edit_obj else "add",
                        "campaign_save_action": "save_campaign_stay" if edit_obj else "add_campaign_stay",
                        "flow_error_title": False,
                        "flow_error_sending_list": True,
                        "flow_error_mailbox": False,
                    }
                )
                return render(request, "panels/aap_campaigns/campaigns_flow.html", ctx)

            start_at = _parse_date_from_post(request, "start_date") or date.today()
            end_at = _parse_date_from_post(request, "end_date")

            send_after_days = 0
            if edit_obj and edit_obj.campaign_parent_id:
                try:
                    send_after_days = int(request.POST.get("send_after_parent_days") or 0)
                except Exception:
                    send_after_days = 0
                if send_after_days < 0:
                    send_after_days = 0

            use_global_window = True if request.POST.get("use_global_window") else False
            window_raw = (request.POST.get("window") or "").strip()
            window_obj: dict = {}
            if not use_global_window:
                try:
                    parsed = json.loads(window_raw) if window_raw else {}
                    window_obj = parsed if isinstance(parsed, dict) else {}
                except Exception:
                    window_obj = {}

            if action in ("add_campaign", "add_campaign_stay", "add_campaign_close"):
                camp = Campaign.objects.create(
                    workspace_id=ws_id,
                    title=title,
                    sending_list_id=sending_list_pk,
                    mailbox_id=mailbox_pk,
                    start_at=start_at,
                    end_at=end_at,
                    window=window_obj,
                )
                new_ui_id = encode_id(int(camp.id))
                if action == "add_campaign_close":
                    return redirect("campaigns:campaigns")
                return redirect(_flow_url("campaign", new_ui_id))

            if not edit_obj:
                return redirect(_flow_url("campaign"))

            edit_obj.title = title
            edit_obj.sending_list_id = sending_list_pk
            edit_obj.mailbox_id = mailbox_pk
            edit_obj.start_at = start_at
            edit_obj.end_at = end_at
            edit_obj.send_after_parent_days = send_after_days
            edit_obj.window = window_obj
            edit_obj.save(
                update_fields=[
                    "title",
                    "sending_list",
                    "mailbox",
                    "start_at",
                    "end_at",
                    "send_after_parent_days",
                    "window",
                    "updated_at",
                ]
            )

            if action == "save_campaign_close":
                return redirect("campaigns:campaigns")
            return redirect(_flow_url("campaign", encode_id(int(edit_obj.id))))

        if action in ("save_letter", "save_ready"):
            if not edit_obj:
                return redirect(_flow_url("campaign"))

            _update_campaign_title_if_needed(edit_obj, posted_title)

            let = _ensure_letter(ws_id, edit_obj)
            editor_mode = (request.POST.get("editor_mode") or "user").strip()
            editor_html = request.POST.get("editor_html") or ""
            subjects_json = request.POST.get("subjects_json") or "[]"
            headers_json = (request.POST.get("headers_json") or "").strip() or "{}"

            try:
                subs = json.loads(subjects_json)
                subs = [str(x).strip() for x in subs if str(x).strip()] if isinstance(subs, list) else []
            except Exception:
                subs = []

            try:
                hdrs = json.loads(headers_json) if headers_json else {}
                hdrs = hdrs if isinstance(hdrs, dict) else {}
            except Exception:
                hdrs = {}

            content_html = editor_html
            if editor_mode != "advanced":
                content_html = letter_editor_extract_content(editor_html or "")

            let.html_content = sanitize(content_html or "")
            let.subjects = subs
            let.headers = hdrs
            let.save(update_fields=["html_content", "subjects", "headers", "updated_at"])

            tpl = let.template if let.template_id else None
            if tpl:
                ready = render_html(
                    template_html=tpl.template_html or "",
                    content_html=sanitize(let.html_content or ""),
                    styles=_styles_pick_main(tpl.styles or {}),
                    vars_json=None,
                )
                let.ready_content = ready or ""
                let.save(update_fields=["ready_content", "updated_at"])

            return redirect(_flow_url("letter", campaign_ui_id))

    flow_ctx = {
        "flow_current_step": step,
        "flow_step_states": _build_flow_step_states(step, campaign_ui_id, template_saved=template_saved),
        "flow_close_url": reverse("campaigns:campaigns"),
        "flow_title": edit_obj.title if edit_obj else _("Новая кампания"),
        "flow_campaign_ui_id": campaign_ui_id,
        "flow_campaign_title_input": (edit_obj.title if edit_obj else ""),
        "flow_header_form_id": ("yyFlowTitleForm" if campaign_ui_id else ""),
        "flow_header_save_action": ("rename_campaign_title" if campaign_ui_id else ""),
        "flow_template_step_url": _flow_url("template", campaign_ui_id),
        "flow_template_add_url": _flow_url("template", campaign_ui_id, tpl_state="add"),
        "flow_selected_template_ui": (
            encode_id(int(edit_obj.letter.template_id))
            if (edit_obj and getattr(edit_obj, "letter", None) and getattr(edit_obj.letter, "template_id", None))
            else ""
        ),
        "campaign_form_saved": bool(edit_obj),
        "flow_error_title": False,
        "flow_error_sending_list": False,
        "flow_error_mailbox": False,
    }

    if step == "template":
        tpl_ctx = _build_template_step_context(request, ws_id)
        flow_ctx.update(tpl_ctx)
        if tpl_ctx["tpl_state"] == "add":
            flow_ctx["flow_tpl_name_input"] = tpl_name_input or _suggest_flow_template_name(request, ws_id, edit_obj, mb_items)
        else:
            flow_ctx["flow_tpl_name_input"] = tpl_name_input
        if tpl_ctx["tpl_state"] == "add":
            flow_ctx["flow_header_form_id"] = "yyTplForm"
            flow_ctx["flow_header_save_action"] = "tpl_add"
        elif tpl_ctx["tpl_state"] == "edit" and tpl_ctx["tpl_edit_obj"]:
            flow_ctx["flow_header_form_id"] = "yyTplForm"
            flow_ctx["flow_header_save_action"] = "tpl_save"
        flow_ctx["step_template"] = "panels/aap_campaigns/flow_step_template.html"
        flow_ctx["tpl_error_msg"] = tpl_error_msg
        return render(request, "panels/aap_campaigns/campaigns_flow.html", flow_ctx)

    if step == "letter" and (not edit_obj or not template_saved):
        if not edit_obj:
            return redirect(_flow_url("campaign"))
        return redirect(_flow_url("template", campaign_ui_id))

    state = "letter" if step == "letter" else ("edit" if edit_obj else "add")
    ctx = _ctx_build(
        ws_id,
        state,
        edit_obj,
        list_items,
        mb_items,
        tpl_items,
        parent_items,
        global_window_json,
        letter_obj,
        letter_init_html,
        letter_init_css,
        letter_init_subjects,
        letter_init_headers,
        letter_template_html,
        deleted_tpl_ui,
        deleted_tpl_id,
        test_msg=test_msg,
    )
    flow_ctx.update(ctx)
    flow_ctx["campaign_mode"] = "edit" if edit_obj else "add"
    flow_ctx["campaign_save_action"] = "save_campaign_stay" if edit_obj else "add_campaign_stay"
    if step == "letter":
        flow_ctx["flow_header_form_id"] = "yySendingForm"
        flow_ctx["flow_header_save_action"] = "save_letter"
    else:
        flow_ctx["flow_header_form_id"] = "yyCampaignForm"
        flow_ctx["flow_header_save_action"] = "save_campaign_stay" if edit_obj else "add_campaign_stay"
    flow_ctx["step_template"] = "panels/aap_campaigns/flow_step_letter.html" if step == "letter" else "panels/aap_campaigns/flow_step_campaign.html"
    return render(request, "panels/aap_campaigns/campaigns_flow.html", flow_ctx)


def campaigns_flow_campaign_view(request, item_id: str = ""):
    return campaigns_flow_view(request, step_key="campaign", item_id=item_id)


def campaigns_flow_template_view(request, item_id: str = ""):
    return campaigns_flow_view(request, step_key="template", item_id=item_id)


def campaigns_flow_letter_view(request, item_id: str = ""):
    return campaigns_flow_view(request, step_key="letter", item_id=item_id)
