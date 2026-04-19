from __future__ import annotations

from typing import Any

from django.urls import reverse
from django.utils.translation import gettext_lazy as _trans


FLOW_STEP_LABELS: dict[str, Any] = {
    "identity": _trans("Email и лимиты"),
    "smtp": _trans("SMTP сервер"),
    "imap": _trans("IMAP сервер"),
}


def build_mail_servers_flow_step_states(
    *,
    current_step: str,
    mailbox_ui_id: str = "",
    saved: bool = False,
) -> list[dict[str, Any]]:
    current = (current_step or "identity").strip().lower()
    mb_id = (mailbox_ui_id or "").strip()
    is_saved = bool(saved and mb_id)

    identity_url = (
        reverse("settings:mail_servers") + f"?state=edit&id={mb_id}"
        if is_saved
        else reverse("settings:mail_servers") + "?state=add"
    )
    smtp_url = reverse("settings:mail_servers_smtp", kwargs={"id": mb_id}) if is_saved else ""
    imap_url = reverse("settings:mail_servers_imap", kwargs={"id": mb_id}) if is_saved else ""

    rows = [
        {"key": "identity", "url": identity_url},
        {"key": "smtp", "url": smtp_url},
        {"key": "imap", "url": imap_url},
    ]

    out: list[dict[str, Any]] = []
    for row in rows:
        key = row["key"]
        out.append(
            {
                "key": key,
                "label": FLOW_STEP_LABELS[key],
                "url": row["url"],
                "is_current": key == current,
                "is_clickable": bool(row["url"]) and key != current,
            }
        )
    return out
