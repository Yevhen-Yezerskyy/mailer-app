# FILE: web/panel/aap_settings/forms.py
# DATE: 2026-01-24
# PURPOSE: Settings → Mail servers: формы для Mailbox / SMTP / IMAP + отдельная SMTP-страница.
# CHANGE:
# - LOGIN-поля (host/port/security/username/password) строятся по ключам TypedDict из engine/common/mail/types.py.
# - ConnSecurity choices/validation берутся из ConnSecurity (single source of truth).
# - НИКАКИХ JS id / legacy hooks в формах. Это зона HTML/JS.
# - Формы НЕ знают ничего про OAuth и auth_type (кроме «поле существует как строка»).

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple, get_args

from django import forms
from django.utils.translation import gettext_lazy as _

from engine.common.mail.types import ConnSecurity, ImapCredsLogin, SmtpCredsLogin
from panel.aap_settings.models import Mailbox


def _typed_dict_keys(td: Any) -> List[str]:
    ann = getattr(td, "__annotations__", {}) or {}
    return list(ann.keys())


SMTP_LOGIN_KEYS = _typed_dict_keys(SmtpCredsLogin)  # canonical keys
IMAP_LOGIN_KEYS = _typed_dict_keys(ImapCredsLogin)  # canonical keys (alias today)


SECURITY_VALUES = list(get_args(ConnSecurity))
SECURITY_SET = set(SECURITY_VALUES)

SECURITY_LABELS: Dict[str, str] = {
    "ssl": "SSL / TLS",
    "starttls": "STARTTLS",
    "none": "None",
}
SECURITY_CHOICES = [(v, SECURITY_LABELS[v]) for v in SECURITY_VALUES]  # KeyError -> fail fast if drift


def _any_filled(cleaned: Dict[str, Any], keys: List[str]) -> bool:
    for k in keys:
        v = cleaned.get(k)
        if v is None:
            continue
        if isinstance(v, str):
            if v.strip():
                return True
        else:
            if str(v).strip():
                return True
    return False


def _require_all_or_error(form: forms.Form, cleaned: Dict[str, Any], keys: List[str], msg: str) -> bool:
    for k in keys:
        v = cleaned.get(k)
        if v is None or (isinstance(v, str) and not v.strip()):
            form.add_error(None, msg)
            return False
    return True


# ============
# Canonical LOGIN field builders (strict dict map)
# Missing key => KeyError => fail fast (desired)
# ============

LOGIN_FIELD_BUILDERS: Dict[str, Callable[[], forms.Field]] = {
    "host": lambda: forms.CharField(required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT"})),
    "port": lambda: forms.IntegerField(required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT !w-28"})),
    "security": lambda: forms.ChoiceField(
        required=False,
        choices=SECURITY_CHOICES,
        widget=forms.Select(attrs={"class": "YY-INPUT !px-1"}),
    ),
    "username": lambda: forms.CharField(required=False, widget=forms.TextInput(attrs={"class": "YY-INPUT"})),
    "password": lambda: forms.CharField(
        required=False,
        widget=forms.PasswordInput(attrs={"class": "YY-INPUT", "autocomplete": "off"}, render_value=True),
    ),
}


class MailboxAddForm(forms.Form):
    email = forms.EmailField(
        label=_("Email"),
        required=True,
        widget=forms.TextInput(attrs={"class": "YY-INPUT", "placeholder": "name@domain.tld"}),
    )

    def __init__(self, *args, workspace_id=None, mailbox_id: int | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.workspace_id = workspace_id
        self.mailbox_id = int(mailbox_id) if mailbox_id is not None else None

    def clean(self):
        cleaned = super().clean()
        email = (cleaned.get("email") or "").strip().lower()
        if not email:
            self.add_error(None, _("Заполните Email."))
            return cleaned

        q = Mailbox.objects.filter(email=email)
        if self.mailbox_id is not None:
            q = q.exclude(id=self.mailbox_id)
        if q.exists():
            self.add_error(None, _("Этот Email уже используется."))
            return cleaned

        return cleaned


class _LoginFieldsFromTypesMixin:
    login_keys: List[str] = []

    def _attach_login_fields(self) -> None:
        for k in self.login_keys:
            builder = LOGIN_FIELD_BUILDERS[k]  # KeyError is desired
            self.fields[k] = builder()


class SmtpConnForm(forms.Form, _LoginFieldsFromTypesMixin):
    """
    Combined mail_servers page: SMTP block.
    Contains SMTP identity fields + canonical LOGIN creds fields.
    """

    login_keys = SMTP_LOGIN_KEYS

    # exists, but not interpreted here
    auth_type = forms.CharField(required=False, widget=forms.HiddenInput())

    from_name = forms.CharField(
        label=_("Отправитель:"),
        required=False,
        widget=forms.TextInput(attrs={"class": "YY-INPUT", "placeholder": _("Отправитель")}),
    )

    limit_hour_sent = forms.IntegerField(
        label=_("Лимит/час"),
        required=False,
        initial=50,
        widget=forms.TextInput(
            attrs={
                "class": "YY-INPUT",
                "inputmode": "numeric",
                "pattern": r"[0-9]*",
                "maxlength": "3",
                "min": "1",
                "max": "300",
                "autocomplete": "off",
            }
        ),
    )

    def __init__(self, *args, require_password: bool = True, password_masked: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.require_password = bool(require_password)
        self._attach_login_fields()

        if not self.require_password:
            self.fields["password"].required = False

        if password_masked:
            self.fields["password"].widget.attrs.update({"readonly": "readonly", "data-yy-masked": "1", "oncopy": "return false;"})

    def clean(self):
        cleaned = super().clean()

        touched = _any_filled(cleaned, ["from_name", "limit_hour_sent"] + self.login_keys)
        if not touched:
            return cleaned

        required = ["from_name", "limit_hour_sent", "host", "port", "security", "username"]
        if self.require_password:
            required.append("password")

        if not _require_all_or_error(self, cleaned, required, _("Заполните все поля SMTP.")):
            return cleaned

        try:
            lim = int(cleaned.get("limit_hour_sent") or 0)
        except Exception:
            lim = 0
        if lim < 1:
            self.add_error(None, _("Лимит должен быть не меньше 1 письма в час."))
            return cleaned
        if lim > 300:
            self.add_error(None, _("Максимум 300 писем в час."))
            return cleaned

        sec = (cleaned.get("security") or "").strip()
        if sec not in SECURITY_SET:
            self.add_error("security", _("Некорректное значение шифрования."))
            return cleaned

        return cleaned


class ImapConnForm(forms.Form, _LoginFieldsFromTypesMixin):
    """
    Combined mail_servers page: IMAP block.
    Contains canonical LOGIN creds fields.
    """

    login_keys = IMAP_LOGIN_KEYS

    auth_type = forms.CharField(required=False, widget=forms.HiddenInput())

    def __init__(self, *args, require_password: bool = False, password_masked: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.require_password = bool(require_password)
        self._attach_login_fields()

        if not self.require_password:
            self.fields["password"].required = False

        if password_masked:
            self.fields["password"].widget.attrs.update({"readonly": "readonly", "data-yy-masked": "1", "oncopy": "return false;"})

    def clean(self):
        cleaned = super().clean()

        if not _any_filled(cleaned, self.login_keys):
            return cleaned

        required = ["host", "port", "security", "username"]
        if self.require_password:
            required.append("password")

        if not _require_all_or_error(self, cleaned, required, _("Заполните все поля IMAP.")):
            return cleaned

        sec = (cleaned.get("security") or "").strip()
        if sec not in SECURITY_SET:
            self.add_error("security", _("Некорректное значение шифрования."))
            return cleaned

        return cleaned


class SmtpServerForm(forms.Form, _LoginFieldsFromTypesMixin):
    """
    Separate SMTP server page.
    Contains mailbox identity fields + canonical LOGIN creds fields.
    """

    login_keys = SMTP_LOGIN_KEYS

    auth_type = forms.CharField(required=False, widget=forms.HiddenInput())

    sender_name = forms.CharField(
        label=_("Отправитель"),
        required=True,
        widget=forms.TextInput(attrs={"class": "YY-INPUT"}),
    )

    email = forms.EmailField(
        label=_("Email"),
        required=True,
        widget=forms.EmailInput(attrs={"class": "YY-INPUT", "autocomplete": "off"}),
    )

    limit_hour_sent = forms.IntegerField(
        label=_("Лимит/час"),
        required=True,
        initial=50,
        widget=forms.TextInput(
            attrs={
                "class": "YY-INPUT",
                "inputmode": "numeric",
                "pattern": r"[0-9]*",
                "maxlength": "3",
                "min": "1",
                "max": "300",
                "autocomplete": "off",
            }
        ),
    )

    def __init__(self, *args, require_password: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self.require_password = bool(require_password)
        self._attach_login_fields()

        if not self.require_password:
            self.fields["password"].required = False

    def clean(self):
        cleaned = super().clean()

        try:
            lim = int(cleaned.get("limit_hour_sent") or 0)
        except Exception:
            lim = 0
        if lim < 1:
            self.add_error(None, _("Лимит должен быть не меньше 1 письма в час."))
            return cleaned
        if lim > 300:
            self.add_error(None, _("Максимум 300 писем в час."))
            return cleaned

        if not _any_filled(cleaned, self.login_keys):
            return cleaned

        required = ["host", "port", "security", "username"]
        if self.require_password:
            required.append("password")

        if not _require_all_or_error(self, cleaned, required, _("Заполните все поля SMTP (LOGIN).")):
            return cleaned

        sec = (cleaned.get("security") or "").strip()
        if sec not in SECURITY_SET:
            self.add_error("security", _("Некорректное значение шифрования."))
            return cleaned

        return cleaned
