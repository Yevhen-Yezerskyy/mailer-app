# FILE: web/public/aap_auth/forms.py
# DATE: 2026-03-07
# PURPOSE: public registration form validation and persistence payload.

from __future__ import annotations

import re

from django import forms
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _trans

from mailer_web.models import ClientUser


_PHONE_RE = re.compile(r"^[0-9+()\-\s]{6,32}$")


class RegistrationForm(forms.Form):
    company_name = forms.CharField(max_length=255, required=True)
    company_address = forms.CharField(required=True)

    last_name = forms.CharField(max_length=150, required=True)
    first_name = forms.CharField(max_length=150, required=True)
    phone = forms.CharField(max_length=64, required=True)
    position = forms.CharField(max_length=255, required=False)

    email = forms.EmailField(required=True)
    password = forms.CharField(required=True, widget=forms.PasswordInput)
    password2 = forms.CharField(required=True, widget=forms.PasswordInput)

    def __init__(self, *args, **kwargs):
        self.existing_user = kwargs.pop("existing_user", None)
        self.is_edit = bool(kwargs.pop("is_edit", False))
        super().__init__(*args, **kwargs)
        if self.is_edit:
            self.fields["password"].required = False
            self.fields["password2"].required = False
        self.fields["company_name"].widget.attrs.update({"autocomplete": "off", "placeholder": _trans("Название компании")})
        self.fields["company_address"].widget.attrs.update({"autocomplete": "off", "placeholder": _trans("Адрес компании")})
        self.fields["last_name"].widget.attrs.update({"autocomplete": "off", "placeholder": _trans("Фамилия")})
        self.fields["first_name"].widget.attrs.update({"autocomplete": "off", "placeholder": _trans("Имя")})
        self.fields["phone"].widget.attrs.update({"autocomplete": "off", "placeholder": _trans("Телефон")})
        self.fields["position"].widget.attrs.update({"autocomplete": "off", "placeholder": _trans("Должность")})
        self.fields["email"].widget.attrs.update({"autocomplete": "username", "placeholder": _trans("Email / Логин")})
        self.fields["password"].widget.attrs.update({"autocomplete": "new-password", "placeholder": _trans("Пароль")})
        self.fields["password2"].widget.attrs.update({"autocomplete": "new-password", "placeholder": _trans("Подтверждение пароля")})

    def clean_phone(self):
        v = (self.cleaned_data.get("phone") or "").strip()
        if not _PHONE_RE.match(v):
            raise ValidationError(_trans("Неверный формат телефона"))
        return v

    def clean_email(self):
        email = (self.cleaned_data.get("email") or "").strip().lower()
        qs = ClientUser.objects.filter(email__iexact=email)
        if self.existing_user is not None:
            qs = qs.exclude(id=self.existing_user.id)
        if qs.exists():
            raise ValidationError(_trans("Пользователь с таким email уже существует"))
        return email

    def clean(self):
        cleaned = super().clean()
        p1 = cleaned.get("password") or ""
        p2 = cleaned.get("password2") or ""

        if self.is_edit and not p1 and not p2:
            return cleaned

        if (p1 and not p2) or (p2 and not p1):
            self.add_error("password2", _trans("Пароли не совпадают"))
            return cleaned

        if p1 and p2 and p1 != p2:
            self.add_error("password2", _trans("Пароли не совпадают"))

        if p1:
            try:
                validate_password(p1)
            except ValidationError as exc:
                self.add_error("password", " ".join(exc.messages))

        return cleaned


class PasswordResetRequestForm(forms.Form):
    email = forms.EmailField(required=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["email"].widget.attrs.update(
            {"autocomplete": "username", "placeholder": _trans("Email / Логин")}
        )


class PasswordResetConfirmForm(forms.Form):
    password = forms.CharField(required=True, widget=forms.PasswordInput)
    password2 = forms.CharField(required=True, widget=forms.PasswordInput)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["password"].widget.attrs.update(
            {"autocomplete": "new-password", "placeholder": _trans("Новый пароль")}
        )
        self.fields["password2"].widget.attrs.update(
            {"autocomplete": "new-password", "placeholder": _trans("Подтверждение пароля")}
        )

    def clean(self):
        cleaned = super().clean()
        p1 = cleaned.get("password") or ""
        p2 = cleaned.get("password2") or ""

        if p1 and p2 and p1 != p2:
            self.add_error("password2", _trans("Пароли не совпадают"))

        if p1:
            try:
                validate_password(p1)
            except ValidationError as exc:
                self.add_error("password", " ".join(exc.messages))

        return cleaned
