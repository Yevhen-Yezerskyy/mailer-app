# FILE: web/panel/aap_campaigns/forms.py
# DATE: 2026-01-14
# PURPOSE: Формы для раздела Campaigns.
# CHANGE: (new) TemplatesForm для добавления/редактирования шаблонов (HTML + styles JSON как textarea).

from __future__ import annotations

import json

from django import forms
from django.core.exceptions import ValidationError

from panel.aap_campaigns.models import Templates


class TemplatesForm(forms.Form):
    template_name = forms.CharField(
        required=True,
        max_length=255,
        widget=forms.TextInput(attrs={"class": "YY-INPUT"}),
    )
    template_html = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={"class": "YY-TEXTAREA", "rows": 10}),
    )
    styles = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={"class": "YY-TEXTAREA", "rows": 8}),
        help_text="JSON",
    )

    def clean_styles(self):
        raw = (self.cleaned_data.get("styles") or "").strip()
        if not raw:
            return {}
        try:
            val = json.loads(raw)
        except Exception:
            raise ValidationError("styles: invalid JSON")
        if not isinstance(val, dict):
            raise ValidationError("styles: must be a JSON object")
        return val

    def to_model_fields(self) -> dict:
        return {
            "template_name": (self.cleaned_data.get("template_name") or "").strip(),
            "template_html": (self.cleaned_data.get("template_html") or ""),
            "styles": self.cleaned_data.get("styles") or {},
        }
