# FILE: web/panel/aap_lists/forms.py  (обновлено — 2026-01-11)
# PURPOSE: форма списка рассылки в стиле панели: YY-INPUT + обязательные поля (title, audience).

from django import forms
from django.utils.translation import gettext_lazy as _


class MailingListForm(forms.Form):
    title = forms.CharField(
        label=_("Название списка"),
        required=True,
        widget=forms.TextInput(
            attrs={"class": "YY-INPUT", "placeholder": _("Название списка")}
        ),
    )

    audience_task_id = forms.ChoiceField(
        label=_("Аудитория"),
        required=True,
        widget=forms.Select(attrs={"class": "YY-INPUT"}),
    )

    def __init__(self, *args, audience_choices=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["audience_task_id"].choices = audience_choices or []

    def clean(self):
        cleaned = super().clean()

        title = (cleaned.get("title") or "").strip()
        aud = (cleaned.get("audience_task_id") or "").strip()

        missing = []
        if not title:
            missing.append("title")
        if not aud:
            missing.append("audience_task_id")

        if missing:
            self.add_error(None, _("Заполните все поля."))
            for f in missing:
                self.add_error(f, "")

        return cleaned
