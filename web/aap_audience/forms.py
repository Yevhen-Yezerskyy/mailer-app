# FILE: web/aap_audience/forms.py   (новое — 2025-12-08)

from django import forms


class AudienceHowForm(forms.Form):
    what = forms.CharField(
        label="Что продаём?",
        required=False,
        widget=forms.Textarea(attrs={"class": "panel-textarea"}),
    )
    who = forms.CharField(
        label="Кто продавец?",
        required=False,
        widget=forms.Textarea(attrs={"class": "panel-textarea"}),
    )
    geo = forms.CharField(
        label="География?",
        required=False,
        widget=forms.Textarea(attrs={"class": "panel-textarea"}),
    )

    # Уточняющие вопросы и подсказки для каждого блока
    question_what = forms.CharField(required=False, widget=forms.HiddenInput())
    hint_what = forms.CharField(required=False, widget=forms.HiddenInput())

    question_who = forms.CharField(required=False, widget=forms.HiddenInput())
    hint_who = forms.CharField(required=False, widget=forms.HiddenInput())

    question_geo = forms.CharField(required=False, widget=forms.HiddenInput())
    hint_geo = forms.CharField(required=False, widget=forms.HiddenInput())
