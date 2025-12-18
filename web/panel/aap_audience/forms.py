# FILE: web/panel/aap_audience/forms.py  (обновлено — 2025-12-18)
# Смысл: формы audience, без привязки к app paths; изменений по логике нет.

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

    question_what = forms.CharField(required=False, widget=forms.HiddenInput())
    hint_what = forms.CharField(required=False, widget=forms.HiddenInput())

    question_who = forms.CharField(required=False, widget=forms.HiddenInput())
    hint_who = forms.CharField(required=False, widget=forms.HiddenInput())

    question_geo = forms.CharField(required=False, widget=forms.HiddenInput())
    hint_geo = forms.CharField(required=False, widget=forms.HiddenInput())

    edit_id = forms.IntegerField(required=False, widget=forms.HiddenInput())


class AudienceClarForm(forms.Form):
    title = forms.CharField(
        label="Название задачи",
        required=True,
        widget=forms.TextInput(attrs={"class": "panel-input"}),
    )
    task = forms.CharField(
        label="Основной task",
        required=True,
        widget=forms.Textarea(attrs={"class": "panel-textarea", "rows": 8}),
    )
    task_branches = forms.CharField(
        label="Branches-задача",
        required=True,
        widget=forms.Textarea(attrs={"class": "panel-textarea", "rows": 8}),
    )
    task_geo = forms.CharField(
        label="Geo-задача",
        required=True,
        widget=forms.Textarea(attrs={"class": "panel-textarea", "rows": 8}),
    )
    task_client = forms.CharField(
        label="Client-задача",
        required=False,
        widget=forms.Textarea(attrs={"class": "panel-textarea", "rows": 8}),
    )

    run_processing = forms.BooleanField(
        label="Запустить в процессинг",
        required=False,
        widget=forms.CheckboxInput(attrs={"class": "panel-checkbox"}),
    )

    subscribers_limit = forms.IntegerField(
        label="Найти сабскрайберов",
        required=True,
        initial=500,
        min_value=1,
        max_value=1_000_000,
        widget=forms.NumberInput(attrs={"class": "panel-input"}),
    )

    edit_id = forms.IntegerField(required=True, widget=forms.HiddenInput())
