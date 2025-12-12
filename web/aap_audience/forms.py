# FILE: web/aap_audience/forms.py  (новое) 2025-12-11

from django import forms


class AudienceHowForm(forms.Form):
    # Три основных блока ввода
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

    # Уточняющие вопросы и подсказки для каждого блока (заполняются GPT)
    question_what = forms.CharField(
        required=False,
        widget=forms.HiddenInput(),
    )
    hint_what = forms.CharField(
        required=False,
        widget=forms.HiddenInput(),
    )

    question_who = forms.CharField(
        required=False,
        widget=forms.HiddenInput(),
    )
    hint_who = forms.CharField(
        required=False,
        widget=forms.HiddenInput(),
    )

    question_geo = forms.CharField(
        required=False,
        widget=forms.HiddenInput(),
    )
    hint_geo = forms.CharField(
        required=False,
        widget=forms.HiddenInput(),
    )

    # Для режима редактирования сохранённой задачи HOW
    edit_id = forms.IntegerField(
        required=False,
        widget=forms.HiddenInput(),
    )


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

    edit_id = forms.IntegerField(
        required=True,
        widget=forms.HiddenInput(),
    )
