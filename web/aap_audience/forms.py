# FILE: web/aap_audience/forms.py

from django import forms


class AudienceHowForm(forms.Form):
    system = forms.CharField(
        label="System prompt",
        widget=forms.Textarea(
            attrs={
                "rows": 4,
                "placeholder": "Системное задание (что делать с запросом).",
            }
        ),
    )
    user = forms.CharField(
        label="User prompt",
        widget=forms.Textarea(
            attrs={
                "rows": 4,
                "placeholder": "Пользовательский запрос.",
            }
        ),
    )
    result = forms.CharField(
        label="Ответ GPT (nano)",
        widget=forms.Textarea(
            attrs={
                "rows": 6,
                "placeholder": "Здесь появится ответ от gpt-5-nano.",
            }
        ),
        required=False,
    )
