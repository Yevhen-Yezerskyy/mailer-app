# FILE: web/panel/aap_audience/forms.py
# DATE: 2025-12-22
# PURPOSE: AudienceHowForm для HOW-страницы: все оформление (классы/rows/placeholder) внутри формы; убраны hidden question/hint/edit_id как мусорный артефакт. AudienceClarForm не трогаем.

from django import forms
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _


class AudienceHowForm(forms.Form):
    what = forms.CharField(
        label=_("Что Вы продаете? Товар, услуга, характеристики"),
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 4,
                "placeholder": _(
                    "Опишите продукт или услугу в нескольких коротких предложениях."
                ),
            }
        ),
    )
    who = forms.CharField(
        label=_(
            "Кто продает? Информация о компании, которая будет осуществлять продажу"
        ),
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 4,
                "placeholder": _(
                    "Название компании, сайт, страна и ключевые факты о вашем бизнесе."
                ),
            }
        ),
    )
    geo = forms.CharField(
        label=_(
            "Где в Германии? Все, что поможет уточнить, где искать клиентов"
        ),
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 4,
                "placeholder": _(
                    "Ваше присутствие в Германии - адреса компании, дилеров, представителей. "
                    "Опишите логистику, географические ограничения"
                ),
            }
        ),
    )

# FILE: web/panel/aap_audience/forms.py
# DATE: 2025-12-22
# PURPOSE: HOW-формы для sell / buy. Buy наследует Sell и меняет ТОЛЬКО label + placeholder.


class AudienceHowSellForm(forms.Form):
    what = forms.CharField(
        label=_("Что Вы продаете? Товар, услуга, характеристики"),
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 4,
                "placeholder": _(
                    "Опишите продукт или услугу в нескольких коротких предложениях."
                ),
            }
        ),
    )
    who = forms.CharField(
        label=_(
            "Кто продает? Информация о компании, которая будет осуществлять продажу"
        ),
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 4,
                "placeholder": _(
                    "Название компании, сайт, страна и ключевые факты о вашем бизнесе."
                ),
            }
        ),
    )
    geo = forms.CharField(
        label=_(
            "Где в Германии? Все, что поможет уточнить, где искать клиентов"
        ),
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 4,
                "placeholder": _(
                    "Ваше присутствие в Германии - адреса компании, дилеров, представителей. "
                    "Опишите логистику, географические ограничения"
                ),
            }
        ),
    )
    def clean(self):
        cleaned = super().clean()

        what = (cleaned.get("what") or "").strip()
        who = (cleaned.get("who") or "").strip()
        geo = (cleaned.get("geo") or "").strip()

        if not any([what, who, geo]):
            # form-level ошибка
            self.add_error(None, _("Заполните хотя бы одно поле."))

            # field-level ошибки — чтобы Django пометил поля как invalid
            self.add_error("what", "")
            self.add_error("who", "")
            self.add_error("geo", "")

        return cleaned

class AudienceHowBuyForm(AudienceHowSellForm):
    """
    BUY-сценарий: ищем поставщиков / подрядчиков.
    Меняются только тексты (label + placeholder).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.fields["what"].label = _(
            "Что Вы хотите купить? Товар, услуга, требования"
        )
        self.fields["what"].widget.attrs["placeholder"] = _(
            "Опишите, что именно Вы ищете и какие требования важны."
        )

        self.fields["who"].label = _(
            "Кто покупает? Информация о компании-заказчике"
        )
        self.fields["who"].widget.attrs["placeholder"] = _(
            "Название компании, сайт, страна и краткая информация о заказчике."
        )

        self.fields["geo"].label = _(
            "Где в Германии? География поиска поставщиков или подрядчиков"
        )
        self.fields["geo"].widget.attrs["placeholder"] = _(
            "Регионы Германии, допустимая логистика, удаленная или локальная работа."
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
