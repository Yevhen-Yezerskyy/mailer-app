# FILE: web/panel/aap_audience/forms.py
# DATE: 2025-12-22
# PURPOSE: AudienceHowForm для HOW-страницы: все оформление (классы/rows/placeholder) внутри формы; убраны hidden question/hint/edit_id как мусорный артефакт. AudienceClarForm не трогаем.

from django import forms
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _


# FILE: web/panel/aap_audience/forms.py
# DATE: 2025-12-22
# PURPOSE: HOW-формы для sell / buy. Buy наследует Sell и меняет ТОЛЬКО label + placeholder.


class AudienceHowSellForm(forms.Form):
    what = forms.CharField(
        label=_("Ваш продукт / услуга. Что именно продаётся? Суть, бизнес-характеристики"),
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 5,
                "placeholder": _(
                    "Кратко опишите продукт / услугу. Что именно продаётся. Что получает покупатель.."
                ),
            }
        ),
    )
    who = forms.CharField(
        label=_(
            "Компания-продавец. Кто Вы? Направление деятельности, опыт, достижения, репутация."
        ),
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 5,
                "placeholder": _(
                    "Название компании, страна, адрес, юридическая форма, адрес сайта, размер компании, специализация, сколько лет на рынке."
                ),
            }
        ),
    )
    geo = forms.CharField(
        label=_(
            "География работы в Германии. Возможности и ограничения. Территория, логистика."
        ),
        required=False,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 5,
                "placeholder": _(
                    "Партнеры, дилеры, представители в Германии, их адреса. Логистика поставок. Территория предоставления услуг. Где продукт применим в Германии."
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
            "Продукт / услуга. Что именно Вы приобретаете? Суть, бизнес-характеристики"
        )
        self.fields["what"].widget.attrs["placeholder"] = _(
            "Продукт / услуга. Что именно Вы приобретаете? Суть, бизнес-характеристики"
        )
        self.fields["what"].initial = _(
            "Необходимо приобрести: "
        )
        
        self.fields["who"].label = _(
            "Компания-покупатель. Кто Вы? Направление деятельности, опыт, достижения, репутация."
        )
        self.fields["who"].widget.attrs["placeholder"] = _(
            "Название компании, сайт, страна и краткая информация о покупателе"
        )
        self.fields["who"].initial = _(
            "Покупатель:"
        )

        self.fields["geo"].label = _(
            "География покупки в Германии. Возможности и ограничения. Территория, логистика."
        )
        self.fields["geo"].widget.attrs["placeholder"] = _(
            "Регионы Германии, допустимая логистика, требования к месту доставки / месту оказания услуги."
        )
        self.fields["geo"].initial = _(
            "География:"
        )




class AudienceEditSellForm(AudienceHowSellForm):
    title = forms.CharField(
        label=_("Название задачи ПОИСК КЛИЕНТОВ"),
        required=True,
        widget=forms.TextInput(
            attrs={"class": "YY-INPUT", "placeholder": _("Коротко и по делу")}
        ),
    )

    task_client = forms.CharField(
        label=_("КЛИЕНТ: Критерии для ранжирования клиентов"),
        required=True,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 5,
                "placeholder": _("КЛИЕНТ: Критерии для ранжирования клиентов"),
            }
        ),
    )

    task_branches = forms.CharField(
        label=_("КАТЕГОРИИ: Критерии для ранжирования бизнес-категорий"),
        required=True,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 5,
                "placeholder": _("КАТЕГОРИИ: Критерии для ранжирования бизнес-категорий"),
            }
        ),
    )

    task_geo = forms.CharField(
        label=_("ГЕОГРАФИЯ: Критерии для ранжирования городов (Германия)"),
        required=True,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 5,
                "placeholder": _("Какая география подходит."),
            }
        ),
    )

    def clean(self):
        cleaned = super().clean()

        # В edit ВСЁ обязательно
        required_fields = [
            "what",
            "who",
            "geo",
            "title",
            "task_client",
            "task_branches",
            "task_geo",
        ]

        missing = []
        for f in required_fields:
            val = (cleaned.get(f) or "").strip()
            if not val:
                missing.append(f)

        if missing:
            self.add_error(None, _("Заполните все поля."))
            for f in missing:
                self.add_error(f, "")

        return cleaned


class AudienceEditBuyForm(AudienceHowBuyForm):
    title = forms.CharField(
        label=_("Название задачи"),
        required=True,
        widget=forms.TextInput(
            attrs={
                "class": "YY-INPUT",
                "placeholder": _("Название задачи ПОИСК ПОСТАВЩИКОВ / ПОДРЯДЧИКОВ"),
            }
        ),
    )

    task_client = forms.CharField(
        label=_("ПРОДАВЕЦ: Критерии для ранжирования продавцов"),
        required=True,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 6,
                "placeholder": _("Критерии для ранжирования продавцов."),
            }
        ),
    )

    task_branches = forms.CharField(
        label=_("КАТЕГОРИИ: Критерии для ранжирования бизнес-категорий"),
        required=True,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 6,
                "placeholder": _("Какие типы компаний/специализаций подходят."),
            }
        ),
    )

    task_geo = forms.CharField(
        label=_("ГЕОГРАФИЯ: Критерии для ранжирования городов (Германия)"),
        required=True,
        widget=forms.Textarea(
            attrs={
                "class": "YY-TEXTAREA",
                "rows": 6,
                "placeholder": _("Регионы Германии, логистика, удаленная/локальная работа."),
            }
        ),
    )

    def clean(self):
        cleaned = super().clean()

        required_fields = [
            "what",
            "who",
            "geo",
            "title",
            "task_client",
            "task_branches",
            "task_geo",
        ]

        missing = []
        for f in required_fields:
            val = (cleaned.get(f) or "").strip()
            if not val:
                missing.append(f)

        if missing:
            self.add_error(None, _("Заполните все поля."))
            for f in missing:
                self.add_error(f, "")

        return cleaned



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
