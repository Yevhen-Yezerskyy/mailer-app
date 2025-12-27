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
        label=_("Название задачи"),
        required=True,
        widget=forms.TextInput(
            attrs={"class": "YY-INPUT", "placeholder": _("Название задачи")}
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
                "placeholder": _("Название задачи"),
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



class _AudienceClarBaseMixin:
    """
    База CLAR:
    - оставляем только: title, task, task_client, task_branches, task_geo
    - what/who/geo удаляем (если пришли от родителя)
    - task добавляем централизованно (YY-TEXTAREA)
    - общий clean (без вызова super().clean() родителей, чтобы не требовать what/who/geo)
    """

    TASK_LABEL = _("Задача")
    TASK_PLACEHOLDER = _("Опишите основную задачу.")
    TASK_INITIAL = ""

    REQUIRED_FIELDS = ["title", "task", "task_client", "task_branches", "task_geo"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # убрать HOW-поля, если они пришли от родителя (AudienceHow*/AudienceEdit*)
        for f in ("what", "who", "geo"):
            self.fields.pop(f, None)

        # добавить/переопределить task централизованно
        self.fields["task"] = forms.CharField(
            label=self.TASK_LABEL,
            required=True,
            widget=forms.Textarea(
                attrs={
                    "class": "YY-TEXTAREA",
                    "rows": 6,
                    "placeholder": self.TASK_PLACEHOLDER,
                }
            ),
        )
        if self.TASK_INITIAL:
            self.fields["task"].initial = self.TASK_INITIAL

    def clean(self):
        # ВАЖНО: не вызываем super().clean(), потому что у AudienceEdit* clean() требует what/who/geo
        cleaned = forms.Form.clean(self)

        missing = []
        for f in self.REQUIRED_FIELDS:
            val = (cleaned.get(f) or "").strip()
            if not val:
                missing.append(f)

        if missing:
            self.add_error(None, _("Заполните все поля."))
            for f in missing:
                self.add_error(f, "")

        return cleaned


class AudienceClarSellForm(_AudienceClarBaseMixin, AudienceEditSellForm):
    """
    CLAR SELL: фиксация задачи + критериев ранжирования для поиска клиентов.
    """

    TASK_LABEL = _("Задача. Что продаём и кого хотим привлечь")
    TASK_PLACEHOLDER = _(
        "1–2 предложения: что продаёте (суть), кто целевой клиент в Германии и какой результат нужен (лиды/встречи/заявки)."
    )
    TASK_INITIAL = _("Нужно найти клиентов: ")


class AudienceClarBuyForm(_AudienceClarBaseMixin, AudienceEditBuyForm):
    """
    CLAR BUY: фиксация задачи + критериев ранжирования для поиска поставщиков/подрядчиков.
    """

    TASK_LABEL = _("Задача. Что нужно купить и какой подрядчик подходит")
    TASK_PLACEHOLDER = _(
        "1–2 предложения: что нужно купить/заказать, ключевые требования и какой поставщик/подрядчик нужен (тип, опыт, условия)."
    )
    TASK_INITIAL = _("Нужно найти поставщиков / подрядчиков: ")
