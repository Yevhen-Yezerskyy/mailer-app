# FILE: web/aap_settings/forms.py

from django import forms

from .models import MailConnection


ENCRYPTION_CHOICES = [
    ("ssl", "SSL / TLS"),
    ("starttls", "STARTTLS"),
    ("none", "Без шифрования (не рекомендуется)"),
]


class MailConnectionForm(forms.ModelForm):
    """
    Форма для создания/редактирования почтового подключения.
    - На входе: человеческие поля (host/port/login/password).
    - На выходе: smtp_config / imap_config в JSON внутри модели.
    """

    # SMTP
    smtp_host = forms.CharField(
        label="SMTP хост",
        max_length=255,
        required=True,
    )
    smtp_port = forms.IntegerField(
        label="SMTP порт",
        required=True,
        initial=587,
    )
    smtp_encryption = forms.ChoiceField(
        label="SMTP шифрование",
        choices=ENCRYPTION_CHOICES,
        initial="starttls",
        required=True,
    )
    smtp_username = forms.CharField(
        label="SMTP логин",
        max_length=255,
        required=True,
    )
    smtp_password = forms.CharField(
        label="SMTP пароль",
        widget=forms.PasswordInput(render_value=True),
        required=True,
    )

    # IMAP
    imap_host = forms.CharField(
        label="IMAP хост",
        max_length=255,
        required=True,
    )
    imap_port = forms.IntegerField(
        label="IMAP порт",
        required=True,
        initial=993,
    )
    imap_encryption = forms.ChoiceField(
        label="IMAP шифрование",
        choices=ENCRYPTION_CHOICES,
        initial="ssl",
        required=True,
    )
    imap_username = forms.CharField(
        label="IMAP логин",
        max_length=255,
        required=True,
    )
    imap_password = forms.CharField(
        label="IMAP пароль",
        widget=forms.PasswordInput(render_value=True),
        required=True,
    )

    class Meta:
        model = MailConnection
        fields = ["name", "from_name", "from_email"]

    def __init__(self, *args, workspace_id=None, **kwargs):
        """
        workspace_id прокидываем из вьюхи, чтобы не пихать его в форму руками.
        При редактировании подставляем значения из JSON-конфигов в поля.
        """
        self.workspace_id = workspace_id
        super().__init__(*args, **kwargs)

        if self.instance and self.instance.pk:
            smtp = self.instance.smtp_config or {}
            imap = self.instance.imap_config or {}

            self.fields["smtp_host"].initial = smtp.get("host", "")
            self.fields["smtp_port"].initial = smtp.get("port", 587)
            self.fields["smtp_encryption"].initial = smtp.get("encryption", "starttls")
            self.fields["smtp_username"].initial = smtp.get("username", "")
            # Пароль — спорный момент, можно не подставлять.
            self.fields["smtp_password"].initial = smtp.get("password", "")

            self.fields["imap_host"].initial = imap.get("host", "")
            self.fields["imap_port"].initial = imap.get("port", 993)
            self.fields["imap_encryption"].initial = imap.get("encryption", "ssl")
            self.fields["imap_username"].initial = imap.get("username", "")
            self.fields["imap_password"].initial = imap.get("password", "")

    def clean_smtp_port(self):
        port = self.cleaned_data["smtp_port"]
        if port <= 0 or port > 65535:
            raise forms.ValidationError("Некорректный порт SMTP.")
        return port

    def clean_imap_port(self):
        port = self.cleaned_data["imap_port"]
        if port <= 0 or port > 65535:
            raise forms.ValidationError("Некорректный порт IMAP.")
        return port

    def save(self, commit=True):
        """
        Собираем SMTP/IMAP в JSON и сохраняем в модель.
        Статусы не трогаем — ими управляет только код.
        """
        instance = super().save(commit=False)

        if self.workspace_id is not None:
            instance.workspace_id = self.workspace_id

        instance.smtp_config = {
            "host": self.cleaned_data["smtp_host"],
            "port": self.cleaned_data["smtp_port"],
            "encryption": self.cleaned_data["smtp_encryption"],
            "username": self.cleaned_data["smtp_username"],
            "password": self.cleaned_data["smtp_password"],
        }

        instance.imap_config = {
            "host": self.cleaned_data["imap_host"],
            "port": self.cleaned_data["imap_port"],
            "encryption": self.cleaned_data["imap_encryption"],
            "username": self.cleaned_data["imap_username"],
            "password": self.cleaned_data["imap_password"],
            # пустой список = "используем все папки" (автообнаружение)
            "folders": [],
        }

        if commit:
            instance.save()
        return instance
