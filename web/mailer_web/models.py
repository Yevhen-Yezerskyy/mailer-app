# FILE: web/mailer_web/models.py  (обновлено — 2026-03-07)
# CHANGE: подцепляем client/workspace модели из models_accounts.py + базовые модели шаблонов/писем.

from django.db import models

from .models_accounts import ClientUser, Workspace, UserActionToken


class MailTemplate(models.Model):
    template_name = models.CharField(max_length=255)
    template_html = models.TextField(blank=True, default="")
    styles = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "mailer_web_mail_templates"
        ordering = ["template_name", "id"]


class MailLetter(models.Model):
    name = models.CharField(max_length=255)
    slug = models.SlugField(max_length=255, blank=True, default="", db_index=True)
    template = models.ForeignKey(
        MailTemplate,
        on_delete=models.PROTECT,
        related_name="letters",
        null=True,
        blank=True,
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "mailer_web_mail_letters"
        ordering = ["name", "id"]


class MailLetterLang(models.Model):
    letter = models.ForeignKey(
        MailLetter,
        on_delete=models.CASCADE,
        related_name="langs",
    )
    lang = models.CharField(max_length=16)
    subject = models.CharField(max_length=255, blank=True, default="")
    letter_html = models.TextField(blank=True, default="")
    send_html = models.TextField(blank=True, default="")

    class Meta:
        db_table = "mailer_web_mail_letter_langs"
        ordering = ["letter_id", "lang", "id"]
