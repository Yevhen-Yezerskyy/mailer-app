# FILE: web/mailer_web/settings.py  (обновлено — 2026-01-06)
# PURPOSE: Единый формат DB env (DB_HOST/DB_PORT/...) + DEBUG из env.
#          Дефолты совпадают с engine: localhost:5433 (хост), в Docker переопределяется на mailer-db:5432.

"""
Django settings for mailer_web project.
"""

import os
from pathlib import Path

# --- BASE DIR ---

BASE_DIR = Path(__file__).resolve().parent.parent

# --- SECURITY ---

SECRET_KEY = os.environ.get(
    "DJANGO_SECRET_KEY",
    "django-insecure-3m*cqi8#r2wwaw=n_*mj2@7u+%(wys52q*n$!lr8f3r9jg#ksg",
)

# DEBUG from env (default = True for backward compat)
DEBUG = os.environ.get("DEBUG", "1") not in ("0", "false", "False", "no", "NO")

ALLOWED_HOSTS = [
    "serenity-mail.com",
    "serenity-mail.de",
    "dev.serenity-mail.com",
    "dev.serenity-mail.de",
    "localhost",
    "127.0.0.1",
]

CSRF_TRUSTED_ORIGINS = [
    "https://serenity-mail.com",
    "https://serenity-mail.de",
    "https://dev.serenity-mail.com",
    "https://dev.serenity-mail.de",
]

# если Django стоит за nginx с HTTPS:
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")

# --- APPLICATIONS ---

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "mailer_web",
    "public",
    "panel",
    # panel sub-apps (ОБЯЗАТЕЛЬНО)
    "panel.aap_audience",
    "panel.aap_settings",
]

# --- MIDDLEWARE ---

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    # Django i18n — сначала
    "django.middleware.locale.LocaleMiddleware",
    # ТВОЯ логика выбора языка (cookie / geo / url)
    "mailer_web.middleware_public_lang.PublicLangMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "mailer_web.middleware.WorkspaceMiddleware",
    # tailwind class-map (работает только если request._tw_classmap_enabled=True)
    "mailer_web.tw_classmap_middleware.TailwindClassMapMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "mailer_web.urls"

# --- TEMPLATES ---

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [
            BASE_DIR / "templates",
        ],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "panel.context_processors.panel_context",
            ],
        },
    },
]

WSGI_APPLICATION = "mailer_web.wsgi.application"

# --- DATABASE (Unified DB_* env) ---

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("DB_NAME", "mailersys"),
        "USER": os.environ.get("DB_USER", "mailersys_user"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "secret"),
        # defaults = host-mode (same as engine/common/db.py)
        "HOST": os.environ.get("DB_HOST", "localhost"),
        "PORT": os.environ.get("DB_PORT", "5433"),
    }
}

# --- PASSWORD VALIDATION ---

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

# --- I18N / TIMEZONE ---

LANGUAGE_CODE = "ru"

LANGUAGES = [
    ("ru", "Русский"),
    ("de", "Deutsch"),
    ("uk", "Українська"),
    ("en", "English"),
]

TIME_ZONE = "Europe/Berlin"

USE_I18N = True
USE_TZ = True

LOCALE_PATHS = [
    BASE_DIR / "locale",
]

# --- GEOIP (Country only) ---

GEOIP_PATH = BASE_DIR / "logs" / "GeoLite2-Country"

# --- STATIC FILES ---

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_DIRS = [
    BASE_DIR / "static",
]

# --- AUTH / LOGIN REDIRECTS ---

LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "dashboard"
LOGOUT_REDIRECT_URL = "landing"

# --- MISC ---

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
