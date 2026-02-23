# FILE: web-admin/web_admin/settings.py
# DATE: 2026-02-22
# PURPOSE: Settings for standalone admin contour with default Django auth_user.

from __future__ import annotations

import os
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BASE_DIR.parent
WEB_DIR = REPO_ROOT / "web"

if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))
if str(WEB_DIR) not in sys.path:
    sys.path.append(str(WEB_DIR))


SECRET_KEY = os.environ["DJANGO_SECRET_KEY"]
DEBUG = os.environ.get("DEBUG", "1") not in ("0", "false", "False", "no", "NO")

ALLOWED_HOSTS = [
    "admin.serenity-mail.com",
    "admin.serenity-mail.de",
    "dev-admin.serenity-mail.com",
    "dev-admin.serenity-mail.de",
    "localhost",
    "127.0.0.1",
]

CSRF_TRUSTED_ORIGINS = [
    "https://admin.serenity-mail.com",
    "https://admin.serenity-mail.de",
    "https://dev-admin.serenity-mail.com",
    "https://dev-admin.serenity-mail.de",
]

SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")


INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "public",
    "mailer_web",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.locale.LocaleMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "web_admin.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "web_admin.wsgi.application"
ASGI_APPLICATION = "web_admin.asgi.application"


DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": os.environ["DB_HOST"],
        "PORT": os.environ["DB_PORT"],
        "NAME": os.environ["DB_NAME"],
        "USER": os.environ["DB_USER"],
        "PASSWORD": os.environ["DB_PASSWORD"],
    }
}


AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]


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
LOCALE_PATHS = []


STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_DIRS = []


DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
}
