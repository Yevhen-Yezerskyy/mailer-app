# FILE: web/mailer_web/settings.py

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

DEBUG = True

ALLOWED_HOSTS = [
    "mail.fenster-ukraine.de",
    "localhost",
    "127.0.0.1",
]

CSRF_TRUSTED_ORIGINS = [
    "https://mail.fenster-ukraine.de",
    "http://mail.fenster-ukraine.de",
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

    "aap_console",
    "aap_settings",
    "aap_audience",
    "accounts",
]


# --- MIDDLEWARE ---

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",

    "mailer_web.middleware.WorkspaceMiddleware",

    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "mailer_web.urls"


# --- TEMPLATES ---

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
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


WSGI_APPLICATION = "mailer_web.wsgi.application"


# --- DATABASE (Postgres в Docker) ---

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("DB_NAME", "mailersys"),
        "USER": os.environ.get("DB_USER", "mailersys_user"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "secret"),
        "HOST": os.environ.get("DB_HOST", "mailer-db"),
        "PORT": os.environ.get("DB_PORT", "5432"),
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

LANGUAGE_CODE = "en-us"
TIME_ZONE = "Europe/Berlin"

USE_I18N = True
USE_TZ = True


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
