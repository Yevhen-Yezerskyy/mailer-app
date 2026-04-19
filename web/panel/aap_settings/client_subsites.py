# FILE: web/panel/aap_settings/client_subsites.py
# DATE: 2026-03-19
# PURPOSE: Validation and filesystem helpers for per-domain client subsite directories.

from __future__ import annotations

import re
import shutil
from pathlib import Path

from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _trans


_RE_DOMAIN = re.compile(
    r"^(?=.{1,255}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
)
_REPO_ROOT = Path(__file__).resolve().parents[3]
CLIENT_SUBSITES_ROOT = _REPO_ROOT / "client-subsites"


def normalize_client_domain(value: str) -> str:
    domain = (value or "").strip().lower().rstrip(".")
    if not domain:
        raise ValidationError(_trans("Укажите домен."))
    if "://" in domain or "/" in domain or "\\" in domain:
        raise ValidationError(_trans("Укажите только домен без протокола и пути."))
    if not _RE_DOMAIN.fullmatch(domain):
        raise ValidationError(_trans("Укажите корректный домен."))
    return domain


def client_subsite_relpath(domain: str) -> str:
    normalized = normalize_client_domain(domain)
    return str(Path("client-subsites") / normalized)


def client_subsite_path(domain: str) -> Path:
    normalized = normalize_client_domain(domain)
    root = CLIENT_SUBSITES_ROOT.resolve()
    path = (root / normalized).resolve()
    if path.parent != root:
        raise ValidationError(_trans("Некорректный домен."))
    return path


def ensure_client_subsite_dir(domain: str) -> Path:
    path = client_subsite_path(domain)
    CLIENT_SUBSITES_ROOT.mkdir(parents=True, exist_ok=True)
    if path.exists() and not path.is_dir():
        raise ValidationError(_trans("Путь домена уже занят не каталогом."))
    path.mkdir(exist_ok=True)
    return path


def delete_client_subsite_dir(domain: str) -> None:
    path = client_subsite_path(domain)
    if path.is_symlink():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()
