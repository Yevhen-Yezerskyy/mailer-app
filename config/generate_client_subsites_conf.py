# FILE: config/generate_client_subsites_conf.py
# DATE: 2026-03-19
# PURPOSE: Manually generate nginx config for static client subsites from DB.

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))
WEB_ROOT = REPO_ROOT / "web"
if str(WEB_ROOT) not in sys.path:
    sys.path.append(str(WEB_ROOT))

RUNTIME_ENV_PATH = Path(os.environ.get("SECRETS_ENV_FILE") or "/run/serenity-secrets/runtime.env")
OUTPUT_PATH = Path(
    os.environ.get("CLIENT_SUBSITES_NGINX_CONF_PATH")
    or (REPO_ROOT / "config/nginx/conf.d/client-subsites-generated.conf")
).resolve()
SITE_ROOT = Path(os.environ.get("CLIENT_SUBSITES_NGINX_ROOT") or "/app/client-subsites")
ACME_ROOT = os.environ.get("CLIENT_SUBSITES_ACME_ROOT") or "/var/www/certbot"
LETSENCRYPT_ROOT = Path(os.environ.get("CLIENT_SUBSITES_LE_ROOT") or "/etc/letsencrypt/live")


def _ensure_runtime_env_loaded() -> None:
    required = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
    if all(os.environ.get(key) for key in required):
        return
    if not RUNTIME_ENV_PATH.is_file():
        missing = ", ".join(key for key in required if not os.environ.get(key))
        raise RuntimeError(f"Missing env vars: {missing}. Also not found: {RUNTIME_ENV_PATH}")

    proc = subprocess.run(
        ["/bin/sh", "-c", f"set -a && . {RUNTIME_ENV_PATH} >/dev/null 2>&1 && env -0"],
        capture_output=True,
        check=True,
    )
    for chunk in proc.stdout.split(b"\0"):
        if not chunk:
            continue
        key, sep, value = chunk.partition(b"=")
        if not sep:
            continue
        os.environ.setdefault(key.decode("utf-8"), value.decode("utf-8"))


_ensure_runtime_env_loaded()

from engine.common.db import fetch_all  # noqa: E402
from panel.aap_settings.client_subsites import ensure_client_subsite_dir, normalize_client_domain  # noqa: E402


def _load_domains() -> list[str]:
    rows = fetch_all(
        """
        SELECT domain
        FROM aap_settings_workspace_domains
        ORDER BY domain, id
        """
    ) or []
    result: list[str] = []
    seen: set[str] = set()
    for (raw_domain,) in rows:
        domain = normalize_client_domain(str(raw_domain or ""))
        if domain in seen:
            continue
        seen.add(domain)
        result.append(domain)
    return result


def _has_certificate(domain: str) -> bool:
    cert_dir = LETSENCRYPT_ROOT / domain
    return (cert_dir / "fullchain.pem").is_file() and (cert_dir / "privkey.pem").is_file()


def _render_server_80_only(domain: str) -> str:
    return f"""server {{
    listen 80;
    listen [::]:80;
    server_name {domain};

    location /.well-known/acme-challenge/ {{
        root {ACME_ROOT};
        try_files $uri =404;
    }}

    location / {{
        return 404;
    }}
}}
"""


def _render_server_80_and_443(domain: str) -> str:
    root_path = SITE_ROOT / domain
    return f"""server {{
    listen 80;
    listen [::]:80;
    server_name {domain};

    location /.well-known/acme-challenge/ {{
        root {ACME_ROOT};
        try_files $uri =404;
    }}

    location / {{
        return 301 https://$host$request_uri;
    }}
}}

server {{
    listen 443 ssl;
    listen [::]:443 ssl;
    server_name {domain};

    ssl_certificate     /etc/letsencrypt/live/{domain}/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/{domain}/privkey.pem;

    root {root_path};
    index index.html;

    location ~* \\.php(?:$|/) {{ return 444; }}
    location ~ /\\.(?!well-known) {{ return 444; }}

    location / {{
        try_files $uri $uri/ =404;
    }}
}}
"""


def _render_config(domains: list[str]) -> str:
    stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    parts = [
        "# FILE: config/nginx/conf.d/client-subsites-generated.conf",
        f"# DATE: {stamp}",
        "# PURPOSE: Generated nginx config for static client subsites.",
        "# GENERATED: run python config/generate_client_subsites_conf.py manually to rebuild.",
        "",
    ]
    if not domains:
        parts.append("# No client subsite domains found.")
        parts.append("")
        return "\n".join(parts)

    for domain in domains:
        if _has_certificate(domain):
            parts.append(_render_server_80_and_443(domain).rstrip())
        else:
            parts.append(_render_server_80_only(domain).rstrip())
        parts.append("")

    return "\n".join(parts)


def _write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        tmp.write(content)
        tmp_name = tmp.name
    os.replace(tmp_name, path)
    path.chmod(0o644)


def main() -> int:
    domains = _load_domains()
    for domain in domains:
        ensure_client_subsite_dir(domain)

    content = _render_config(domains)
    _write_atomic(OUTPUT_PATH, content)

    print(f"generated: {OUTPUT_PATH}")
    print(f"domains: {len(domains)}")
    for domain in domains:
        status = "cert" if _has_certificate(domain) else "no-cert"
        print(f"{domain}\t{status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
