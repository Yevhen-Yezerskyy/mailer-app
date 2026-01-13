# FILE: web/panel/aap_settings/management/commands/seed_provider_presets.py
# DATE: 2026-01-13
# PURPOSE: Засеять (upsert) базовые provider_presets (SMTP/IMAP) для Германии + популярные провайдеры.

from __future__ import annotations

from django.core.management.base import BaseCommand

from panel.aap_settings.models import ProviderPreset


class Command(BaseCommand):
    help = "Seed provider presets (SMTP/IMAP) into aap_settings_provider_presets."

    def handle(self, *args, **options):
        presets = [
            # --- Gmail ---
            dict(
                code="gmail",
                name="Gmail",
                kind="smtp",
                host="smtp.gmail.com",
                ports_json=[587, 465],
                security="starttls",
                auth_type="login",
                extra_json={"alt": {"port": 465, "security": "ssl"}},
                is_active=True,
                order=10,
            ),
            dict(
                code="gmail",
                name="Gmail",
                kind="imap",
                host="imap.gmail.com",
                ports_json=[993],
                security="ssl",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=10,
            ),
            # --- Outlook.com / Microsoft ---
            dict(
                code="outlook",
                name="Outlook.com",
                kind="smtp",
                host="smtp-mail.outlook.com",
                ports_json=[587],
                security="starttls",
                auth_type="oauth2",
                extra_json={},
                is_active=True,
                order=20,
            ),
            dict(
                code="outlook",
                name="Outlook.com",
                kind="imap",
                host="outlook.office365.com",
                ports_json=[993],
                security="ssl",
                auth_type="oauth2",
                extra_json={},
                is_active=True,
                order=20,
            ),
            # --- IONOS ---
            dict(
                code="ionos",
                name="IONOS",
                kind="smtp",
                host="smtp.ionos.com",
                ports_json=[465, 587],
                security="ssl",
                auth_type="login",
                extra_json={"alt": {"port": 587, "security": "starttls"}},
                is_active=True,
                order=30,
            ),
            dict(
                code="ionos",
                name="IONOS",
                kind="imap",
                host="imap.ionos.com",
                ports_json=[993],
                security="ssl",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=30,
            ),
            # --- GMX ---
            dict(
                code="gmx",
                name="GMX",
                kind="smtp",
                host="mail.gmx.net",
                ports_json=[587],
                security="starttls",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=40,
            ),
            dict(
                code="gmx",
                name="GMX",
                kind="imap",
                host="imap.gmx.net",
                ports_json=[993],
                security="ssl",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=40,
            ),
            # --- WEB.DE ---
            dict(
                code="webde",
                name="WEB.DE",
                kind="smtp",
                host="smtp.web.de",
                ports_json=[587],
                security="starttls",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=50,
            ),
            dict(
                code="webde",
                name="WEB.DE",
                kind="imap",
                host="imap.web.de",
                ports_json=[993],
                security="ssl",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=50,
            ),
            # --- Telekom / T-Online / Magenta ---
            dict(
                code="t-online",
                name="Telekom (t-online.de / magenta.de)",
                kind="smtp",
                host="smtp.mail.t-online.de",
                ports_json=[587],
                security="starttls",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=60,
            ),
            dict(
                code="t-online",
                name="Telekom (t-online.de / magenta.de)",
                kind="imap",
                host="secureimap.t-online.de",
                ports_json=[993],
                security="ssl",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=60,
            ),
            # --- STRATO ---
            dict(
                code="strato",
                name="STRATO",
                kind="smtp",
                host="smtp.strato.de",
                ports_json=[465],
                security="ssl",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=70,
            ),
            dict(
                code="strato",
                name="STRATO",
                kind="imap",
                host="imap.strato.de",
                ports_json=[993],
                security="ssl",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=70,
            ),
            # --- mailbox.org ---
            dict(
                code="mailbox",
                name="mailbox.org",
                kind="smtp",
                host="smtp.mailbox.org",
                ports_json=[587, 465],
                security="starttls",
                auth_type="login",
                extra_json={"alt": {"port": 465, "security": "ssl"}},
                is_active=True,
                order=80,
            ),
            dict(
                code="mailbox",
                name="mailbox.org",
                kind="imap",
                host="imap.mailbox.org",
                ports_json=[993],
                security="ssl",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=80,
            ),
            # --- Yahoo (часто встречается) ---
            dict(
                code="yahoo",
                name="Yahoo Mail",
                kind="smtp",
                host="smtp.mail.yahoo.com",
                ports_json=[587, 465],
                security="starttls",
                auth_type="login",
                extra_json={"alt": {"port": 465, "security": "ssl"}},
                is_active=True,
                order=90,
            ),
            dict(
                code="yahoo",
                name="Yahoo Mail",
                kind="imap",
                host="imap.mail.yahoo.com",
                ports_json=[993],
                security="ssl",
                auth_type="login",
                extra_json={},
                is_active=True,
                order=90,
            ),
        ]

        created = 0
        updated = 0

        for p in presets:
            obj, is_created = ProviderPreset.objects.update_or_create(
                code=p["code"],
                kind=p["kind"],
                defaults={
                    "name": p["name"],
                    "host": p["host"],
                    "ports_json": p["ports_json"],
                    "security": p["security"],
                    "auth_type": p["auth_type"],
                    "extra_json": p.get("extra_json") or {},
                    "is_active": bool(p.get("is_active", True)),
                    "order": int(p.get("order", 0)),
                },
            )
            if is_created:
                created += 1
            else:
                updated += 1

        self.stdout.write(self.style.SUCCESS(f"ProviderPreset: created={created}, updated={updated}"))
