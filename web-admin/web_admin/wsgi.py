# FILE: web-admin/web_admin/wsgi.py
# DATE: 2026-02-22
# PURPOSE: WSGI entrypoint for standalone admin contour.

from __future__ import annotations

import os
import sys
from pathlib import Path

from django.core.wsgi import get_wsgi_application


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
WEB_DIR = REPO_ROOT / "web"

if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))
if str(WEB_DIR) not in sys.path:
    sys.path.append(str(WEB_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "web_admin.settings")

application = get_wsgi_application()

