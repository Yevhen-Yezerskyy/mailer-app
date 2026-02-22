#!/usr/bin/env python
# FILE: web-admin/manage.py
# DATE: 2026-02-22
# PURPOSE: Django manage entrypoint for standalone admin contour (web-admin).

import os
import sys
from pathlib import Path


def main():
    repo_root = Path(__file__).resolve().parent.parent
    web_dir = repo_root / "web"

    if str(repo_root) not in sys.path:
        sys.path.append(str(repo_root))
    if str(web_dir) not in sys.path:
        sys.path.append(str(web_dir))

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "web_admin.settings")

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
