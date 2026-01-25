# FILE: engine/common/db.py  (обновлено — 2026-01-06)
# PURPOSE: Универсальный PostgreSQL-коннектор.
#          По умолчанию ведёт себя КАК СЕЙЧАС (localhost:5433 для хоста),
#          но в Docker полностью управляется через env (mailer-db:5432).

from __future__ import annotations

import os
import psycopg


# -------------------------
# НАСТРОЙКИ (BACKWARD-COMPATIBLE)
# -------------------------
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]


def get_connection(autocommit: bool = False) -> psycopg.Connection:
    """
    PostgreSQL connection.
    - Host mode (default): localhost:5433
    - Docker mode: DB_HOST=mailer-db, DB_PORT=5432
    """
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        autocommit=autocommit,
    )


def fetch_all(sql: str, params=None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()


def fetch_one(sql: str, params=None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()


def execute(sql: str, params=None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()


if __name__ == "__main__":
    print(fetch_one("SELECT 1"))
