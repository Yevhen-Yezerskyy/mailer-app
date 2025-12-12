# FILE: engine/common/db.py  (новое) 2025-12-12

from __future__ import annotations

import psycopg


# -------------------------
# ЖЁСТКИЕ НАСТРОЙКИ ДЛЯ ЛОКАЛКИ
# -------------------------
DB_HOST = "localhost"     # всегда ходим с хоста
DB_PORT = 5433            # проброшенный порт контейнера
DB_NAME = "mailersys"
DB_USER = "mailersys_user"
DB_PASSWORD = "secret"


def get_connection(autocommit: bool = False) -> psycopg.Connection:
    """
    Соединение с PostgreSQL внутри Docker:
      localhost:5433 → mailer-db:5432
    """
    conn = psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        autocommit=autocommit,
    )
    return conn


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
