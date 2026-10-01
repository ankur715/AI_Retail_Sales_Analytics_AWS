"""Redshift connection helpers. Redshift speaks the Postgres wire protocol,
so plain psycopg2 works; port 5439, SSL required."""
import time

import psycopg2

from etl import config


def get_connection(dbname: str | None = None, attempts: int = 4):
    # A paused Serverless workgroup takes a few seconds to resume on the first
    # query, so the timeout is generous; transient drops retry with backoff.
    for attempt in range(1, attempts + 1):
        try:
            return psycopg2.connect(
                host=config.REDSHIFT_HOST,
                port=config.REDSHIFT_PORT,
                dbname=dbname or config.REDSHIFT_DB,
                user=config.REDSHIFT_USER,
                password=config.REDSHIFT_PASSWORD,
                sslmode="require",
                connect_timeout=30,
            )
        except psycopg2.OperationalError:
            if attempt == attempts:
                raise
            time.sleep(5 * 2 ** (attempt - 1))   # 5s, 10s, 20s


def run(statements: list[tuple[str, dict | None]]) -> list[int]:
    """Run several statements in ONE transaction (all or nothing) and return
    each statement's rowcount. Values always go through psycopg2 params --
    never f-strings -- except identifiers, which come from code, not data."""
    conn = get_connection()
    try:
        counts = []
        with conn.cursor() as cur:
            for sql, params in statements:
                cur.execute(sql, params)
                counts.append(cur.rowcount)
        conn.commit()
        return counts
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_all(sql: str, params: dict | None = None) -> list[tuple]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def fetch_dicts(sql: str, params: dict | None = None) -> list[dict]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()
