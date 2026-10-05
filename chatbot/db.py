"""Read-only Redshift access for the chatbot, as CHAT_REDSHIFT_USER
(chat_reader: SELECT on the chat schema only, 30s statement timeout --
see chatbot/setup_reader.py). Never the ETL admin user."""
import time
from datetime import date, datetime
from decimal import Decimal

from etl import config, redshift

KNOWN_VALUES_TTL = 3600   # seconds; brand/retailer lists change only when a feed is onboarded
_known_cache: dict[str, tuple[float, list]] = {}


def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def query(sql: str) -> dict:
    conn = redshift.get_connection(user=config.CHAT_REDSHIFT_USER, password=config.CHAT_REDSHIFT_PASSWORD)
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout TO 30000;")   # also set on the user; belt and braces
            started = time.monotonic()
            cur.execute(sql)
            columns = [d[0] for d in cur.description]
            rows = [[_jsonable(v) for v in row] for row in cur.fetchall()]
        return {"columns": columns, "rows": rows, "row_count": len(rows),
                "elapsed_ms": int((time.monotonic() - started) * 1000)}
    finally:
        conn.close()


def known_values(columns_by_view: dict[str, list[str]]) -> dict[str, list]:
    """Distinct values for the catalog's known_values columns of the routed
    views, cached for an hour so routing a question doesn't wake Redshift
    for lookups it already did."""
    out, now = {}, time.monotonic()
    for view, cols in columns_by_view.items():
        for col in cols:
            key = f"{view}.{col}"
            hit = _known_cache.get(key)
            if not hit or now - hit[0] > KNOWN_VALUES_TTL:
                rows = query(f"SELECT DISTINCT {col} FROM {view} WHERE {col} IS NOT NULL ORDER BY 1 LIMIT 200")["rows"]
                _known_cache[key] = (now, [r[0] for r in rows])
            out[key] = _known_cache[key][1]
    return out


def data_as_of() -> str | None:
    rows = query("SELECT MAX(week_ending) FROM chat.v_calendar")["rows"]
    return rows[0][0] if rows else None
