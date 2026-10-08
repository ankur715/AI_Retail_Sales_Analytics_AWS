"""Read-only Redshift access for the triage agent.

  - Connects as TRIAGE_REDSHIFT_USER (triage_reader): SELECT on the five tables
    below and nothing else -- created by `python -m triage.setup_reader`.
  - No free-form SQL: callers pass the NAME of one of the fixed queries in
    QUERIES plus parameters; there is no way to hand this module a SQL string.
  - One connection per triage run, opened on first use, so Serverless wakes
    once; results are cached per (query, params), so asking twice is free.
"""
from datetime import date, datetime
from decimal import Decimal

from etl import config, redshift

# The tables the triage user can read (setup_reader grants exactly these).
TABLES = ("etl.load_audit", "etl.etl_stats", "etl.load_files", "etl.pipeline_config", "stage.stg_sales")

QUERIES = {
    # get_load_audit: the run itself
    "load_audit": """
        SELECT load_id, pipeline_id, dag_run_id, load_type, status, source_file,
               window_start, window_end, parse_stats, fact_inserted, fact_updated,
               error_message, started_at, finished_at
        FROM etl.load_audit
        WHERE load_id = %(load_id)s;""",
    # get_stats: src / stg / rej side by side per week, in ONE pass over etl_stats
    "stats_by_week": """
        SELECT week_date,
               SUM(CASE WHEN source = 'src' THEN row_count END) AS src_rows,
               SUM(CASE WHEN source = 'stg' THEN row_count END) AS stg_rows,
               SUM(CASE WHEN source = 'rej' THEN row_count END) AS rej_rows,
               SUM(CASE WHEN source = 'src' THEN sales END)     AS src_sales,
               SUM(CASE WHEN source = 'stg' THEN sales END)     AS stg_sales,
               SUM(CASE WHEN source = 'rej' THEN sales END)     AS rej_sales,
               SUM(CASE WHEN source = 'src' THEN inventory END) AS src_inventory,
               SUM(CASE WHEN source = 'stg' THEN inventory END) AS stg_inventory,
               SUM(CASE WHEN source = 'rej' THEN inventory END) AS rej_inventory
        FROM etl.etl_stats
        WHERE load_id = %(load_id)s
        GROUP BY week_date
        ORDER BY week_date;""",
    # get_unmapped_products (fallback when no unmapped_products.csv was written)
    "unmapped_keys": """
        SELECT src_product_key, COUNT(*) AS rows, SUM(sales) AS sales,
               MIN(week_date) AS first_week, MAX(week_date) AS last_week
        FROM stage.stg_sales
        WHERE load_id = %(load_id)s AND product_id IS NULL
        GROUP BY src_product_key
        ORDER BY sales DESC
        LIMIT 50;""",
    # get_file_history: recent loads of this pipeline with each file's contribution
    "file_history": """
        SELECT a.load_id, a.status, a.started_at, a.window_start, a.window_end,
               f.source_file, f.file_week_end, f.parsed_rows, f.rows_used,
               f.superseded_rows, f.parse_rejects, f.out_of_window
        FROM etl.load_audit a
        LEFT JOIN etl.load_files f ON f.load_id = a.load_id
        WHERE a.pipeline_id = %(pipeline_id)s
        ORDER BY a.started_at DESC, f.source_file
        LIMIT %(limit)s;""",
}


def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


class ReadOnlySession:
    """One read-only connection + result cache for a single triage run."""

    def __init__(self):
        self._conn = None
        self._cache: dict[tuple, list[dict]] = {}
        self.queries_run = 0

    def query(self, name: str, **params) -> list[dict]:
        sql = QUERIES[name]          # KeyError for anything that isn't a fixed, named query
        key = (name, tuple(sorted(params.items())))
        if key not in self._cache:
            if self._conn is None:
                if not config.TRIAGE_REDSHIFT_PASSWORD:
                    raise RuntimeError("TRIAGE_REDSHIFT_PASSWORD is not set (run python -m triage.setup_reader)")
                self._conn = redshift.get_connection(user=config.TRIAGE_REDSHIFT_USER,
                                                     password=config.TRIAGE_REDSHIFT_PASSWORD)
            with self._conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [d[0] for d in cur.description]
                self._cache[key] = [{c: _jsonable(v) for c, v in zip(cols, row)} for row in cur.fetchall()]
            self._conn.rollback()    # read-only: never leave a transaction open
            self.queries_run += 1
        return self._cache[key]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
