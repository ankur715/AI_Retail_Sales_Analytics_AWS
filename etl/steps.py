"""The pipeline steps. Each one is a plain function of a LoadContext, so the
Airflow DAG (one task per step) and the CLI (etl.run, all steps in a row)
execute exactly the same code.

Old ibi subflow              step here
---------------------------  -------------------------------------------
(parameters)                 begin          -> load_id, week window, audit row
parser script                parse          -> every pending file -> s3 parsed/.../output_<client>_<retailer>.csv
                             record_parse   -> parse stats + per-file lineage (etl.load_files)
load to tmp (synonym)        load_tmp       -> COPY into landing.tmp_<level>
load to prestg (sku/style)   load_prestg    -> aggregate tmp to key columns
src_stats                    stats('src')
load to staging (STM join)   load_staging   -> stage.stg_sales, product_id NULL if unmapped
stg_stats / rej_stats        stats('stg') / stats('rej')
(new) reconcile              reconcile      -> src = stg + rej, else fail before touching the fact
merge to facts               merge_fact     -> MERGE over the window
(file housekeeping)          archive        -> landing/ -> archive/
                             finish         -> audit SUCCESS

Redshift writes: only begin, record_parse and the steps after it touch
Redshift; extract_api and parse are pure S3/API work, so they can run in
parallel across pipelines while Redshift writers queue in one Airflow pool.

Idempotency: every write step first deletes what a previous attempt of the
same load (or the previous load of the same client/retailer) left behind,
inside the same transaction as its insert. Retrying any task is safe.
"""
import io
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from etl import config, parsers, redshift, s3_io
from etl.levels import LEVELS
from etl.weeks import resolve_window

log = logging.getLogger(__name__)


# A backlog bigger than this is loaded over several runs (oldest first), so
# one load never pulls an unbounded number of files into memory.
MAX_FILES_PER_LOAD = 20


class ReconciliationError(Exception):
    """src totals != stg + rej totals: something was dropped or duplicated."""


@dataclass
class LoadContext:
    pipeline_id: str
    client_id: str
    retailer_id: str
    product_level: str
    source_type: str
    file_format: str
    load_id: str
    load_type: str                 # automation | history
    window_start: date
    window_end: date
    raw_keys: list[str] = field(default_factory=list)   # source files (or the saved API payload) in this load
    parsed_key: str | None = None
    dag_run_id: str | None = None
    parse_stats: dict = field(default_factory=dict)
    file_stats: list[dict] = field(default_factory=list)   # per source file, written by record_parse
    fact_inserted: int | None = None
    fact_updated: int | None = None

    # Airflow passes the context between tasks as XCom (JSON).
    def to_dict(self) -> dict:
        d = asdict(self)
        d["window_start"], d["window_end"] = self.window_start.isoformat(), self.window_end.isoformat()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "LoadContext":
        d = dict(d)
        d["window_start"], d["window_end"] = date.fromisoformat(d["window_start"]), date.fromisoformat(d["window_end"])
        return cls(**d)

    @property
    def level(self):
        return LEVELS[self.product_level]

    @property
    def pair(self) -> dict:
        return {"client_id": self.client_id, "retailer_id": self.retailer_id, "load_id": self.load_id}


def _parse_week(v) -> date | None:
    if v in (None, "", "None"):
        return None
    return v if isinstance(v, date) else date.fromisoformat(str(v))


# ---------------------------------------------------------------------------
# begin / extract / parse
# ---------------------------------------------------------------------------

def _files_label(keys: list[str]) -> str | None:
    if not keys:
        return None
    label = keys[0] if len(keys) == 1 else f"{len(keys)} files: {keys[0]} .. {keys[-1].rsplit('/', 1)[-1]}"
    return label[:500]


def begin(pipeline: dict, run_date: date, *, raw_keys: list[str] | None = None,
          start_week=None, end_week=None, dag_run_id: str | None = None) -> LoadContext:
    """Fix the load's identity and week window, and open its audit row.

    Window precedence: explicit start/end (history load or backfill) >
    the weeks in the source file names > the run date. With several files
    the automation window runs from the oldest file's 5 weeks to the newest
    file's week, i.e. exactly what loading them one by one would touch."""
    start_week, end_week = _parse_week(start_week), _parse_week(end_week)
    load_type = "history" if (start_week or end_week) else "automation"
    raw_keys = list(raw_keys or [])
    file_weeks = [w for w in map(s3_io.file_week_end, raw_keys) if w]
    if file_weeks and not end_week:
        end_week = max(file_weeks)
    if file_weeks and not start_week and load_type == "automation":
        start_week = min(file_weeks) - timedelta(weeks=pipeline["lookback_weeks"] - 1)
    window_start, window_end = resolve_window(run_date, pipeline["lookback_weeks"], start_week, end_week)

    ctx = LoadContext(
        pipeline_id=pipeline["pipeline_id"], client_id=pipeline["client_id"], retailer_id=pipeline["retailer_id"],
        product_level=pipeline["product_level"], source_type=pipeline["source_type"],
        file_format=pipeline["file_format"],
        load_id=f"{pipeline['pipeline_id']}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}",
        load_type=load_type, window_start=window_start, window_end=window_end,
        raw_keys=raw_keys, dag_run_id=dag_run_id,
    )
    redshift.run([("""
        INSERT INTO etl.load_audit (load_id, pipeline_id, dag_run_id, load_type, source_file,
                                    window_start, window_end, status)
        VALUES (%(load_id)s, %(pipeline_id)s, %(dag_run_id)s, %(load_type)s, %(files)s,
                %(window_start)s, %(window_end)s, 'RUNNING');""", ctx.to_dict() | {"files": _files_label(raw_keys)})])
    log.info("%s %s load %s, weeks %s..%s", ctx.pipeline_id, load_type, ctx.load_id, window_start, window_end)
    return ctx


def extract_api(ctx: LoadContext) -> LoadContext:
    """Page through the retailer API for the window and land the combined
    payload in S3 as-received, so an API load is replayable like a file."""
    import requests   # only API pipelines need it

    records, cursor = [], 0
    while cursor is not None:
        resp = requests.get(
            f"{config.RETAILER_API_URL}/v1/sales",
            params={"client_id": ctx.client_id, "retailer_id": ctx.retailer_id,
                    "week_start": ctx.window_start.isoformat(), "week_end": ctx.window_end.isoformat(),
                    "cursor": cursor},
            headers={"Authorization": f"Bearer {config.RETAILER_API_TOKEN}"},
            timeout=30,
        )
        resp.raise_for_status()
        page = resp.json()
        records += page["data"]
        cursor = page["next_cursor"]
    name = f"{ctx.retailer_id}_{ctx.client_id}_{ctx.window_end:%Y%m%d}.json"
    key = config.s3_key("landing", ctx.client_id, ctx.retailer_id, name)
    s3_io.put_bytes(key, json.dumps(records).encode())
    ctx.raw_keys = [key]   # record_parse writes it to the audit row
    log.info("Fetched %d records from the retailer API -> s3://%s/%s", len(records), config.S3_BUCKET, key)
    return ctx


def parse(ctx: LoadContext) -> LoadContext:
    """Parse every file in the load into one output file in S3. No Redshift
    here -- record_parse writes the results."""
    files = [(k, s3_io.file_week_end(k), s3_io.get_bytes(k)) for k in ctx.raw_keys]
    result = parsers.parse_files(files, ctx.file_format, level=ctx.product_level, client_id=ctx.client_id,
                                 retailer_id=ctx.retailer_id, load_id=ctx.load_id,
                                 window=(ctx.window_start, ctx.window_end))
    if result.output.empty:
        raise ValueError(f"No rows for weeks {ctx.window_start}..{ctx.window_end} in {ctx.raw_keys} "
                         f"(stats: {result.stats}) -- wrong file, or the window needs start/end params")

    base = f"{ctx.client_id}/{ctx.retailer_id}/{ctx.load_id}"
    ctx.parsed_key = config.s3_key("parsed", base, f"output_{ctx.client_id}_{ctx.retailer_id}.csv")
    s3_io.put_bytes(ctx.parsed_key, result.output_csv())
    if not result.rejects.empty:
        s3_io.put_bytes(config.s3_key("rejects", base, "parse_rejects.csv"), result.rejects_csv())
    ctx.parse_stats = {k: v for k, v in result.stats.items() if k != "per_file"}
    ctx.file_stats = result.stats["per_file"]
    log.info("Parsed %d file(s): %s", len(files), ctx.parse_stats)
    for f in ctx.file_stats:
        log.info("  %s: %s", f["file"], {k: v for k, v in f.items() if k != "file"})
    return ctx


def record_parse(ctx: LoadContext) -> LoadContext:
    """Write what parse found: the audit row's files + stats, and one
    etl.load_files row per source file (lineage)."""
    redshift.run([
        ("UPDATE etl.load_audit SET source_file = %(files)s, parse_stats = %(stats)s WHERE load_id = %(load_id)s;",
         {"files": _files_label(ctx.raw_keys), "stats": json.dumps(ctx.parse_stats), "load_id": ctx.load_id}),
        ("DELETE FROM etl.load_files WHERE load_id = %(load_id)s;", ctx.pair),
        *[("""INSERT INTO etl.load_files (load_id, source_file, file_week_end, raw_rows, parsed_rows, rows_used,
                                          superseded_rows, parse_rejects, out_of_window)
              VALUES (%(load_id)s, %(file)s, %(week_end)s, %(raw_rows)s, %(parsed_rows)s, %(rows_used)s,
                      %(superseded_rows)s, %(parse_rejects)s, %(out_of_window)s);""", {"load_id": ctx.load_id, **f})
          for f in ctx.file_stats],
    ])
    return ctx


# ---------------------------------------------------------------------------
# Redshift steps
# ---------------------------------------------------------------------------

def load_tmp(ctx: LoadContext) -> int:
    """COPY output.csv into landing.tmp_<level>, replacing this pair's previous load."""
    lvl = ctx.level
    iam = "IAM_ROLE %(role)s" if config.REDSHIFT_IAM_ROLE_ARN else "IAM_ROLE default"
    redshift.run([
        (f"DELETE FROM {lvl.tmp_table} WHERE client_id = %(client_id)s AND retailer_id = %(retailer_id)s;", ctx.pair),
        (f"""COPY {lvl.tmp_table} ({', '.join(lvl.copy_columns)})
             FROM %(uri)s {iam}
             FORMAT AS CSV IGNOREHEADER 1 DATEFORMAT 'YYYY-MM-DD' REGION %(region)s;""",
         {"uri": f"s3://{config.S3_BUCKET}/{ctx.parsed_key}", "role": config.REDSHIFT_IAM_ROLE_ARN,
          "region": config.AWS_REGION}),
    ])
    # psycopg2 reports -1 for COPY, so count what landed.
    copied = redshift.fetch_all(f"SELECT COUNT(*) FROM {lvl.tmp_table} WHERE load_id = %(load_id)s;", ctx.pair)[0][0]
    log.info("COPY %s: %s rows", lvl.tmp_table, copied)
    return copied


def load_prestg(ctx: LoadContext) -> int:
    """Roll tmp rows (one per store) up to the feed's key columns."""
    lvl = ctx.level
    keys = ", ".join(lvl.key_columns)
    _, inserted = redshift.run([
        (f"DELETE FROM {lvl.prestg_table} WHERE client_id = %(client_id)s AND retailer_id = %(retailer_id)s;", ctx.pair),
        (f"""INSERT INTO {lvl.prestg_table}
                (load_id, week_date, client_id, retailer_id, {keys}, sales, inventory, source_rows)
             SELECT load_id, week_date, client_id, retailer_id, {keys},
                    SUM(sales), SUM(inventory), COUNT(*)
             FROM {lvl.tmp_table}
             WHERE load_id = %(load_id)s
             GROUP BY load_id, week_date, client_id, retailer_id, {keys};""", ctx.pair),
    ])
    log.info("%s: %s key rows", lvl.prestg_table, inserted)
    return inserted


STATS_FILTER = {
    "src": ("{source_table}", ""),
    "stg": ("stage.stg_sales", "AND product_id IS NOT NULL"),
    "rej": ("stage.stg_sales", "AND product_id IS NULL"),
}


def stats(ctx: LoadContext, source: str) -> int:
    """Control totals by week for one point in the flow (src | stg | rej)."""
    table, extra = STATS_FILTER[source]
    table = table.format(source_table=ctx.level.source_table)
    params = {**ctx.pair, "source": source}
    _, inserted = redshift.run([
        ("DELETE FROM etl.etl_stats WHERE load_id = %(load_id)s AND source = %(source)s;", params),
        (f"""INSERT INTO etl.etl_stats (load_id, source, week_date, client_id, retailer_id, row_count, sales, inventory)
             SELECT load_id, %(source)s, week_date, client_id, retailer_id, COUNT(*), SUM(sales), SUM(inventory)
             FROM {table}
             WHERE load_id = %(load_id)s {extra}
             GROUP BY load_id, week_date, client_id, retailer_id;""", params),
    ])
    return inserted


def load_staging(ctx: LoadContext) -> int:
    """Map source product keys to product_id through the STM (LEFT JOIN: an
    unmapped key keeps its row, with product_id NULL)."""
    lvl = ctx.level
    _, inserted = redshift.run([
        ("DELETE FROM stage.stg_sales WHERE client_id = %(client_id)s AND retailer_id = %(retailer_id)s;", ctx.pair),
        (f"""INSERT INTO stage.stg_sales
                (load_id, week_date, client_id, retailer_id, product_level, src_product_key, product_id, sales, inventory)
             SELECT s.load_id, s.week_date, s.client_id, s.retailer_id, %(level)s,
                    {lvl.src_key_sql}, m.product_id, s.sales, s.inventory
             FROM {lvl.source_table} s
             LEFT JOIN dim.product_stm m
                    ON m.client_id = s.client_id
                   AND m.product_level = %(level)s
                   AND {lvl.stm_match_sql}
             WHERE s.load_id = %(load_id)s;""", {**ctx.pair, "level": lvl.name}),
    ])
    log.info("stage.stg_sales: %s rows", inserted)
    return inserted


def reconcile(ctx: LoadContext) -> dict:
    """Gate before the fact table: for every week, src must equal stg + rej
    in rows, sales and inventory. A mismatch means the STM join fanned out
    (duplicate mapping) or dropped rows -- fail loudly, merge nothing."""
    rows = redshift.fetch_dicts("""
        SELECT week_date,
               SUM(CASE WHEN source = 'src' THEN row_count ELSE 0 END)        AS src_rows,
               SUM(CASE WHEN source IN ('stg','rej') THEN row_count ELSE 0 END) AS out_rows,
               SUM(CASE WHEN source = 'src' THEN sales ELSE 0 END)            AS src_sales,
               SUM(CASE WHEN source IN ('stg','rej') THEN sales ELSE 0 END)     AS out_sales,
               SUM(CASE WHEN source = 'src' THEN inventory ELSE 0 END)        AS src_inv,
               SUM(CASE WHEN source IN ('stg','rej') THEN inventory ELSE 0 END) AS out_inv,
               SUM(CASE WHEN source = 'rej' THEN row_count ELSE 0 END)        AS rej_rows
        FROM etl.etl_stats WHERE load_id = %(load_id)s
        GROUP BY week_date ORDER BY week_date;""", ctx.pair)
    if not rows:
        raise ReconciliationError(f"No stats recorded for {ctx.load_id}")
    bad = [r for r in rows if r["src_rows"] != r["out_rows"] or r["src_sales"] != r["out_sales"]
           or r["src_inv"] != r["out_inv"]]
    if bad:
        raise ReconciliationError(f"src != stg + rej for {ctx.load_id}: {bad}")

    rejected = sum(r["rej_rows"] for r in rows)
    if rejected:
        # Hand the unmapped keys to whoever maintains the STM.
        unmapped = redshift.fetch_dicts("""
            SELECT src_product_key, MIN(week_date) AS first_week, MAX(week_date) AS last_week,
                   COUNT(*) AS rows, SUM(sales) AS sales
            FROM stage.stg_sales WHERE load_id = %(load_id)s AND product_id IS NULL
            GROUP BY src_product_key ORDER BY sales DESC;""", ctx.pair)
        buf = io.StringIO()
        pd.DataFrame(unmapped).to_csv(buf, index=False)
        key = config.s3_key("rejects", ctx.client_id, ctx.retailer_id, ctx.load_id, "unmapped_products.csv")
        s3_io.put_bytes(key, buf.getvalue().encode())
        log.warning("%d rejected rows (%d unmapped keys) -> s3://%s/%s",
                    rejected, len(unmapped), config.S3_BUCKET, key)
    return {"weeks": len(rows), "rejected_rows": int(rejected)}


def merge_fact(ctx: LoadContext) -> LoadContext:
    """Upsert mapped staging rows into fact.fact_sales for the load window.

    The MERGE source is aggregated to the fact grain first: Redshift (like the
    SQL standard) rejects a MERGE where two source rows hit the same target
    row, and two retailer keys may legitimately map to one product_id."""
    params = {**ctx.pair, "start": ctx.window_start, "end": ctx.window_end}
    # One transaction (one session, so the temp table is visible throughout).
    # The SELECT's rowcount is how many source rows already exist in the fact.
    _, matched, _ = redshift.run([
        ("""CREATE TEMP TABLE merge_src AS
            SELECT week_date, client_id, retailer_id, product_id,
                   SUM(sales) AS sales, SUM(inventory) AS inventory, MAX(load_id) AS load_id
            FROM stage.stg_sales
            WHERE load_id = %(load_id)s AND product_id IS NOT NULL
              AND week_date BETWEEN %(start)s AND %(end)s
            GROUP BY week_date, client_id, retailer_id, product_id;""", params),
        ("""SELECT 1 FROM merge_src s JOIN fact.fact_sales f
              ON f.week_date = s.week_date AND f.client_id = s.client_id
             AND f.retailer_id = s.retailer_id AND f.product_id = s.product_id;""", None),
        ("""MERGE INTO fact.fact_sales
            USING merge_src s
               ON fact.fact_sales.week_date   = s.week_date
              AND fact.fact_sales.client_id   = s.client_id
              AND fact.fact_sales.retailer_id = s.retailer_id
              AND fact.fact_sales.product_id  = s.product_id
            WHEN MATCHED THEN UPDATE SET
                 sales = s.sales, inventory = s.inventory, load_id = s.load_id, updated_at = GETDATE()
            WHEN NOT MATCHED THEN INSERT
                 (week_date, client_id, retailer_id, product_id, sales, inventory, load_id, inserted_at, updated_at)
                 VALUES (s.week_date, s.client_id, s.retailer_id, s.product_id, s.sales, s.inventory, s.load_id,
                         GETDATE(), GETDATE());""", None),
    ])
    # Every merged row now carries this load_id; whatever wasn't matched was inserted.
    total = redshift.fetch_all("""SELECT COUNT(*) FROM fact.fact_sales WHERE load_id = %(load_id)s;""", ctx.pair)[0][0]
    ctx.fact_updated, ctx.fact_inserted = matched, total - matched
    redshift.run([("""UPDATE etl.load_audit SET fact_inserted = %(ins)s, fact_updated = %(upd)s
                      WHERE load_id = %(load_id)s;""",
                   {"ins": ctx.fact_inserted, "upd": ctx.fact_updated, "load_id": ctx.load_id})])
    log.info("fact.fact_sales MERGE: %s inserted, %s updated", ctx.fact_inserted, ctx.fact_updated)
    return ctx


# ---------------------------------------------------------------------------
# housekeeping
# ---------------------------------------------------------------------------

def archive(ctx: LoadContext) -> list[str]:
    """Move the load's source files out of landing/ so the next run doesn't see them."""
    moved = []
    for key in ctx.raw_keys:
        name = key.rsplit("/", 1)[-1]
        dest = config.s3_key("archive", ctx.client_id, ctx.retailer_id, f"{ctx.load_id}__{name}")
        s3_io.move(key, dest)
        moved.append(dest)
    return moved


def finish(ctx: LoadContext) -> None:
    redshift.run([("""UPDATE etl.load_audit SET status = 'SUCCESS', finished_at = GETDATE()
                      WHERE load_id = %(load_id)s;""", ctx.pair)])


def fail(load_id: str, error: str, attempts: int = 5) -> None:
    """Close the audit row as FAILED. Called from Airflow's failure callback,
    which can't run inside a pool, so it may collide with another pipeline's
    write; Redshift then aborts one side (serializable isolation, error 1023)
    and this simply tries again."""
    import time
    for attempt in range(1, attempts + 1):
        try:
            redshift.run([("""UPDATE etl.load_audit SET status = 'FAILED', finished_at = GETDATE(),
                                     error_message = LEFT(%(err)s, 2000)
                              WHERE load_id = %(load_id)s;""", {"load_id": load_id, "err": error})])
            return
        except Exception as e:   # psycopg2 surfaces 1023 as a generic error with this text
            if "1023" not in str(e) and "Serializable isolation" not in str(e) or attempt == attempts:
                raise
            time.sleep(2 * attempt)
