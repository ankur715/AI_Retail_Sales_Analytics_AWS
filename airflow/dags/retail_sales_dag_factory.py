"""DAG factory: one DAG per active row of etl.pipeline_config.

Replaces one hand-built ibi Data Migrator flow per client/retailer with a
single template. Adding a feed = adding a metadata row; the product_level
column picks the subflow, source_type picks how data arrives.

    retail_sales__C101_R201__serial   (s3, daily 07:00)
      wait_for_file -> begin -> parse
        -> serial_subflow[ load_tmp -> src_stats -> load_staging -> stg_stats, rej_stats -> reconcile ]
        -> merge_fact -> archive -> finish

    retail_sales__C101_R202__sku      (s3, daily 07:00)
      ... sku_subflow[ load_tmp -> load_prestg -> src_stats -> ... ]

    retail_sales__C102_R202__sku      (api, Sundays 08:00)
      begin -> extract_api -> parse -> sku_subflow[...] -> merge_fact -> archive -> finish

S3 pipelines run daily and wait (in reschedule mode, so no worker slot is
held) for that week's file. If nothing lands before the sensor times out,
the run is skipped, not failed -- most days there is no file. If several
files are waiting (missed days, a drop split into _partN files), one run
loads all of them; the newest file wins each overlapping week.

Metadata is read from a local snapshot, never from Redshift at parse time;
see etl/metadata.py and the retail_metadata_sync DAG below.

Params (Trigger DAG w/ config): start_week / end_week (Saturdays) turn a
run into a history load or backfill over exactly those weeks. Empty =
automation: the newest file's (or last completed) week, minus 4 weeks.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, PokeReturnValue, TaskGroup, get_current_context, task

# Project root on sys.path, so the DAG uses the same etl package as the CLI.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from etl import config, metadata  # noqa: E402

REDSHIFT_POOL = "redshift"   # 1 slot: Redshift's serializable isolation aborts concurrent writers to shared tables


def _ctx(d: dict):
    from etl.steps import LoadContext
    return LoadContext.from_dict(d)


def _mark_failed(context) -> None:
    """on_failure_callback: close the load's audit row as FAILED."""
    load = context["ti"].xcom_pull(task_ids="begin")
    if load:
        from etl import steps
        steps.fail(load["load_id"], repr(context.get("exception")))


def build_dag(p: dict) -> DAG:
    level = p["product_level"]
    dag_id = f"retail_sales__{p['pipeline_id']}__{level}"

    with DAG(
        dag_id=dag_id,
        description=f"{p['client_id']} @ {p['retailer_id']}: {level}-level {p['source_type']} feed -> fact.fact_sales",
        schedule=p["schedule"],
        start_date=datetime(2026, 9, 1),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["retail", config.ENV, level, p["source_type"], p["client_id"], p["retailer_id"]],
        params={
            "start_week": Param(None, type=["null", "string"], format="date",
                                description="History/backfill: first week-ending Saturday (blank = automation)"),
            "end_week": Param(None, type=["null", "string"], format="date",
                              description="History/backfill: last week-ending Saturday"),
        },
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=2),
            "on_failure_callback": _mark_failed,
        },
        doc_md=__doc__,
    ) as dag:

        @task
        def begin(raw_keys: list[str] | None = None) -> dict:
            from etl import steps
            c = get_current_context()
            run = c["dag_run"]
            return steps.begin(p, run.run_after.date(), raw_keys=raw_keys,
                               start_week=c["params"].get("start_week"), end_week=c["params"].get("end_week"),
                               dag_run_id=run.run_id).to_dict()

        @task
        def parse(load: dict) -> dict:
            from etl import steps
            return steps.parse(_ctx(load)).to_dict()

        @task(pool=REDSHIFT_POOL)
        def load_tmp(load: dict) -> dict:
            from etl import steps
            steps.load_tmp(_ctx(load))
            return load

        @task(pool=REDSHIFT_POOL)
        def load_prestg(load: dict) -> dict:
            from etl import steps
            steps.load_prestg(_ctx(load))
            return load

        @task(pool=REDSHIFT_POOL)
        def src_stats(load: dict) -> dict:
            from etl import steps
            steps.stats(_ctx(load), "src")
            return load

        @task(pool=REDSHIFT_POOL)
        def load_staging(load: dict) -> dict:
            from etl import steps
            steps.load_staging(_ctx(load))
            return load

        @task(pool=REDSHIFT_POOL)
        def stg_stats(load: dict) -> dict:
            from etl import steps
            steps.stats(_ctx(load), "stg")
            return load

        @task(pool=REDSHIFT_POOL)
        def rej_stats(load: dict) -> dict:
            from etl import steps
            steps.stats(_ctx(load), "rej")
            return load

        @task(pool=REDSHIFT_POOL)
        def reconcile(load: dict, _stg: dict, _rej: dict) -> dict:
            from etl import steps
            steps.reconcile(_ctx(load))
            return load

        @task(pool=REDSHIFT_POOL)
        def merge_fact(load: dict) -> dict:
            from etl import steps
            return steps.merge_fact(_ctx(load)).to_dict()

        @task
        def archive(load: dict) -> dict:
            from etl import steps
            steps.archive(_ctx(load))
            return load

        @task(pool=REDSHIFT_POOL)
        def finish(load: dict) -> None:
            from etl import steps
            steps.finish(_ctx(load))

        # --- how data arrives ---
        if p["source_type"] == "s3":
            landing = config.s3_key(p["source_location"])

            @task.sensor(poke_interval=30 * 60, timeout=8 * 3600, mode="reschedule", soft_fail=True)
            def wait_for_file() -> PokeReturnValue:
                from etl import s3_io, steps
                pending = s3_io.pending_files(landing)[:steps.MAX_FILES_PER_LOAD]
                return PokeReturnValue(is_done=bool(pending), xcom_value=pending)

            load = parse(begin(wait_for_file()))
        else:
            @task
            def extract_api(load: dict) -> dict:
                from etl import steps
                return steps.extract_api(_ctx(load)).to_dict()

            load = parse(extract_api(begin()))

        # --- the product-level subflow (ibi: one flow per level) ---
        with TaskGroup(group_id=f"{level}_subflow"):
            landed = load_tmp(load)
            if level in ("sku", "style"):
                landed = load_prestg(landed)
            staged = load_staging(src_stats(landed))
            checked = reconcile(staged, stg_stats(staged), rej_stats(staged))

        finish(archive(merge_fact(checked)))

    return dag


# One DAG per active pipeline in this environment's metadata.
for _p in metadata.active_pipelines():
    _dag = build_dag(_p)
    globals()[_dag.dag_id] = _dag


with DAG(
    dag_id="retail_metadata_sync",
    description="Refresh the local snapshot of etl.pipeline_config that the DAG factory reads",
    schedule="0 6 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["retail", config.ENV, "metadata"],
    doc_md=metadata.__doc__,
):
    @task(pool=REDSHIFT_POOL)
    def sync_pipeline_config() -> list[str]:
        return [r["pipeline_id"] for r in metadata.sync_snapshot()]

    sync_pipeline_config()
