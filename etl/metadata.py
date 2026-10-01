"""Pipeline metadata for the Airflow DAG factory.

The source of truth is etl.pipeline_config in Redshift. The DAG factory,
though, must NOT query Redshift: Airflow re-parses DAG files every ~30s,
and each query would wake the Serverless workgroup and bill for it. So:

    etl.pipeline_config (Redshift)
        --[retail_metadata_sync DAG, daily + on demand]-->
    airflow/dags/pipeline_config/<env>.json  (local snapshot)
        --[DAG factory, every parse, free]--> one DAG per active pipeline

If no snapshot exists yet (fresh clone, CI), the factory falls back to the
seed file config/pipeline_config.csv filtered for the environment -- the
same rows `etl.seed` loads -- so DAGs still render.
"""
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from data_gen.catalog import load_pipelines
from etl import config

SNAPSHOT_DIR = Path(__file__).resolve().parent.parent / "airflow" / "dags" / "pipeline_config"
FIELDS = ["pipeline_id", "client_id", "retailer_id", "product_level", "source_type", "source_location",
          "file_format", "schedule", "lookback_weeks", "is_active"]


def snapshot_path(env: str = config.ENV) -> Path:
    return SNAPSHOT_DIR / f"{env}.json"


def _jsonable(v):
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return int(v)
    return v


def sync_snapshot() -> list[dict]:
    """Pull etl.pipeline_config into the local snapshot. Written atomically
    (temp file + rename) so the DAG processor never reads a half-written file."""
    from etl import redshift   # keeps the DAG factory's import path free of psycopg2
    rows = redshift.fetch_dicts(f"SELECT {', '.join(FIELDS)} FROM etl.pipeline_config ORDER BY pipeline_id;")
    rows = [{k: _jsonable(v) for k, v in r.items()} for r in rows]
    path = snapshot_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"env": config.ENV, "synced_at": datetime.utcnow().isoformat(), "pipelines": rows}, indent=2))
    tmp.replace(path)
    return rows


def active_pipelines(env: str = config.ENV) -> list[dict]:
    path = snapshot_path(env)
    if path.exists():
        rows = json.loads(path.read_text())["pipelines"]
    else:
        rows = [{k: p[k] for k in FIELDS} for p in load_pipelines() if env == "dev" or p["promoted_to_prod"]]
    return [r for r in rows if r["is_active"]]


def get_pipeline(pipeline_id: str, env: str = config.ENV) -> dict:
    for p in active_pipelines(env):
        if p["pipeline_id"] == pipeline_id:
            return p
    raise KeyError(f"No active pipeline {pipeline_id!r} in {env}")
