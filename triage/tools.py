"""The triage agent's tools: a fixed set of read-only Python functions bound
to ONE failed load. The model chooses which to call; it can't choose another
load, another pipeline, a table or a query.

    tool                    reads                                         Redshift queries
    get_load_audit          etl.load_audit row (error, window, files)     1 (cached)
    get_stats               src / stg / rej by week + the differences     1
    get_unmapped_products   S3 rejects/.../unmapped_products.csv,         0, or 1 if the file
                            else stage.stg_sales                          wasn't written
    get_parse_rejects       S3 rejects/.../parse_rejects.csv              0
    get_file_history        etl.load_files x load_audit + S3 landing      1
    get_pipeline_config     local metadata snapshot (or seed CSV)         0

A whole investigation is at most 4 small queries on one connection, so the
Serverless workgroup wakes once.
"""
import csv
import io
import json
from collections import Counter
from dataclasses import dataclass

from botocore.exceptions import ClientError

from data_gen.catalog import load_pipelines
from etl import config, metadata, s3_io
from triage.readonly import ReadOnlySession

MAX_RESULT_CHARS = 6000   # each tool result is truncated to this before it goes to the model

TOOL_SPECS = [
    {"name": "get_load_audit",
     "description": "The failed load's audit row: pipeline, status, error message, week window, source files, "
                    "parse statistics and fact counts. Start here.",
     "inputSchema": {"json": {"type": "object", "properties": {}}}},
    {"name": "get_stats",
     "description": "Control totals by week for the load: src (landing), stg (mapped) and rej (unmapped) rows, "
                    "sales and inventory, plus src - (stg + rej) per week. The reconcile gate fails when that "
                    "difference is not zero.",
     "inputSchema": {"json": {"type": "object", "properties": {}}}},
    {"name": "get_unmapped_products",
     "description": "Source product keys that had no row in the product mapping table (STM), with rows, sales "
                    "and weeks. These are the 'rej' rows.",
     "inputSchema": {"json": {"type": "object", "properties": {}}}},
    {"name": "get_parse_rejects",
     "description": "Rows the parser rejected (bad dates, non-Saturday weeks, unparseable numbers, wrong client "
                    "or retailer, summary/TOTAL rows): counts by reason and sample rows.",
     "inputSchema": {"json": {"type": "object", "properties": {
         "max_rows": {"type": "integer", "description": "Sample rows to return (1-20).", "minimum": 1, "maximum": 20}}}}},
    {"name": "get_file_history",
     "description": "Recent loads of this pipeline with each source file's contribution (rows used, superseded, "
                    "rejected, out of window), plus files still waiting in landing.",
     "inputSchema": {"json": {"type": "object", "properties": {
         "limit": {"type": "integer", "description": "Rows of history (1-20).", "minimum": 1, "maximum": 20}}}}},
    {"name": "get_pipeline_config",
     "description": "The pipeline's metadata: client, retailer, product level, source type and location, file "
                    "format, schedule and lookback weeks.",
     "inputSchema": {"json": {"type": "object", "properties": {}}}},
]
TOOL_NAMES = [t["name"] for t in TOOL_SPECS]


@dataclass
class TriageTarget:
    load_id: str
    pipeline_id: str


def _pipeline_row(pipeline_id: str) -> dict:
    """The pipeline's metadata row, active or not, from the local snapshot (no Redshift)."""
    path = metadata.snapshot_path()
    rows = json.loads(path.read_text())["pipelines"] if path.exists() else load_pipelines()
    for r in rows:
        if r["pipeline_id"] == pipeline_id:
            return {k: r[k] for k in metadata.FIELDS}
    raise KeyError(f"pipeline {pipeline_id!r} is not in the metadata")


def _read_s3_csv(key: str) -> list[dict] | None:
    try:
        body = s3_io.get_bytes(key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    return list(csv.DictReader(io.StringIO(body.decode("utf-8", errors="replace"))))


class Toolbox:
    """Runs the tools for one target. Never writes anything."""

    def __init__(self, target: TriageTarget, session: ReadOnlySession):
        self.target, self.session = target, session
        self.calls: list[str] = []

    @property
    def _pipeline(self) -> dict:
        return _pipeline_row(self.target.pipeline_id)

    def _rejects_key(self, name: str) -> str:
        p = self._pipeline
        return config.s3_key("rejects", p["client_id"], p["retailer_id"], self.target.load_id, name)

    # --- the tools -------------------------------------------------------------

    def get_load_audit(self) -> dict:
        rows = self.session.query("load_audit", load_id=self.target.load_id)
        if not rows:
            return {"error": f"no audit row for load {self.target.load_id}"}
        row = rows[0]
        if row.get("parse_stats"):
            try:
                row["parse_stats"] = json.loads(row["parse_stats"])
            except (TypeError, ValueError):
                pass
        return row

    def get_stats(self) -> dict:
        weeks = self.session.query("stats_by_week", load_id=self.target.load_id)
        out, mismatched, findings = [], [], []
        for w in weeks:
            diff = {m: round((w[f"src_{m}"] or 0) - (w[f"stg_{m}"] or 0) - (w[f"rej_{m}"] or 0), 2)
                    for m in ("rows", "sales", "inventory")}
            out.append({**w, "src_minus_stg_rej": diff})
            if any(diff.values()):
                mismatched.append(w["week_date"])
                # Spelled out, so a small model can't misread the raw numbers.
                parts = []
                for m, d in diff.items():
                    if d:
                        src, rest = w[f"src_{m}"] or 0, (w[f"stg_{m}"] or 0) + (w[f"rej_{m}"] or 0)
                        kind = "duplicated" if d < 0 else "lost"
                        parts.append(f"{m} src {src} vs stg + rej {round(rest, 2)} ({abs(d)} {kind})")
                findings.append(f"{w['week_date']}: " + "; ".join(parts))
        return {"findings": findings or ["every week reconciles: src = stg + rej"],
                "weeks_not_reconciling": mismatched,
                "meaning": "stg + rej LARGER than src = rows duplicated after landing (e.g. a source key "
                           "matching more than one STM row); SMALLER = rows lost" if mismatched else "",
                "weeks": out}

    def get_unmapped_products(self) -> dict:
        rows = _read_s3_csv(self._rejects_key("unmapped_products.csv"))
        if rows is not None:
            return {"source": "s3 unmapped_products.csv", "unmapped_keys": rows[:50]}
        rows = self.session.query("unmapped_keys", load_id=self.target.load_id)
        return {"source": "stage.stg_sales", "unmapped_keys": rows}

    def get_parse_rejects(self, max_rows: int = 10) -> dict:
        rows = _read_s3_csv(self._rejects_key("parse_rejects.csv"))
        if not rows:
            return {"parse_rejects": 0, "note": "the parser rejected no rows for this load"}
        reasons = Counter(r.get("reject_reason", "") for r in rows)
        return {"parse_rejects": len(rows), "by_reason": dict(reasons.most_common()),
                "sample": rows[:max(1, min(int(max_rows), 20))]}

    def get_file_history(self, limit: int = 10) -> dict:
        limit = max(1, min(int(limit), 20))
        history = self.session.query("file_history", pipeline_id=self.target.pipeline_id, limit=limit)
        p = self._pipeline
        pending = (s3_io.pending_files(config.s3_key(p["source_location"])) if p["source_type"] == "s3" else [])
        return {"recent_loads": history, "files_waiting_in_landing": pending}

    def get_pipeline_config(self) -> dict:
        return self._pipeline

    # --- dispatch --------------------------------------------------------------

    def run(self, name: str, tool_input: dict | None) -> tuple[str, bool]:
        """(JSON result text for the model, ok). Unknown tools and tool errors are
        reported back to the model as errors, never raised."""
        self.calls.append(name)
        if name not in TOOL_NAMES:
            return json.dumps({"error": f"unknown tool {name!r}; available: {TOOL_NAMES}"}), False
        try:
            result = getattr(self, name)(**(tool_input or {}))
        except TypeError as e:            # model sent arguments the tool doesn't take
            return json.dumps({"error": f"bad arguments for {name}: {e}"}), False
        except Exception as e:            # data/S3/Redshift problem: let the model see it and continue
            return json.dumps({"error": f"{type(e).__name__}: {e}"[:500]}), False
        text = json.dumps(result, default=str)
        if len(text) > MAX_RESULT_CHARS:
            text = text[:MAX_RESULT_CHARS] + ' ..."[truncated]"'
        return text, True
