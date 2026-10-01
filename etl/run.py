"""Run one pipeline end to end from the command line -- the same steps, in
the same order, as its Airflow DAG. Handy for development and debugging.

    python -m etl.run C101_R201                                     # automation: oldest pending file, 5 weeks
    python -m etl.run C101_R201 --start-week 2026-07-11 --end-week 2026-09-26   # history load
    python -m etl.run C102_R202                                     # API pipeline: last 5 completed weeks
"""
import argparse
import logging
from datetime import date

from etl import config, metadata, s3_io, steps


def run_pipeline(pipeline_id: str, run_date: date, start_week=None, end_week=None) -> steps.LoadContext | None:
    p = metadata.get_pipeline(pipeline_id)
    raw_key = None
    if p["source_type"] == "s3":
        pending = s3_io.pending_files(config.s3_key(p["source_location"]))
        if not pending:
            print(f"No files waiting in s3://{config.S3_BUCKET}/{config.s3_key(p['source_location'])}")
            return None
        raw_key = pending[0]

    ctx = steps.begin(p, run_date, raw_key=raw_key, start_week=start_week, end_week=end_week)
    try:
        if p["source_type"] == "api":
            steps.extract_api(ctx)
        steps.parse(ctx)
        steps.load_tmp(ctx)
        if ctx.level.prestg_table:
            steps.load_prestg(ctx)
        steps.stats(ctx, "src")
        steps.load_staging(ctx)
        steps.stats(ctx, "stg")
        steps.stats(ctx, "rej")
        steps.reconcile(ctx)
        steps.merge_fact(ctx)
        steps.archive(ctx)
        steps.finish(ctx)
    except Exception as e:
        steps.fail(ctx.load_id, repr(e))
        raise
    print(f"{ctx.load_id}: weeks {ctx.window_start}..{ctx.window_end}, parse {ctx.parse_stats}, "
          f"fact +{ctx.fact_inserted} inserted / {ctx.fact_updated} updated")
    return ctx


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pipeline_id")
    ap.add_argument("--run-date", type=date.fromisoformat, default=date.today())
    ap.add_argument("--start-week", type=date.fromisoformat)
    ap.add_argument("--end-week", type=date.fromisoformat)
    args = ap.parse_args()
    run_pipeline(args.pipeline_id, args.run_date, args.start_week, args.end_week)


if __name__ == "__main__":
    main()
