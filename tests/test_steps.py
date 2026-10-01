"""Step logic without Redshift: S3 is moto, and redshift.run/fetch_* are
replaced by a recorder so the generated SQL can be inspected."""
from datetime import date

import boto3
import pytest
from moto import mock_aws

from etl import config, s3_io, steps

PIPELINE = {"pipeline_id": "C101_R202", "client_id": "C101", "retailer_id": "R202", "product_level": "sku",
            "source_type": "s3", "source_location": "landing/C101/R202/", "file_format": "xlsx", "lookback_weeks": 5}


@pytest.fixture
def sql(monkeypatch):
    """Record every statement instead of sending it to Redshift."""
    calls = []

    def fake_run(statements):
        calls.extend(statements)
        return [1] * len(statements)

    monkeypatch.setattr(steps.redshift, "run", fake_run)
    return calls


@pytest.fixture
def s3():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=config.S3_BUCKET)
        yield


def ctx_for(level="sku", **kw):
    p = PIPELINE | {"product_level": level} | kw
    return steps.LoadContext(
        pipeline_id=p["pipeline_id"], client_id=p["client_id"], retailer_id=p["retailer_id"],
        product_level=level, source_type=p["source_type"], file_format=p["file_format"], load_id="LOAD1",
        load_type="automation", window_start=date(2026, 8, 29), window_end=date(2026, 9, 26),
        raw_keys=["dev/landing/C101/R202/R202_C101_20260926.xlsx"], parsed_key="dev/parsed/x/output.csv")


def test_begin_takes_window_from_file_name(sql):
    ctx = steps.begin(PIPELINE, date(2026, 10, 30), raw_keys=["dev/landing/C101/R202/R202_C101_20260926.xlsx"])
    assert (ctx.window_start, ctx.window_end, ctx.load_type) == (date(2026, 8, 29), date(2026, 9, 26), "automation")
    assert "INSERT INTO etl.load_audit" in sql[0][0] and sql[0][1]["load_id"] == ctx.load_id


def test_begin_history_params_win(sql):
    ctx = steps.begin(PIPELINE, date(2026, 10, 1), raw_keys=["dev/landing/C101/R202/R202_C101_20260926.xlsx"],
                      start_week="2026-07-11", end_week="2026-09-26")
    assert (ctx.window_start, ctx.load_type) == (date(2026, 7, 11), "history")


def test_begin_with_a_backlog_spans_every_file(sql):
    """Two weekly drops waiting: the window covers the oldest file's 5 weeks
    through the newest file's week -- what loading them one by one touches."""
    keys = ["dev/landing/C101/R202/R202_C101_20261003.xlsx", "dev/landing/C101/R202/R202_C101_20261010.xlsx"]
    ctx = steps.begin(PIPELINE, date(2026, 10, 12), raw_keys=keys)
    assert (ctx.window_start, ctx.window_end) == (date(2026, 9, 5), date(2026, 10, 10))
    assert sql[0][1]["files"].startswith("2 files:")


def test_context_round_trips_through_xcom_json():
    ctx = ctx_for()
    assert steps.LoadContext.from_dict(ctx.to_dict()) == ctx


def test_staging_sql_maps_through_the_stm_for_each_level(sql):
    for level, match in [("serial", "m.src_product_id = s.product_id"),
                         ("sku", "m.color = s.color"),
                         ("style", "m.style = s.style")]:
        sql.clear()
        steps.load_staging(ctx_for(level))
        delete, insert = sql
        assert "DELETE FROM stage.stg_sales" in delete[0]
        assert "LEFT JOIN dim.product_stm" in insert[0] and match in insert[0]
        source = "landing.tmp_serial" if level == "serial" else f"landing.prestg_{level}"
        assert f"FROM {source} s" in insert[0]


def test_stats_filters(sql):
    steps.stats(ctx_for(), "rej")
    assert "FROM stage.stg_sales" in sql[1][0] and "product_id IS NULL" in sql[1][0]
    sql.clear()
    steps.stats(ctx_for(), "src")
    assert "FROM landing.prestg_sku" in sql[1][0]


def test_copy_uses_parsed_key_and_level_columns(sql):
    steps.load_tmp(ctx_for())
    copy_sql, params = sql[1]
    assert copy_sql.strip().startswith("COPY landing.tmp_sku (load_id, week_date, client_id, retailer_id, style, color, size")
    assert params["uri"] == f"s3://{config.S3_BUCKET}/dev/parsed/x/output.csv"


def test_reconcile_fails_on_mismatch(monkeypatch):
    monkeypatch.setattr(steps.redshift, "fetch_dicts", lambda *a, **k: [
        {"week_date": date(2026, 9, 26), "src_rows": 10, "out_rows": 11, "src_sales": 100, "out_sales": 110,
         "src_inv": 5, "out_inv": 5, "rej_rows": 0}])
    with pytest.raises(steps.ReconciliationError):
        steps.reconcile(ctx_for())


def test_pending_files_are_ordered_and_archive_moves_them_all(s3):
    for name in ["20260926", "20260912", "20260919_part2", "20260919_part1"]:
        s3_io.put_bytes(f"dev/landing/C101/R202/R202_C101_{name}.xlsx", b"x")
    s3_io.put_bytes("dev/landing/C101/R202/notes.txt", b"ignored")
    pending = s3_io.pending_files("dev/landing/C101/R202/")
    assert [k.rsplit("_", 1)[-1] if "part" in k else k[-13:-5] for k in pending] == \
        ["20260912", "part1.xlsx", "part2.xlsx", "20260926"]

    ctx = ctx_for()
    ctx.raw_keys = pending
    moved = steps.archive(ctx)
    assert moved[0] == "dev/archive/C101/R202/LOAD1__R202_C101_20260912.xlsx"
    assert s3_io.pending_files("dev/landing/C101/R202/") == []
