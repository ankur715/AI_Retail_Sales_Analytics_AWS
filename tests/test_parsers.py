import json
from datetime import date

import pytest

from data_gen.catalog import UNMAPPED_UPC, load_pipelines
from data_gen.generate import api_records, normalized_rows, render
from etl.parsers import OUTPUT_COLUMNS, parse
from etl.weeks import week_range

END = date(2026, 9, 26)
HISTORY = week_range(date(2026, 7, 11), END)   # 12 weeks
WINDOW = (date(2026, 8, 29), END)              # last 5
PIPELINES = {p["pipeline_id"]: p for p in load_pipelines()}


def raw_file(pipeline_id, weeks=HISTORY):
    p = PIPELINES[pipeline_id]
    rows = normalized_rows(p["client_id"], p["retailer_id"], p["product_level"], weeks, as_of=weeks[-1])
    return p, render(p, rows)


def run(pipeline_id, body=None, window=WINDOW, **overrides):
    p, generated = raw_file(pipeline_id)
    kw = dict(level=p["product_level"], client_id=p["client_id"], retailer_id=p["retailer_id"],
              load_id="L1", window=window) | overrides
    return parse(body if body is not None else generated, p["file_format"], **kw)


@pytest.mark.parametrize("pipeline_id", ["C101_R201", "C101_R202", "C101_R203"])
def test_every_layout_normalizes_to_standard_columns(pipeline_id):
    res = run(pipeline_id)
    level = PIPELINES[pipeline_id]["product_level"]
    assert list(res.output.columns) == OUTPUT_COLUMNS[level]
    assert set(res.output["week_date"]) == set(week_range(*WINDOW))
    assert (res.output["load_id"] == "L1").all()


def test_serial_upcs_keep_leading_zeros_and_money_is_parsed():
    res = run("C101_R201")
    assert res.output["product_id"].str.len().eq(12).all()
    assert res.output["product_id"].str.startswith("0").all()
    assert UNMAPPED_UPC["C101"] in set(res.output["product_id"])
    assert res.output["sales"].dtype.kind == "f" and res.output["sales"].ge(0).all()


def test_rows_outside_window_are_dropped_and_counted():
    res = run("C101_R201")
    per_week = len(res.output) // 5
    assert res.out_of_window == per_week * 7   # 12 weeks in the file, 5 in the window


def test_harbor_total_row_is_rejected_with_reason():
    res = run("C101_R203")
    assert res.rejects["reject_reason"].tolist() == ["summary/total row"]


def test_xlsx_title_row_is_skipped():
    res = run("C101_R202")
    assert res.stats["parse_rejects"] == 0 and len(res.output) > 0


def test_misrouted_file_rejects_every_row():
    _, body = raw_file("C102_R201")   # C102's file dropped in C101's folder
    res = run("C101_R201", body=body)
    assert res.output.empty
    assert set(res.rejects["reject_reason"]) == {"client is not C101"}


def test_non_saturday_and_bad_numbers_are_rejected():
    body = (b"Week_Date,Client,Retailer,Product_ID,Sales,Inventory\n"
            b"09/25/2026,C101,R201,010100000001,$10.00,5\n"
            b"09/26/2026,C101,R201,010100000001,abc,5\n"
            b"09/26/2026,C101,R201,010100000002,\"$1,234.50\",7\n")
    res = run("C101_R201", body=body)
    assert res.rejects["reject_reason"].tolist() == ["week_date is not a Saturday", "unparseable sales"]
    assert res.output["sales"].tolist() == [1234.50]


def test_missing_column_fails_the_parse():
    with pytest.raises(ValueError, match="missing columns"):
        run("C101_R201", body=b"Week_Date,Client,Retailer,Sales\n09/26/2026,C101,R201,1\n")


def test_api_json_layout():
    rows = normalized_rows("C102", "R202", "sku", week_range(*WINDOW), as_of=END)
    body = json.dumps(api_records(rows)).encode()
    res = parse(body, "json", level="sku", client_id="C102", retailer_id="R202", load_id="L2", window=WINDOW)
    assert len(res.output) == len(rows) and res.rejects.empty


def test_rows_after_window_fail_instead_of_being_dropped():
    from etl.parsers import WindowError
    with pytest.raises(WindowError, match="after the window end"):
        run("C101_R201", window=(date(2026, 7, 11), date(2026, 9, 19)))
