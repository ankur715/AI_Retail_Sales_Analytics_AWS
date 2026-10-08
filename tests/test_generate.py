from datetime import date, timedelta

from data_gen.catalog import CLIENTS, skus
from data_gen.generate import normalized_rows
from etl.weeks import week_range

END = date(2026, 9, 26)


def by_key(rows):
    return {(r["week_date"], r["product_id"]): r["sales"] for r in rows}


def test_generation_is_deterministic():
    weeks = week_range(date(2026, 8, 29), END)
    assert normalized_rows("C101", "R201", "serial", weeks, END) == normalized_rows("C101", "R201", "serial", weeks, END)


def test_restatements_only_touch_recent_weeks():
    """A file produced a week later re-sends the same weeks with drift in
    the last 4, while settled weeks stay identical."""
    weeks = week_range(date(2026, 8, 1), END)
    first = by_key(normalized_rows("C101", "R201", "serial", weeks, as_of=END))
    later = by_key(normalized_rows("C101", "R201", "serial", weeks, as_of=END + timedelta(weeks=1)))
    settled = [k for k in first if k[0] <= END - timedelta(weeks=4)]
    recent = [k for k in first if k[0] > END - timedelta(weeks=4)]
    assert all(first[k] == later[k] for k in settled)
    assert any(first[k] != later[k] for k in recent)


def test_catalog_shape():
    for client in CLIENTS:
        catalog = skus(client)
        assert len(catalog) == 20
        assert len({s.serial for s in catalog}) == 20 and len({s.product_id for s in catalog}) == 20
