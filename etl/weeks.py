"""Retail weeks end on Saturday. Every week_date in this project is the
week-ending Saturday, and every load window is a run of whole weeks.
"""
from datetime import date, timedelta

SATURDAY = 5  # date.weekday(): Monday=0 ... Saturday=5


def is_week_end(d: date) -> bool:
    return d.weekday() == SATURDAY


def week_ending(d: date) -> date:
    """The Saturday that closes the week containing d (d itself if a Saturday)."""
    return d + timedelta(days=(SATURDAY - d.weekday()) % 7)


def last_completed_week(d: date) -> date:
    """Most recent Saturday strictly before d -- the newest week a retailer can
    have reported on day d. On a Saturday the week is still in progress, so
    that day's own week doesn't count yet."""
    return week_ending(d) - timedelta(days=7)


def week_range(start: date, end: date) -> list[date]:
    """Every week-ending Saturday from start to end, inclusive."""
    if not (is_week_end(start) and is_week_end(end)):
        raise ValueError(f"Window bounds must be Saturdays, got {start} .. {end}")
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    return [start + timedelta(weeks=i) for i in range((end - start).days // 7 + 1)]


def resolve_window(run_date: date, lookback_weeks: int,
                   start_week: date | None = None, end_week: date | None = None) -> tuple[date, date]:
    """The weeks one pipeline run is responsible for.

    Automation (no overrides): the last `lookback_weeks` completed weeks --
    retailers restate recent weeks (returns, late POS uploads), so every
    weekly file re-sends them and the fact table is upserted over the window.

    History / backfill (start_week and/or end_week given, e.g. via DAG params):
    exactly the requested weeks, used for the initial dev load or a one-off
    reload before promoting a pipeline to prod.
    """
    end = end_week or last_completed_week(run_date)
    start = start_week or end - timedelta(weeks=lookback_weeks - 1)
    week_range(start, end)  # validates both are Saturdays and in order
    return start, end
