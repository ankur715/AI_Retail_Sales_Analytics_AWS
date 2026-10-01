from datetime import date

import pytest

from etl.weeks import is_week_end, last_completed_week, resolve_window, week_ending, week_range

SAT = date(2026, 9, 26)


def test_week_ending_is_the_closing_saturday():
    assert week_ending(date(2026, 9, 21)) == SAT   # Monday
    assert week_ending(SAT) == SAT
    assert week_ending(date(2026, 9, 27)) == date(2026, 10, 3)   # Sunday starts the next week


def test_last_completed_week_excludes_the_week_in_progress():
    assert last_completed_week(date(2026, 10, 1)) == SAT   # Thursday
    assert last_completed_week(date(2026, 9, 27)) == SAT   # Sunday after
    assert last_completed_week(SAT) == date(2026, 9, 19)   # Saturday itself isn't over yet


def test_automation_window_is_five_weeks():
    start, end = resolve_window(date(2026, 10, 1), 5)
    assert (start, end) == (date(2026, 8, 29), SAT)
    assert len(week_range(start, end)) == 5


def test_history_window_uses_explicit_bounds():
    assert resolve_window(date(2026, 10, 1), 5, date(2026, 7, 11), SAT) == (date(2026, 7, 11), SAT)


def test_end_week_only_still_gets_lookback():
    assert resolve_window(date(2026, 10, 1), 5, end_week=date(2026, 9, 19)) == (date(2026, 8, 22), date(2026, 9, 19))


@pytest.mark.parametrize("start,end", [(date(2026, 9, 25), SAT), (SAT, date(2026, 9, 1)), (date(2026, 10, 3), SAT)])
def test_bad_windows_raise(start, end):
    with pytest.raises(ValueError):
        week_range(start, end)


def test_is_week_end():
    assert is_week_end(SAT) and not is_week_end(date(2026, 9, 25))
