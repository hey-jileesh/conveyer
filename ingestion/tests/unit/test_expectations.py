"""Unit tests for `ingestion.core.expectations` — LLD §7.5 (D-11).

`expectation_dates` and `expected_by` are example-tested across the closed
D-11 grammar, including DST-pinned dates for `America/New_York` (2026 spring
forward: 2026-03-08; fall back: 2026-11-01) per §12.3. `overdue_dates` is
example-tested for lookback range, per-feed-tz-day qualification, and the
deadline-not-yet-passed exclusion.
"""

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from ingestion.core import expectations, model

_NY = "America/New_York"


def _expectation(expected: str, by: str = "09:00", timezone: str = "UTC") -> model.Expectation:
    return model.Expectation(expected=expected, by=by, timezone=timezone)


# --- expectation_dates ---------------------------------------------------------


def test_daily_requires_every_date() -> None:
    exp = _expectation("daily")
    assert expectations.expectation_dates(exp, date(2026, 7, 24)) is True
    assert expectations.expectation_dates(exp, date(2026, 7, 25)) is True


def test_weekdays_excludes_saturday_and_sunday() -> None:
    exp = _expectation("weekdays")
    assert expectations.expectation_dates(exp, date(2026, 7, 24)) is True  # Friday
    assert expectations.expectation_dates(exp, date(2026, 7, 25)) is False  # Saturday
    assert expectations.expectation_dates(exp, date(2026, 7, 26)) is False  # Sunday
    assert expectations.expectation_dates(exp, date(2026, 7, 27)) is True  # Monday


def test_weekly_matches_only_its_day_of_week() -> None:
    exp = _expectation("weekly:mon")
    assert expectations.expectation_dates(exp, date(2026, 7, 20)) is True  # Monday
    assert expectations.expectation_dates(exp, date(2026, 7, 21)) is False  # Tuesday
    assert expectations.expectation_dates(exp, date(2026, 7, 27)) is True  # next Monday


def test_weekly_sunday_maps_correctly() -> None:
    exp = _expectation("weekly:sun")
    assert expectations.expectation_dates(exp, date(2026, 7, 26)) is True  # Sunday
    assert expectations.expectation_dates(exp, date(2026, 7, 27)) is False  # Monday


def test_monthly_matches_only_its_day_of_month() -> None:
    exp = _expectation("monthly:15")
    assert expectations.expectation_dates(exp, date(2026, 7, 15)) is True
    assert expectations.expectation_dates(exp, date(2026, 7, 16)) is False
    assert expectations.expectation_dates(exp, date(2026, 8, 15)) is True


def test_unrecognized_grammar_never_requires_a_delivery() -> None:
    """`expectation_dates`'s final `return False` is a defensive fallback: as
    of conveyer-4ot.25, `Expectation.expected`'s `@field_validator` rejects
    any string outside the closed D-11 grammar, so this branch is
    unreachable through validated construction (`model.Expectation(...)`
    now raises `ValidationError` for "bogus" before this function ever
    runs). Build the invalid-grammar `Expectation` via `model_construct`
    (bypasses validation) to keep exercising and covering the fallback
    branch directly."""
    exp = model.Expectation.model_construct(expected="bogus", by="09:00", timezone="UTC")
    assert expectations.expectation_dates(exp, date(2026, 7, 24)) is False


# --- expected_by ------------------------------------------------------------


def test_expected_by_combines_date_time_and_converts_to_utc() -> None:
    exp = _expectation("daily", by="13:30", timezone="UTC")
    assert expectations.expected_by(exp, date(2026, 7, 24)) == datetime(
        2026, 7, 24, 13, 30, tzinfo=UTC
    )


def test_expected_by_localizes_wall_time_in_feed_timezone() -> None:
    exp = _expectation("daily", by="09:00", timezone=_NY)
    # 9am EDT (UTC-4, non-DST-transition date in July) == 13:00 UTC.
    assert expectations.expected_by(exp, date(2026, 7, 24)) == datetime(
        2026, 7, 24, 13, 0, tzinfo=UTC
    )


def test_expected_by_spring_forward_dst_pinned() -> None:
    # America/New_York springs forward 2026-03-08 (2am -> 3am): EST (UTC-5)
    # before, EDT (UTC-4) after. Never derived by a fixed offset (LLD §7.5).
    exp = _expectation("daily", by="09:00", timezone=_NY)
    before = expectations.expected_by(exp, date(2026, 3, 7))
    after = expectations.expected_by(exp, date(2026, 3, 9))
    assert before == datetime(2026, 3, 7, 14, 0, tzinfo=UTC)  # EST: UTC-5
    assert after == datetime(2026, 3, 9, 13, 0, tzinfo=UTC)  # EDT: UTC-4


def test_expected_by_fall_back_dst_pinned() -> None:
    # America/New_York falls back 2026-11-01 (2am -> 1am): EDT (UTC-4)
    # before, EST (UTC-5) after.
    exp = _expectation("daily", by="09:00", timezone=_NY)
    before = expectations.expected_by(exp, date(2026, 10, 31))
    after = expectations.expected_by(exp, date(2026, 11, 2))
    assert before == datetime(2026, 10, 31, 13, 0, tzinfo=UTC)  # EDT: UTC-4
    assert after == datetime(2026, 11, 2, 14, 0, tzinfo=UTC)  # EST: UTC-5


# --- overdue_dates -----------------------------------------------------------


def test_overdue_dates_all_lookback_days_when_nothing_registered() -> None:
    exp = _expectation("daily", by="09:00", timezone=_NY)
    now = datetime(2026, 7, 24, 15, 0, tzinfo=UTC)  # after 9am EDT deadline (13:00 UTC)
    result = expectations.overdue_dates(exp, [], now, lookback_days=3)
    assert result == [date(2026, 7, 21), date(2026, 7, 22), date(2026, 7, 23), date(2026, 7, 24)]


def test_overdue_dates_excludes_a_date_with_a_qualifying_registered_delivery() -> None:
    exp = _expectation("daily", by="09:00", timezone=_NY)
    now = datetime(2026, 7, 24, 15, 0, tzinfo=UTC)
    received_on_23 = datetime(2026, 7, 23, 12, 0, tzinfo=ZoneInfo(_NY))
    result = expectations.overdue_dates(exp, [received_on_23], now, lookback_days=3)
    assert result == [date(2026, 7, 21), date(2026, 7, 22), date(2026, 7, 24)]


def test_overdue_dates_excludes_today_when_deadline_not_yet_passed() -> None:
    exp = _expectation("daily", by="09:00", timezone=_NY)
    now = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)  # 6am EDT, before the 9am EDT deadline
    result = expectations.overdue_dates(exp, [], now, lookback_days=3)
    assert date(2026, 7, 24) not in result
    assert result == [date(2026, 7, 21), date(2026, 7, 22), date(2026, 7, 23)]


def test_overdue_dates_only_lists_expected_weekdays() -> None:
    exp = _expectation("weekdays", by="09:00", timezone=_NY)
    now = datetime(2026, 7, 27, 15, 0, tzinfo=UTC)  # Monday
    result = expectations.overdue_dates(exp, [], now, lookback_days=3)
    # lookback covers Fri(7/24) Sat(7/25) Sun(7/26) Mon(7/27); weekends excluded.
    assert result == [date(2026, 7, 24), date(2026, 7, 27)]


def test_overdue_dates_qualification_is_per_feed_timezone_day_not_utc_day() -> None:
    exp = _expectation("daily", by="23:00", timezone=_NY)
    # 23:30 NY-local on 7/24 is 03:30 UTC on 7/25 — a UTC-day-naive check
    # would miss it; the feed-tz-day check must not.
    late_local_delivery = datetime(2026, 7, 24, 23, 30, tzinfo=ZoneInfo(_NY))
    now = datetime(2026, 7, 25, 4, 0, tzinfo=UTC)  # after the 23:00 EDT deadline for 7/24
    result = expectations.overdue_dates(exp, [late_local_delivery], now, lookback_days=0)
    assert result == []
