"""Expectation calendar and overdue decision — LLD §7.5 (D-11).

DST is handled by `zoneinfo` arithmetic: every wall-time deadline is
localized fresh for its own date via `ZoneInfo(exp.timezone)`, never derived
by adding a fixed UTC offset to another instant. Nonexistent/ambiguous wall
times resolve per zoneinfo defaults (`fold=0`) — accepted (LLD §7.5).
"""

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from ingestion.core.model import Expectation

_WEEKDAY_INDEX = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def expectation_dates(exp: Expectation, on_date: date) -> bool:
    """Does `on_date` (a wall date in `exp.timezone`) require a delivery?"""
    if exp.expected == "daily":
        return True
    if exp.expected == "weekdays":
        return on_date.weekday() < 5
    if exp.expected.startswith("weekly:"):
        dow = exp.expected.split(":", 1)[1]
        return _WEEKDAY_INDEX.get(dow) == on_date.weekday()
    if exp.expected.startswith("monthly:"):
        day_str = exp.expected.split(":", 1)[1]
        return day_str.isdigit() and on_date.day == int(day_str)
    return False


def expected_by(exp: Expectation, on_date: date) -> datetime:
    hour_str, minute_str = exp.by.split(":")
    tz = ZoneInfo(exp.timezone)
    local = datetime(
        on_date.year, on_date.month, on_date.day, int(hour_str), int(minute_str), tzinfo=tz
    )
    return local.astimezone(UTC)


def overdue_dates(
    exp: Expectation,
    registered_received: Sequence[datetime],
    now: datetime,
    lookback_days: int = 3,
) -> list[date]:
    """Expected dates in [today - lookback_days, today] (feed tz) whose
    deadline has passed with no registered delivery received on that date
    (feed tz). Received-on-the-expected-date is the Phase 1 qualification
    rule; per-feed grace windows are a Phase 2 additive field.
    """
    tz = ZoneInfo(exp.timezone)
    today = now.astimezone(tz).date()
    overdue: list[date] = []
    for offset in range(lookback_days, -1, -1):
        candidate = today - timedelta(days=offset)
        if not expectation_dates(exp, candidate):
            continue
        if now < expected_by(exp, candidate):
            continue
        day_start = datetime(candidate.year, candidate.month, candidate.day, tzinfo=tz)
        day_end = day_start + timedelta(days=1)
        received_that_day = any(day_start <= received < day_end for received in registered_received)
        if not received_that_day:
            overdue.append(candidate)
    return overdue
