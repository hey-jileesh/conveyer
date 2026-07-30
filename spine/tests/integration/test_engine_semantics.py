"""A-15 — engine-semantics tables are executable (005.1 §6.2/§12.3/§12.4).

Every row of §6.2's cast-semantics table, asserted verbatim against the
REAL implementation (`frames.checks._typed_expr`, not a re-derived F.expr
call in this file) under the exact §6.2/§12.1 session pins
(`checks.SESSION_PINS`, already wired into `tests/conftest.py::_BASE_CONF`
— this suite's own first test PROVES that wiring is live, not merely
documented). A Spark version upgrade that shifts any of this table's
verdicts fails HERE, in CI, not seven stages into a production batch (§1's
own stated purpose for this suite).

Also covers: the `\\A(?:...)\\z` fullmatch anchoring discriminator ([DC-4] —
`\\Z` wrongly matches a trailing newline, `\\z` correctly rejects it) and
`core/canonical.py`'s naive-datetime rejection + the plan-side `date_format`
rendering (under `CANONICAL_TIMESTAMP_SPARK_PATTERN`, the pinned UTC session
zone) cross-checked against `canonical_json`'s own Python-side rendering of
the SAME instant, property-tested (§7.3/§12.4 [DC-3]) — the one thing that
makes §7.3's UDF-boundary timestamp hazard closed rather than merely
asserted.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from spine.core.canonical import (
    CANONICAL_TIMESTAMP_FORMAT,
    CANONICAL_TIMESTAMP_SPARK_PATTERN,
    canonical_json,
)
from spine.core.contract import parse_column_type
from spine.frames import checks

# --- session pins are actually live on the shared test session --------------


def test_session_pins_are_live_on_the_shared_test_session(spark: SparkSession) -> None:
    """`tests/conftest.py::_BASE_CONF` wires `checks.SESSION_PINS` into the
    ONE shared `spark` fixture every suite under `tests/` uses -- this is
    the load-bearing proof that wiring is live, not just documented (§12.1).
    """
    for key, value in checks.SESSION_PINS.items():
        assert spark.conf.get(key) == value, f"{key} not pinned on the shared test session"


# --- §6.2 cast-semantics table, verbatim, via the REAL `_typed_expr` --------


def _cast_one(spark: SparkSession, type_str: str, value: str | None) -> Any:
    """One row, one string column `v`, cast through `checks._typed_expr` —
    the SAME function `evaluate`'s check-3 predicate and `typed_projection`
    both use in production (D-5). Returns the collected Python value (or
    `None`)."""
    schema = StructType([StructField("v", StringType(), True)])
    df = spark.createDataFrame([(value,)], schema=schema)
    column_type = parse_column_type(type_str)
    out = df.select(checks._typed_expr("v", column_type).alias("out")).collect()[0]
    return out["out"]


def test_string_cast_is_identity_never_fails(spark: SparkSession) -> None:
    for value in ["anything", "", "  spaced  ", "123", "not-a-date"]:
        assert _cast_one(spark, "string", value) == value


@pytest.mark.parametrize(
    "value,expected",
    [
        (" 42 ", 42),  # trims whitespace
        ("42", 42),  # leading zeros ok
        ("007", 7),
        ("1.5", None),  # fractions REJECTED (try_cast, not plain cast's truncation)
        ("99999999999999999999", None),  # overflow -> NULL
        ("-99999999999999999999", None),  # negative overflow -> NULL
        ("garbage", None),
        ("", None),
    ],
)
def test_int_cast_semantics_table(spark: SparkSession, value: str, expected: int | None) -> None:
    assert _cast_one(spark, "int", value) == expected


def test_long_cast_semantics_matches_int(spark: SparkSession) -> None:
    assert _cast_one(spark, "long", " 42 ") == 42
    assert _cast_one(spark, "long", "1.5") is None


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1.239", Decimal("1.24")),  # excess scale rounds
        ("1.245", Decimal("1.25")),  # HALF_UP discriminator (NOT half-even -- probe-verified)
        ("0.125", Decimal("0.13")),  # a second half-up/half-even discriminator, unambiguous
        ("999.99", Decimal("999.99")),  # exactly at precision(5,2)'s boundary -- fits
        ("garbage", None),
        ("9999.99", None),  # precision(5) overflow: 6 significant digits doesn't fit
        # Scientific notation is ACCEPTED by decimal's try_cast -- not in
        # 005.1 §6.2's original table, probe-verified (conveyer-azr.28) and
        # pinned here so a Spark upgrade shifting this silently doesn't pass
        # CI. Asymmetric to int/long, which reject the identical tokens --
        # see test_int_long_cast_rejects_scientific_notation below.
        ("1.5e2", Decimal("150.00")),
        (" 1.5e2 ", Decimal("150.00")),  # whitespace-trimmed, same as any other numeric token
    ],
)
def test_decimal_cast_semantics_table(
    spark: SparkSession, value: str, expected: Decimal | None
) -> None:
    assert _cast_one(spark, "decimal(5,2)", value) == expected


@pytest.mark.parametrize("value", ["1.5e2", " 1.5e2 "])
def test_int_long_cast_rejects_scientific_notation(spark: SparkSession, value: str) -> None:
    """The exact tokens `test_decimal_cast_semantics_table` proves decimal's
    `try_cast` ACCEPTS -- `int`/`long` `try_cast` REJECTS them (probe-
    verified, conveyer-azr.28). Not asserted anywhere before this bead, so
    a Spark upgrade closing or widening either side of this asymmetry
    passed CI silently, defeating this suite's own stated purpose."""
    assert _cast_one(spark, "int", value) is None
    assert _cast_one(spark, "long", value) is None


@pytest.mark.parametrize(
    "value,expected",
    [
        ("true", True),
        ("false", False),
        ("t", True),
        ("f", False),
        ("y", True),
        ("n", False),
        ("yes", True),
        ("no", False),
        ("0", False),
        ("1", True),
        ("TRUE", True),  # case-insensitive
        ("Y", True),
        ("No", False),
        ("2", None),  # broader than true/false, but NOT unbounded
        ("yesno", None),
    ],
)
def test_bool_cast_semantics_table(spark: SparkSession, value: str, expected: bool | None) -> None:
    assert _cast_one(spark, "bool", value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2024-01-01", "2024-01-01"),
        ("2024-02-29", "2024-02-29"),  # valid leap day
        ("2024-02-30", None),  # invalid day -> NULL, never raises (CORRECTED)
        ("2024-2-3", None),  # unpadded vs yyyy-MM-dd -> NULL (strict, non-lenient)
        ("garbage", None),
    ],
)
def test_date_cast_semantics_table(spark: SparkSession, value: str, expected: str | None) -> None:
    result = _cast_one(spark, "date(yyyy-MM-dd)", value)
    assert (str(result) if result is not None else None) == expected


def test_timestamp_cast_naive_input_interprets_in_pinned_utc_session_zone(
    spark: SparkSession,
) -> None:
    """Rendered via `date_format` (session-tz-aware), NOT a raw `.collect()`
    of the `TimestampType` value: PySpark's driver-side `TimestampType` ->
    Python `datetime` conversion uses the OS-LOCAL zone regardless of
    `spark.sql.session.timeZone` (the exact hazard `core/canonical.py`'s
    own §7.3 UDF-boundary docstring names for a DIFFERENT boundary) — a
    raw `.collect()` comparison here would silently depend on the test
    runner's own OS timezone instead of proving the SESSION pin's effect."""
    schema = StructType([StructField("v", StringType(), True)])
    df = spark.createDataFrame([("2026-01-02 03:04:05",)], schema=schema)
    column_type = parse_column_type("timestamp(yyyy-MM-dd HH:mm:ss)")
    rendered = df.select(
        F.date_format(checks._typed_expr("v", column_type), CANONICAL_TIMESTAMP_SPARK_PATTERN)
    ).collect()[0][0]
    assert rendered == "2026-01-02T03:04:05.000000Z"  # naive local == UTC under the pin


def test_timestamp_cast_semantics_table(spark: SparkSession) -> None:
    assert _cast_one(spark, "timestamp(yyyy-MM-dd HH:mm:ss)", "2024-02-30 00:00:00") is None
    assert _cast_one(spark, "timestamp(yyyy-MM-dd HH:mm:ss)", "2024-1-1 00:00:00") is None


# --- rlike fullmatch anchoring: \A(?:...)\z discriminator ([DC-4]) ----------


@pytest.mark.parametrize(
    "value,z_matches,upper_z_matches",
    [
        ("foo", True, True),
        ("foo\n", False, True),  # the whole discriminator: \Z wrongly accepts a trailing "\n"
        ("foobar", False, False),
        ("xfoo", False, False),
    ],
)
def test_pattern_fullmatch_lowercase_z_rejects_trailing_newline(
    spark: SparkSession, value: str, z_matches: bool, upper_z_matches: bool
) -> None:
    df = spark.createDataFrame([(value,)], ["v"])
    row: Row = df.select(
        F.col("v").rlike(r"\A(?:foo)\z").alias("z"),
        F.col("v").rlike(r"\A(?:foo)\Z").alias("upper_z"),
    ).collect()[0]
    assert row["z"] == z_matches
    assert row["upper_z"] == upper_z_matches


# --- canonical_json: naive-datetime rejection [DC-3] -------------------------


def test_canonical_json_raises_on_naive_datetime() -> None:
    with pytest.raises(ValueError, match="naive datetime"):
        canonical_json(datetime(2026, 1, 2, 3, 4, 5))


def test_canonical_json_accepts_aware_datetime() -> None:
    rendered = canonical_json(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
    assert rendered == '"2026-01-02T03:04:05.000000Z"'


# --- plan-side timestamp rendering ≡ canonical_json's own rendering [DC-3] --


def test_plan_side_date_format_matches_canonical_json_pinned_example(
    spark: SparkSession,
) -> None:
    """One concrete, non-hypothesis pin (the property test below covers the
    general case) — `date_format` under `CANONICAL_TIMESTAMP_SPARK_PATTERN`
    and the pinned UTC session zone renders BYTE-IDENTICAL text to
    `canonical_json`'s own `.astimezone(UTC).strftime(CANONICAL_TIMESTAMP_
    FORMAT)`."""
    instant = datetime(2026, 3, 3, 10, 0, 0, 500000, tzinfo=timezone(timedelta(hours=5)))
    df = spark.createDataFrame([(instant,)], ["ts"])
    rendered = df.select(F.date_format("ts", CANONICAL_TIMESTAMP_SPARK_PATTERN)).collect()[0][0]

    assert rendered == instant.astimezone(UTC).strftime(CANONICAL_TIMESTAMP_FORMAT)
    assert canonical_json(instant) == f'"{rendered}"'


# A day off each end, same idiom `test_canonical.py`'s own `_timestamp_leaf()`
# uses to keep the domain valid (no `OverflowError`-triggering boundary
# values, conveyer-azr.24) — offsets bounded to ±14h (the real IANA range)
# so no generated instant can walk itself past `[datetime.min, datetime.max]`.
_BOUNDED_AWARE_DATETIMES = st.datetimes(
    min_value=datetime.min + timedelta(days=1),
    max_value=datetime.max - timedelta(days=1),
    timezones=st.sampled_from(
        [UTC] + [timezone(timedelta(hours=h)) for h in (-12, -8, -5, 0, 1, 5, 8, 12, 14)]
    ),
)


@given(instant=_BOUNDED_AWARE_DATETIMES)
@settings(max_examples=30, deadline=None)
def test_plan_side_timestamp_rendering_matches_canonical_json_over_generated_instants(
    spark: SparkSession, instant: datetime
) -> None:
    df = spark.createDataFrame([(instant,)], ["ts"])
    rendered = df.select(F.date_format("ts", CANONICAL_TIMESTAMP_SPARK_PATTERN)).collect()[0][0]

    assert canonical_json(instant) == f'"{rendered}"'
