"""`frames.quarantine` — the 005.1 §8.1 rewrite: `shape_pre_quarantine`/
`shape_post_quarantine`, `candidate_row_hash` (§8.2.4, bead conveyer-azr.19),
the §7.3 UDF seam, and [DC-3]'s in-plan timestamp rendering (bead
conveyer-azr.15, n1-quarantine). A-09: every `pre-check-snapshot.json`/
`post-check-snapshot.json` canonical-json vector reproduces byte-exact
THROUGH the shaper path (not just `canonical_json` directly, already covered
by `tests/unit/test_canonical.py`); both §7.1 snapshot structures; `row_hash`
stability across repeated runs; [DC-3]'s in-plan rendering exercised with a
real `TimestampType` column.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spine.core import canonical
from spine.core.model import LineageStamp
from spine.frames import quarantine

_STAMP = LineageStamp(
    batch_id="b1", delivery_id="d1", feed_id="f1", received_at=datetime(2026, 1, 1, tzinfo=UTC)
)
_QAT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

_FIXTURES_DIR = Path(__file__).resolve().parents[3] / "contracts" / "fixtures" / "canonical-json"

_QUARANTINE_COLUMNS = {
    "batch_id",
    "delivery_id",
    "feed_id",
    "check_stage",
    "source_uri",
    "object_seq",
    "row_index",
    "domain_id",
    "record_key",
    "row_hash",
    "reason_code",
    "reason_detail",
    "check_version",
    "quarantined_at",
    "row_snapshot",
}

# §6.4-shaped pre_check violation frame: durable raw columns (locators,
# lineage, declared string columns `domain_id`/`amount`/`status`, `extras`,
# `malformed_text`) plus `reason_code`/`reason_detail` -- matches
# `pre-check-snapshot.json`'s two vectors exactly (both are `{domain_id,
# amount, status, extras, malformed_text}` snapshots), built by hand per
# this bead's brief ("no dependency on n1-checks code").
_PRE_VIOL_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("delivery_id", StringType(), False),
        StructField("feed_id", StringType(), False),
        StructField("received_at", TimestampType(), False),
        StructField("source_uri", StringType(), False),
        StructField("object_seq", IntegerType(), False),
        StructField("row_index", LongType(), False),
        StructField("read_spec_version", StringType(), False),
        StructField("malformed_text", StringType(), True),
        StructField("domain_id", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("extras", MapType(StringType(), StringType()), False),
        StructField("reason_code", StringType(), False),
        StructField("reason_detail", StringType(), True),
    ]
)


def _pre_viol_row(vec_value: dict[str, Any], object_seq: int, row_index: int) -> tuple[Any, ...]:
    return (
        "b1",
        "d1",
        "f1",
        datetime(2026, 1, 1, tzinfo=UTC),
        "s3://bucket/obj1.csv",
        object_seq,
        row_index,
        "rsv-hash",
        vec_value["malformed_text"],
        vec_value["domain_id"],
        vec_value["amount"],
        vec_value["status"],
        vec_value["extras"],
        "unreadable/malformed-row" if vec_value["malformed_text"] else "contract/null-violation",
        '[{"code":"contract/null-violation","column":"domain_id","expected":"non-null"}]',
    )


def _pre_vectors() -> list[dict[str, Any]]:
    return json.loads((_FIXTURES_DIR / "pre-check-snapshot.json").read_text())


def _post_vectors() -> list[dict[str, Any]]:
    return json.loads((_FIXTURES_DIR / "post-check-snapshot.json").read_text())


# ============================================================================
# §8.1 rewrite: shape_pre_quarantine -- §7.1 pre snapshot, §7.3 UDF seam
# ============================================================================


def test_shape_pre_quarantine_produces_exactly_the_442_columns(spark: SparkSession) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, 1)], _PRE_VIOL_SCHEMA)

    shaped = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT)

    assert set(shaped.columns) == _QUARANTINE_COLUMNS


@pytest.mark.parametrize("vector_index", [0, 1], ids=["typed-row", "malformed-row"])
def test_shape_pre_quarantine_reproduces_the_committed_snapshot_vector_byte_exact(
    spark: SparkSession, vector_index: int
) -> None:
    # A-09: `pre-check-snapshot.json`'s two vectors reproduce byte-exact
    # THROUGH the shaper, not just via a direct `canonical_json` call.
    vec = _pre_vectors()[vector_index]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, vector_index + 1)], _PRE_VIOL_SCHEMA)

    shaped = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT)
    row = shaped.collect()[0]

    assert row["row_snapshot"] == vec["canonical"]
    assert row["row_hash"] == vec["sha256"]


def test_shape_pre_quarantine_domain_id_and_record_key_are_null(spark: SparkSession) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, 1)], _PRE_VIOL_SCHEMA)

    row = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]

    assert row["domain_id"] is None
    assert row["record_key"] is None


def test_shape_pre_quarantine_locators_pass_through_and_check_stage_is_literal(
    spark: SparkSession,
) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 3, 42)], _PRE_VIOL_SCHEMA)

    row = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]

    assert row["source_uri"] == "s3://bucket/obj1.csv"
    assert row["object_seq"] == 3
    assert row["row_index"] == 42
    assert row["check_stage"] == "pre_check"
    assert row["batch_id"] == "b1"
    assert row["delivery_id"] == "d1"
    assert row["feed_id"] == "f1"
    assert row["check_version"] == "cv-hash"


def test_shape_pre_quarantine_reason_code_and_detail_pass_through(spark: SparkSession) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, 1)], _PRE_VIOL_SCHEMA)

    row = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]

    assert row["reason_code"] == "contract/null-violation"
    assert row["reason_detail"] == (
        '[{"code":"contract/null-violation","column":"domain_id","expected":"non-null"}]'
    )


@pytest.mark.parametrize(
    "missing_column", ["extras", "malformed_text", "reason_code", "reason_detail"]
)
def test_shape_pre_quarantine_rejects_viol_df_missing_a_required_column(
    spark: SparkSession, missing_column: str
) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, 1)], _PRE_VIOL_SCHEMA).drop(
        missing_column
    )

    with pytest.raises(ValueError, match="missing §6.4-shaped column"):
        quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT)


def test_shape_pre_quarantine_row_hash_stable_across_repeated_runs(spark: SparkSession) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, 1)], _PRE_VIOL_SCHEMA)

    hash_1 = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]["row_hash"]
    hash_2 = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]["row_hash"]

    assert hash_1 == hash_2 == vec["sha256"]


def test_shape_pre_quarantine_declared_columns_derived_structurally(
    spark: SparkSession,
) -> None:
    # A declared column set different from the fixture vector's -- proves
    # the shaper derives "declared" by exclusion (FRAMEWORK_RAW_COLUMNS +
    # reason columns), not from any hardcoded name list.
    schema = StructType(
        [
            StructField("batch_id", StringType(), False),
            StructField("delivery_id", StringType(), False),
            StructField("feed_id", StringType(), False),
            StructField("received_at", TimestampType(), False),
            StructField("source_uri", StringType(), False),
            StructField("object_seq", IntegerType(), False),
            StructField("row_index", LongType(), False),
            StructField("read_spec_version", StringType(), False),
            StructField("malformed_text", StringType(), True),
            StructField("widget_code", StringType(), True),
            StructField("extras", MapType(StringType(), StringType()), False),
            StructField("reason_code", StringType(), False),
            StructField("reason_detail", StringType(), True),
        ]
    )
    row = (
        "b1",
        "d1",
        "f1",
        datetime(2026, 1, 1, tzinfo=UTC),
        "s3://bucket/o.csv",
        1,
        1,
        "rsv",
        None,
        "WZ-1",
        {},
        "contract/pattern-mismatch",
        "[]",
    )
    df = spark.createDataFrame([row], schema)

    shaped_row = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]

    expected_value = {"widget_code": "WZ-1", "extras": {}, "malformed_text": None}
    assert shaped_row["row_snapshot"] == canonical.canonical_json(expected_value)
    assert shaped_row["row_hash"] == canonical.row_hash(expected_value)


# ============================================================================
# §8.1 rewrite: shape_post_quarantine -- §7.1 post snapshot, [DC-3], A-14
# ============================================================================


@pytest.fixture
def _utc_session_tz(spark: SparkSession):
    """[DC-3]'s in-plan `F.date_format` rendering DOES honor
    `spark.sql.session.timeZone` (that is exactly why it closes the UDF-
    boundary hazard, module docstring) — the byte-exact vector/[DC-3] tests
    below need the LLD's mandated UTC pin (§6.2), which is not yet wired
    into the shared `tests/conftest.py` session build (a separate bead's
    FILES, the "session pins shared constant" item of the N1 milestone
    table) — probe-verified (bead conveyer-azr.15): the local default is
    `America/Vancouver`. A test-local override, restored afterwards so it
    never leaks into other tests sharing the session-scoped `spark` fixture.
    """
    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        yield
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)


_POST_CANDIDATE_SCHEMA = StructType(
    [
        StructField("domain_id", StringType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("event_time", TimestampType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("note", StringType(), True),
        StructField("reason", StringType(), False),
    ]
)


def _post_candidate_row(vec_value: dict[str, Any], reason: str) -> tuple[Any, ...]:
    return (
        vec_value["domain_id"],
        Decimal(vec_value["amount"]["$decimal"]),
        datetime.fromisoformat(vec_value["event_time"]["$timestamp"]),
        vec_value["quantity"],
        vec_value["is_active"],
        vec_value["note"],
        reason,
    )


def test_shape_post_quarantine_produces_exactly_the_442_columns(spark: SparkSession) -> None:
    vec = _post_vectors()[0]
    df = spark.createDataFrame(
        [_post_candidate_row(vec["value"], "business/negative-amount")], _POST_CANDIDATE_SCHEMA
    )

    shaped = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id")

    assert set(shaped.columns) == _QUARANTINE_COLUMNS


def test_shape_post_quarantine_reproduces_the_committed_snapshot_vector_byte_exact(
    spark: SparkSession, _utc_session_tz: None
) -> None:
    # A-09 + [DC-3]: the ONE post-check-snapshot.json vector carries a real
    # `event_time` timestamp field -- this is the in-plan rendering path
    # exercised with a timestamp column, through the actual shaper (not a
    # direct `canonical_json` call).
    vec = _post_vectors()[0]
    df = spark.createDataFrame(
        [_post_candidate_row(vec["value"], "business/negative-amount")], _POST_CANDIDATE_SCHEMA
    )

    shaped = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id")
    row = shaped.collect()[0]

    assert row["row_snapshot"] == vec["canonical"]
    assert row["row_hash"] == vec["sha256"]


def test_shape_post_quarantine_locators_null_record_key_null_reason_detail_null(
    spark: SparkSession,
) -> None:
    vec = _post_vectors()[0]
    df = spark.createDataFrame(
        [_post_candidate_row(vec["value"], "business/negative-amount")], _POST_CANDIDATE_SCHEMA
    )

    row = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id").collect()[0]

    assert row["source_uri"] is None
    assert row["object_seq"] is None
    assert row["row_index"] is None
    assert row["record_key"] is None
    assert row["reason_detail"] is None
    assert row["check_stage"] == "post_check"
    assert row["reason_code"] == "business/negative-amount"


def test_shape_post_quarantine_domain_id_from_domain_id_col_when_present(
    spark: SparkSession,
) -> None:
    vec = _post_vectors()[0]
    df = spark.createDataFrame(
        [_post_candidate_row(vec["value"], "business/negative-amount")], _POST_CANDIDATE_SCHEMA
    )

    row = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id").collect()[0]

    assert row["domain_id"] == "abc-123"


def test_shape_post_quarantine_domain_id_null_when_domain_id_col_absent(
    spark: SparkSession,
) -> None:
    schema = StructType([StructField("x", IntegerType()), StructField("reason", StringType())])
    df = spark.createDataFrame([(1, "business/foo-bar")], schema)

    row = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id").collect()[0]

    assert row["domain_id"] is None


def test_shape_post_quarantine_rejects_viol_df_with_no_reason_column(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame([(1,)], ["x"])

    with pytest.raises(ValueError, match="has no 'reason' column"):
        quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id")


@pytest.mark.parametrize(
    "reason_value",
    [
        "not-a-valid-reason",
        "Business/x",  # uppercase leading segment
        "business/-abc",  # dash cannot be the first char after the slash
        "business/",  # empty after slash
        "business/foo\n",  # trailing newline -- the \z discriminator [DC-4]-style
        "business/UPPER",
        "biz/foo",  # wrong namespace
        None,
    ],
    ids=[
        "no-namespace",
        "uppercase-namespace",
        "leading-dash",
        "empty-suffix",
        "trailing-newline",
        "uppercase-suffix",
        "wrong-namespace",
        "null",
    ],
)
def test_nonconforming_reasons_flags_grammar_violations(
    spark: SparkSession, reason_value: str | None
) -> None:
    """§8.2(1)/A-14's PURE half (critique F1, bead conveyer-azr.30) --
    `nonconforming_reasons` is a plain filter, no `.count()`/action/raise of
    its own; the caller (`stages/post_check.py`) materializes it and raises
    the A-14 named `ValueError`, covered by that stage's own test suite, not
    here. `shape_post_quarantine` itself no longer asserts this grammar at
    all -- see `test_shape_post_quarantine_passes_a_nonconforming_reason_
    through_unchanged` below."""
    schema = StructType([StructField("x", IntegerType()), StructField("reason", StringType())])
    df = spark.createDataFrame([(1, reason_value)], schema)

    assert quarantine.nonconforming_reasons(df).count() == 1


@pytest.mark.parametrize(
    "reason_value",
    ["business/a", "business/0", "business/ab--cd", "business/negative-amount"],
)
def test_nonconforming_reasons_empty_for_conforming_grammar(
    spark: SparkSession, reason_value: str
) -> None:
    schema = StructType([StructField("x", IntegerType()), StructField("reason", StringType())])
    df = spark.createDataFrame([(1, reason_value)], schema)

    assert quarantine.nonconforming_reasons(df).count() == 0


def test_shape_post_quarantine_passes_a_nonconforming_reason_through_unchanged(
    spark: SparkSession,
) -> None:
    """critique F1 (bead conveyer-azr.30): the A-14 grammar assertion moved
    OUT of this pure shaper and into `stages/post_check.py` (which now
    raises BEFORE ever calling this function on a nonconforming row) --
    `shape_post_quarantine` stays a plain DataFrame-in/DataFrame-out plan
    builder even over a value that would have failed the grammar, never
    executing an action or raising mid-composition."""
    schema = StructType([StructField("x", IntegerType()), StructField("reason", StringType())])
    df = spark.createDataFrame([(1, "not-a-valid-reason")], schema)

    row = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id").collect()[0]

    assert row["reason_code"] == "not-a-valid-reason"


@pytest.mark.parametrize(
    "reason_value",
    ["business/a", "business/0", "business/ab--cd", "business/negative-amount"],
)
def test_shape_post_quarantine_accepts_conforming_reason_grammar(
    spark: SparkSession, reason_value: str
) -> None:
    schema = StructType([StructField("x", IntegerType()), StructField("reason", StringType())])
    df = spark.createDataFrame([(1, reason_value)], schema)

    row = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id").collect()[0]

    assert row["reason_code"] == reason_value


def test_shape_post_quarantine_row_hash_stable_across_repeated_runs(
    spark: SparkSession, _utc_session_tz: None
) -> None:
    vec = _post_vectors()[0]
    df = spark.createDataFrame(
        [_post_candidate_row(vec["value"], "business/negative-amount")], _POST_CANDIDATE_SCHEMA
    )

    hash_1 = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id").collect()[
        0
    ]["row_hash"]
    hash_2 = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id").collect()[
        0
    ]["row_hash"]

    assert hash_1 == hash_2 == vec["sha256"]


def test_shape_post_quarantine_multiple_timestamp_columns_all_rendered(
    spark: SparkSession, _utc_session_tz: None
) -> None:
    # [DC-3]: a second `TimestampType` column beyond the fixture vector's
    # single `event_time` field -- proves `_render_timestamps_for_snapshot`
    # walks every timestamp field, not just the first.
    schema = StructType(
        [
            StructField("domain_id", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("reason", StringType(), False),
        ]
    )
    event_time = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    updated_at = datetime(2026, 6, 1, 0, 0, 0, 1, tzinfo=UTC)
    df = spark.createDataFrame([("d1", event_time, updated_at, "business/multi-ts")], schema)

    row = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id").collect()[0]

    expected_value = {"domain_id": "d1", "event_time": event_time, "updated_at": updated_at}
    assert row["row_snapshot"] == canonical.canonical_json(expected_value)
    assert row["row_hash"] == canonical.row_hash(expected_value)


def test_shape_post_quarantine_snapshot_excludes_reason(spark: SparkSession) -> None:
    vec = _post_vectors()[0]
    df = spark.createDataFrame(
        [_post_candidate_row(vec["value"], "business/negative-amount")], _POST_CANDIDATE_SCHEMA
    )

    row = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id").collect()[0]

    assert "reason" not in row["row_snapshot"]


def test_shape_post_quarantine_quarantined_at_lands_the_literal_instant(
    spark: SparkSession,
) -> None:
    # Spark-side equality (not a `.collect()`-then-compare-python-datetime
    # assertion): pyspark's own driver-side `TimestampType` marshaling on
    # `.collect()` goes through the SAME OS-local-zone conversion [DC-3]
    # documents for the UDF boundary, so comparing collected python
    # `datetime` values against a hand-built UTC literal is itself exactly
    # the hazard this bead is about -- staying inside Spark's own instant
    # comparison sidesteps it entirely.
    vec = _post_vectors()[0]
    df = spark.createDataFrame(
        [_post_candidate_row(vec["value"], "business/negative-amount")], _POST_CANDIDATE_SCHEMA
    )

    shaped = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "domain_id")

    assert shaped.filter(F.col("quarantined_at") == F.lit(_QAT)).count() == 1


# ============================================================================
# candidate_row_hash: post_check's §8.2.4 guard-skip rerun UDF cost
# ============================================================================


def test_candidate_row_hash_adds_row_hash_leaves_other_columns_unchanged(
    spark: SparkSession,
) -> None:
    candidate = spark.createDataFrame([("a", 1, "p1")], ["domain_id", "n", "payload"])

    hashed = quarantine.candidate_row_hash(candidate)

    assert set(hashed.columns) == {"domain_id", "n", "payload", "row_hash"}
    row = hashed.collect()[0]
    assert row["domain_id"] == "a"
    assert row["n"] == 1
    assert row["payload"] == "p1"


def test_candidate_row_hash_matches_canonical_row_hash_over_all_columns(
    spark: SparkSession,
) -> None:
    candidate = spark.createDataFrame([("a", 1, "p1")], ["domain_id", "n", "payload"])

    hashed = quarantine.candidate_row_hash(candidate)

    expected = canonical.row_hash({"domain_id": "a", "n": 1, "payload": "p1"})
    assert hashed.collect()[0]["row_hash"] == expected


def test_candidate_row_hash_is_stable_across_repeated_calls(spark: SparkSession) -> None:
    candidate = spark.createDataFrame([("a", 1)], ["domain_id", "n"])

    hash_1 = quarantine.candidate_row_hash(candidate).collect()[0]["row_hash"]
    hash_2 = quarantine.candidate_row_hash(candidate).collect()[0]["row_hash"]

    assert hash_1 == hash_2


def test_candidate_row_hash_renders_timestamp_columns_in_plan_before_the_udf(
    spark: SparkSession,
) -> None:
    """[DC-3]: a `TimestampType` candidate column (a real fact column shape,
    unlike pre_check's always-`string` declared columns) must be rendered
    via `F.date_format` before the UDF boundary, honoring the session's UTC
    pin -- the same mechanism `shape_post_quarantine` already relies on."""
    schema = StructType(
        [StructField("domain_id", StringType(), True), StructField("event_time", TimestampType())]
    )
    candidate = spark.createDataFrame([("a", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))], schema)

    hashed = quarantine.candidate_row_hash(candidate)

    expected = canonical.row_hash({"domain_id": "a", "event_time": "2026-01-02T03:04:05.000000Z"})
    assert hashed.collect()[0]["row_hash"] == expected


def test_candidate_row_hash_distinguishes_different_rows(spark: SparkSession) -> None:
    candidate = spark.createDataFrame([("a", 1), ("b", 2)], ["domain_id", "n"])

    hashed = quarantine.candidate_row_hash(candidate)

    hashes = {r["row_hash"] for r in hashed.collect()}
    assert len(hashes) == 2
