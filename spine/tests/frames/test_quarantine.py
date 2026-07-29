"""`frames.quarantine.shape_quarantine` — candidate cols + reason + guard keys. I-12, §7.5."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from spine.core.model import LineageStamp
from spine.frames import quarantine

_STAMP = LineageStamp(
    batch_id="b1", delivery_id="d1", feed_id="f1", received_at=datetime(2026, 1, 1, tzinfo=UTC)
)


def test_shape_quarantine_adds_literal_reason_when_absent(spark: SparkSession) -> None:
    violations = spark.createDataFrame([(1, None), (2, None)], "id int, req string")

    shaped = quarantine.shape_quarantine(
        violations, _STAMP, "pre_check", reason="null in required column"
    )

    assert set(shaped.columns) == {"id", "req", "reason", "batch_id", "check_stage"}
    rows = shaped.collect()
    assert all(r["reason"] == "null in required column" for r in rows)
    assert all(r["batch_id"] == "b1" for r in rows)
    assert all(r["check_stage"] == "pre_check" for r in rows)


def test_shape_quarantine_uses_existing_reason_column_when_present(
    spark: SparkSession,
) -> None:
    violations = spark.createDataFrame([(1, "bad domain_id")], ["domain_id", "reason"])

    shaped = quarantine.shape_quarantine(violations, _STAMP, "post_check")

    assert set(shaped.columns) == {"domain_id", "reason", "batch_id", "check_stage"}
    row = shaped.collect()[0]
    assert row["reason"] == "bad domain_id"
    assert row["check_stage"] == "post_check"


def test_shape_quarantine_rejects_literal_reason_when_column_already_present(
    spark: SparkSession,
) -> None:
    violations = spark.createDataFrame([(1, "bad domain_id")], ["domain_id", "reason"])

    with pytest.raises(ValueError, match="already carries a 'reason' column"):
        quarantine.shape_quarantine(violations, _STAMP, "post_check", reason="oops")


def test_shape_quarantine_rejects_missing_reason_with_no_literal_supplied(
    spark: SparkSession,
) -> None:
    violations = spark.createDataFrame([(1,)], ["id"])

    with pytest.raises(ValueError, match="has no 'reason' column"):
        quarantine.shape_quarantine(violations, _STAMP, "pre_check")


def test_shape_quarantine_nothing_beyond_candidate_cols_reason_and_guard_keys(
    spark: SparkSession,
) -> None:
    violations = spark.createDataFrame([(1, "x", "bad")], ["id", "extra", "reason"])

    shaped = quarantine.shape_quarantine(violations, _STAMP, "post_check")

    assert set(shaped.columns) == {"id", "extra", "reason", "batch_id", "check_stage"}
