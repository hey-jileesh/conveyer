"""`frames.lineage` — stamp functions add exactly the declared columns. §7.5."""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import SparkSession
from spine.core.model import LineageStamp
from spine.frames import lineage

_RECEIVED_AT = datetime(2026, 1, 1, tzinfo=UTC)


def test_stamp_raw_lineage_omits_source_uri_when_absent(spark: SparkSession) -> None:
    stamp = LineageStamp(batch_id="b1", delivery_id="d1", feed_id="f1", received_at=_RECEIVED_AT)
    raw = spark.createDataFrame([(1,), (2,)], ["x"])

    stamped = lineage.stamp_raw_lineage(raw, stamp, "rsv-abc123")

    assert set(stamped.columns) == {
        "x",
        "batch_id",
        "delivery_id",
        "feed_id",
        "received_at",
        "read_spec_version",
    }
    rows = stamped.collect()
    assert all(r["batch_id"] == "b1" for r in rows)
    assert all(r["delivery_id"] == "d1" for r in rows)
    assert all(r["feed_id"] == "f1" for r in rows)
    assert all(r["read_spec_version"] == "rsv-abc123" for r in rows)


def test_stamp_fact_lineage_includes_source_uri_when_present(spark: SparkSession) -> None:
    stamp = LineageStamp(
        batch_id="b1",
        delivery_id="d1",
        feed_id="f1",
        received_at=_RECEIVED_AT,
        source_uri="s3://bucket/key.csv",
    )
    facts = spark.createDataFrame([(1,), (2,)], ["domain_id"])

    stamped = lineage.stamp_fact_lineage(facts, stamp)

    assert set(stamped.columns) == {
        "domain_id",
        "batch_id",
        "delivery_id",
        "feed_id",
        "received_at",
        "source_uri",
    }
    assert all(r["source_uri"] == "s3://bucket/key.csv" for r in stamped.collect())


def test_stamp_functions_add_only_the_declared_columns_no_more(spark: SparkSession) -> None:
    stamp = LineageStamp(batch_id="b1", delivery_id="d1", feed_id="f1", received_at=_RECEIVED_AT)
    df = spark.createDataFrame([(1, "a")], ["x", "y"])
    before = set(df.columns)

    raw_stamped = lineage.stamp_raw_lineage(df, stamp, "rsv-abc123")
    fact_stamped = lineage.stamp_fact_lineage(df, stamp)

    raw_added = {"batch_id", "delivery_id", "feed_id", "received_at", "read_spec_version"}
    fact_added = {"batch_id", "delivery_id", "feed_id", "received_at"}
    assert set(raw_stamped.columns) == before | raw_added
    assert set(fact_stamped.columns) == before | fact_added
    assert raw_stamped.count() == df.count()
    assert fact_stamped.count() == df.count()
