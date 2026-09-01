"""Unit tests for `spine.core.record` — LLD 007.1 §5.1 fragment 4, §6.1,
§8.1 (F-1/F-6's code home).
"""

from __future__ import annotations

from spine.core import record


def test_fact_stamp_columns_is_exactly_the_seven_framework_stamps() -> None:
    assert record.FACT_STAMP_COLUMNS == frozenset(
        {
            "batch_id",
            "delivery_id",
            "feed_id",
            "received_at",
            "source_ts",
            "content_hash",
            "record_key",
        }
    )


def test_fact_stamp_types_covers_exactly_the_same_columns() -> None:
    assert set(record.FACT_STAMP_TYPES) == record.FACT_STAMP_COLUMNS


def test_fact_stamp_types_nullability_matches_006_1_section_6_1() -> None:
    non_null = {"batch_id", "delivery_id", "feed_id", "received_at", "content_hash", "record_key"}
    for name in non_null:
        assert record.FACT_STAMP_TYPES[name].nullable is False, name
    assert record.FACT_STAMP_TYPES["source_ts"].nullable is True


def test_fact_stamp_types_temporal_columns_are_timestamptz() -> None:
    assert record.FACT_STAMP_TYPES["received_at"].type == "timestamptz"
    assert record.FACT_STAMP_TYPES["source_ts"].type == "timestamptz"


def test_fact_stamp_types_string_columns() -> None:
    for name in ("batch_id", "delivery_id", "feed_id", "content_hash", "record_key"):
        assert record.FACT_STAMP_TYPES[name].type == "string"


def test_ordering_comparable_types_is_the_closed_set() -> None:
    assert record.ORDERING_COMPARABLE_TYPES == frozenset(
        {"string", "int", "long", "decimal", "date", "timestamp"}
    )


def test_ordering_comparable_types_excludes_bool() -> None:
    # F-6: no customer, no recency meaning -- additive later.
    assert "bool" not in record.ORDERING_COMPARABLE_TYPES


def test_ordering_comparable_types_excludes_float_double() -> None:
    # Outside the canonical value domain -- cannot be fact columns at all.
    assert "float" not in record.ORDERING_COMPARABLE_TYPES
    assert "double" not in record.ORDERING_COMPARABLE_TYPES
