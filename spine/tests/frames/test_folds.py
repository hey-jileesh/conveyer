"""`frames.folds` — ordering-struct comparator (I-11 [T-11]), `winners_per_domain`,
`default_lww_fold`, the `delta_filter` seam. §7.5.
"""

from __future__ import annotations

import functools
from datetime import UTC, datetime

from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from spine.frames import folds

# --- ordering_struct_gt: pure-Python reference model, property-tested ------
#
# `_field_rank` below is an INDEPENDENT re-derivation of "null ranks lowest"
# (not a call into `folds.ordering_struct_gt`'s own internals) -- the cross-
# check is only meaningful if the reference model doesn't share the
# implementation it's meant to validate.

_OPTIONAL_INT = st.one_of(st.none(), st.integers(min_value=-3, max_value=3))
_OPTIONAL_STR = st.one_of(st.none(), st.text(alphabet="abc", min_size=1, max_size=2))
_TRIPLE = st.tuples(_OPTIONAL_INT, _OPTIONAL_INT, _OPTIONAL_STR)


def _field_rank(value: object) -> tuple[int, object]:
    return (0, None) if value is None else (1, value)


def test_lww_ordering_columns_is_the_public_export_of_the_hardcoded_key() -> None:
    assert folds.LWW_ORDERING_COLUMNS == ("event_time", "source_ts", "content_hash")


@given(_TRIPLE)
@settings(max_examples=200)
def test_ordering_struct_gt_is_irreflexive(t: tuple[object, ...]) -> None:
    assert not folds.ordering_struct_gt(t, t)


@given(_TRIPLE, _TRIPLE)
@settings(max_examples=200)
def test_ordering_struct_gt_is_antisymmetric(a: tuple[object, ...], b: tuple[object, ...]) -> None:
    assert not (folds.ordering_struct_gt(a, b) and folds.ordering_struct_gt(b, a))


@given(_TRIPLE, _TRIPLE)
@settings(max_examples=200)
def test_ordering_struct_gt_trichotomy_against_independent_reference(
    a: tuple[object, ...], b: tuple[object, ...]
) -> None:
    gt_ab = folds.ordering_struct_gt(a, b)
    gt_ba = folds.ordering_struct_gt(b, a)
    if not gt_ab and not gt_ba:
        # a "tie": every field must independently rank equal (equal value,
        # or both null) under the reference model -- never a real difference
        # left un-decided.
        assert all(_field_rank(x) == _field_rank(y) for x, y in zip(a, b, strict=True))


@given(st.lists(_TRIPLE, min_size=0, max_size=8))
@settings(max_examples=100)
def test_ordering_struct_gt_gives_a_consistent_total_preorder(
    items: list[tuple[object, ...]],
) -> None:
    def cmp(a: tuple[object, ...], b: tuple[object, ...]) -> int:
        if folds.ordering_struct_gt(a, b):
            return 1
        if folds.ordering_struct_gt(b, a):
            return -1
        return 0

    ordered = sorted(items, key=functools.cmp_to_key(cmp))
    for a, b in zip(ordered, ordered[1:]):  # noqa: B905 -- deliberate pairwise/sliding zip
        assert not folds.ordering_struct_gt(a, b)  # non-decreasing


@given(_OPTIONAL_INT, _OPTIONAL_STR, _OPTIONAL_INT, _OPTIONAL_STR)
@settings(max_examples=200)
def test_null_leading_field_never_displaces_a_real_leading_field(
    tail_a0: object, tail_a1: object, tail_b0: object, tail_b1: object
) -> None:
    a = (None, tail_a0, tail_a1)
    b = (1, tail_b0, tail_b1)
    assert not folds.ordering_struct_gt(a, b)


def test_ordering_struct_gt_lww_tie_is_not_greater_than() -> None:
    # The named acceptance scenario in domain language: an equal ordering
    # struct is NOT `>` -- the MERGE's `WHEN MATCHED AND src > tgt` condition
    # does not fire on a tie, so a healthy rerun never re-updates state.
    key = (datetime(2026, 1, 1, tzinfo=UTC), 1, "h1")
    assert not folds.ordering_struct_gt(key, key)


# --- winners_per_domain / default_lww_fold: Spark-backed ---------------------

_FACTS_SCHEMA = "domain_id string, event_time timestamp, source_ts int, content_hash string"


def test_winners_per_domain_null_event_time_never_displaces_non_null(
    spark: SparkSession,
) -> None:
    facts = spark.createDataFrame(
        [
            ("d1", datetime(2026, 1, 1, tzinfo=UTC), 1, "h1"),
            ("d1", datetime(2026, 1, 2, tzinfo=UTC), 1, "h2"),
            ("d1", None, 99, "h3"),  # highest source_ts, but null event_time
        ],
        schema=_FACTS_SCHEMA,
    )

    winners = folds.winners_per_domain(
        facts, "domain_id", ("event_time", "source_ts", "content_hash")
    )

    rows = winners.collect()
    assert len(rows) == 1  # at most one row per domain_id (I-11 cardinality precondition)
    assert rows[0]["content_hash"] == "h2"


def test_winners_per_domain_all_null_ordering_still_picks_exactly_one_row(
    spark: SparkSession,
) -> None:
    facts = spark.createDataFrame(
        [("d2", None, None, "h4"), ("d2", None, None, "h5")], schema=_FACTS_SCHEMA
    )

    winners = folds.winners_per_domain(
        facts, "domain_id", ("event_time", "source_ts", "content_hash")
    )

    assert winners.count() == 1


def test_winners_per_domain_emits_at_most_one_row_per_domain(spark: SparkSession) -> None:
    facts = spark.createDataFrame(
        [
            ("d1", datetime(2026, 1, 1, tzinfo=UTC), 1, "h1"),
            ("d1", datetime(2026, 1, 2, tzinfo=UTC), 1, "h2"),
            ("d2", datetime(2026, 1, 1, tzinfo=UTC), 1, "h3"),
        ],
        schema=_FACTS_SCHEMA,
    )

    winners = folds.winners_per_domain(
        facts, "domain_id", ("event_time", "source_ts", "content_hash")
    )

    assert winners.count() == facts.select("domain_id").distinct().count()


def test_default_lww_fold_uses_hardcoded_ordering_key_and_ignores_state_slice(
    spark: SparkSession,
) -> None:
    facts = spark.createDataFrame(
        [
            ("d1", datetime(2026, 1, 1, tzinfo=UTC), 1, "h1"),
            ("d1", datetime(2026, 1, 2, tzinfo=UTC), 1, "h2"),
        ],
        schema=_FACTS_SCHEMA,
    )
    empty_state = spark.createDataFrame([], schema=_FACTS_SCHEMA)
    non_empty_state = spark.createDataFrame(
        [("d1", datetime(2099, 1, 1, tzinfo=UTC), 1, "future")], schema=_FACTS_SCHEMA
    )

    from_empty = folds.default_lww_fold(empty_state, facts, "domain_id").collect()
    from_non_empty = folds.default_lww_fold(non_empty_state, facts, "domain_id").collect()

    assert [r["content_hash"] for r in from_empty] == [r["content_hash"] for r in from_non_empty]
    assert from_empty[0]["content_hash"] == "h2"


def test_default_lww_fold_is_idempotent_on_rerun_over_the_same_facts(
    spark: SparkSession,
) -> None:
    facts = spark.createDataFrame(
        [
            ("d1", datetime(2026, 1, 1, tzinfo=UTC), 1, "h1"),
            ("d1", datetime(2026, 1, 2, tzinfo=UTC), 1, "h2"),
            ("d2", None, None, "h4"),
        ],
        schema=_FACTS_SCHEMA,
    )
    state_slice = spark.createDataFrame([], schema=_FACTS_SCHEMA)

    first = folds.default_lww_fold(state_slice, facts, "domain_id").collect()
    second = folds.default_lww_fold(state_slice, facts, "domain_id").collect()

    to_key = lambda rows: sorted((r["domain_id"], r["content_hash"]) for r in rows)  # noqa: E731
    assert to_key(first) == to_key(second)


# --- delta_filter: the named 007 seam, identity in Phase 1 ------------------


def test_delta_filter_is_identity(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])

    result = folds.delta_filter(df)

    to_tuples = lambda frame: sorted(tuple(r) for r in frame.collect())  # noqa: E731
    assert to_tuples(result) == to_tuples(df)
    assert result.columns == df.columns
