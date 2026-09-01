"""`frames.fold::reduce_batch_winners` — §8.2's intra-batch reduce, K-14's
reduce-side pin. Mirrors `tests/frames/test_folds.py`'s own shape (the v1,
plural-`folds` module's sibling) — see `frames/fold.py`'s own module
docstring for why this is a NEW, singular-named module rather than an
extension of `folds.py`.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from spine.core.merge import MergeSpec
from spine.frames.fold import reduce_batch_winners

_SCHEMA = StructType(
    [
        StructField("domain_id", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("source_ts", TimestampType(), True),
        StructField("content_hash", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)

_SPEC = MergeSpec(
    target_table="db.t",
    key_cols=("domain_id",),
    ordering_cols=("event_time", "source_ts", "content_hash"),
    update_cols=("event_time", "payload"),
)


def _row(domain_id, event_time, source_ts, content_hash, payload) -> Row:
    return Row(
        domain_id=domain_id,
        event_time=event_time,
        source_ts=source_ts,
        content_hash=content_hash,
        payload=payload,
    )


def test_reduce_batch_winners_emits_at_most_one_row_per_key(spark: SparkSession) -> None:
    facts = spark.createDataFrame(
        [
            _row("d1", datetime(2026, 1, 1, tzinfo=UTC), None, "h1", "p1"),
            _row("d1", datetime(2026, 1, 2, tzinfo=UTC), None, "h2", "p2"),
            _row("d2", datetime(2026, 1, 1, tzinfo=UTC), None, "h3", "p3"),
        ],
        schema=_SCHEMA,
    )

    winners = reduce_batch_winners(facts, _SPEC)

    assert winners.count() == facts.select("domain_id").distinct().count()


def test_reduce_batch_winners_null_event_time_never_displaces_non_null(
    spark: SparkSession,
) -> None:
    facts = spark.createDataFrame(
        [
            _row("d1", datetime(2026, 1, 1, tzinfo=UTC), None, "h1", "p1"),
            _row("d1", datetime(2026, 1, 2, tzinfo=UTC), None, "h2", "p2"),
            _row("d1", None, datetime(2026, 1, 3, tzinfo=UTC), "h3", "p3"),  # latest
            # source_ts, but null event_time -- must still lose (field-wise
            # lexicographic, event_time decides first, [T-11]).
        ],
        schema=_SCHEMA,
    )

    winners = reduce_batch_winners(facts, _SPEC)

    rows = winners.collect()
    assert len(rows) == 1
    assert rows[0]["content_hash"] == "h2"


def test_reduce_batch_winners_all_null_ordering_still_picks_exactly_one_row(
    spark: SparkSession,
) -> None:
    facts = spark.createDataFrame(
        [
            _row("d2", None, None, "h4", "p4"),
            _row("d2", None, None, "h5", "p5"),
        ],
        schema=_SCHEMA,
    )

    winners = reduce_batch_winners(facts, _SPEC)

    assert winners.count() == 1  # content_hash (F-6: never null) still breaks the tie


def test_reduce_batch_winners_content_hash_breaks_a_full_tie_on_declared_columns(
    spark: SparkSession,
) -> None:
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    facts = spark.createDataFrame(
        [
            _row("d1", ts, None, "h-lower", "p-lower"),
            _row("d1", ts, None, "h-upper", "p-upper"),  # "h-upper" > "h-lower", byte order
        ],
        schema=_SCHEMA,
    )

    winners = reduce_batch_winners(facts, _SPEC)

    rows = winners.collect()
    assert len(rows) == 1
    assert rows[0]["content_hash"] == "h-upper"  # codepoint/byte order, F-6


def test_reduce_batch_winners_is_idempotent_on_rerun_over_the_same_facts(
    spark: SparkSession,
) -> None:
    facts = spark.createDataFrame(
        [
            _row("d1", datetime(2026, 1, 1, tzinfo=UTC), None, "h1", "p1"),
            _row("d1", datetime(2026, 1, 2, tzinfo=UTC), None, "h2", "p2"),
            _row("d2", None, None, "h4", "p4"),
        ],
        schema=_SCHEMA,
    )

    first = reduce_batch_winners(facts, _SPEC).collect()
    second = reduce_batch_winners(facts, _SPEC).collect()

    to_key = lambda rows: sorted((r["domain_id"], r["content_hash"]) for r in rows)  # noqa: E731
    assert to_key(first) == to_key(second)


def test_reduce_batch_winners_respects_declared_ordering_cols_order_not_alphabetical(
    spark: SparkSession,
) -> None:
    """A per-type declared ordering key (§4.1: `MergeSpec.ordering_cols` =
    declared `ordering:` cols + the framework suffix) must decide winners in
    DECLARED order -- this spec's ordering starts with `event_time`, so a
    later `content_hash` must never override an earlier `event_time`
    difference, regardless of either value's own sort order."""
    facts = spark.createDataFrame(
        [
            _row("d1", datetime(2026, 1, 2, tzinfo=UTC), None, "a-lowest-hash", "newer"),
            _row("d1", datetime(2026, 1, 1, tzinfo=UTC), None, "z-highest-hash", "older"),
        ],
        schema=_SCHEMA,
    )

    winners = reduce_batch_winners(facts, _SPEC).collect()

    assert len(winners) == 1
    assert winners[0]["payload"] == "newer"  # event_time decided it, not content_hash
