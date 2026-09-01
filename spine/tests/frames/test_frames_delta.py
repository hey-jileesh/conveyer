"""`frames.delta.delta_filter` -- LLD 007.1 §7.2 (F-5). Plan-level unit
tests for the pure halves of K-05's fail-open paths (§13.1: "`delta_filter`
fail-open goldens: §7.2's paths 1-9, each asserting behavior and
direction"): the full K-05 goldens additionally exercise `core/delta.py::
resolve_predecessors` (B6, already covered by `tests/unit/test_delta.py`'s
own path-1-9 unit matrix) plus the framework's marker reads and predecessor-
partition projection (B9's remaining wiring, out of this milestone's scope)
-- this file covers exactly what `delta_filter` itself decides, given a
`predecessor_facts_t` shaped the way each named path's resolution would
produce it.

Basename note: `tests/unit/test_delta.py` already covers `core/delta.py`
(a DIFFERENT module) -- `tests/frames/test_frames_delta.py`'s own basename
stays repo-unique (no `__init__.py` under `tests/`, so same-basename files
in different subdirectories collide at collection).

Path numbering matches §7.2's own table (module docstring's own citation).
At `delta_filter`'s own grain, paths 1-3 and 5-9 collapse to the SAME shape
(`resolve_predecessors` already returns `predecessor_batch_ids = ()` for
every one of them, so the framework's `predecessor_facts_t` projection is
empty) -- one shared test covers that shape; path 4 (a resolved predecessor
batch that itself committed zero facts) gets its own test since it is named
distinctly in §7.2's table even though it also reduces to "no rows from
that batch in `predecessor_facts_t`"."""

from __future__ import annotations

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from spine.frames import delta

_CAND_SCHEMA = StructType(
    [
        StructField("record_key", StringType(), True),
        StructField("content_hash", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)
_PRED_SCHEMA = StructType(
    [StructField("record_key", StringType(), True), StructField("content_hash", StringType(), True)]
)


def _cand(spark: SparkSession, *rows: tuple[str, str, str]):
    return spark.createDataFrame(
        [Row(record_key=r, content_hash=h, payload=p) for r, h, p in rows], _CAND_SCHEMA
    )


def _pred(spark: SparkSession, *rows: tuple[str, str]):
    return spark.createDataFrame([Row(record_key=r, content_hash=h) for r, h in rows], _PRED_SCHEMA)


def _empty_pred(spark: SparkSession):
    return spark.createDataFrame([], _PRED_SCHEMA)


def _sorted_rows(df) -> list[tuple[str, str, str]]:
    return sorted((r["record_key"], r["content_hash"], r["payload"]) for r in df.collect())


# --- (a) unconditional within-batch collapse --------------------------------


def test_within_batch_collapse_is_unconditional(spark: SparkSession) -> None:
    # Two IDENTICAL (record_key, content_hash) candidate rows -- N identical
    # in-batch assertions are one fact, not N (§7.2(a)) -- collapse to one,
    # with NO predecessor rows at all (the collapse does not depend on
    # predecessor_facts_t's contents).
    cand = _cand(spark, ("K1", "H1", "p1"), ("K1", "H1", "p1"))
    out = _sorted_rows(delta.delta_filter(cand, _empty_pred(spark)))
    assert out == [("K1", "H1", "p1")]


def test_within_batch_collapse_runs_even_under_a_nonempty_predecessor_set(
    spark: SparkSession,
) -> None:
    # §7.2(a): "it runs even under a probe refusal" -- generalized here to
    # "runs regardless of predecessor_facts_t's own contents": the collapse
    # is evaluated before, and independent of, the cross-batch comparison.
    cand = _cand(spark, ("K1", "H1", "p1"), ("K1", "H1", "p1"), ("K2", "H2", "p2"))
    pred = _pred(spark, ("K2", "H2"))  # K2 would drop on its own merits too
    out = _sorted_rows(delta.delta_filter(cand, pred))
    assert out == [("K1", "H1", "p1")]  # K1 collapsed to one row, then kept; K2 dropped


# --- (b) divergent within-batch duplicates both commit ----------------------


def test_divergent_within_batch_duplicates_both_commit(spark: SparkSession) -> None:
    # Same record_key, DIFFERENT content_hash -- the observable-data
    # condition, D-2(b) -- both survive; NOT collapsed by (a) since their
    # (record_key, content_hash) pairs differ.
    cand = _cand(spark, ("K1", "H1", "p1"), ("K1", "H2", "p2"))
    out = _sorted_rows(delta.delta_filter(cand, _empty_pred(spark)))
    assert out == [("K1", "H1", "p1"), ("K1", "H2", "p2")]


# --- (c) drop iff content_hash unchanged in every predecessor batch ---------


def test_drop_iff_content_hash_unchanged_against_a_single_predecessor_batch(
    spark: SparkSession,
) -> None:
    cand = _cand(spark, ("K1", "H1", "p1"), ("K2", "H9", "p9"))
    pred = _pred(spark, ("K1", "H1"))  # K1 exactly matches; K2 has no predecessor row
    out = _sorted_rows(delta.delta_filter(cand, pred))
    assert out == [("K2", "H9", "p9")]  # K1 dropped, K2 kept (novel)


def test_keep_when_content_hash_changed_against_predecessor(spark: SparkSession) -> None:
    # Same record_key, but the candidate's content_hash differs from the
    # predecessor's -- a genuine value change -- kept, never dropped.
    cand = _cand(spark, ("K1", "H2", "p2"))
    pred = _pred(spark, ("K1", "H1"))
    out = _sorted_rows(delta.delta_filter(cand, pred))
    assert out == [("K1", "H2", "p2")]


def test_keep_on_never_before_seen_record_key(spark: SparkSession) -> None:
    cand = _cand(spark, ("K3", "H3", "p3"))
    pred = _pred(spark, ("K1", "H1"))  # unrelated key
    out = _sorted_rows(delta.delta_filter(cand, pred))
    assert out == [("K3", "H3", "p3")]


# --- paths 1-3, 5-9: an empty resolved predecessor set -> everything novel --


def test_vacuous_predecessor_set_keeps_everything(spark: SparkSession) -> None:
    """Paths 1 (genesis/fresh feed), 2 (in-flight-sibling coherence-clause
    veto), 3 (marker-without-facts kill, indistinguishable from path 2), and
    5-9 (every probe refusal, plus the interim floor's own keep direction)
    all resolve `resolve_predecessors` to `predecessor_batch_ids == ()`
    (`core/delta.py`'s own module docstring / K-09/K-10) -- the framework
    then projects an EMPTY `predecessor_facts_t`, and this filter keeps
    every within-batch-collapsed candidate: not a special case, just the
    ordinary "no predecessor pairs to match against" outcome."""
    cand = _cand(spark, ("K1", "H1", "p1"), ("K2", "H2", "p2"))
    out = _sorted_rows(delta.delta_filter(cand, _empty_pred(spark)))
    assert out == [("K1", "H1", "p1"), ("K2", "H2", "p2")]


def test_zero_fact_predecessor_batch_is_vacuous_path_4(spark: SparkSession) -> None:
    """Path 4: the latest-completed batch is zero-fact -- its projection
    into `predecessor_facts_t` contributes NO rows (it committed none), so
    the comparison against it is vacuous and every candidate is novel --
    the same shape as the paths-1-3/5-9 test above, named separately here
    because §7.2's table names it as its own row ("comparison vacuous;
    everything novel; the minted duplicates re-arm the next batch's
    filter")."""
    cand = _cand(spark, ("K1", "H1", "p1"))
    out = _sorted_rows(delta.delta_filter(cand, _empty_pred(spark)))
    assert out == [("K1", "H1", "p1")]


# --- Track E: a two-batch resolved predecessor set -------------------------


def test_track_e_two_batch_predecessor_agreement_still_drops(spark: SparkSession) -> None:
    # Both resolved predecessor batches agree on K1's content_hash -- "its
    # content_hash is unchanged in EVERY batch of the resolved predecessor
    # set" holds -- K1 drops exactly as the single-batch case does.
    cand = _cand(spark, ("K1", "H1", "p1"), ("K2", "H2", "p2"))
    pred = _pred(spark, ("K1", "H1"), ("K1", "H1"))  # two batches, same pair
    out = _sorted_rows(delta.delta_filter(cand, pred))
    assert out == [("K2", "H2", "p2")]


def test_track_e_two_batch_predecessor_disagreement_never_drops(spark: SparkSession) -> None:
    # The two resolved predecessor batches DISAGREE on K1's content_hash --
    # "unchanged in every batch" can never hold for ANY candidate value of
    # K1 -- fail-open, the candidate keeps regardless of which of the two
    # values (or neither) it carries. Never pick a row to believe (the L-3
    # idiom `core/delta.py::resolve_predecessors` already applies to marker
    # rows, restated here for fact rows).
    cand = _cand(spark, ("K1", "H1", "p1"))
    pred = _pred(spark, ("K1", "H1"), ("K1", "H9"))  # two batches, disagreeing
    out = _sorted_rows(delta.delta_filter(cand, pred))
    assert out == [("K1", "H1", "p1")]


def test_track_e_disagreement_never_drops_regardless_of_which_value_candidate_carries(
    spark: SparkSession,
) -> None:
    # Same disagreeing predecessor set as above, but the candidate carries
    # the OTHER disagreeing value -- still kept, for the same reason.
    cand = _cand(spark, ("K1", "H9", "p9"))
    pred = _pred(spark, ("K1", "H1"), ("K1", "H9"))
    out = _sorted_rows(delta.delta_filter(cand, pred))
    assert out == [("K1", "H9", "p9")]


# --- structural: novel_t preserves the candidate frame's own schema --------


def test_novel_t_preserves_every_original_candidate_column(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("record_key", StringType(), True),
            StructField("content_hash", StringType(), True),
            StructField("batch_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(
        [Row(record_key="K1", content_hash="H1", batch_id="b1", quantity=7)], schema
    )
    out = delta.delta_filter(df, _empty_pred(spark))
    assert set(out.columns) == {"record_key", "content_hash", "batch_id", "quantity"}
    row = out.collect()[0]
    assert row["batch_id"] == "b1"
    assert row["quantity"] == 7
