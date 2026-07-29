"""`effects/spark.py` — reads, guards, one-commit append, `MERGE INTO`. LLD
§7.6, I-3, I-4, I-6, I-11, I-19, [S-6][T-4][T-6][T-9][T-10][T-19].

Uses `local_runner_fx` (the REAL production assembly, `effects.build.
make_runner_fx`, over the session's `spine_cat` Hadoop catalog) for every fx
call under test — no parallel test-only builder. `unique_table` returns a
fully-qualified `spine_cat.<db>.<table>` identifier; every `RunnerFx`
callable takes the BARE `<db>.<table>` form (`core.naming.qualified`
reconstructs the qualified name internally, matching `CoEffectDecl.table`'s
own documented shape) — `_bare` strips the fixture's `spine_cat.` prefix once
per table, so every call site below reads as "the fx call under test," not
"prefix-stripping boilerplate."
"""

from __future__ import annotations

import csv
import dataclasses
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

# `tests/` has no `__init__.py` anywhere (deliberate, see `tests/conftest.py`'s
# own docstring on per-subdir collision-avoidance): pytest's default
# "prepend" import mode puts THIS file's own directory (`tests/integration/`)
# on `sys.path`, so a sibling module in the same directory is a bare
# top-level import, never a `tests.integration.snapshot_asserts` dotted path.
from snapshot_asserts import (
    assert_no_new_snapshot,
    assert_stamped_batch,
    snapshot_delta,
    snapshot_ids,
)
from spine.core.merge import MergeSpec
from spine.effects import build
from spine.effects import spark as spark_fx
from spine.effects.records import MergeResult, RunnerFx, TransientError

_CATALOG_PREFIX = "spine_cat."


def _bare(qualified_table: str) -> str:
    assert qualified_table.startswith(_CATALOG_PREFIX)
    return qualified_table.removeprefix(_CATALOG_PREFIX)


def _create_raw_like_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(f"CREATE TABLE {qualified_table} (id STRING, batch_id STRING) USING iceberg")


def _create_quarantine_like_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(
        f"CREATE TABLE {qualified_table} "
        "(id STRING, batch_id STRING, check_stage STRING) USING iceberg"
    )


def _create_state_table(spark: SparkSession, qualified_table: str) -> None:
    # write.merge.mode=merge-on-read: see effects/spark.py's module docstring
    # -- this bead's own empirical finding on why COW-mode MERGE INTO can
    # never report a logical no-op via summary inspection.
    spark.sql(
        f"CREATE TABLE {qualified_table} "
        "(domain_id STRING, event_time TIMESTAMP, source_ts TIMESTAMP, "
        "content_hash STRING, payload STRING) USING iceberg "
        "TBLPROPERTIES ('write.merge.mode'='merge-on-read')"
    )


# --- read_objects: provisional CSV/UTF-8/header/FAILFAST (I-P1) ------------


def test_read_objects_reads_header_csv_as_all_string_columns(
    local_runner_fx: RunnerFx, tmp_path: Path
) -> None:
    path = tmp_path / "batch.csv"
    path.write_text("id,amount\n1,10.5\n2,20.25\n")

    df = local_runner_fx.read_objects((str(path),), {})

    assert [f.dataType.typeName() for f in df.schema.fields] == ["string", "string"]
    rows = sorted((r["id"], r["amount"]) for r in df.collect())
    assert rows == [("1", "10.5"), ("2", "20.25")]


def test_read_objects_failfast_raises_on_ragged_row(
    local_runner_fx: RunnerFx, tmp_path: Path
) -> None:
    path = tmp_path / "ragged.csv"
    path.write_text("id,amount\n1,10.5\n2,20.25,extra\n")

    df = local_runner_fx.read_objects((str(path),), {})
    with pytest.raises(Exception, match="MALFORMED_CSV_RECORD|FAILFAST"):
        df.collect()  # CSV parsing is lazy -- FAILFAST surfaces on the action


def test_read_objects_accepts_multiple_uris(local_runner_fx: RunnerFx, tmp_path: Path) -> None:
    path_a = tmp_path / "a.csv"
    path_b = tmp_path / "b.csv"
    path_a.write_text("id\n1\n")
    path_b.write_text("id\n2\n")

    df = local_runner_fx.read_objects((str(path_a), str(path_b)), {})

    assert sorted(r["id"] for r in df.collect()) == ["1", "2"]


# --- nvh.38: RFC-4180 doubled-quote decoding (`escape='"'`) -----------------


def test_read_objects_decodes_doubled_quote_with_embedded_comma(
    local_runner_fx: RunnerFx, tmp_path: Path
) -> None:
    # The verifier's repro: a payload containing both a quote and a comma
    # -- `csv.writer`'s QUOTE_MINIMAL wraps the field in quotes and doubles
    # the embedded quote, e.g. `"has""embedded,quote"`. Under Spark's CSV
    # default (`escape` = backslash), this doubled-quote encoding is
    # misread as a ragged row and dies under FAILFAST.
    path = tmp_path / "doubled_quote.csv"
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "payload"])
        writer.writerow(["1", 'has"embedded,quote'])

    df = local_runner_fx.read_objects((str(path),), {})
    rows = {r["id"]: r["payload"] for r in df.collect()}

    assert rows == {"1": 'has"embedded,quote'}


def test_read_objects_decodes_doubled_quote_with_embedded_quote_only(
    local_runner_fx: RunnerFx, tmp_path: Path
) -> None:
    # A field containing only a quote (no comma) also gets QUOTE_MINIMAL
    # quoting + doubling from `csv.writer` -- confirms the fix isn't
    # comma-shape-specific.
    path = tmp_path / "quote_only.csv"
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "payload"])
        writer.writerow(["1", 'say "hi" now'])

    df = local_runner_fx.read_objects((str(path),), {})
    rows = {r["id"]: r["payload"] for r in df.collect()}

    assert rows == {"1": 'say "hi" now'}


def test_read_objects_failfast_still_raises_on_ragged_row_after_escape_fix(
    local_runner_fx: RunnerFx, tmp_path: Path
) -> None:
    # Regression guard: `escape='"'` must not loosen FAILFAST's rejection
    # of a genuinely malformed (wrong column count) row.
    path = tmp_path / "ragged.csv"
    path.write_text("id,amount\n1,10.5\n2,20.25,extra\n")

    df = local_runner_fx.read_objects((str(path),), {})
    with pytest.raises(Exception, match="MALFORMED_CSV_RECORD|FAILFAST"):
        df.collect()


# --- read_table: pinned read (I-6), zero-snapshot sentinel [T-19] -----------


def test_read_table_zero_snapshot_returns_sentinel(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("zero_snapshot")
    _create_raw_like_table(spark, qt)

    df, sid = local_runner_fx.read_table(_bare(qt))

    assert sid == -1
    assert df.count() == 0


def test_read_table_resolves_current_snapshot_id(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("current_snapshot")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)

    df, sid = local_runner_fx.read_table(bare)

    assert sid == spark_fx._current_snapshot_id(spark, qt)
    assert sorted(r["id"] for r in df.collect()) == ["1"]


def test_read_table_pin_is_perceived_under_a_concurrent_append(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    """I-6: a pinned read must NOT see a sibling commit that lands after the
    read was pinned, even though Spark's plan is lazy and the actual scan
    executes later (`.count()` below, well after the second append)."""
    qt = unique_table("pin_under_concurrent_append")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)
    sid_at_pin = spark_fx._current_snapshot_id(spark, qt)

    df, sid = local_runner_fx.read_table(bare)
    assert sid == sid_at_pin

    # a sibling commit lands AFTER the pin was taken, BEFORE the pinned df's
    # own lazy plan is ever executed
    sibling = spark.createDataFrame([("2", "b2")], ["id", "batch_id"])
    local_runner_fx.append(bare, sibling, "b2", None)

    assert df.count() == 1  # still just the pre-pin row -- recorded == perceived
    assert sorted(r["id"] for r in df.collect()) == ["1"]
    # the CURRENT (unpinned) table now has both rows -- proves the second
    # append really did commit, so the assertion above is meaningful
    assert spark.table(qt).count() == 2
    assert spark_fx._current_snapshot_id(spark, qt) != sid_at_pin


# --- read_batch: current-snapshot, column-object batch_id predicate [S-6] ---


def test_read_batch_filters_to_the_named_batch_only(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("read_batch")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    batch_a = spark.createDataFrame([("1", "A"), ("2", "A")], ["id", "batch_id"])
    batch_b = spark.createDataFrame([("3", "B")], ["id", "batch_id"])
    local_runner_fx.append(bare, batch_a, "A", None)
    local_runner_fx.append(bare, batch_b, "B", None)

    df = local_runner_fx.read_batch(bare, "A")

    assert sorted(r["id"] for r in df.collect()) == ["1", "2"]


# --- table_has_batch: I-3 guard, incl. quarantine stage_key disambiguation --


def test_table_has_batch_true_and_false_on_real_data(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("guard")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "present")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "present", None)

    assert local_runner_fx.table_has_batch(bare, "present", None) is True
    assert local_runner_fx.table_has_batch(bare, "absent", None) is False


def test_table_has_batch_stage_key_disambiguates_quarantine_substreams(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("quarantine_guard")
    _create_quarantine_like_table(spark, qt)
    bare = _bare(qt)
    df = spark.createDataFrame([("1", "b1", "pre_check")], ["id", "batch_id", "check_stage"])
    local_runner_fx.append(bare, df, "b1", "pre_check")

    assert local_runner_fx.table_has_batch(bare, "b1", "pre_check") is True
    assert local_runner_fx.table_has_batch(bare, "b1", "post_check") is False
    assert local_runner_fx.table_has_batch(bare, "other_batch", "pre_check") is False


# --- append: summary added-records + stamps present; one-commit (R-13) -----


def test_append_returns_added_records_and_stamped_summary(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("append_summary")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    df = spark.createDataFrame([("1", "b1"), ("2", "b1")], ["id", "batch_id"])

    rows_appended, summary = local_runner_fx.append(bare, df, "b1", None)

    assert rows_appended == 2
    assert_stamped_batch(summary, "b1")


def test_append_stamps_stage_key_when_given(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("append_stage_stamp")
    _create_quarantine_like_table(spark, qt)
    bare = _bare(qt)
    df = spark.createDataFrame([("1", "b1", "post_check")], ["id", "batch_id", "check_stage"])

    _rows, summary = local_runner_fx.append(bare, df, "b1", "post_check")

    assert_stamped_batch(summary, "b1", "post_check")


def test_append_repartitions_before_write_when_shuffle_partitions_configured(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    """`RunConfig.repartition_before_write=True` + an explicit
    `shuffle_partitions` together fire the repartition-before-write branch
    (a coalesce/repartition changes file layout, never row content)."""
    qt = unique_table("append_repartition")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    run_config_json = json.dumps({"repartition_before_write": True, "shuffle_partitions": 2})
    config = dataclasses.replace(local_runner_fx.config, run_config_json=run_config_json)
    fx = build.make_runner_fx(spark, config)
    df = spark.createDataFrame([("1", "b1"), ("2", "b1"), ("3", "b1")], ["id", "batch_id"])

    rows_appended, summary = fx.append(bare, df, "b1", None)

    assert rows_appended == 3
    assert sorted(r["id"] for r in spark.table(qt).collect()) == ["1", "2", "3"]
    assert_stamped_batch(summary, "b1")


def test_append_is_exactly_one_commit_r13_harness_self_test(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("one_commit")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    before = snapshot_ids(spark, qt)

    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)

    new_id, summary = snapshot_delta(spark, qt, before)
    assert_stamped_batch(summary, "b1")
    assert summary.get("added-records") == "1"
    assert new_id == spark_fx._current_snapshot_id(spark, qt)


def test_snapshot_delta_harness_raises_on_zero_new_snapshots(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("harness_zero")
    _create_raw_like_table(spark, qt)
    before = snapshot_ids(spark, qt)

    with pytest.raises(AssertionError, match="one-commit invariant"):
        snapshot_delta(spark, qt, before)  # nothing committed since `before`


def test_assert_no_new_snapshot_passes_on_a_true_guard_skip(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("guard_skip_zero_commits")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)

    before = snapshot_ids(spark, qt)
    if local_runner_fx.table_has_batch(bare, "b1", None):
        pass  # the stage would skip its append entirely -- no fx.append call happens
    assert_no_new_snapshot(spark, qt, before)


# --- resolve_batch_snapshot: hit / miss / stage-filtered (I-19, lineage) ----


def test_resolve_batch_snapshot_hit(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("resolve_hit")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)

    sid = local_runner_fx.resolve_batch_snapshot(bare, "b1", None)

    assert sid == spark_fx._current_snapshot_id(spark, qt)


def test_resolve_batch_snapshot_miss_returns_none(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("resolve_miss")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)

    assert local_runner_fx.resolve_batch_snapshot(bare, "never-written", None) is None


def test_resolve_batch_snapshot_stage_filtered(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("resolve_stage_filtered")
    _create_quarantine_like_table(spark, qt)
    bare = _bare(qt)
    local_runner_fx.append(
        bare,
        spark.createDataFrame([("1", "b1", "pre_check")], ["id", "batch_id", "check_stage"]),
        "b1",
        "pre_check",
    )
    local_runner_fx.append(
        bare,
        spark.createDataFrame([("2", "b1", "post_check")], ["id", "batch_id", "check_stage"]),
        "b1",
        "post_check",
    )

    pre_sid = local_runner_fx.resolve_batch_snapshot(bare, "b1", "pre_check")
    post_sid = local_runner_fx.resolve_batch_snapshot(bare, "b1", "post_check")

    assert pre_sid is not None
    assert post_sid is not None
    assert pre_sid != post_sid


# --- merge: fresh insert, ordering-conditional update, no-op, unique-child --


def _row(domain_id: str, dt: datetime, content_hash: str, payload: str) -> tuple[str, ...]:
    return (domain_id, dt, dt, content_hash, payload)


_T1 = datetime(2024, 1, 1, tzinfo=UTC)
_T2 = datetime(2024, 1, 2, tzinfo=UTC)

_STATE_COLS = ["domain_id", "event_time", "source_ts", "content_hash", "payload"]


def test_merge_fresh_insert(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("merge_insert")
    _create_state_table(spark, qt)
    spec = MergeSpec(
        target_table=_bare(qt),
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    source = spark.createDataFrame([_row("a", _T1, "h1", "new-a")], _STATE_COLS)

    result = local_runner_fx.merge(spec, source)

    assert result.snapshot_id is not None
    assert result.summary is not None
    rows = spark.table(qt).collect()
    assert [(r["domain_id"], r["payload"]) for r in rows] == [("a", "new-a")]


def test_merge_ordering_conditional_update_newer_wins(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("merge_newer_wins")
    _create_state_table(spark, qt)
    spark.createDataFrame([_row("a", _T1, "h1", "old-a")], _STATE_COLS).writeTo(qt).append()
    spec = MergeSpec(
        target_table=_bare(qt),
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    source = spark.createDataFrame([_row("a", _T2, "h2", "new-a")], _STATE_COLS)

    result = local_runner_fx.merge(spec, source)

    assert result.snapshot_id is not None
    rows = spark.table(qt).collect()
    assert [(r["domain_id"], r["payload"]) for r in rows] == [("a", "new-a")]


def test_merge_older_loses_and_ties_are_no_op(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("merge_older_and_tie")
    _create_state_table(spark, qt)
    spark.createDataFrame([_row("a", _T2, "h2", "current-a")], _STATE_COLS).writeTo(qt).append()
    spec = MergeSpec(
        target_table=_bare(qt),
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    older_source = spark.createDataFrame([_row("a", _T1, "h1", "older-a")], _STATE_COLS)

    older_result = local_runner_fx.merge(spec, older_source)

    assert older_result == MergeResult(None, None)
    rows = spark.table(qt).collect()
    assert [(r["domain_id"], r["payload"]) for r in rows] == [("a", "current-a")]

    tie_source = spark.createDataFrame([_row("a", _T2, "h2", "current-a")], _STATE_COLS)
    tie_result = local_runner_fx.merge(spec, tie_source)

    assert tie_result == MergeResult(None, None)


def test_merge_unique_child_of_before_id(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("merge_unique_child")
    _create_state_table(spark, qt)
    spec = MergeSpec(
        target_table=_bare(qt),
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    before_id = spark_fx._current_snapshot_id(spark, qt)
    assert before_id is None  # zero-snapshot state table, pre-capture

    source = spark.createDataFrame([_row("a", _T1, "h1", "a")], _STATE_COLS)
    result = local_runner_fx.merge(spec, source)

    assert result.snapshot_id is not None
    assert result.snapshot_id != before_id
    assert result.snapshot_id == spark_fx._current_snapshot_id(spark, qt)


# --- nvh.40 [F1]: own-commit attribution under a concurrent sibling fold ---
#
# `_merge_race_probe` is `merge`'s one test seam (module docstring): a
# no-op in production, monkeypatched here to commit a SIBLING write to the
# same state table at a named point around our own `MERGE INTO` --
# reproducing, deterministically and single-threaded, the two races this
# bead's own scratch probe found empirically against a real local Iceberg
# table (see `spine/effects/spark.py`'s module docstring for the full
# account).


def test_merge_survives_a_sibling_commit_between_our_commit_and_resolution(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sibling committing AFTER our own `MERGE INTO` succeeds, BEFORE
    own-commit resolution runs, must never have its snapshot id/summary
    attributed to our batch -- unique-child-of-`before_id` survives this
    race (unlike the original "read current after MERGE" implementation,
    which would have returned the sibling's snapshot here)."""
    qt = unique_table("merge_race_post_commit")
    _create_state_table(spark, qt)
    bare = _bare(qt)
    spec = MergeSpec(
        target_table=bare,
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    spark.createDataFrame([_row("a", _T1, "h1", "seed-a")], _STATE_COLS).writeTo(qt).append()

    def fake_probe(probe_spark: SparkSession, probe_qt: str, point: str) -> None:
        if point == spark_fx._MERGE_POST_COMMIT:
            probe_spark.createDataFrame([_row("b", _T1, "h1", "sibling-b")], _STATE_COLS).writeTo(
                probe_qt
            ).append()

    monkeypatch.setattr(spark_fx, "_merge_race_probe", fake_probe)

    source = spark.createDataFrame([_row("a", _T2, "h2", "our-update-a")], _STATE_COLS)
    result = local_runner_fx.merge(spec, source)

    assert result.attributable is True
    assert result.snapshot_id is not None
    assert result.summary is not None
    # "current" (after both our commit AND the sibling's) must NOT be what
    # we attributed to ourselves -- proves the fix isn't accidentally
    # degenerating back to reading "current".
    assert result.snapshot_id != spark_fx._current_snapshot_id(spark, qt)
    rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(qt).collect())
    assert rows == [("a", "our-update-a"), ("b", "sibling-b")]  # both commits really landed


def test_merge_reports_unattributable_when_a_sibling_commits_before_our_statement_executes(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sibling that commits BEFORE our own `MERGE INTO` executes (using
    the same `before_id` as its own base -- Spark's MERGE reads *live*
    table state, never a value pinned at `before_id`-capture time) shifts
    our own eventual commit's parent off `before_id`; chain topology alone
    cannot then tell the sibling's commit apart from ours. Caught instead
    by the pre-commit base-shift check -- reported as an explicit
    unattributable result, never a fabricated (wrong) attribution, and
    never conflated with a logical no-op (real rows DID merge)."""
    qt = unique_table("merge_race_pre_commit")
    _create_state_table(spark, qt)
    bare = _bare(qt)
    spec = MergeSpec(
        target_table=bare,
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    spark.createDataFrame([_row("a", _T1, "h1", "seed-a")], _STATE_COLS).writeTo(qt).append()

    def fake_probe(probe_spark: SparkSession, probe_qt: str, point: str) -> None:
        if point == spark_fx._MERGE_PRE_COMMIT:
            probe_spark.createDataFrame([_row("b", _T1, "h1", "sibling-b")], _STATE_COLS).writeTo(
                probe_qt
            ).append()

    monkeypatch.setattr(spark_fx, "_merge_race_probe", fake_probe)

    source = spark.createDataFrame([_row("a", _T2, "h2", "our-update-a")], _STATE_COLS)
    result = local_runner_fx.merge(spec, source)

    assert result == MergeResult(None, None, attributable=False)
    # our own MERGE still genuinely ran and converged the state table --
    # unattributable is about NAMING the commit, not about it not happening.
    rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(qt).collect())
    assert rows == [("a", "our-update-a"), ("b", "sibling-b")]


def test_merge_unattributable_path_is_distinguishable_from_a_logical_no_op(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard against conflating the two `(None, None)` states
    (`MergeResult`'s own docstring): a genuine no-op (`test_merge_older_
    loses_and_ties_are_no_op`) and an unattributable race must remain
    distinguishable via `attributable`, not identical values."""
    qt = unique_table("merge_race_vs_no_op")
    _create_state_table(spark, qt)
    bare = _bare(qt)
    spec = MergeSpec(
        target_table=bare,
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    spark.createDataFrame([_row("a", _T2, "h2", "current-a")], _STATE_COLS).writeTo(qt).append()
    older_source = spark.createDataFrame([_row("a", _T1, "h1", "older-a")], _STATE_COLS)
    no_op_result = local_runner_fx.merge(spec, older_source)
    assert no_op_result == MergeResult(None, None)
    assert no_op_result.attributable is True

    def fake_probe(probe_spark: SparkSession, probe_qt: str, point: str) -> None:
        if point == spark_fx._MERGE_PRE_COMMIT:
            probe_spark.createDataFrame([_row("c", _T1, "h1", "sibling-c")], _STATE_COLS).writeTo(
                probe_qt
            ).append()

    monkeypatch.setattr(spark_fx, "_merge_race_probe", fake_probe)
    fresh_source = spark.createDataFrame([_row("a", _T2, "h2", "newer-again-a")], _STATE_COLS)
    unattributable_result = local_runner_fx.merge(spec, fresh_source)

    assert unattributable_result.attributable is False
    assert unattributable_result != no_op_result  # same (None, None) shape, different meaning
    assert unattributable_result == MergeResult(None, None, attributable=False)


# --- CommitFailed/Validation/CommitStateUnknown -> TransientError [T-10] ----
#
# A genuine concurrent-commit conflict needs a second live writer racing the
# same table -- impractical from one local[2] JVM. Two complementary checks
# instead: (1) the exported predicate directly, against a `Py4JJavaError`
# built with a duck-typed fake `java_exception` (real `Py4JJavaError.
# __init__` only touches `._target_id`; no live py4j gateway needed -- see
# effects/spark.py's module docstring); (2) `append`/`merge` really do let a
# NON-matching real local failure (a genuine schema mismatch) propagate
# UNTOUCHED, proving the "else raise" branch isn't accidentally swallowing
# everything.


class _FakeJavaClass:
    def __init__(self, name: str) -> None:
        self._name = name

    def getName(self) -> str:
        return self._name


class _FakeJavaException:
    def __init__(self, name: str) -> None:
        self._name = name
        self._target_id = "o1"  # Py4JJavaError.__init__ needs this attribute

    def getClass(self) -> _FakeJavaClass:
        return _FakeJavaClass(self._name)


@pytest.mark.parametrize(
    "class_name",
    [
        "org.apache.iceberg.exceptions.CommitFailedException",
        "org.apache.iceberg.exceptions.CommitStateUnknownException",
        "org.apache.iceberg.exceptions.ValidationException",
    ],
)
def test_is_transient_iceberg_failure_true_for_the_three_mapped_exceptions(
    class_name: str,
) -> None:
    fake_exc = Py4JJavaError("An error occurred", _FakeJavaException(class_name))
    assert spark_fx.is_transient_iceberg_failure(fake_exc) is True


@pytest.mark.parametrize(
    "class_name",
    [
        "org.apache.spark.sql.AnalysisException",
        "java.lang.IllegalStateException",
        "org.apache.iceberg.exceptions.NoSuchTableException",
    ],
)
def test_is_transient_iceberg_failure_false_for_other_exceptions(class_name: str) -> None:
    fake_exc = Py4JJavaError("An error occurred", _FakeJavaException(class_name))
    assert spark_fx.is_transient_iceberg_failure(fake_exc) is False


def test_is_transient_iceberg_failure_false_for_a_plain_python_exception() -> None:
    assert spark_fx.is_transient_iceberg_failure(ValueError("boom")) is False


def test_append_reraises_a_non_transient_failure_untouched(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("append_non_transient")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    # a genuine, locally-reproducible failure that is NOT one of the three
    # mapped Iceberg exceptions: the target table has no `extra_col` column.
    bad_df = spark.createDataFrame([("1", "b1", "x")], ["id", "batch_id", "extra_col"])

    with pytest.raises(Exception) as exc_info:
        local_runner_fx.append(bare, bad_df, "b1", None)
    assert not isinstance(exc_info.value, TransientError)


def test_merge_reraises_a_non_transient_failure_untouched(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("merge_non_transient")
    _create_state_table(spark, qt)
    spec = MergeSpec(
        target_table=_bare(qt),
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload", "extra_col"),
    )
    # `extra_col` is neither in the state table's schema nor the source's --
    # a genuine, locally-reproducible AnalysisException, not a transient one.
    source = spark.createDataFrame([_row("a", _T1, "h1", "a")], _STATE_COLS)

    with pytest.raises(Exception) as exc_info:
        local_runner_fx.merge(spec, source)
    assert not isinstance(exc_info.value, TransientError)
