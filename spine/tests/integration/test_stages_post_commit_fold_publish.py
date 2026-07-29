"""`stages/{post_check,commit,fold,publish}.py` — LLD §7.5, I-11, I-12, I-19,
I-24, [H-1][H-2][E-5][E-14][C-7][T-8].

Uses `local_runner_fx` (the REAL production assembly) throughout, matching
every other integration suite in this package (`test_spark_fx.py`,
`test_stages_land_pre_pull_apply.py`). Since this bead's four stages start
mid-sequence (`post_check` needs `candidate_facts_df`, as if `apply` already
ran), `BatchContext` seeds are built directly with `candidate_facts_df` (and,
for `commit`/`fold`/`publish` tests, the upstream stage's own output)
pre-populated — bypassing `land`/`pre_check`/`pull`/`apply` entirely, which
is a different bead's file scope.

Table shapes: the quarantine table carries exactly `shape_quarantine`'s own
output columns (candidate columns + `reason` + `batch_id` + `check_stage` —
**no** `delivery_id`/`feed_id`/`received_at`, since `post_check`'s violations
are shaped straight from `candidate_facts_df`, never lineage-stamped); the
fact table carries `stamp_fact_lineage`'s columns (`batch_id`, `delivery_id`,
`feed_id`, `received_at`) in addition; the state table is the narrower
domain/ordering/payload shape `frames.folds.default_lww_fold`'s ordering key
needs, created with `write.merge.mode = merge-on-read` (the fold no-op
detection precondition, `effects/spark.py`'s own documented empirical
finding).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from snapshot_asserts import snapshot_delta, snapshot_ids
from spine.binding import Transforms
from spine.config import RunConfig
from spine.context import BatchContext
from spine.core import run_facts
from spine.core.model import PipelineSpecModel
from spine.effects.records import RunnerFx
from spine.frames import folds
from spine.stages import commit, fold, post_check, publish

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from tests.conftest import MotoEventsBus

_CATALOG_PREFIX = "spine_cat."

_COLS = ("domain_id", "event_time", "source_ts", "content_hash", "payload")
_T1 = datetime(2024, 1, 1, tzinfo=UTC)
_T2 = datetime(2024, 1, 2, tzinfo=UTC)


def _bare(qualified_table: str) -> str:
    assert qualified_table.startswith(_CATALOG_PREFIX)
    return qualified_table.removeprefix(_CATALOG_PREFIX)


def _create_quarantine_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(
        f"CREATE TABLE {qualified_table} "
        "(domain_id STRING, event_time TIMESTAMP, source_ts TIMESTAMP, "
        "content_hash STRING, payload STRING, reason STRING, batch_id STRING, "
        "check_stage STRING) USING iceberg"
    )


def _create_fact_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(
        f"CREATE TABLE {qualified_table} "
        "(domain_id STRING, event_time TIMESTAMP, source_ts TIMESTAMP, "
        "content_hash STRING, payload STRING, batch_id STRING, delivery_id STRING, "
        "feed_id STRING, received_at TIMESTAMP) USING iceberg"
    )


def _create_state_table(spark: SparkSession, qualified_table: str) -> None:
    # write.merge.mode=merge-on-read: the fold no-op detection precondition
    # (effects/spark.py's own documented empirical finding).
    spark.sql(
        f"CREATE TABLE {qualified_table} "
        "(domain_id STRING, event_time TIMESTAMP, source_ts TIMESTAMP, "
        "content_hash STRING, payload STRING) USING iceberg "
        "TBLPROPERTIES ('write.merge.mode'='merge-on-read')"
    )


def _row(domain_id: str, dt: datetime, content_hash: str, payload: str) -> tuple[object, ...]:
    return (domain_id, dt, dt, content_hash, payload)


def _make_spec(*, quarantine_table: str, fact_table: str, state_table: str) -> PipelineSpecModel:
    return PipelineSpecModel(
        pipeline="pipelines/m3-post-commit-fold-publish",
        transforms_module="pipelines.m3_post_commit_fold_publish",
        raw_table="spine_test_tables.unused_raw",
        quarantine_table=quarantine_table,
        fact_table=fact_table,
        state_table=state_table,
    )


def _passthrough_post_check(candidate_df: DataFrame, co_effects: object) -> DataFrame:
    return candidate_df.limit(0).withColumn("reason", F.lit("unused").cast("string"))


def _default_fold(spec: PipelineSpecModel) -> Callable[[DataFrame, DataFrame], DataFrame]:
    # A real Transforms.fold must narrow its output to the state table's own
    # schema; default_lww_fold returns full-shape facts rows (incl. lineage
    # columns) -- the `.select(*_COLS)` here stands in for that narrowing
    # (binding/identity-exemplar concern, not stages/fold.py's own).
    def _fold(state_slice: DataFrame, facts_df: DataFrame) -> DataFrame:
        return folds.default_lww_fold(state_slice, facts_df, spec.domain_id_col).select(*_COLS)

    return _fold


def _make_ctx(
    *,
    spec: PipelineSpecModel,
    batch_id: str,
    candidate_facts_df: DataFrame | None = None,
    admitted_facts_df: DataFrame | None = None,
    committed_facts_df: DataFrame | None = None,
    fact_snapshot_id: int | None = None,
    state_snapshot_id: int | None = None,
    post_check_fn: Callable[[DataFrame, object], DataFrame] | None = None,
    fold_fn: Callable[[DataFrame, DataFrame], DataFrame] | None = None,
    raw_count: int = 10,
) -> BatchContext:
    transforms = Transforms(
        apply=lambda valid_df, co_effects: valid_df,
        post_check=post_check_fn or _passthrough_post_check,
        fold=fold_fn or _default_fold(spec),
    )
    return BatchContext(
        pipeline="pipelines/m3-post-commit-fold-publish",
        feed_id="feed/m3-pcfp",
        delivery_id=str(uuid.UUID(int=1, version=4)),
        batch_id=batch_id,
        delivery_key="statement.csv",
        content_hash="sha256:" + "a" * 64,
        object_uris=("s3://unused/x",),
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        spec=spec,
        run=RunConfig(),
        transforms=transforms,
        attempt_id="attempt-1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        raw_count=raw_count,
        co_effects={},
        candidate_facts_df=candidate_facts_df,
        admitted_facts_df=admitted_facts_df,
        committed_facts_df=committed_facts_df,
        fact_snapshot_id=fact_snapshot_id,
        state_snapshot_id=state_snapshot_id,
    )


def _batch_id(n: int) -> str:
    return str(uuid.UUID(int=n, version=5))


# --- post_check: fresh compute (count identity asserted, [H-2]) ------------


def test_post_check_fresh_zero_violations_admits_all_no_write(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    qtn_qt = unique_table("post_check_clean_qtn")
    _create_quarantine_table(spark, qtn_qt)
    spec = _make_spec(
        quarantine_table=_bare(qtn_qt),
        fact_table="spine_test_tables.unused_fact",
        state_table="spine_test_tables.unused_state",
    )
    batch_id = _batch_id(1)
    candidate = spark.createDataFrame(
        [_row("a", _T1, "h1", "payload-a"), _row("b", _T1, "h2", "payload-b")], list(_COLS)
    )
    ctx = _make_ctx(spec=spec, batch_id=batch_id, candidate_facts_df=candidate)
    before = snapshot_ids(spark, qtn_qt)

    after = post_check.run(ctx, local_runner_fx)

    assert snapshot_ids(spark, qtn_qt) == before  # no write at all
    assert after.post_quarantined_count == 0
    assert after.post_quarantine_snapshot_id is None
    assert after.admitted_facts_df.count() == 2
    assert local_runner_fx.table_has_batch(_bare(qtn_qt), batch_id, "post_check") is False


def test_post_check_fresh_with_violations_quarantines_and_holds_count_identity(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    qtn_qt = unique_table("post_check_dirty_qtn")
    _create_quarantine_table(spark, qtn_qt)
    spec = _make_spec(
        quarantine_table=_bare(qtn_qt),
        fact_table="spine_test_tables.unused_fact",
        state_table="spine_test_tables.unused_state",
    )
    batch_id = _batch_id(2)
    candidate = spark.createDataFrame(
        [_row("c", _T1, "h3", "payload-c"), _row("d", _T1, "h4", "payload-d")], list(_COLS)
    )

    def _post_check(candidate_df: DataFrame, co_effects: object) -> DataFrame:
        return candidate_df.filter(F.col("domain_id") == "d").withColumn(
            "reason", F.lit("bad payload")
        )

    ctx = _make_ctx(
        spec=spec, batch_id=batch_id, candidate_facts_df=candidate, post_check_fn=_post_check
    )
    before = snapshot_ids(spark, qtn_qt)

    after = post_check.run(ctx, local_runner_fx)

    new_id, summary = snapshot_delta(spark, qtn_qt, before)  # R-13: exactly one commit
    assert summary.get("conveyer.batch-id") == batch_id
    assert summary.get("conveyer.stage") == "post_check"
    assert after.post_quarantined_count == 1
    assert after.post_quarantine_snapshot_id == new_id
    admitted_ids = sorted(r["domain_id"] for r in after.admitted_facts_df.collect())
    assert admitted_ids == ["c"]
    assert local_runner_fx.table_has_batch(_bare(qtn_qt), batch_id, "post_check") is True


def test_post_check_fresh_non_conforming_transform_raises_loudly(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    qtn_qt = unique_table("post_check_fabricated_qtn")
    _create_quarantine_table(spark, qtn_qt)
    spec = _make_spec(
        quarantine_table=_bare(qtn_qt),
        fact_table="spine_test_tables.unused_fact",
        state_table="spine_test_tables.unused_state",
    )
    batch_id = _batch_id(3)
    candidate = spark.createDataFrame([_row("e", _T1, "h5", "payload-e")], list(_COLS))

    def _fabricating_post_check(candidate_df: DataFrame, co_effects: object) -> DataFrame:
        # fabricates a violation row NOT present in candidate_df -- breaks
        # the multiplicity-preserving-subset contract [C-8]/I-12.
        return spark.createDataFrame(
            [_row("e", _T1, "h5", "payload-e"), _row("not-real", _T1, "hX", "px")], list(_COLS)
        ).withColumn("reason", F.lit("fabricated"))

    ctx = _make_ctx(
        spec=spec,
        batch_id=batch_id,
        candidate_facts_df=candidate,
        post_check_fn=_fabricating_post_check,
    )
    before = snapshot_ids(spark, qtn_qt)

    with pytest.raises(AssertionError, match="I-12 count identity violated"):
        post_check.run(ctx, local_runner_fx)

    assert snapshot_ids(spark, qtn_qt) == before  # loud failure -- no partial write


# --- post_check: guard-skip (rerun) path, durable subtraction [E-5][H-2] ----


def test_post_check_guard_skip_uses_durable_quarantine_rows(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    qtn_qt = unique_table("post_check_rerun_qtn")
    _create_quarantine_table(spark, qtn_qt)
    spec = _make_spec(
        quarantine_table=_bare(qtn_qt),
        fact_table="spine_test_tables.unused_fact",
        state_table="spine_test_tables.unused_state",
    )
    batch_id = _batch_id(4)
    candidate = spark.createDataFrame(
        [_row("c", _T1, "h3", "payload-c"), _row("d", _T1, "h4", "payload-d")], list(_COLS)
    )

    def _post_check(candidate_df: DataFrame, co_effects: object) -> DataFrame:
        return candidate_df.filter(F.col("domain_id") == "d").withColumn(
            "reason", F.lit("bad payload")
        )

    ctx = _make_ctx(
        spec=spec, batch_id=batch_id, candidate_facts_df=candidate, post_check_fn=_post_check
    )
    first = post_check.run(ctx, local_runner_fx)
    before = snapshot_ids(spark, qtn_qt)

    second = post_check.run(first, local_runner_fx)

    assert snapshot_ids(spark, qtn_qt) == before  # guard-skip: no new write
    assert second.post_quarantined_count == 1  # durable count, not recomputed
    assert second.post_quarantine_snapshot_id == first.post_quarantine_snapshot_id
    assert sorted(r["domain_id"] for r in second.admitted_facts_df.collect()) == ["c"]


def test_post_check_guard_skip_drift_sets_ctx_field_without_raising(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    """The stage itself carries zero instrumentation (critique F4, bead
    conveyer-nvh.43): it only ever sets `ctx.post_check_drift` (a short,
    counts-only string, [S-7]), never raising and never logging/emitting
    directly. `effects/ledger.py`'s own `test_ledger.py` suite covers the
    WARNING + EMF `PostCheckDrift` emission that `record_run` now derives
    from the resulting `RunFact`."""
    qtn_qt = unique_table("post_check_drift_qtn")
    _create_quarantine_table(spark, qtn_qt)
    spec = _make_spec(
        quarantine_table=_bare(qtn_qt),
        fact_table="spine_test_tables.unused_fact",
        state_table="spine_test_tables.unused_state",
    )
    batch_id = _batch_id(5)
    candidate = spark.createDataFrame([_row("f", _T1, "h6", "payload-f")], list(_COLS))

    def _flags_f(candidate_df: DataFrame, co_effects: object) -> DataFrame:
        return candidate_df.filter(F.col("domain_id") == "f").withColumn("reason", F.lit("bad f"))

    ctx = _make_ctx(
        spec=spec, batch_id=batch_id, candidate_facts_df=candidate, post_check_fn=_flags_f
    )
    first = post_check.run(ctx, local_runner_fx)
    assert first.post_quarantined_count == 1  # only "f" quarantined durably

    # Rerun whose recomputed CANDIDATE no longer contains "f" at all -- a
    # genuine cross-attempt perception drift (I-12 [E-5]): the durable
    # violation's value-tuple has nothing to anti-join against, so it
    # saturates at zero (removes nothing) and the count identity fails.
    drifted_candidate = spark.createDataFrame(
        [_row("g", _T1, "h7", "payload-g"), _row("hh", _T1, "h77", "payload-hh")], list(_COLS)
    )
    rerun_ctx = replace(
        first,
        candidate_facts_df=drifted_candidate,
        transforms=replace(first.transforms, post_check=_passthrough_post_check),
    )

    second = post_check.run(rerun_ctx, local_runner_fx)

    admitted_ids = sorted(r["domain_id"] for r in second.admitted_facts_df.collect())
    assert admitted_ids == ["g", "hh"]  # durable "f" row matched nothing -- nothing removed
    assert second.post_quarantined_count == 1  # durable count, not the recomputed 0
    assert second.post_quarantine_snapshot_id == first.post_quarantine_snapshot_id

    # The drift is folded into `ctx.post_check_drift` -- counts only, no row
    # values [S-7] -- and, via `run_facts.transition`, into the run-ledger
    # row's `error_message` for this (non-failed) transition. The WARNING +
    # EMF `PostCheckDrift` emission itself now lives in `effects/ledger.py::
    # record_run` (critique F4), covered by `test_ledger.py`, not here.
    assert second.post_check_drift == "post-check drift: durable=1 recomputed=2 subset=False"
    fact = run_facts.transition("post_check", rerun_ctx, second, _T1, _T1)
    assert fact.outcome == "skipped-guard"  # guard-skip rerun, never "failed" (I-12 [H-2])
    assert fact.error_message == second.post_check_drift


# --- commit: happy path, guard-skip, structural defects (I-24) --------------


def test_commit_happy_path_stamps_lineage_and_appends(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("commit_happy_fact")
    _create_fact_table(spark, fact_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=_bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    batch_id = _batch_id(6)
    admitted = spark.createDataFrame([_row("c", _T1, "h3", "payload-c")], list(_COLS))
    ctx = _make_ctx(spec=spec, batch_id=batch_id, admitted_facts_df=admitted)
    before = snapshot_ids(spark, fact_qt)

    after = commit.run(ctx, local_runner_fx)

    new_id, summary = snapshot_delta(spark, fact_qt, before)  # R-13: exactly one commit
    assert summary.get("conveyer.batch-id") == batch_id
    assert after.facts_appended == 1
    assert after.fact_snapshot_id == new_id
    committed_ids = sorted(r["domain_id"] for r in after.committed_facts_df.collect())
    assert committed_ids == ["c"]
    stamped_row = spark.table(fact_qt).where(f"batch_id = '{batch_id}'").collect()[0]
    assert stamped_row["delivery_id"] == ctx.delivery_id
    assert stamped_row["feed_id"] == ctx.feed_id


def test_commit_guard_skip_rerun_zero_appended_same_snapshot(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("commit_rerun_fact")
    _create_fact_table(spark, fact_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=_bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    batch_id = _batch_id(7)
    admitted = spark.createDataFrame([_row("c", _T1, "h3", "payload-c")], list(_COLS))
    ctx = _make_ctx(spec=spec, batch_id=batch_id, admitted_facts_df=admitted)
    first = commit.run(ctx, local_runner_fx)
    before = snapshot_ids(spark, fact_qt)

    second = commit.run(first, local_runner_fx)

    assert snapshot_ids(spark, fact_qt) == before  # guard-skip: no new write
    assert second.facts_appended == 0
    assert second.fact_snapshot_id == first.fact_snapshot_id
    assert sorted(r["domain_id"] for r in second.committed_facts_df.collect()) == ["c"]


def test_commit_null_domain_id_raises_named_defect_no_append(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("commit_null_domain_fact")
    _create_fact_table(spark, fact_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=_bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    batch_id = _batch_id(8)
    admitted = spark.createDataFrame(
        [_row("z", _T1, "h8", "payload-null")], list(_COLS)
    ).withColumn("domain_id", F.lit(None).cast("string"))
    ctx = _make_ctx(spec=spec, batch_id=batch_id, admitted_facts_df=admitted)
    before = snapshot_ids(spark, fact_qt)

    with pytest.raises(ValueError, match="I-24"):
        commit.run(ctx, local_runner_fx)

    assert snapshot_ids(spark, fact_qt) == before  # fail fast -- no append


def test_commit_schema_drift_raises_named_defect_no_append(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("commit_schema_drift_fact")
    _create_fact_table(spark, fact_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=_bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    batch_id = _batch_id(9)
    admitted = spark.createDataFrame(
        [(*_row("h", _T1, "h9", "payload-h"), "unexpected-extra")],
        [*_COLS, "extra_col"],
    )
    ctx = _make_ctx(spec=spec, batch_id=batch_id, admitted_facts_df=admitted)
    before = snapshot_ids(spark, fact_qt)

    with pytest.raises(ValueError, match="schema drift"):
        commit.run(ctx, local_runner_fx)

    assert snapshot_ids(spark, fact_qt) == before  # fail fast -- no append


# --- fold: happy insert, logical no-op rerun, empty-facts skip (I-11) -------


def test_fold_happy_path_inserts_via_merge(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("fold_happy_fact")
    state_qt = unique_table("fold_happy_state")
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    batch_id = _batch_id(10)
    admitted = spark.createDataFrame([_row("c", _T1, "h3", "payload-c")], list(_COLS))
    committed_ctx = commit.run(
        _make_ctx(spec=spec, batch_id=batch_id, admitted_facts_df=admitted), local_runner_fx
    )
    before = snapshot_ids(spark, state_qt)

    after = fold.run(committed_ctx, local_runner_fx)

    assert after.state_read_snapshot_id == -1  # zero-snapshot state table sentinel (I-6)
    new_id, _summary = snapshot_delta(spark, state_qt, before)
    assert after.state_snapshot_id == new_id
    assert after.merge_summary is not None
    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == [("c", "payload-c")]


def test_fold_rerun_is_a_logical_noop(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("fold_rerun_fact")
    state_qt = unique_table("fold_rerun_state")
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    batch_id = _batch_id(11)
    admitted = spark.createDataFrame([_row("c", _T1, "h3", "payload-c")], list(_COLS))
    committed_first = commit.run(
        _make_ctx(spec=spec, batch_id=batch_id, admitted_facts_df=admitted), local_runner_fx
    )
    fold.run(committed_first, local_runner_fx)

    committed_second = commit.run(committed_first, local_runner_fx)  # commit guard-skip rerun

    after = fold.run(committed_second, local_runner_fx)

    # NOT a `snapshot_ids` equality check here: effects/spark.py's own
    # documented empirical finding is that a healthy-rerun MERGE INTO still
    # commits a harmless PHYSICAL snapshot even when logically a no-op
    # (`changed-partition-count = "0"` under merge-on-read) -- `merge`
    # reports that as `MergeResult(None, None)` at the LOGICAL level, which
    # is the contract this stage/test cares about (snapshot_asserts.py's own
    # `assert_no_new_snapshot` docstring names this exact carve-out: it is
    # for stages that make NO fx call at all on a guard-skip, not for
    # fold's own MergeResult no-op).
    assert after.state_snapshot_id is None  # logical no-op, [C-7][T-8]
    assert after.merge_summary is None
    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == [("c", "payload-c")]  # unchanged


def test_fold_empty_committed_facts_skips_merge_entirely(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("fold_empty_fact")
    state_qt = unique_table("fold_empty_state")
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    batch_id = _batch_id(12)
    non_empty_schema = spark.createDataFrame([_row("x", _T1, "h", "p")], list(_COLS)).schema
    empty_admitted = spark.createDataFrame([], non_empty_schema)
    committed_ctx = commit.run(
        _make_ctx(spec=spec, batch_id=batch_id, admitted_facts_df=empty_admitted), local_runner_fx
    )
    assert committed_ctx.committed_facts_df.count() == 0
    before = snapshot_ids(spark, state_qt)

    after = fold.run(committed_ctx, local_runner_fx)

    assert snapshot_ids(spark, state_qt) == before  # merge truly skipped -- no phantom commit
    assert after.state_snapshot_id is None
    assert after.state_read_snapshot_id is None
    assert after.merge_summary is None


# --- publish: durable-sourced payload, unconditional emit (I-19) ------------


def test_publish_payload_is_durable_sourced_and_emits_event(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    qtn_qt = unique_table("publish_happy_qtn")
    fact_qt = unique_table("publish_happy_fact")
    state_qt = unique_table("publish_happy_state")
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        quarantine_table=_bare(qtn_qt), fact_table=_bare(fact_qt), state_table=_bare(state_qt)
    )
    batch_id = _batch_id(13)
    candidate = spark.createDataFrame(
        [_row("c", _T1, "h3", "payload-c"), _row("d", _T1, "h4", "payload-d")], list(_COLS)
    )

    def _flags_d(candidate_df: DataFrame, co_effects: object) -> DataFrame:
        return candidate_df.filter(F.col("domain_id") == "d").withColumn("reason", F.lit("bad d"))

    post_ctx = post_check.run(
        _make_ctx(
            spec=spec, batch_id=batch_id, candidate_facts_df=candidate, post_check_fn=_flags_d
        ),
        local_runner_fx,
    )
    commit_ctx = commit.run(post_ctx, local_runner_fx)
    fold_ctx = fold.run(commit_ctx, local_runner_fx)

    after = publish.run(fold_ctx, local_runner_fx)

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert len(envelopes) == 1
    assert envelopes[0]["detail-type"] == "batch-completed"
    detail = envelopes[0]["detail"]
    assert detail["raw_count"] == fold_ctx.raw_count
    assert detail["pre_quarantined"] == 0
    assert detail["post_quarantined"] == 1
    assert detail["fact_count"] == 1
    assert detail["fact_snapshot_id"] == fold_ctx.fact_snapshot_id
    assert detail["state_snapshot_id"] == fold_ctx.state_snapshot_id
    assert after.published is True
    assert after.completed_event.fact_count == 1


def test_publish_rerun_reemits_with_same_batch_truth_except_declared_exceptions(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    qtn_qt = unique_table("publish_rerun_qtn")
    fact_qt = unique_table("publish_rerun_fact")
    state_qt = unique_table("publish_rerun_state")
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        quarantine_table=_bare(qtn_qt), fact_table=_bare(fact_qt), state_table=_bare(state_qt)
    )
    batch_id = _batch_id(14)
    candidate = spark.createDataFrame([_row("c", _T1, "h3", "payload-c")], list(_COLS))

    post_ctx_1 = post_check.run(
        _make_ctx(spec=spec, batch_id=batch_id, candidate_facts_df=candidate), local_runner_fx
    )
    commit_ctx_1 = commit.run(post_ctx_1, local_runner_fx)
    fold_ctx_1 = fold.run(commit_ctx_1, local_runner_fx)
    first = publish.run(fold_ctx_1, local_runner_fx)
    moto_events_bus.read_events()  # drain the first attempt's event

    post_ctx_2 = post_check.run(post_ctx_1, local_runner_fx)  # post_check guard-skip
    commit_ctx_2 = commit.run(post_ctx_2, local_runner_fx)  # commit guard-skip
    fold_ctx_2 = fold.run(commit_ctx_2, local_runner_fx)  # fold logical no-op
    second = publish.run(fold_ctx_2, local_runner_fx)

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert len(envelopes) == 1  # unconditional re-emit, I-7
    detail = envelopes[0]["detail"]
    first_detail = first.completed_event
    assert detail["raw_count"] == first_detail.raw_count
    assert detail["pre_quarantined"] == first_detail.pre_quarantined
    assert detail["post_quarantined"] == first_detail.post_quarantined
    assert detail["fact_count"] == first_detail.fact_count
    assert detail["fact_snapshot_id"] == first_detail.fact_snapshot_id
    # state_snapshot_id is NOT field-equal here: MERGE INTO commits carry no
    # conveyer.batch-id/stage stamp (only `append` stamps), so a no-op
    # rerun's own MergeResult is None regardless of the ORIGINAL attempt's
    # real commit id -- documented, accepted [C-7][T-8]; R-09 pins the same
    # None value for the empty-batch case. Flagged in the handoff report as
    # a real gap against R-02's blanket "payloads field-equal" claim (M4's
    # scenario-suite concern, not this bead's).
    assert detail["state_snapshot_id"] is None
    assert first_detail.state_snapshot_id is not None
    assert second.published is True
