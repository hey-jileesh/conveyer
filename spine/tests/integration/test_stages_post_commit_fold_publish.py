"""`stages/{post_check,commit,fold,publish}.py` — LLD §7.5, I-11, I-12, I-19,
I-24, [H-1][H-2][E-5][E-14][C-7][T-8]. **Migrated for B10 (bead conveyer-
6pg.22): the N-table register + the mechanical §8.2 fold plan.**

Uses `local_runner_fx` (the REAL production assembly) throughout, matching
every other integration suite in this package (`test_spark_fx.py`,
`test_stages_land_pre_pull_apply.py`). Since this bead's four stages start
mid-sequence (`post_check` needs `candidate_facts`, as if `apply` already
ran), `BatchContext` seeds are built directly with `candidate_facts` (and,
for `commit`/`fold`/`publish` tests, the upstream stage's own output)
pre-populated — bypassing `land`/`pre_check`/`pull`/`apply` entirely, which
is a different bead's file scope.

**Full B10 migration, not a small patch — the drift predates this bead.**
This file's own former "mirrors, never imports `scenario_helpers.py`"
convention (a deliberate, small-helper-duplication choice at v1) is
DROPPED here: 006.1's post_check rewrite (bead conveyer-6pg.13, B3) deleted
`Transforms.post_check` entirely (business rules are now declared data,
`ctx.spec.checks`, never pipeline-authored Python) and 006.1 P-1 replaced
the singular `fact_table`/`state_table`/`candidate_facts_df`/`admitted_
facts_df`/`committed_facts_df` surface with the per-type `fact_types`/
`candidate_facts`/`admitted_facts` register — re-deriving a second,
independent copy of `scenario_helpers.IDENTITY_FACT_SCHEMA`/`create_fact_
table`/`create_state_table`/`create_markers_table_for`/`unique_pipeline`'s
now-substantial correctness burden (F-2's `record_key` column alone sank
`test_scenarios_ledger.py`'s own un-migrated copy, this bead's own finding)
costs far more than the small-helper-duplication convention was meant to
save. Reusing `scenario_helpers` here is the considered trade.

**Two post_check tests retired outright, not merely patched (structurally
unreachable under 006.1's rewrite, cited by section):**

- The OLD "non-conforming custom `post_check` fabricates a row absent from
  `candidate_df`, violating I-12 count identity" scenario has NO surviving
  seam: `stages/post_check.py`'s `run()` composes `frames/business_checks.
  py::evaluate` (a pure `F.when(predicate, struct(...))` FILTER over the
  real candidate frame, this module's own docstring) — there is no
  pipeline-authored code in the post_check path any more that could
  fabricate a row not present in its own input. The count-identity assert
  in `stages/post_check.py::run` (§8.1's FRESH branch) is now a permanent,
  unreachable-by-construction defensive backstop, not a reachable pipeline-
  authoring defect — the SAME shape `test_scenarios_core.py`'s own "K6
  supersedes A-14a" skip already documents for a sibling scenario. Skipped,
  cited.
- The OLD "a nonconforming `reason` string reaches `post_check.run` and
  raises `ValueError` matching 'A-14'" scenario moved ENTIRELY to BIND time:
  `core.model.RowCheckModel.reason` is now `Field(pattern=BUSINESS_REASON_
  RE)` (A-14's grammar, "now bind-time" per that field's own code comment) —
  a nonconforming reason cannot even construct a `RowCheckModel`, let alone
  reach `post_check.run`. Rewritten (not skipped) to assert the REAL, current
  enforcement point: `pydantic.ValidationError` at `RowCheckModel(...)`
  construction, never `post_check.run`.

Table shapes: `scenario_helpers.create_raw_table`/`create_quarantine_table`/
`create_fact_table`/`create_state_table`/`create_markers_table_for` — the
SAME production bootstrap DDL builders every other migrated integration
suite in this package now uses (§6.5's "one authored schema, both
substrates"). `stages/post_check.py`'s own [R2-1] fact-presence probe
(`table_has_batch(fact_table, batch_id, None)`) runs unconditionally per
declared fact type, so every `post_check.run` call site needs a REAL
(possibly empty) fact table — an absent table raises `AnalysisException`,
not `False`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
import scenario_helpers as sh
from pydantic import ValidationError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from snapshot_asserts import snapshot_delta, snapshot_ids
from spine.binding import Transforms
from spine.config import RunConfig
from spine.context import BatchContext
from spine.core import run_facts
from spine.core.checks import checks_version
from spine.core.contract import check_version, read_spec_version
from spine.core.model import ChecksModel, PipelineSpecModel, RowCheckModel
from spine.effects.records import RunnerFx
from spine.stages import commit, fold, post_check, publish

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from tests.conftest import MotoEventsBus

_T1 = datetime(2024, 1, 1, tzinfo=UTC)


def _row(domain_id: str, event_time: str, payload: str) -> tuple[object, ...]:
    return (domain_id, event_time, payload)


def _candidate_df(spark: SparkSession, rows: list[tuple[object, ...]]) -> DataFrame:
    return spark.createDataFrame(rows, ["domain_id", "event_time", "payload"])


def _make_spec(
    *,
    quarantine_table: str,
    fact_table: str,
    state_table: str,
    checks: ChecksModel | None = None,
) -> PipelineSpecModel:
    return sh.make_spec(
        transforms_module="pipelines.identity.transforms",
        raw_table="spine_test_tables.unused_raw",
        quarantine_table=quarantine_table,
        fact_table=fact_table,
        state_table=state_table,
        checks=checks,
        pipeline=sh.unique_pipeline("m3pcfp"),
    )


def _make_ctx(
    *,
    spec: PipelineSpecModel,
    batch_id: str,
    candidate_facts: dict[str, DataFrame] | None = None,
    admitted_facts: dict[str, DataFrame] | None = None,
    raw_count: int = 10,
) -> BatchContext:
    # `Transforms.apply` is never invoked by any stage this file drives
    # directly (post_check/commit/fold/publish all start mid-sequence) --
    # kept only to satisfy the dataclass's own required field. `Transforms.
    # post_check` no longer exists as a field at all (006.1 §4.4's hard
    # cut); `.fold` doesn't either now (critique gate wf_24a3125f-ecc F2,
    # bead conveyer-6pg.31 -- B10's own `stages/fold.py` docstring already
    # noted `Transforms.fold` was never invoked by the mechanical §8.2
    # design, and F2 removed the dead member outright).
    transforms = Transforms(apply=lambda valid_df, co_effects: {"identity": valid_df})
    return BatchContext(
        pipeline=spec.pipeline,
        feed_id="feed/m3-pcfp",
        delivery_id="00000000-0000-4000-8000-000000000001",
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
        read_spec_version=read_spec_version(spec.read),
        check_version=check_version(spec.raw_contract, spec.read),
        checks_version=checks_version(spec.checks),
        raw_count=raw_count,
        co_effects={},
        candidate_facts=candidate_facts,
        admitted_facts=admitted_facts,
    )


def _batch_id(n: int) -> str:
    return sh.batch_id(n)


# --- post_check: fresh compute (count identity asserted, [H-2]) ------------


def test_post_check_fresh_zero_violations_admits_all_no_write(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    qtn_qt = unique_table("post_check_clean_qtn")
    fact_qt = unique_table("post_check_clean_fact")
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)
    spec = _make_spec(
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    batch_id = _batch_id(1)
    candidate = _candidate_df(
        spark, [_row("a", "2024-01-01", "payload-a"), _row("b", "2024-01-01", "payload-b")]
    )
    ctx = _make_ctx(spec=spec, batch_id=batch_id, candidate_facts={"identity": candidate})

    after = post_check.run(ctx, local_runner_fx)

    assert after.post_quarantined_count == 0
    assert after.post_quarantine_snapshot_id is None
    assert after.admitted_facts["identity"].count() == 2
    assert local_runner_fx.table_has_batch(sh.bare(qtn_qt), batch_id, "post_check") is False


def test_post_check_fresh_with_violations_quarantines_and_holds_count_identity(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    qtn_qt = unique_table("post_check_dirty_qtn")
    fact_qt = unique_table("post_check_dirty_fact")
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)
    checks = ChecksModel(
        checks=[
            RowCheckModel(
                kind="row",
                id="no-d",
                fact_type="identity",
                expr="domain_id != 'd'",
                reason="business/bad-payload",
            )
        ]
    )
    spec = _make_spec(
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table="spine_test_tables.unused_state",
        checks=checks,
    )
    batch_id = _batch_id(2)
    candidate = _candidate_df(
        spark, [_row("c", "2024-01-01", "payload-c"), _row("d", "2024-01-01", "payload-d")]
    )
    ctx = _make_ctx(spec=spec, batch_id=batch_id, candidate_facts={"identity": candidate})

    after = post_check.run(ctx, local_runner_fx)

    assert after.post_quarantined_count == 1
    assert after.post_quarantine_snapshot_id is not None
    admitted_ids = sorted(r["domain_id"] for r in after.admitted_facts["identity"].collect())
    assert admitted_ids == ["c"]
    assert local_runner_fx.table_has_batch(sh.bare(qtn_qt), batch_id, "post_check") is True


@pytest.mark.skip(
    reason=(
        "structurally unreachable under 006.1's post_check rewrite (bead "
        "conveyer-6pg.13, B3): business rules are declared data evaluated by "
        "the framework's own pure interpreter (frames/business_checks.py::"
        "evaluate) -- there is no pipeline-authored code left in the "
        "post_check path that could fabricate a violation row absent from "
        "its own candidate input, so the I-12 count-identity assert in "
        "stages/post_check.py::run is a permanent, unreachable-by-"
        "construction backstop (the same shape as test_scenarios_core.py's "
        "own 'K6 supersedes A-14a' skip). No named future wait -- this is a "
        "closed architectural fact, not a pending dependency."
    )
)
def test_post_check_fresh_non_conforming_transform_raises_loudly() -> None:
    pass


def test_post_check_nonconforming_reason_grammar_rejected_at_bind() -> None:
    """B10-rewritten (module docstring): A-14's grammar enforcement moved
    ENTIRELY to bind time (`core.model.RowCheckModel.reason`'s own `Field
    (pattern=BUSINESS_REASON_RE)`) -- a nonconforming reason can no longer
    reach `post_check.run` at all; it never survives `RowCheckModel(...)`
    construction. This is the REAL, current enforcement point."""
    with pytest.raises(ValidationError):
        RowCheckModel(
            kind="row",
            id="bad-reason-check",
            fact_type="identity",
            expr="payload != 'z'",
            reason="not-a-valid-reason",
        )


# --- post_check: guard-skip (rerun) path, durable subtraction [E-5][H-2] ----


def test_post_check_guard_skip_uses_durable_quarantine_rows(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    qtn_qt = unique_table("post_check_rerun_qtn")
    fact_qt = unique_table("post_check_rerun_fact")
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)
    checks = ChecksModel(
        checks=[
            RowCheckModel(
                kind="row",
                id="no-d",
                fact_type="identity",
                expr="domain_id != 'd'",
                reason="business/bad-payload",
            )
        ]
    )
    spec = _make_spec(
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table="spine_test_tables.unused_state",
        checks=checks,
    )
    batch_id = _batch_id(4)
    candidate = _candidate_df(
        spark, [_row("c", "2024-01-01", "payload-c"), _row("d", "2024-01-01", "payload-d")]
    )
    ctx = _make_ctx(spec=spec, batch_id=batch_id, candidate_facts={"identity": candidate})
    first = post_check.run(ctx, local_runner_fx)

    before = snapshot_ids(spark, qtn_qt)

    second = post_check.run(first, local_runner_fx)

    assert snapshot_ids(spark, qtn_qt) == before  # guard-skip: no new write
    assert second.post_quarantined_count == 1  # durable count, not recomputed
    assert second.post_quarantine_snapshot_id == first.post_quarantine_snapshot_id
    assert sorted(r["domain_id"] for r in second.admitted_facts["identity"].collect()) == ["c"]


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
    fact_qt = unique_table("post_check_drift_fact")
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)
    checks = ChecksModel(
        checks=[
            RowCheckModel(
                kind="row",
                id="no-f",
                fact_type="identity",
                expr="domain_id != 'f'",
                reason="business/bad-f",
            )
        ]
    )
    spec = _make_spec(
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table="spine_test_tables.unused_state",
        checks=checks,
    )
    batch_id = _batch_id(5)
    candidate = _candidate_df(spark, [_row("f", "2024-01-01", "payload-f")])
    ctx = _make_ctx(spec=spec, batch_id=batch_id, candidate_facts={"identity": candidate})
    first = post_check.run(ctx, local_runner_fx)
    assert first.post_quarantined_count == 1  # only "f" quarantined durably

    # Rerun whose recomputed CANDIDATE no longer contains "f" at all -- a
    # genuine cross-attempt perception drift (I-12 [E-5]): the durable
    # violation's value-tuple has nothing to anti-join against, so it
    # saturates at zero (removes nothing) and the count identity fails.
    drifted_candidate = _candidate_df(
        spark, [_row("g", "2024-01-01", "payload-g"), _row("hh", "2024-01-01", "payload-hh")]
    )
    rerun_ctx = replace(first, candidate_facts={"identity": drifted_candidate})

    second = post_check.run(rerun_ctx, local_runner_fx)

    admitted_ids = sorted(r["domain_id"] for r in second.admitted_facts["identity"].collect())
    assert admitted_ids == ["g", "hh"]  # durable "f" row matched nothing -- nothing removed
    assert second.post_quarantined_count == 1  # durable count, not the recomputed 0
    assert second.post_quarantine_snapshot_id == first.post_quarantine_snapshot_id

    # The drift is folded into `ctx.post_check_drift` -- counts only, no row
    # values [S-7] -- and, via `run_facts.transition`, into the run-ledger
    # row's `error_message` for this (non-failed) transition.
    assert second.post_check_drift is not None
    assert second.post_check_drift.startswith("post_check drift: durable=1 recomputed=0 ")
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
    sh.create_fact_table(spark, fact_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=sh.bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = _batch_id(6)
    admitted = _candidate_df(spark, [_row("c", "2024-01-01", "payload-c")])
    ctx = _make_ctx(spec=spec, batch_id=batch_id, admitted_facts={"identity": admitted})

    before = snapshot_ids(spark, fact_qt)

    after = commit.run(ctx, local_runner_fx)

    new_id, summary = snapshot_delta(spark, fact_qt, before)  # R-13: exactly one commit
    assert summary.get("conveyer.batch-id") == batch_id
    assert after.facts_appended_by_table[sh.bare(fact_qt)] == 1
    assert after.commit_snapshot_ids[sh.bare(fact_qt)] == new_id
    committed = local_runner_fx.read_batch(sh.bare(fact_qt), batch_id)
    committed_ids = sorted(r["domain_id"] for r in committed.collect())
    assert committed_ids == ["c"]
    stamped_row = spark.table(fact_qt).where(f"batch_id = '{batch_id}'").collect()[0]
    assert stamped_row["delivery_id"] == ctx.delivery_id
    assert stamped_row["feed_id"] == ctx.feed_id
    assert stamped_row["source_ts"] is not None
    # U-1 (bead conveyer-swb.11): HLD 007 D-3(b) -- `source_ts` := the
    # delivery's own `received_at`, stamped at commit, never a literal
    # `NULL` (the pre-fix behavior this test used to leave unasserted).
    # `.collect()`'s driver-side `TimestampType` marshaling uses the
    # OS-local zone regardless of `spark.sql.session.timeZone`
    # ([[spine-quarantine-udf-and-timestamp-hazard]]) -- assert via an
    # in-Spark equality filter, never a Python `datetime` equality on the
    # already-collected value.
    assert (
        spark.table(fact_qt)
        .where(f"batch_id = '{batch_id}'")
        .filter(F.col("source_ts") == F.lit(ctx.received_at))
        .count()
        == 1
    )


def test_commit_guard_skip_rerun_zero_appended_same_snapshot(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("commit_rerun_fact")
    sh.create_fact_table(spark, fact_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=sh.bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = _batch_id(7)
    admitted = _candidate_df(spark, [_row("c", "2024-01-01", "payload-c")])
    ctx = _make_ctx(spec=spec, batch_id=batch_id, admitted_facts={"identity": admitted})
    first = commit.run(ctx, local_runner_fx)

    before = snapshot_ids(spark, fact_qt)

    second = commit.run(first, local_runner_fx)

    assert snapshot_ids(spark, fact_qt) == before  # guard-skip: no new write
    assert second.facts_appended_by_table[sh.bare(fact_qt)] == 0
    assert sh.bare(fact_qt) not in second.commit_snapshot_ids  # absent key = skip (§4.2)
    assert first.commit_snapshot_ids[sh.bare(fact_qt)] == local_runner_fx.resolve_batch_snapshot(
        sh.bare(fact_qt), batch_id, None
    )
    committed = local_runner_fx.read_batch(sh.bare(fact_qt), batch_id)
    assert sorted(r["domain_id"] for r in committed.collect()) == ["c"]


def test_commit_null_domain_id_raises_named_defect_no_append(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("commit_null_domain_fact")
    sh.create_fact_table(spark, fact_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=sh.bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = _batch_id(8)
    admitted = _candidate_df(spark, [_row("z", "2024-01-01", "payload-null")]).withColumn(
        "domain_id", F.lit(None).cast("string")
    )
    ctx = _make_ctx(spec=spec, batch_id=batch_id, admitted_facts={"identity": admitted})

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
    sh.create_fact_table(spark, fact_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=sh.bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = _batch_id(9)
    admitted = _candidate_df(spark, [_row("h", "2024-01-01", "payload-h")]).withColumn(
        "extra_col", F.lit("unexpected-extra")
    )
    ctx = _make_ctx(spec=spec, batch_id=batch_id, admitted_facts={"identity": admitted})

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
    sh.create_fact_table(spark, fact_qt)
    sh.create_state_table(spark, state_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=sh.bare(fact_qt),
        state_table=sh.bare(state_qt),
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = _batch_id(10)
    admitted = _candidate_df(spark, [_row("c", "2024-01-01", "payload-c")])
    committed_ctx = commit.run(
        _make_ctx(spec=spec, batch_id=batch_id, admitted_facts={"identity": admitted}),
        local_runner_fx,
    )

    before = snapshot_ids(spark, state_qt)

    after = fold.run(committed_ctx, local_runner_fx)

    # B10: `stages/fold.py` no longer performs a separate state read at all
    # (module docstring) -- the singular `BatchContext.state_read_
    # snapshot_id` field this once would have asserted `None` on was
    # deleted outright (critique gate wf_24a3125f-ecc ruling 1, bead
    # conveyer-6pg.29, F4); the per-table maps are the real signal.
    new_id, _summary = snapshot_delta(spark, state_qt, before)
    assert after.fold_snapshot_ids[sh.bare(state_qt)] == new_id
    assert after.rows_merged_by_table[sh.bare(state_qt)] == 1
    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == [("c", "payload-c")]


def test_fold_rerun_is_a_logical_noop(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("fold_rerun_fact")
    state_qt = unique_table("fold_rerun_state")
    sh.create_fact_table(spark, fact_qt)
    sh.create_state_table(spark, state_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=sh.bare(fact_qt),
        state_table=sh.bare(state_qt),
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = _batch_id(11)
    admitted = _candidate_df(spark, [_row("c", "2024-01-01", "payload-c")])
    committed_first = commit.run(
        _make_ctx(spec=spec, batch_id=batch_id, admitted_facts={"identity": admitted}),
        local_runner_fx,
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
    assert sh.bare(state_qt) not in after.fold_snapshot_ids  # logical no-op, [C-7][T-8]
    assert after.rows_merged_by_table[sh.bare(state_qt)] == 0
    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == [("c", "payload-c")]  # unchanged


def test_fold_empty_committed_facts_skips_merge_entirely(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    fact_qt = unique_table("fold_empty_fact")
    state_qt = unique_table("fold_empty_state")
    sh.create_fact_table(spark, fact_qt)
    sh.create_state_table(spark, state_qt)
    spec = _make_spec(
        quarantine_table="spine_test_tables.unused_qtn",
        fact_table=sh.bare(fact_qt),
        state_table=sh.bare(state_qt),
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = _batch_id(12)
    empty_admitted = _candidate_df(spark, [_row("x", "2024-01-01", "p")]).limit(0)
    committed_ctx = commit.run(
        _make_ctx(spec=spec, batch_id=batch_id, admitted_facts={"identity": empty_admitted}),
        local_runner_fx,
    )
    assert committed_ctx.facts_appended_by_table[sh.bare(fact_qt)] == 0

    before = snapshot_ids(spark, state_qt)

    after = fold.run(committed_ctx, local_runner_fx)

    assert snapshot_ids(spark, state_qt) == before  # merge truly skipped -- no phantom commit
    assert sh.bare(state_qt) not in after.fold_snapshot_ids
    assert after.rows_merged_by_table[sh.bare(state_qt)] == 0


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
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)
    sh.create_state_table(spark, state_qt)
    checks = ChecksModel(
        checks=[
            RowCheckModel(
                kind="row",
                id="no-d",
                fact_type="identity",
                expr="domain_id != 'd'",
                reason="business/bad-d",
            )
        ]
    )
    spec = _make_spec(
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table=sh.bare(state_qt),
        checks=checks,
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = _batch_id(13)
    candidate = _candidate_df(
        spark, [_row("c", "2024-01-01", "payload-c"), _row("d", "2024-01-01", "payload-d")]
    )

    post_ctx = post_check.run(
        _make_ctx(spec=spec, batch_id=batch_id, candidate_facts={"identity": candidate}),
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
    assert detail["fact_snapshot_id"] == after.completed_event.fact_snapshot_id
    assert detail["state_snapshot_id"] == after.completed_event.state_snapshot_id
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
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)
    sh.create_state_table(spark, state_qt)
    spec = _make_spec(
        quarantine_table=sh.bare(qtn_qt), fact_table=sh.bare(fact_qt), state_table=sh.bare(state_qt)
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = _batch_id(14)
    candidate = _candidate_df(spark, [_row("c", "2024-01-01", "payload-c")])

    post_ctx_1 = post_check.run(
        _make_ctx(spec=spec, batch_id=batch_id, candidate_facts={"identity": candidate}),
        local_runner_fx,
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
    # None value for the empty-batch case (`[[spine-post-commit-fold-
    # publish-gaps]]` point 4, carried forward unchanged by B10's rewrite of
    # `stages/publish.py`, that module's own docstring).
    assert detail["state_snapshot_id"] is None
    assert first_detail.state_snapshot_id is not None
    assert second.published is True
