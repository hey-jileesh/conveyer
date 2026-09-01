"""R-03 (KillFx, 8 kill points), R-08 (merge-conflict rerun), R-13 (one-commit
invariant), A-07(b), A-10's hash-keyed-rerun leg — LLD §12.4, I-4, I-19
[T-4], [E-16]; 005.1 §6.5/§8.2.4, A-9, A-10, A-14.

Bead `conveyer-nvh.23`, M4. Reuses `scenario_helpers.py`'s table-DDL and
spec/seed-building helpers by import (`tests/` has no `__init__.py`
anywhere in this repo — same-directory modules import as bare top-level
names, per `[[spine-test-basename-collisions-and-scope-conflicts]]`/
`[[spine-merge-noop-and-append-signature]]`'s point 5) rather than
re-deriving them -- promoted (critique F7, bead conveyer-nvh.43) out of
`test_scenarios_core.py`, which this file used to import these from
directly (a test file importing another test file's helpers), into a
dedicated non-test module, the same shape `killfx.py`/`snapshot_asserts.py`
already are; `killfx.py` (this directory) supplies the two
failure-injection wrappers R-03/R-08 both need, over `make_wrapped_fx`'s
mechanism (`tests/conftest.py`, architect G-3).

**R-03/R-13 fixture choice, and why it differs from R-01's own.** R-03 and
R-13 each need a specific fixture shape for a different reason:

- R-03 needs EVERY one of the 7 non-`apply` stages to make a genuine,
  killable `fx` call on a single fresh attempt. The identity CLEAN fixture
  (R-01's own) has zero violations, so `pre_check`/`post_check` never call
  `fx.append` at all (§7.5's "zero violations ⇒ no write, no guard row"
  branch, `[[spine-stages-append-return-and-stage-key]]` point 3) — killing
  "after pre_check's/post_check's commit" needs a fixture that actually
  produces one, so R-03 reuses R-04's own **violations** fixture instead
  (`pipelines.identity_violations`, `tests/exemplar/identity/fixtures/
  violations/object_1.csv`: 4 raw rows, one null-`domain_id` (pre_check
  violation), one `payload == "INVALID"` (post_check violation), 2 admitted
  facts — `id-101`/`delta` and `id-103`/`foxtrot`). `pull` similarly never
  calls `fx.read_table` at all when `spec.co_effects` is empty (identity's
  own deployed spec declares none) — a harmless `probe` co-effect (a bare,
  always-empty Iceberg table) gives `pull` exactly one real read to kill
  after, with zero behavioral effect on `apply`/`post_check` (both discard
  `co_effects` unconditionally, `pipelines/identity/transforms.py`'s own
  module docstring).
- R-13 uses the CLEAN fixture instead, matching R-01 literally (not just in
  spirit): with zero violations, `pre_check`/`post_check` make NO commit at
  all — exactly the "pre_check → quarantine only when violations" case
  R-13's own LLD text names — so the quarantine table gets a clean,
  zero-new-snapshot assertion across a full 8-stage run, and only
  `land`/`commit`/`fold` need a one-new-snapshot check.

**Kill-point call-order table** (R-03's 8 params — verified empirically
against real Spark/Iceberg via a scratch script, `repl-driven-python`'s
workflow, before this file was written). Across ONE fresh attempt over the
violations+probe fixture: `RunnerFx.append` is called exactly 4 times, in
`SEQUENCE` order (`land`, `pre_check`, `post_check`, `commit`); `read_table`
exactly 3 times (`pull`, then `commit`'s own I-24 structural check, then
`fold`'s state read) but `pull`'s is always the FIRST since it runs earliest
in `SEQUENCE`; `merge` exactly once (`fold`); `emit` exactly twice (`land`'s
"batch-started", then `publish`'s "batch-completed"). `killfx.kill_after`'s
counter is per-FIELD, not shared globally (its own docstring) — so a
`(field, occurrence)` pair identifies one specific stage's own commit
unambiguously:

| label      | field      | occ | before | killed stage | raw snaps @1 | events @1 |
|------------|------------|----:|--------|---------------|-------------:|----------:|
| pre_land   | append     |  1  | True   | land          | 0 (no commit) | 0        |
| land       | append     |  1  | False  | land          | 1            | 0 (pre-emit) |
| pre_check  | append     |  2  | False  | pre_check     | 1            | 1 (started)  |
| pull       | read_table |  1  | False  | pull          | 1            | 1         |
| post_check | append     |  3  | False  | post_check    | 1            | 1         |
| commit     | append     |  4  | False  | commit        | 1            | 1         |
| fold       | merge      |  1  | False  | fold          | 1            | 1         |
| publish    | emit       |  2  | False  | publish       | 1            | 2 (both out) |

Whichever point kills the attempt, `run.py`'s own `except BaseException`
clause (§7.3) records a `"failed"` `RunFact` for the stage that was actually
executing (never a separate "pre_land" stage name — the pre-land kill point
fires from INSIDE `land.run`, before its own `fx.append` ever executes, so
the ledger's failed row still names `stage="land"`) and re-raises
`SimulatedKill` unchanged, unwrapped — the whole job "fails" exactly as a
real process kill would. A FRESH `BatchContext` (same `batch_id`, a fresh
seed, plain `local_runner_fx` with no wrapper) then converges to the same
end state an unkilled run reaches (R-01/R-04's own convergence pattern):
every already-durably-committed effect is guard-skipped, every not-yet-
committed one runs for the first time, and the durable fact/quarantine/state
content is identical regardless of which point killed the first attempt.

**N4 additions (bead conveyer-azr.20, n4-rerun-matrix): A-07(b) and A-10's
own hash-keyed-rerun leg.** Both reuse this file's `kill_after`/
`make_wrapped_fx` machinery rather than the R-03 parametrized matrix itself
(neither is one of R-03's own 8 kill points — A-07(b) kills at the SAME
point R-03's own "pre_check" row does, occurrence 2, but then reruns under a
DELIBERATELY MUTATED contract rather than an unchanged one; A-10 kills at
R-03's own "post_check" point, occurrence 3, then asserts A-10's own
`row_hash`/zero-new-quarantine-rows claims specifically, which R-03's own
parametrized case never inspects). Every new leg was scratch-validated
against a real local Spark/Iceberg session (`uv run -p 3.11 --package
conveyer-spine python <script>`) before being written here — the exact
drift-string shape and cast-failure-retention behavior in A-07(b) are
probe-confirmed, not derived from the LLD text alone.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import killfx
import pytest
from pyspark.sql import SparkSession
from scenario_helpers import FIXTURES_DIR as _FIXTURES_DIR
from scenario_helpers import IDENTITY_FACT_SCHEMA as _IDENTITY_FACT_SCHEMA
from scenario_helpers import IDENTITY_RAW_CONTRACT as _IDENTITY_RAW_CONTRACT
from scenario_helpers import IDENTITY_READ as _IDENTITY_READ
from scenario_helpers import VIOLATIONS_CHECKS as _VIOLATIONS_CHECKS
from scenario_helpers import bare as _bare
from scenario_helpers import batch_id as _batch_id
from scenario_helpers import create_fact_table as _create_fact_table
from scenario_helpers import create_markers_table_for as _create_markers_table_for
from scenario_helpers import create_quarantine_table as _create_quarantine_table
from scenario_helpers import create_raw_table as _create_raw_table
from scenario_helpers import create_state_table as _create_state_table
from scenario_helpers import facts_appended_total as _facts_appended_total
from scenario_helpers import quarantine_rows as _quarantine_rows
from scenario_helpers import rows_merged_total as _rows_merged_total
from scenario_helpers import unique_pipeline as _unique_pipeline
from snapshot_asserts import (
    assert_no_new_snapshot,
    assert_stamped_batch,
    snapshot_delta,
    snapshot_ids,
)
from spine.binding import bind_transforms
from spine.config import RunConfig
from spine.context import BatchContext
from spine.core.checks import checks_version
from spine.core.contract import check_version, read_spec_version
from spine.core.model import (
    ChecksModel,
    CoEffectDecl,
    ColumnSpec,
    FactTypeModel,
    PipelineSpecModel,
    RawContractModel,
)
from spine.effects.records import RunnerFx, TransientError
from spine.run import run as run_sequence
from spine.stages import land, pre_check

if TYPE_CHECKING:
    from pathlib import Path

    from tests.conftest import LedgerCatalogFixture, MotoEventsBus

_WrapperMap = Mapping[str, Callable[[Callable[..., Any]], Callable[..., Any]]]
_MakeWrappedFx = Callable[[RunnerFx, _WrapperMap], RunnerFx]

_VIOLATIONS_OBJECT_URIS = (str(_FIXTURES_DIR / "violations" / "object_1.csv"),)
_CLEAN_OBJECT_URIS = (
    str(_FIXTURES_DIR / "clean" / "object_1.csv"),
    str(_FIXTURES_DIR / "clean" / "object_2.csv"),
)
_VIOLATIONS_FACT_GOLDEN = [("id-101", "delta"), ("id-103", "foxtrot")]
_CLEAN_FACT_GOLDEN = [("id-001", "alpha"), ("id-002", "bravo"), ("id-003", "charlie")]


def _create_probe_table(spark: SparkSession, qualified_table: str) -> None:
    """A bare, always-empty co-effect target — `pull` needs one real
    `fx.read_table` call to have something to kill after; `apply`/
    `post_check` both discard `co_effects` unconditionally (module
    docstring), so its content (there is none) is irrelevant."""
    spark.sql(f"CREATE TABLE {qualified_table} (id INT) USING iceberg")


def _make_spec_with_probe(
    *,
    transforms_module: str,
    raw_table: str,
    quarantine_table: str,
    fact_table: str,
    state_table: str,
    probe_table: str,
    pipeline: str | None = None,
    checks: ChecksModel | None = None,
) -> PipelineSpecModel:
    # B10 (bead conveyer-6pg.22): per-call-site unique `pipeline` -- see
    # `scenario_helpers.unique_pipeline`'s own docstring for why a shared
    # literal would collide every test's markers table onto one name.
    return PipelineSpecModel(
        pipeline=pipeline if pipeline is not None else _unique_pipeline("kill"),
        transforms_module=transforms_module,
        co_effects={"probe": CoEffectDecl(table=probe_table, own_state=False)},
        raw_table=raw_table,
        quarantine_table=quarantine_table,
        fact_types={
            "identity": FactTypeModel(
                fact_table=fact_table, state_table=state_table, schema=_IDENTITY_FACT_SCHEMA
            )
        },
        checks=checks if checks is not None else ChecksModel(),
        read=_IDENTITY_READ,
        raw_contract=_IDENTITY_RAW_CONTRACT,
    )


def _fact_types(fact_table: str, state_table: str) -> dict[str, FactTypeModel]:
    """006.1 P-1: the per-type `fact_types` mapping every direct
    `PipelineSpecModel(...)` construction in this file now needs, in place
    of the deleted singular `fact_table`/`state_table` fields -- mirrors
    `test_scenarios_core.py`'s own identically-named helper."""
    return {
        "identity": FactTypeModel(
            fact_table=fact_table, state_table=state_table, schema=_IDENTITY_FACT_SCHEMA
        )
    }


def _make_seed(
    *,
    spec: PipelineSpecModel,
    batch_id: str,
    attempt_id: str,
    object_uris: tuple[str, ...],
) -> BatchContext:
    """Same shape as `scenario_helpers.make_seed`, plus an explicit
    `attempt_id` (that helper hardcodes `"attempt-1"`) — R-08's 3-attempt
    flow needs each attempt distinguishable in the run ledger."""
    return BatchContext(
        pipeline=spec.pipeline,
        feed_id="feed/identity",
        delivery_id=str(uuid.UUID(int=1, version=4)),
        batch_id=batch_id,
        delivery_key="statement.csv",
        content_hash="sha256:" + "a" * 64,
        object_uris=object_uris,
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        spec=spec,
        run=RunConfig(),
        transforms=bind_transforms(spec),
        attempt_id=attempt_id,
        sfn_retry_count=0,
        sfn_redrive_count=0,
        read_spec_version=read_spec_version(spec.read),
        check_version=check_version(spec.raw_contract, spec.read),
        checks_version=checks_version(spec.checks),
    )


def _assert_converged_violations(spark: SparkSession, fact_qt: str, state_qt: str) -> None:
    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == _VIOLATIONS_FACT_GOLDEN
    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == _VIOLATIONS_FACT_GOLDEN  # distinct domain_ids -- one winner each


def _failed_rows(
    ledger_catalog: LedgerCatalogFixture, batch_id: str, attempt_id: str
) -> list[dict[str, object]]:
    return [
        r
        for r in ledger_catalog.rows()
        if r["batch_id"] == batch_id and r["attempt_id"] == attempt_id and r["outcome"] == "failed"
    ]


# --- R-03: KillFx at each of the 8 kill points, fresh rerun converges -------

_KILL_POINTS: tuple[tuple[str, str, int, bool, str, int, int], ...] = (
    # (label, fx_field, occurrence, before, expected_failed_stage,
    #  expected_new_raw_snapshots_after_attempt_1, expected_events_after_attempt_1)
    ("pre_land", "append", 1, True, "land", 0, 0),
    ("land", "append", 1, False, "land", 1, 0),
    ("pre_check", "append", 2, False, "pre_check", 1, 1),
    ("pull", "read_table", 1, False, "pull", 1, 1),
    ("post_check", "append", 3, False, "post_check", 1, 1),
    ("commit", "append", 4, False, "commit", 1, 1),
    ("fold", "merge", 1, False, "fold", 1, 1),
    ("publish", "emit", 2, False, "publish", 1, 2),
)


@pytest.mark.parametrize(
    "label,fx_field,occurrence,before,expected_stage,expected_raw_snaps,expected_events",
    _KILL_POINTS,
    ids=[point[0] for point in _KILL_POINTS],
)
def test_r03_kill_at_each_point_then_restart_converges(
    label: str,
    fx_field: str,
    occurrence: int,
    before: bool,
    expected_stage: str,
    expected_raw_snaps: int,
    expected_events: int,
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    ledger_catalog: LedgerCatalogFixture,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
    make_wrapped_fx: _MakeWrappedFx,
) -> None:
    raw_qt = unique_table(f"r03_{label}_raw")
    qtn_qt = unique_table(f"r03_{label}_qtn")
    fact_qt = unique_table(f"r03_{label}_fact")
    state_qt = unique_table(f"r03_{label}_state")
    probe_qt = unique_table(f"r03_{label}_probe")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    _create_probe_table(spark, probe_qt)
    spec = _make_spec_with_probe(
        transforms_module="pipelines.identity_violations.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
        probe_table=_bare(probe_qt),
        # `label` (e.g. "pre_land") may carry `_`, illegal in the pipeline
        # grammar's segment (`_PIPELINE_SEGMENT` -- lowercase alnum + single
        # dashes only) -- stripped, uniqueness still comes from `unique_
        # pipeline`'s own uuid suffix.
        pipeline=_unique_pipeline(f"r03{label.replace('_', '')}"),
        checks=_VIOLATIONS_CHECKS,
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(300)

    killed_fx = make_wrapped_fx(
        local_runner_fx, {fx_field: killfx.kill_after(occurrence, before=before)}
    )
    raw_before = snapshot_ids(spark, raw_qt)

    seed1 = _make_seed(
        spec=spec, batch_id=batch_id, attempt_id="attempt-1", object_uris=_VIOLATIONS_OBJECT_URIS
    )
    with pytest.raises(killfx.SimulatedKill):
        run_sequence(seed1, killed_fx)

    failed = _failed_rows(ledger_catalog, batch_id, "attempt-1")
    assert len(failed) == 1
    assert failed[0]["stage"] == expected_stage
    assert failed[0]["error_type"] == "SimulatedKill"

    raw_after_attempt1 = snapshot_ids(spark, raw_qt)
    assert len(raw_after_attempt1 - raw_before) == expected_raw_snaps
    events_after_attempt1 = moto_events_bus.read_events()
    assert len(events_after_attempt1) == expected_events

    # Fresh seed, SAME batch_id, CLEAN fx (no wrapper) -- a real restart.
    seed2 = _make_seed(
        spec=spec, batch_id=batch_id, attempt_id="attempt-2", object_uris=_VIOLATIONS_OBJECT_URIS
    )
    result2 = run_sequence(seed2, local_runner_fx)

    assert result2.raw_count == 4  # durable read-back, every attempt (D-3)
    assert result2.published is True
    assert _failed_rows(ledger_catalog, batch_id, "attempt-2") == []

    # Durable content converges regardless of which point killed attempt-1
    # (I-12's own quarantine-never-drops identity, reused from R-04):
    pre_rows = _quarantine_rows(spark, qtn_qt, batch_id, "pre_check")
    assert len(pre_rows) == 1
    assert pre_rows[0]["domain_id"] is None
    post_rows = _quarantine_rows(spark, qtn_qt, batch_id, "post_check")
    assert len(post_rows) == 1
    # 005.1 §4.2's fixed, candidate-independent quarantine shape: the row's
    # own candidate columns (incl. `payload`) live inside `row_snapshot`
    # (JSON), not as a table column -- see `test_scenarios_core.py`'s R-04
    # identical fix.
    assert '"payload":"INVALID"' in post_rows[0]["row_snapshot"]
    _assert_converged_violations(spark, fact_qt, state_qt)


# --- R-08: merge-conflict rerun ---------------------------------------------


def test_r08_merge_conflict_rerun_converges_then_healthy_rerun_is_a_logical_noop(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    ledger_catalog: LedgerCatalogFixture,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
    make_wrapped_fx: _MakeWrappedFx,
) -> None:
    raw_qt = unique_table("r08_raw")
    qtn_qt = unique_table("r08_qtn")
    fact_qt = unique_table("r08_fact")
    state_qt = unique_table("r08_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = PipelineSpecModel(
        pipeline=_unique_pipeline("r08"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_types=_fact_types(_bare(fact_qt), _bare(state_qt)),
        read=_IDENTITY_READ,
        raw_contract=_IDENTITY_RAW_CONTRACT,
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(380)

    # A SINGLE flaky-merge fx, reused across attempts 1 and 2: the wrapper's
    # own one-shot contract (`killfx.flaky_once`) is what makes attempt 2's
    # merge call pass through for real -- not a fresh wrapper per attempt.
    flaky_fx = make_wrapped_fx(
        local_runner_fx,
        {"merge": killfx.flaky_once(lambda: TransientError("simulated merge conflict"))},
    )

    # Attempt 1: the injected merge conflict fails the job at fold.
    seed1 = _make_seed(
        spec=spec, batch_id=batch_id, attempt_id="attempt-1", object_uris=_CLEAN_OBJECT_URIS
    )
    with pytest.raises(TransientError):
        run_sequence(seed1, flaky_fx)

    failed = _failed_rows(ledger_catalog, batch_id, "attempt-1")
    assert len(failed) == 1
    assert failed[0]["stage"] == "fold"
    assert failed[0]["error_type"] == "TransientError"

    # Attempt 2: SAME flaky_fx (now past its one shot) -- real recovery, a
    # genuine (non-no-op) merge since this batch's facts are new.
    seed2 = _make_seed(
        spec=spec, batch_id=batch_id, attempt_id="attempt-2", object_uris=_CLEAN_OBJECT_URIS
    )
    result2 = run_sequence(seed2, flaky_fx)
    assert result2.published is True
    assert result2.completed_event.state_snapshot_id is not None
    assert _failed_rows(ledger_catalog, batch_id, "attempt-2") == []

    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == _CLEAN_FACT_GOLDEN

    # Attempt 3: a plain, unwrapped, healthy rerun -- I-19's logical no-op
    # merge contract, observed via the run ledger's OWN fold row (not just
    # `result3`/context fields): `snapshot_id` (this ledger column is `core/
    # run_facts.py::one_snapshot(ctx.fold_snapshot_ids)` for a fold row) null,
    # `rows_merged` zero.
    seed3 = _make_seed(
        spec=spec, batch_id=batch_id, attempt_id="attempt-3", object_uris=_CLEAN_OBJECT_URIS
    )
    result3 = run_sequence(seed3, local_runner_fx)
    assert result3.fold_snapshot_ids == {}
    assert _rows_merged_total(result3) == 0

    fold_rows_attempt3 = [
        r
        for r in ledger_catalog.rows()
        if r["batch_id"] == batch_id and r["attempt_id"] == "attempt-3" and r["stage"] == "fold"
    ]
    assert len(fold_rows_attempt3) == 1
    assert fold_rows_attempt3[0]["outcome"] == "ok"  # fold has no presence guard (§7.5)
    assert fold_rows_attempt3[0]["snapshot_id"] is None
    assert fold_rows_attempt3[0]["rows_merged"] == 0

    fact_rows_final = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows_final == _CLEAN_FACT_GOLDEN  # unchanged by the no-op rerun


# --- R-13: one-commit invariant (I-4) ---------------------------------------


def test_r13_one_commit_invariant_per_effectful_stage_then_rerun_advances_zero(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("r13_raw")
    qtn_qt = unique_table("r13_qtn")
    fact_qt = unique_table("r13_fact")
    state_qt = unique_table("r13_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = PipelineSpecModel(
        pipeline=_unique_pipeline("r13"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_types=_fact_types(_bare(fact_qt), _bare(state_qt)),
        read=_IDENTITY_READ,
        raw_contract=_IDENTITY_RAW_CONTRACT,
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(390)

    raw_before = snapshot_ids(spark, raw_qt)
    qtn_before = snapshot_ids(spark, qtn_qt)
    fact_before = snapshot_ids(spark, fact_qt)
    state_before = snapshot_ids(spark, state_qt)

    seed1 = _make_seed(
        spec=spec, batch_id=batch_id, attempt_id="attempt-1", object_uris=_CLEAN_OBJECT_URIS
    )
    result1 = run_sequence(seed1, local_runner_fx)
    assert result1.raw_count == 3
    assert _facts_appended_total(result1) == 3

    # land -> raw: exactly one new snapshot, stamped with conveyer.batch-id
    # (land's own stage_key is None -- no conveyer.stage property on this
    # write, [[spine-stages-append-return-and-stage-key]] point 2 -- so only
    # the batch-id half of the stamp is checked here).
    _, raw_summary = snapshot_delta(spark, raw_qt, raw_before)
    assert_stamped_batch(raw_summary, batch_id)

    # pre_check/post_check -> quarantine: zero violations in this (CLEAN)
    # fixture, so "only when violations" means zero commits at all here --
    # R-04's own violations-fixture test already covers the has-a-commit
    # case with its own stamped check_stage assertions.
    assert_no_new_snapshot(spark, qtn_qt, qtn_before)

    # commit -> facts: exactly one new snapshot, stamped batch-id (commit's
    # stage_key is also None, same reasoning as land's).
    _, fact_summary = snapshot_delta(spark, fact_qt, fact_before)
    assert_stamped_batch(fact_summary, batch_id)

    # fold -> state (MERGE): exactly one new snapshot -- but `effects/
    # spark.py::_build_merge` never stamps snapshot-property options at all
    # (only `append`'s writer chain does), so a MERGE commit carries NEITHER
    # conveyer.batch-id NOR conveyer.stage -- a documented deviation from
    # R-13's general framing, not asserted as stamped here
    # ([[spine-post-commit-fold-publish-gaps]] point 4).
    _, state_summary = snapshot_delta(spark, state_qt, state_before)
    assert "conveyer.batch-id" not in state_summary

    # --- rerun: fresh seed, same batch_id, clean fx ---
    raw_before2 = snapshot_ids(spark, raw_qt)
    qtn_before2 = snapshot_ids(spark, qtn_qt)
    fact_before2 = snapshot_ids(spark, fact_qt)

    seed2 = _make_seed(
        spec=spec, batch_id=batch_id, attempt_id="attempt-2", object_uris=_CLEAN_OBJECT_URIS
    )
    result2 = run_sequence(seed2, local_runner_fx)

    # raw/quarantine/fact: genuinely zero new snapshots -- these three are
    # guard-skipped outright, no fx call at all on this rerun.
    assert_no_new_snapshot(spark, raw_qt, raw_before2)
    assert_no_new_snapshot(spark, qtn_qt, qtn_before2)
    assert_no_new_snapshot(spark, fact_qt, fact_before2)

    # state: NOT a bare snapshot-log equality check -- a healthy-rerun MERGE
    # still commits a harmless PHYSICAL no-op snapshot on this runtime
    # (effects/spark.py's own documented empirical finding, reused from
    # R-02's identical carve-out). The LOGICAL no-op is what "advances zero"
    # means here (I-19): absent from `fold_snapshot_ids`, per-table §8.2
    # projection rule.
    assert result2.fold_snapshot_ids == {}
    assert _rows_merged_total(result2) == 0

    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == _CLEAN_FACT_GOLDEN  # unchanged by the rerun
    raw_rows_count = spark.table(raw_qt).where(f"batch_id = '{batch_id}'").count()
    assert raw_rows_count == 3  # unchanged by the rerun


# --- A-07(b): contract mutated between attempts, KillFx after pre_check's
# own append -- drift recorded, nothing raised, cast failures retained -----


def test_a07b_pre_check_contract_mutated_between_attempts_drift_recorded_via_killfx(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    ledger_catalog: LedgerCatalogFixture,
    unique_table: Callable[[str], str],
    make_wrapped_fx: _MakeWrappedFx,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("a07b_raw")
    qtn_qt = unique_table("a07b_qtn")
    fact_qt = unique_table("a07b_fact")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)

    original_contract = RawContractModel(
        columns=[
            ColumnSpec(name="domain_id", required=True, nullable=False),
            ColumnSpec(name="event_time"),
            ColumnSpec(name="source_ts"),
            ColumnSpec(name="content_hash"),
            ColumnSpec(name="payload"),
        ]
    )
    # Tightened between attempts: `content_hash` string -> int.
    tightened_contract = RawContractModel(
        columns=[
            ColumnSpec(name="domain_id", required=True, nullable=False),
            ColumnSpec(name="event_time"),
            ColumnSpec(name="source_ts"),
            ColumnSpec(name="content_hash", type="int"),
            ColumnSpec(name="payload"),
        ]
    )
    spec1 = PipelineSpecModel(
        pipeline=_unique_pipeline("a07b"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_types=_fact_types(_bare(fact_qt), "spine_test_tables.unused_state_a07b"),
        read=_IDENTITY_READ,
        raw_contract=original_contract,
    )
    # No `_create_markers_table_for` here -- this scenario only ever drives
    # `land`/`pre_check` (see below, both attempts), never `commit`, so no
    # marker table is ever read or written (mirrors `test_scenarios_core.py`
    # ::test_a04...'s own identical carve-out).
    path = tmp_path / "object_1.csv"
    path.write_text(
        "domain_id,event_time,source_ts,content_hash,payload\n"
        "id-101,2026-05-01T00:00:00Z,2026-05-01T00:00:00Z,12345,ok1\n"
        ",2026-05-01T00:00:01Z,2026-05-01T00:00:01Z,99999,ok2\n"
        "id-103,2026-05-01T00:00:02Z,2026-05-01T00:00:02Z,abc,ok3\n"
    )
    batch_id = _batch_id(320)

    # Kill AFTER pre_check's own append (occurrence 2: land=1, pre_check=2
    # -- this file's own kill-point table): under `original_contract`, the
    # single null-`domain_id` row is the ONE violation, so this is the SAME
    # point R-03's own "pre_check" row kills at. The attempt dies right
    # there -- post_check/commit/fold/publish never run.
    killed_fx = make_wrapped_fx(local_runner_fx, {"append": killfx.kill_after(2, before=False)})
    seed1 = _make_seed(
        spec=spec1, batch_id=batch_id, attempt_id="attempt-1", object_uris=(str(path),)
    )
    with pytest.raises(killfx.SimulatedKill):
        run_sequence(seed1, killed_fx)

    # Attempt 2: fresh seed, SAME batch_id, the TIGHTENED contract -- door 1
    # (quarantine guard present, §6.5's A-9 subtraction path). land+pre_check
    # only, via the real `run()` driver (not direct stage calls) so
    # `fx.record_run` genuinely fires -- the WIRING this leg pins.
    # `by_alias=True`: `FactTypeModel.schema_` is aliased to `schema` at the
    # pydantic boundary (`Field(alias="schema")`) -- a plain `model_dump()`
    # emits the FIELD name (`schema_`), which `PipelineSpecModel(**...)`'s
    # own reconstruction then refuses (`schema_` is `extra_forbidden`, only
    # the alias `schema` is accepted on construction) -- `by_alias=True`
    # round-trips correctly.
    spec2 = PipelineSpecModel(
        **{**spec1.model_dump(by_alias=True), "raw_contract": tightened_contract}
    )
    seed2 = _make_seed(
        spec=spec2, batch_id=batch_id, attempt_id="attempt-2", object_uris=(str(path),)
    )
    capsys.readouterr()  # drain attempt 1's own EMF/log output
    result2 = run_sequence(
        seed2, local_runner_fx, stages=(("land", land.run), ("pre_check", pre_check.run))
    )

    tightened_check_version = check_version(tightened_contract, _IDENTITY_READ)
    expected_drift = (
        "pre_check drift: durable=1 recomputed=2 only_durable=0 only_recomputed=1 "
        f"admitted_cast_failures=1 check_version={tightened_check_version[:16]}"
    )
    assert result2.pre_check_drift == expected_drift
    assert result2.guard_skips == ("land", "pre_check")

    # §6.5's own letter: an admitted row whose cell now fails the CURRENT
    # contract's cast stays admitted with a NULL cell -- recorded, never
    # dropped (id-103's `content_hash`, "abc", cannot cast to int).
    valid_rows = sorted((r["domain_id"], r["content_hash"]) for r in result2.valid_df.collect())
    assert valid_rows == [("id-101", 12345), ("id-103", None)]

    # `PreCheckDrift` EMF emitted via `record_run`, off the SAME ledger row
    # `_stage_fields` folded the drift text into (not re-derived here).
    pre_check_ledger_rows = [
        r
        for r in ledger_catalog.rows()
        if r["batch_id"] == batch_id
        and r["attempt_id"] == "attempt-2"
        and r["stage"] == "pre_check"
    ]
    assert len(pre_check_ledger_rows) == 1
    assert pre_check_ledger_rows[0]["outcome"] == "skipped-guard"
    assert pre_check_ledger_rows[0]["error_message"] == expected_drift
    captured = capsys.readouterr()
    assert '"PreCheckDrift"' in captured.out  # the real EMF line, not just the ledger row


# --- A-10: post_check's own hash-keyed rerun subtraction, kill-based -------


def test_a10_post_check_hash_keyed_rerun_subtraction_exercised_via_killfx(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    make_wrapped_fx: _MakeWrappedFx,
) -> None:
    raw_qt = unique_table("a10_hash_raw")
    qtn_qt = unique_table("a10_hash_qtn")
    fact_qt = unique_table("a10_hash_fact")
    state_qt = unique_table("a10_hash_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = PipelineSpecModel(
        pipeline=_unique_pipeline("a10hash"),
        transforms_module="pipelines.identity_violations.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_types=_fact_types(_bare(fact_qt), _bare(state_qt)),
        checks=_VIOLATIONS_CHECKS,
        read=_IDENTITY_READ,
        raw_contract=_IDENTITY_RAW_CONTRACT,
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(310)

    # Kill AFTER post_check's own append (occurrence 3: land=1, pre_check=2,
    # post_check=3 -- this file's own kill-point table): the one post_check
    # violation (payload == "INVALID") commits, then the attempt dies before
    # commit ever runs.
    killed_fx = make_wrapped_fx(local_runner_fx, {"append": killfx.kill_after(3, before=False)})
    seed1 = _make_seed(
        spec=spec, batch_id=batch_id, attempt_id="attempt-1", object_uris=_VIOLATIONS_OBJECT_URIS
    )
    with pytest.raises(killfx.SimulatedKill):
        run_sequence(seed1, killed_fx)

    durable_post = (
        spark.table(qtn_qt)
        .where(f"batch_id = '{batch_id}' AND check_stage = 'post_check'")
        .collect()
    )
    assert len(durable_post) == 1
    row_hash = durable_post[0]["row_hash"]
    assert len(row_hash) == 64
    assert all(c in "0123456789abcdef" for c in row_hash)  # §7.2: lowercase hex sha256

    # Fresh restart: q_present=True, f_present=False (commit never ran) --
    # PATH 4, §8.2.4's hash-keyed subtraction (the durable row no longer
    # carries candidate columns at all, only `row_hash` -- the all-column/
    # locator-keyed anti-joins pre_check's own doors use are impossible
    # here, [DC-5]'s third mechanism).
    qtn_before = snapshot_ids(spark, qtn_qt)
    seed2 = _make_seed(
        spec=spec, batch_id=batch_id, attempt_id="attempt-2", object_uris=_VIOLATIONS_OBJECT_URIS
    )
    result2 = run_sequence(seed2, local_runner_fx)

    assert result2.post_quarantined_count == 1  # the DURABLE count -- path 4's own signature
    assert_no_new_snapshot(spark, qtn_qt, qtn_before)  # hash subtraction: zero NEW rows
    _assert_converged_violations(spark, fact_qt, state_qt)
