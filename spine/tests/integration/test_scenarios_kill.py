"""R-03 (KillFx, 8 kill points), R-08 (merge-conflict rerun), R-13 (one-commit
invariant) — LLD §12.4, I-4, I-19 [T-4], [E-16].

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
from scenario_helpers import bare as _bare
from scenario_helpers import batch_id as _batch_id
from scenario_helpers import create_fact_table as _create_fact_table
from scenario_helpers import create_quarantine_table as _create_quarantine_table
from scenario_helpers import create_raw_table as _create_raw_table
from scenario_helpers import create_state_table as _create_state_table
from scenario_helpers import quarantine_rows as _quarantine_rows
from snapshot_asserts import (
    assert_no_new_snapshot,
    assert_stamped_batch,
    snapshot_delta,
    snapshot_ids,
)
from spine.binding import bind_transforms
from spine.config import RunConfig
from spine.context import BatchContext
from spine.core.model import CoEffectDecl, PipelineSpecModel
from spine.effects.records import RunnerFx, TransientError
from spine.run import run as run_sequence

if TYPE_CHECKING:
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
) -> PipelineSpecModel:
    return PipelineSpecModel(
        pipeline="pipelines/identity",
        transforms_module=transforms_module,
        co_effects={"probe": CoEffectDecl(table=probe_table, own_state=False)},
        raw_table=raw_table,
        quarantine_table=quarantine_table,
        fact_table=fact_table,
        state_table=state_table,
        required_columns=["domain_id"],
    )


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
        pipeline="pipelines/identity",
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
    )
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
    assert post_rows[0]["payload"] == "INVALID"
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
        pipeline="pipelines/identity",
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
        required_columns=["domain_id"],
    )
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
    assert result2.state_snapshot_id is not None
    assert _failed_rows(ledger_catalog, batch_id, "attempt-2") == []

    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == _CLEAN_FACT_GOLDEN

    # Attempt 3: a plain, unwrapped, healthy rerun -- I-19's logical no-op
    # merge contract, observed via the run ledger's OWN fold row (not just
    # `result3`/context fields): `snapshot_id` (this ledger column IS
    # `state_snapshot_id` for a fold row, `core/run_facts.py::_stage_fields`)
    # null, `rows_merged` zero.
    seed3 = _make_seed(
        spec=spec, batch_id=batch_id, attempt_id="attempt-3", object_uris=_CLEAN_OBJECT_URIS
    )
    result3 = run_sequence(seed3, local_runner_fx)
    assert result3.state_snapshot_id is None
    assert result3.merge_summary is None

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
        pipeline="pipelines/identity",
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
        required_columns=["domain_id"],
    )
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
    assert result1.facts_appended == 3

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
    # means here (I-19): `state_snapshot_id is None`.
    assert result2.state_snapshot_id is None
    assert result2.merge_summary is None

    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == _CLEAN_FACT_GOLDEN  # unchanged by the rerun
    raw_rows_count = spark.table(raw_qt).where(f"batch_id = '{batch_id}'").count()
    assert raw_rows_count == 3  # unchanged by the rerun
