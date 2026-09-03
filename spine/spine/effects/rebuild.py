"""The rebuild swap — the ONE blessed rebuild/swap module. LLD 007.1 §9, F-7.

`[DC2-2]`: this is the single module `tools/linter_configs/spine.py`'s
`.overwrite(` ban exempts (a per-file exemption, `tools/purity_linter.py`'s
new mechanism, B11-local) — the effects layer's sole owner of SQL rendering
renders no SQL overwrite of a state table (§9.2's second forbidden
look-alike: `INSERT OVERWRITE` has no construction site anywhere in this
codebase), and this module is the sole construction site for the
`DataFrameWriterV2.overwrite(...)` call `attempt_state_swap` issues.

**§9.1 — the swap is a conditional, lineage-preserving commit, never a
lock.** Rebuild and live folds are two writers racing on the state table's
own snapshot lineage (Iceberg-native conditional overwrite on the SAME
table identity, RB-1); the storage layer adjudicates via commit-time
snapshot-CAS semantics. Both writers lose loudly on conflict — never a
forced win (RB-2) — and no `--force` variant exists anywhere in this
module, in any form.

**§9.2 — pin order (load-bearing, an L-2-class write-order rule): `before_
id` FIRST, then fact pins.** `swap_with_retry` captures `before_id` (the
state table's current snapshot) at the TOP of each attempt, strictly
before calling `recompute` — the caller-supplied closure that re-pins fact
reads and re-derives the rebuilt frame FRESH each attempt (typically:
`effects/spark.py`'s `read_table`/`table.snapshot`-pinned reads +
`frames/fold.py::reduce_batch_winners`). A batch landing between the two
captures moves state past `before_id` and the swap refuses — over-refusal,
the permitted direction; the reverse order (facts pinned first) is
forbidden by the construction of the hazard itself (§9.2's own account).

**The swap — ONE lawful rendering (normative, probe-verified A1, this
bead's own kernel probe reconfirms it): BOTH options, always, together.**
`swap_write_options` is the pure value both `attempt_state_swap` and this
docstring point to — `validate-from-snapshot-id` ALONE is silently
ignored on the `OverwriteByFilter` path (empirically reconfirmed, this
bead: the swap blindly wins, the forbidden lost update inside an
apparently-safe conditional call) — `isolation-level=serializable` MUST
ride beside it on every single call, no exceptions.

**Refusal -> re-pin -> recompute -> retry (RB-2).** `attempt_state_swap`
recognizes a refusal via `effects.spark.is_transient_iceberg_failure` (the
SAME predicate `append`/`merge` already use for I-11's three Iceberg
[T-10] FQCNs — `ValidationException` is probe A1's observed refusal
class) and reports it as a plain, non-raising `SwapOutcome(committed=
False, ...)` value carrying the OBSERVED wrapped Java class name
(`SwapOutcome.error_type`, A007-9 fix — no longer a hardcoded
`"ValidationException"` literal regardless of which of the three
`is_transient_iceberg_failure`-recognized classes actually fired;
`CommitStateUnknownException`'s own ledger rows now say so, not
`ValidationException`'s name borrowed from probe A1's own single observed
case) — `swap_with_retry` is the loop that turns a sequence of such
outcomes into refuse -> re-pin -> recompute -> retry, budgeted by
`max_attempts`. Budget exhaustion raises `TransientError` (D-1's ordinary
job-failure/SFN-retry channel, the package's one exception class, §7.0
rule 4) — this is NOT a force path: the write is never issued past
refusal; the job simply fails loudly and SFN's own retry/backoff decides
whether the run mode restarts. A persistent-refusal livelock is priced as
an ops-visible metric (`RebuildSwapRetries` below), never disguised as
correctness. `RebuildSwapRetries` carries the state table under an
`extra_dims={"state_table": ...}` entry (U-4's own ruling, this bead) IN
ADDITION to the pre-existing `feed_id` overload below — the observability
API (`observability.emit_metric`'s existing `extra_dims` parameter)
already supports this additively, no schema change, so a dashboard can
group by the correctly-named `state_table` dimension today; `feed_id`
keeps carrying the table name too (unchanged) since no `feed_id`-free
`emit_metric` call shape exists — the ledger's OWN `feed_id` reuse (below)
is untouched by this, still owed to 004.1's vocabulary row.

**Ledger attempt rows — the interim record (§16, until 004.1's rebuild
stage-vocabulary accretion lands).** `_rebuild_attempt_fact` builds a
plain `RunFact` directly (`stage="rebuild"`), bypassing `core.run_facts.
transition`/`.failed` — both require a `BatchContext`, and a rebuild run
has none (it is a run-mode outside the eight-stage batch sequence, §9.4:
"executes under the fold's own principal", not a batch writer). Field
reuse, recorded here rather than silently assumed: `batch_id` carries a
synthetic `rebuild-<uuid>` run id (a rebuild is not itself a batch);
`feed_id` carries the (bare) state table name (a rebuild is table-scoped,
not feed-scoped — there is no natural feed to name); `state_read_snapshot_
id` (permanently `None` on every `fold` row, `stages/fold.py`'s own
docstring) carries the PINNED FACT table's own snapshot id for the
attempt (`rebuild_state_table`'s own reuse, this bead — a rebuild has no
per-table "state read" of its own the way v1's fold once did, and this is
the one ledger column already shaped to hold exactly one snapshot id per
attempt with no existing tenant). This shape is explicitly the INTERIM one
004.1's own registered erratum row (§16, "the rebuild stage-vocabulary
accretion") will eventually replace with a purpose-built shape — recorded
as owed, not silently assumed permanent.

**The production recompute closure — §9.5's pinned fact reads, §9.2's pin
order true BY CONSTRUCTION (A007-1 fix, this bead).** `rebuild_state_table`
is the one production builder of `swap_with_retry`'s `recompute` callable:
its closure pins the fact table's CURRENT snapshot (`effects/spark.py::
pinned_read` — public since bead conveyer-swb.25's M1 fix; this module used
to duplicate that exact rendering locally as `_pinned_read` under a
"duplicate narrow, versionless shapes rather than reach into another
module's private surface" precedent, now retired in favor of importing the
shared public helper, since a second real caller existed) and reduces it
through `frames.fold.reduce_batch_winners(_, core.merge.merge_spec(fact_
type))` — mechanically IDENTICAL to `stages/fold.py`'s own per-type step,
§8.2's normative plan, reused verbatim rather than re-derived. Because
`swap_with_retry` only ever calls `recompute()` AFTER capturing `before_id`
(module docstring above, §9.2), and `rebuild_state_table`'s closure is the
ONLY place the fact snapshot is ever pinned, the load-bearing order —
`before_id` first, fact pin second — holds by CONSTRUCTION for every real
caller of this function, not by a convention a future caller could get
backwards. `rebuild_pipeline` loops `spec.fact_types` in DECLARED
(insertion) order — F-4's own per-table iteration convention (`stages/
fold.py`'s module docstring) — calling `rebuild_state_table` once per
declared fact type, keyed by state table in the returned mapping.

**A never-folded state table now converges instead of refusing (M4, bead
conveyer-swb.25).** `rebuild_state_table` genesis-seeds a virgin state
table (`_genesis_seed_state_table`, above) BEFORE ever calling `swap_with_
retry` — pushing an empty reduce of the fact table's own schema through
`effects/spark.py::build_merge` (the SAME closure production `RunnerFx.
merge` uses, reused rather than rendering a second `MERGE INTO`/`.
overwrite(` call site) establishes a real first snapshot, since a zero-row
MERGE still commits one on this runtime (errata #9). Before this fix,
`swap_with_retry`'s own `before_id is None` branch raised `TransientError`
unconditionally for this case — D-1's retry-class channel, wrongly applied
to what is a PERMANENT condition until the first fold or rebuild runs, not
a transient one. `swap_with_retry`'s own check is UNCHANGED and still
fires for any bare caller that bypasses `rebuild_state_table` (K-17..K-19/
K-26/K-27's own hand-built `recompute` closures, which genesis-seed their
own fixture tables directly, matching this module's pre-existing test
convention) — this fix closes the gap at the PRODUCTION entrypoint's own
grain, not by loosening the lower-level primitive's contract.

**The run-mode entry — `entrypoints/rebuild_main.py` (A007-1 fix, this
bead).** A separate, small entrypoint (not a `--run-mode` flag folded into
`entrypoints/glue_main.py::main`): `glue_main.main`'s own docstring states
it is "pure COMPOSITION ... contains no `if`/`try` of its own", and its
`RunnerConfig`/`from_args` contract hard-requires seed/delivery/SFN-retry/
SLA fields no rebuild invocation has (there is no seed batch to rebuild —
folding a branch into that composition would either fabricate a fake seed
or break the "no `if` of its own" invariant). `rebuild_main.py` reuses
`glue_main.check_spec_uri_allowlist` (I-23, already public) and
`glue_main.default_fetch_spec` by import — never duplicated — and has NO
`--force` flag anywhere in its own argv contract, by construction (RB-2):
there is no flag surface to weaken in the first place.

**`rebuild-completed` — the interim runbook step is discipline, and says
so (§9.3, §16's 008 row).** `RebuildCompletedV1` (pipeline slug, per-
state-table post-swap snapshot ids, `occurred_at`) is 004.1's own
proposed event contract and has NOT landed as of this module — **this
module deliberately emits NO event on a successful swap**. Until
`RebuildCompletedV1` lands, an out-of-band (non-tier-2) rebuild's
completion is announced ONLY by the 008 runbook's own manual re-
materialization step — named, not hidden, exactly as §9.3 requires ("the
interim is discipline and says so"): a human runs the runbook's re-
materialization procedure after a successful out-of-band swap; nothing in
this module substitutes for that step, and nothing here should be read as
having discharged it. A kill between a successful swap and that manual
step is §11's kill-matrix row (K-27): state is already correct; only the
announcement is stale, closed by re-running this module (idempotent by
content, §9.5) or by the runbook's own re-emit/re-notify step.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine import observability
from spine.core import merge as core_merge
from spine.core.naming import qualified
from spine.core.run_facts import RunFact
from spine.effects import spark as spark_fx
from spine.effects.records import TransientError
from spine.effects.spark import is_transient_iceberg_failure
from spine.frames.fold import reduce_batch_winners

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import DataFrame, SparkSession

    from spine.core.merge import MergeSpec
    from spine.core.model import FactTypeModel, PipelineSpecModel

logger = logging.getLogger(__name__)

_VALIDATE_FROM_SNAPSHOT_ID_OPTION = "validate-from-snapshot-id"
_ISOLATION_LEVEL_OPTION = "isolation-level"
_SERIALIZABLE = "serializable"

_REBUILD_STAGE = "rebuild"
_DEFAULT_MAX_ATTEMPTS = 5


def swap_write_options(before_id: int) -> dict[str, str]:
    """007.1 §9.2's ONE lawful `DataFrameWriterV2` rendering, pure: BOTH
    options together, always. Never call `.overwrite(` on a state table
    with only the first of these applied — see module docstring's
    "forbidden look-alike" account (probe-verified, empirically
    reconfirmed by this bead's own kernel validation)."""
    return {
        _VALIDATE_FROM_SNAPSHOT_ID_OPTION: str(before_id),
        _ISOLATION_LEVEL_OPTION: _SERIALIZABLE,
    }


@dataclass(frozen=True)
class SwapOutcome:
    committed: bool
    state_snapshot_id: int | None  # the swap's own new snapshot; None iff refused
    error_type: str | None = None  # A007-9: the observed wrapped Java class name on
    # refusal (e.g. "org.apache.iceberg.exceptions.ValidationException" or
    # "...CommitStateUnknownException"); always None on a committed swap


def attempt_state_swap(
    spark: SparkSession, state_table: str, rebuilt_df: DataFrame, before_id: int
) -> SwapOutcome:
    """ONE swap attempt — §9.2's normative rendering, both options always.
    `state_table` is BARE (`<db>.<table>`, `core.merge.MergeSpec.target_
    table`'s own convention) — qualified here, matching `effects/spark.py`
    throughout. A refusal (`is_transient_iceberg_failure`-recognized —
    `ValidationException`, probe A1's observed class, plus the same two
    Iceberg [T-10] siblings `append`/`merge` already tolerate) is reported
    as a plain value carrying the OBSERVED class name (`SwapOutcome.
    error_type`, A007-9), never raised past this function; any OTHER
    exception propagates untouched (a genuine defect, not a refusal).

    `effects/spark.py::current_snapshot_id`/`java_exception_class_name`
    (bead conveyer-swb.25, M1) -- this module used to duplicate both
    locally (`_current_state_snapshot_id`/`_java_exception_class_name`,
    byte-identical bodies) under a "duplicate narrow, versionless shapes
    rather than reach into another module's private surface" precedent;
    both are now public in `effects/spark.py` and imported directly here
    (via the `spark_fx` module import, not a `from ... import` binding, so
    a test monkeypatching `spark_fx.current_snapshot_id` reaches every call
    site in this module too — `test_k_suite_rebuild.py`'s own pin-order
    test relies on exactly this)."""
    qt = qualified(state_table)
    writer = rebuilt_df.writeTo(qt)
    for key, value in swap_write_options(before_id).items():
        writer = writer.option(key, value)
    try:
        writer.overwrite(F.lit(True))  # [DC2-2]: the one blessed call site
    except Exception as exc:  # noqa: BLE001 -- refusal recognized below or re-raised untouched
        if is_transient_iceberg_failure(exc):
            return SwapOutcome(
                committed=False,
                state_snapshot_id=None,
                error_type=spark_fx.java_exception_class_name(exc),
            )
        raise
    return SwapOutcome(committed=True, state_snapshot_id=spark_fx.current_snapshot_id(spark, qt))


@dataclass(frozen=True)
class RebuildResult:
    state_snapshot_id: int | None
    attempts: int
    before_id: int  # the WINNING attempt's own before_id


def _rebuild_attempt_fact(
    *,
    rebuild_id: str,
    pipeline: str,
    state_table: str,
    attempt: int,
    committed: bool,
    started_at: datetime,
    finished_at: datetime,
    state_snapshot_id: int | None,
    error_type: str | None,
) -> RunFact:
    """§16's interim ledger record (module docstring: field-reuse
    documented, not silently assumed permanent) — a refused attempt is
    `outcome="failed"` (a real, named refusal, not a `TransientError`
    surfaced to the caller: the loop itself retries); the winning attempt
    is `outcome="ok"`. `error_type` is the caller's own `SwapOutcome.
    error_type` (A007-9: the OBSERVED wrapped Java class, never a
    hardcoded literal) — always `None` on a committed attempt, by
    construction of every real caller. `state_read_snapshot_id` (the fact
    table's own pinned snapshot id, module docstring's field-reuse note) is
    left `None` here: `swap_with_retry`'s own bare callers (K-17..K-19/
    K-26/K-27's hand-built `recompute` closures) have no fact pin of their
    own to report; `rebuild_state_table` (below) is the ONE production
    caller that knows it, and stamps it onto this same `RunFact` via a
    `record_run` wrapper rather than threading a fifth parameter through
    `swap_with_retry` itself (which would otherwise have to learn about a
    concept — "a fact snapshot" — that only the production closure has)."""
    return RunFact(
        batch_id=rebuild_id,
        pipeline=pipeline,
        feed_id=state_table,
        attempt_id=str(attempt),
        sfn_retry_count=0,
        sfn_redrive_count=0,
        stage=_REBUILD_STAGE,
        outcome="ok" if committed else "failed",
        started_at=started_at,
        finished_at=finished_at,
        snapshot_id=state_snapshot_id,
        error_type=error_type,
        error_message=None if committed else "rebuild swap refused: state moved past before_id",
    )


def swap_with_retry(
    spark: SparkSession,
    pipeline: str,
    state_table: str,
    recompute: Callable[[], DataFrame],
    record_run: Callable[[RunFact], None],
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
) -> RebuildResult:
    """007.1 §9.2's refuse -> re-pin -> recompute -> retry loop.

    `recompute` is the caller's own fresh fact-snapshot pin + reduce (e.g.
    `frames.fold.reduce_batch_winners` over `effects.spark`'s pinned
    `read_table` reads) — called anew on EVERY attempt, so a straddling
    batch landing between refusals is picked up automatically on re-pin.
    Pin order (load bearing): `before_id` is captured strictly BEFORE
    `recompute()` is called, every attempt (module docstring's §9.2 note).

    `record_run` is called once per attempt, REQUIRED (not optional) —
    every attempt rides the ledger as attempt-truth (§9.2, §16's interim
    record), matching every other call site's own `RunnerFx.record_run`
    convention (never wrapped in a try/except here: that callable is
    itself documented never to raise, §7.3/§11.3). Passed in by the
    caller as a bare callable, never accessed as `fx.record_run` inside
    this module — §11.3's own textual scan restricts THAT attribute-call
    shape to `run.py` alone.

    Raises `TransientError` (D-1's ordinary channel) on budget exhaustion
    or on an empty-snapshot state table (nothing to swap against) — never
    forces a write; RB-2's "no --force path" holds by construction: every
    exit from this function is either a committed, validated swap or a
    raised `TransientError` with the state table untouched.
    """
    qt = qualified(state_table)
    rebuild_id = f"rebuild-{uuid.uuid4().hex[:12]}"
    attempt = 0
    while True:
        attempt += 1
        t0 = now()
        before_id = spark_fx.current_snapshot_id(spark, qt)
        if before_id is None:
            raise TransientError(
                f"rebuild swap: state table {qt!r} has no snapshot yet -- nothing to "
                "swap against (007.1 §9.2 presumes an existing lineage)"
            )
        rebuilt_df = recompute()
        outcome = attempt_state_swap(spark, state_table, rebuilt_df, before_id)
        t1 = now()
        record_run(
            _rebuild_attempt_fact(
                rebuild_id=rebuild_id,
                pipeline=pipeline,
                state_table=state_table,
                attempt=attempt,
                committed=outcome.committed,
                started_at=t0,
                finished_at=t1,
                state_snapshot_id=outcome.state_snapshot_id,
                error_type=outcome.error_type,
            )
        )
        if outcome.committed:
            return RebuildResult(
                state_snapshot_id=outcome.state_snapshot_id, attempts=attempt, before_id=before_id
            )
        # RB-2's attempt-truth at metric grain -- "consecutive refused swaps
        # within one rebuild run, dimensioned by pipeline and state table"
        # (§12): `feed_id` is reused as the state-table dimension slot, the
        # SAME field-reuse `_rebuild_attempt_fact` documents above; U-4's own
        # ruling (this bead) ADDITIONALLY carries the correctly-named
        # `state_table` extra dimension (`observability.emit_metric`'s
        # existing, additive `extra_dims` parameter -- no schema change) so
        # a dashboard need not group by `feed_id` to get a real table name.
        observability.emit_metric(
            "RebuildSwapRetries",
            1.0,
            pipeline,
            state_table,
            extra_dims={"state_table": state_table},
        )
        logger.warning(
            "rebuild swap refused (qt=%s, before_id=%s, attempt=%d) -- RB-2 re-pin/"
            "recompute/retry, no --force path exists",
            qt,
            before_id,
            attempt,
        )
        if attempt >= max_attempts:
            raise TransientError(
                f"rebuild swap on {qt!r} refused {attempt} times consecutively -- RB-3 "
                "livelock candidate (008's alarm row); no --force path exists (RB-2); "
                "the job fails loudly, SFN retries the run mode"
            )


def _genesis_seed_state_table(spark: SparkSession, spec: MergeSpec, fact_qt: str) -> None:
    """M4 (bead conveyer-swb.25, critique gate wf_78ea4599-a5b): a
    bootstrapped-but-never-folded state table has no snapshot lineage for
    `swap_with_retry` to swap against (§9.2 presumes one already exists) --
    genesis-seeds it by pushing an EMPTY reduce of the fact table's own
    (real) schema through `effects/spark.py::build_merge` -- the SAME
    closure production `RunnerFx.merge` is built from, reused here rather
    than rendering a second `MERGE INTO` call site of its own (the 007.1
    writer-set accounting -- 2 appends + 1 rendered MERGE + 1 blessed
    overwrite + ledger append -- stays exactly one rendered-MERGE site; no
    new `.overwrite(` site either, [DC2-2] stays exactly one entry).

    A zero-row MERGE still commits a REAL Iceberg snapshot on this runtime
    (`effects/spark.py::build_merge`'s own "a healthy-rerun MERGE still
    commits a snapshot" account, errata #9) -- exactly the genesis lineage
    `swap_with_retry`'s `before_id` capture needs to stop seeing `None`.
    Kernel-validated (this bead): a virgin state table's `current_snapshot_
    id` moves from `None` to a real int after this call, with zero rows
    written to the table (`spark.table(state_qt).count() == 0` immediately
    after) -- a pure lineage-establishing no-op, never real content."""
    merge = spark_fx.build_merge(spark)
    empty_source = reduce_batch_winners(spark.table(fact_qt).limit(0), spec)
    merge(spec, empty_source)


def rebuild_state_table(
    spark: SparkSession,
    pipeline: str,
    fact_type: FactTypeModel,
    *,
    record_run: Callable[[RunFact], None],
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
) -> RebuildResult:
    """A007-1's production recompute builder — §9.5's pinned fact reads,
    §9.2's pin order true BY CONSTRUCTION (module docstring's own account).

    **M4 (bead conveyer-swb.25): genesis-seeds a never-folded state table**
    before ever calling `swap_with_retry` -- `_genesis_seed_state_table`
    (above) fires iff the state table's own current snapshot is `None`
    (checked BEFORE the retry loop, so `swap_with_retry`'s own `before_id
    is None` `TransientError` branch is never reached from THIS call path;
    that branch stays as the generic API's own defense for a bare caller
    that bypasses this production closure). This replaces the prior
    behavior of refusing a virgin state table with `TransientError` -- a
    retry-class error for what is, until the first fold or rebuild, a
    PERMANENT condition (D-1's channel means "retry might help"; retrying
    against an unseeded table never would have).

    The `recompute` closure this function hands `swap_with_retry` pins the
    fact table's CURRENT snapshot (`effects/spark.py::pinned_read`) and
    reduces it through `frames.fold.reduce_batch_winners(_, core.merge.
    merge_spec(fact_type))` — mechanically identical to `stages/fold.py`'s
    own per-type step (§8.2). Because `swap_with_retry` only ever calls
    `recompute()` strictly AFTER capturing `before_id`, and this closure is
    the ONLY place the fact snapshot is ever pinned, the load-bearing order
    holds by construction for every real caller — never a convention a
    future caller could get backwards.

    `record_run` is wrapped (not passed straight through): the fact
    snapshot id this attempt pinned is stamped onto the SAME `RunFact`
    `swap_with_retry` already built, via `state_read_snapshot_id` (module
    docstring's field-reuse note) — `swap_with_retry` itself stays ignorant
    of "a fact snapshot" as a concept; only this production closure knows
    it. The wrapper reads the pin from a small mutable cell `recompute`
    populates on every call — safe because `swap_with_retry` always calls
    `recompute()` before `record_run()` within one attempt (module
    docstring's own attempt shape), so the cell is never stale when read.
    """
    spec = core_merge.merge_spec(fact_type)
    fact_qt = qualified(fact_type.fact_table)
    state_qt = qualified(fact_type.state_table)
    if spark_fx.current_snapshot_id(spark, state_qt) is None:
        _genesis_seed_state_table(spark, spec, fact_qt)
    pinned: dict[str, int] = {}

    def recompute() -> DataFrame:
        facts_df, fact_snapshot_id = spark_fx.pinned_read(spark, fact_qt)
        pinned["fact_snapshot_id"] = fact_snapshot_id
        return reduce_batch_winners(facts_df, spec)

    def record_run_with_fact_pin(fact: RunFact) -> None:
        record_run(replace(fact, state_read_snapshot_id=pinned.get("fact_snapshot_id")))

    return swap_with_retry(
        spark,
        pipeline,
        fact_type.state_table,
        recompute,
        record_run_with_fact_pin,
        now=now,
        max_attempts=max_attempts,
    )


def rebuild_pipeline(
    spark: SparkSession,
    spec: PipelineSpecModel,
    *,
    record_run: Callable[[RunFact], None],
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
) -> dict[str, RebuildResult]:
    """Per-pipeline rebuild orchestration (A007-1) — loops `spec.fact_types`
    in DECLARED (insertion) order, F-4's own per-table iteration convention
    (`stages/fold.py`'s module docstring, "insertion-ordered = deploy-
    pinned type iteration order every consumer reads"), calling
    `rebuild_state_table` once per declared fact type: one swap per state
    table, one `RunFact` per attempt (via that call's own `record_run`).
    Returns the per-table `RebuildResult` map, keyed by (bare) state table
    name — a rebuild failure on one type's `TransientError` propagates
    immediately (D-1's ordinary channel), leaving any LATER type in
    declared order unattempted; earlier types' own swaps, already
    committed, are untouched by a later type's own failure (each state
    table's swap is independent — F-4's "N MERGEs" plan applied to rebuild,
    never a single cross-table transaction)."""
    return {
        fact_type.state_table: rebuild_state_table(
            spark,
            spec.pipeline,
            fact_type,
            record_run=record_run,
            now=now,
            max_attempts=max_attempts,
        )
        for fact_type in spec.fact_types.values()
    }
