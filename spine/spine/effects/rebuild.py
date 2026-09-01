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
False, ...)` value — `swap_with_retry` is the loop that turns a sequence
of such outcomes into refuse -> re-pin -> recompute -> retry, budgeted by
`max_attempts`. Budget exhaustion raises `TransientError` (D-1's ordinary
job-failure/SFN-retry channel, the package's one exception class, §7.0
rule 4) — this is NOT a force path: the write is never issued past
refusal; the job simply fails loudly and SFN's own retry/backoff decides
whether the run mode restarts. A persistent-refusal livelock is priced as
an ops-visible metric (`RebuildSwapRetries` below), never disguised as
correctness.

**Ledger attempt rows — the interim record (§16, until 004.1's rebuild
stage-vocabulary accretion lands).** `_rebuild_attempt_fact` builds a
plain `RunFact` directly (`stage="rebuild"`), bypassing `core.run_facts.
transition`/`.failed` — both require a `BatchContext`, and a rebuild run
has none (it is a run-mode outside the eight-stage batch sequence, §9.4:
"executes under the fold's own principal", not a batch writer). Field
reuse, recorded here rather than silently assumed: `batch_id` carries a
synthetic `rebuild-<uuid>` run id (a rebuild is not itself a batch);
`feed_id` carries the (bare) state table name (a rebuild is table-scoped,
not feed-scoped — there is no natural feed to name). This shape is
explicitly the INTERIM one 004.1's own registered erratum row (§16, "the
rebuild stage-vocabulary accretion") will eventually replace with a
purpose-built shape — recorded as owed, not silently assumed permanent.

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
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine import observability
from spine.core.naming import qualified
from spine.core.run_facts import RunFact
from spine.effects.records import TransientError
from spine.effects.spark import is_transient_iceberg_failure

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import DataFrame, SparkSession

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


def _current_state_snapshot_id(spark: SparkSession, qt: str) -> int | None:
    """The `main` branch head via `<table>.refs` — the SAME mechanism
    `effects/spark.py::_current_snapshot_id` uses (re-derived locally
    rather than imported: that function is private, and this module's own
    docstring precedent — narrow, versionless shapes duplicated across
    effects modules rather than reaching into another module's private
    surface — is `effects/spark.py`'s own established style, e.g. its
    `_admission_raw_row_schema`'s "built locally rather than imported"
    note). `None` for a zero-snapshot table (I-6 [T-19])."""
    rows = (
        spark.read.format("iceberg")
        .load(f"{qt}.refs")
        .where(F.col("name") == F.lit("main"))
        .select("snapshot_id")
        .collect()
    )
    return int(rows[0]["snapshot_id"]) if rows else None


@dataclass(frozen=True)
class SwapOutcome:
    committed: bool
    state_snapshot_id: int | None  # the swap's own new snapshot; None iff refused


def attempt_state_swap(
    spark: SparkSession, state_table: str, rebuilt_df: DataFrame, before_id: int
) -> SwapOutcome:
    """ONE swap attempt — §9.2's normative rendering, both options always.
    `state_table` is BARE (`<db>.<table>`, `core.merge.MergeSpec.target_
    table`'s own convention) — qualified here, matching `effects/spark.py`
    throughout. A refusal (`is_transient_iceberg_failure`-recognized —
    `ValidationException`, probe A1's observed class, plus the same two
    Iceberg [T-10] siblings `append`/`merge` already tolerate) is reported
    as a plain value, never raised past this function; any OTHER exception
    propagates untouched (a genuine defect, not a refusal)."""
    qt = qualified(state_table)
    writer = rebuilt_df.writeTo(qt)
    for key, value in swap_write_options(before_id).items():
        writer = writer.option(key, value)
    try:
        writer.overwrite(F.lit(True))  # [DC2-2]: the one blessed call site
    except Exception as exc:  # noqa: BLE001 -- refusal recognized below or re-raised untouched
        if is_transient_iceberg_failure(exc):
            return SwapOutcome(committed=False, state_snapshot_id=None)
        raise
    return SwapOutcome(committed=True, state_snapshot_id=_current_state_snapshot_id(spark, qt))


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
) -> RunFact:
    """§16's interim ledger record (module docstring: field-reuse
    documented, not silently assumed permanent) — a refused attempt is
    `outcome="failed"` (a real, named refusal, not a `TransientError`
    surfaced to the caller: the loop itself retries); the winning attempt
    is `outcome="ok"`."""
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
        error_type=None if committed else "ValidationException",
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
        before_id = _current_state_snapshot_id(spark, qt)
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
            )
        )
        if outcome.committed:
            return RebuildResult(
                state_snapshot_id=outcome.state_snapshot_id, attempts=attempt, before_id=before_id
            )
        # RB-2's attempt-truth at metric grain -- "consecutive refused swaps
        # within one rebuild run, dimensioned by pipeline and state table"
        # (§12): `feed_id` is reused as the state-table dimension slot, the
        # SAME field-reuse `_rebuild_attempt_fact` documents above.
        observability.emit_metric("RebuildSwapRetries", 1.0, pipeline, state_table)
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
