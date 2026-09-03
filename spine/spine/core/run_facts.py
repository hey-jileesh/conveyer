"""`transition`/`failed` — `(ctx_before, ctx_after, …) -> RunFact`, pure. LLD §7.3, §7.7.

Stages contain zero instrumentation (004 §13.3); this module derives every
`RunFact` field purely from `(stage, ctx_before, ctx_after, t0, t1)` (or, on
failure, `(stage, ctx, t0, t1, exc)`) — never by reading anything back from
storage. `recorded_at` (§6.5's "append time" column) is deliberately absent
from `RunFact`: it is the ledger *write's own* timestamp, stamped by
`effects/ledger.py` at the actual append, not a value this pure function can
know.

`core/` stays effects-free by design (`spine/effects/records.py` — where
`TransientError` will be defined — is still a stub; importing it here would
break at import time, and even once it exists, `core/` importing `effects/`
would invert the dependency direction §7.0 sets up). `_error_message` below
type-checks the "spine-raised" `TransientError` case by **class name only**
(`type(exc).__name__ == "TransientError"`) rather than `isinstance` — the
recorded assumption this bead flags: if `TransientError` ever grows
subclasses, this name-only check stops recognizing them and needs revisiting
alongside `effects/records.py` (bead conveyer-nvh.18+).

`_stage_fields`' `post_check` branch was, until this bead, the one place a
non-failed transition's `error_message` gets populated: I-12 [H-2]'s
guard-skip read-back subset mismatch is surfaced as data, never raised, so
`ctx_after.post_check_drift` (set only by `stages/post_check.py`'s drift
branch, `None` otherwise) folds straight into the row's `error_message` when
present — `failed()` is a separate function that never calls
`_stage_fields`, so a genuinely failed attempt's `error_message` is
untouched by this.

**005.1 A-9 (bead conveyer-azr.18, n3-context-wiring; `stages/pre_check.py`
starts setting the field in bead conveyer-azr.19, n3-admission-cut)**: the
`pre_check` branch folds `ctx_after.pre_check_drift` into `error_message` on
the identical, non-failed-transition-only terms — the exact mirror
`context.py`'s own docstring promises.

**007.1 §4.2/§12 (errata item 20, bead conveyer-6pg.21, B9b): the per-table
maps, additive (L-4).** Six new fields, all `None` on every stage but
`commit`/`fold`: `facts_appended_by_table`/`snapshot_ids_by_table` (commit,
sourced from `ctx_after.facts_appended_by_table`/`.commit_snapshot_ids`) and
`rows_merged_by_table`/`snapshot_ids_by_table` (fold, sourced from
`ctx_after.rows_merged_by_table`/`.fold_snapshot_ids`) — **`snapshot_ids_by_
table` is ONE ledger column serving BOTH stage rows** (§12: "the commit
row's ... snapshot_ids_by_table ← ... commit_snapshot_ids; the fold row's
... snapshot_ids_by_table ← ... fold_snapshot_ids"), the same pattern the
pre-existing singular `snapshot_id` column already uses across five stages.
Plus commit's own delta-resolution recordings (§4.2's three remaining
set-once slots, F-5): `delta_predecessor_batch_ids`, `delta_read_snapshot_
ids`, `delta_probe_refusal`.

**The pre-existing singular `facts_appended`/`snapshot_id` (commit) follow
§12's "totals / one-snapshot symmetric rule" — a deliberate, DOCUMENTED
choice, since the LLD names the rule without spelling out its exact
arithmetic**: `facts_appended` = `sum(facts_appended_by_table.values())` (a
well-defined total regardless of declared-type count); `snapshot_id` = the
map's own single value when `commit_snapshot_ids` carries EXACTLY one entry,
else `None` (an N-table attempt's own commit has no single representative
snapshot id to report in a scalar column — never guessed, never the first-
in-iteration-order value). This is a **behavior change** from the pre-B9b
singular-fact-type era, worth naming: the old `ctx.fact_snapshot_id` used
`fx.resolve_batch_snapshot` on EITHER path (guard-skip included, recovering
a PRIOR attempt's snapshot via the stamped-summary lookup); the new
`commit_snapshot_ids` map explicitly excludes guard-skip/zero-fact tables
by construction (`BatchContext.commit_snapshot_ids`'s own docstring:
"absent key = skip / zero-fact no-op") — so a wholly-guard-skipped commit
attempt now reports `snapshot_id = None` where the old singular field would
have recovered a real (stale-attempt) value. This is the LLD's OWN §4.2
design, not a regression introduced here.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from pydantic import ValidationError

if TYPE_CHECKING:
    from spine.context import BatchContext

Outcome = Literal["ok", "failed", "skipped-guard"]


@dataclass(frozen=True)
class RunFact:  # one row per stage transition per attempt — §6.5 column table
    batch_id: str
    pipeline: str
    feed_id: str
    attempt_id: str
    sfn_retry_count: int
    sfn_redrive_count: int
    stage: str
    outcome: Outcome
    started_at: datetime
    finished_at: datetime
    rows_in: int | None = None  # not populated in Phase 1 -- no stage tracks a
    # generic "rows entering" count on `BatchContext`; left `None` throughout
    raw_count: int | None = None
    pre_quarantined: int | None = None
    post_quarantined: int | None = None
    facts_appended: int | None = None
    rows_merged: int | None = None
    snapshot_id: int | None = None
    # M2 (bead conveyer-swb.25): one column, two meanings keyed by `stage` --
    # on `stage="rebuild"` rows (`effects/rebuild.py::_rebuild_attempt_fact`)
    # this carries the PINNED FACT-table snapshot id for that attempt; on
    # every `stage="fold"` row it is permanently `None` (`_stage_fields`'s
    # own `fold` branch, below -- the pre-B10 singular fields this column
    # once served were deleted outright, critique gate wf_24a3125f-ecc F4).
    # The per-stage sourcing rule is owed a purpose-built vocabulary row in
    # 004.1's own rebuild stage-vocabulary accretion (007.1 §16) -- this
    # reuse is the documented INTERIM shape, not silently assumed permanent.
    state_read_snapshot_id: int | None = None
    co_effect_snapshot_ids: Mapping[str, int] | None = None
    merge_summary: Mapping[str, str] | None = None
    error_type: str | None = None
    error_message: str | None = None
    # 007.1 §4.2/§12 (errata item 20, B9b): the per-table maps, additive (L-4) --
    # see module docstring for the full field-level ground.
    facts_appended_by_table: Mapping[str, int] | None = None  # commit only
    snapshot_ids_by_table: Mapping[str, int] | None = None  # commit OR fold -- one shared column
    rows_merged_by_table: Mapping[str, int] | None = None  # fold only
    delta_predecessor_batch_ids: tuple[str, ...] | None = None  # commit only (F-5)
    delta_read_snapshot_ids: Mapping[str, int] | None = None  # commit only (F-5)
    delta_probe_refusal: str | None = None  # commit only (F-5)
    # critique gate wf_24a3125f-ecc F1 (bead conveyer-6pg.30): the L-4-
    # additive class, exactly `delta_read_snapshot_ids`'s own path -- see
    # module docstring's account and `effects/ledger.py::
    # _emit_divergent_duplicates`.
    divergent_duplicates_by_table: Mapping[str, int] | None = None  # commit only


def _common_fields(
    stage: str,
    ctx: BatchContext,
    outcome: Outcome,
    t0: datetime,
    t1: datetime,
) -> dict[str, Any]:
    return {
        "batch_id": ctx.batch_id,
        "pipeline": ctx.pipeline,
        "feed_id": ctx.feed_id,
        "attempt_id": ctx.attempt_id,
        "sfn_retry_count": ctx.sfn_retry_count,
        "sfn_redrive_count": ctx.sfn_redrive_count,
        "stage": stage,
        "outcome": outcome,
        "started_at": t0,
        "finished_at": t1,
    }


def one_snapshot(snapshots: Mapping[str, int] | None) -> int | None:
    """§12's "one-snapshot form" for a stage's singular `snapshot_id` column
    (module docstring's own documented choice): `None` snapshots (the stage
    has not run, or produced no per-table map) or an empty/multi-entry map
    both report `None` -- only a map carrying EXACTLY one entry has an
    unambiguous single value to show in a scalar column. `commit_snapshot_
    ids`'/`fold_snapshot_ids`'s own docstrings: "absent key = skip / zero-
    fact no-op", so a wholly-guard-skipped, wholly-zero-fact, or wholly-no-op
    N-table attempt legitimately reports `None` here, never a stale or
    arbitrarily-chosen entry. **Public (not `_`-prefixed, B10, bead
    conveyer-6pg.22)**: `stages/publish.py` applies this SAME rule to its
    own `state_snapshot_id`/`fact_snapshot_id` event-payload projections
    (that module's own docstring has the account) -- one shared derivation,
    not a second copy of the "exactly one entry" logic."""
    if snapshots is None or len(snapshots) != 1:
        return None
    return next(iter(snapshots.values()))


def _stage_fields(stage: str, ctx_after: BatchContext) -> dict[str, Any]:
    """Only the fields THIS stage produced (§6.5) -- every other numeric/
    snapshot field is left at `RunFact`'s `None` default."""
    if stage == "land":
        return {"raw_count": ctx_after.raw_count, "snapshot_id": ctx_after.land_snapshot_id}
    if stage == "pre_check":
        pre_check_fields: dict[str, Any] = {
            "pre_quarantined": ctx_after.pre_quarantined_count,
            "snapshot_id": ctx_after.pre_quarantine_snapshot_id,
        }
        if ctx_after.pre_check_drift is not None:
            # 005.1 A-9: an exact mirror of the post_check branch below --
            # a guard-skip rerun's durable-vs-recomputed drift is surfaced
            # as data on this (non-failed) transition's row, never raised.
            pre_check_fields["error_message"] = ctx_after.pre_check_drift
        return pre_check_fields
    if stage == "pull":
        return {"co_effect_snapshot_ids": ctx_after.co_effect_snapshot_ids}
    if stage == "apply":
        return {}
    if stage == "post_check":
        fields: dict[str, Any] = {
            "post_quarantined": ctx_after.post_quarantined_count,
            "snapshot_id": ctx_after.post_quarantine_snapshot_id,
        }
        if ctx_after.post_check_drift is not None:
            # I-12 [H-2]: a guard-skip read-back subset mismatch is
            # surfaced as data on this (non-failed) transition's row --
            # never raised. `failed()` never calls `_stage_fields`, so a
            # genuinely failed attempt is unaffected by this field.
            fields["error_message"] = ctx_after.post_check_drift
        return fields
    if stage == "commit":
        by_table = ctx_after.facts_appended_by_table
        snaps = ctx_after.commit_snapshot_ids
        return {
            "facts_appended": sum(by_table.values()) if by_table is not None else None,
            "snapshot_id": one_snapshot(snaps),
            "facts_appended_by_table": by_table,
            "snapshot_ids_by_table": snaps,
            "delta_predecessor_batch_ids": ctx_after.delta_predecessor_batch_ids,
            "delta_read_snapshot_ids": ctx_after.delta_read_snapshot_ids,
            "delta_probe_refusal": ctx_after.delta_probe_refusal,
            "divergent_duplicates_by_table": ctx_after.divergent_duplicates_by_table,
        }
    if stage == "fold":
        # B10 (bead conveyer-6pg.22): the SAME totals / one-snapshot
        # symmetric rule as commit's branch above, applied to fold's own
        # per-table maps. The old singular `BatchContext.state_snapshot_id`/
        # `.state_read_snapshot_id`/`.merge_summary` fields this branch used
        # to derive from were deleted outright (critique gate
        # wf_24a3125f-ecc ruling 1, bead conveyer-6pg.29, F4) -- this row's
        # own `RunFact.state_read_snapshot_id`/`.merge_summary` columns
        # (L-4, ledger-governed, unaffected by that deletion) stay `None`
        # here by omission, same as before.
        rows_by_table = ctx_after.rows_merged_by_table
        fold_snaps = ctx_after.fold_snapshot_ids
        return {
            "snapshot_id": one_snapshot(fold_snaps),
            "rows_merged": sum(rows_by_table.values()) if rows_by_table is not None else None,
            "rows_merged_by_table": rows_by_table,
            "snapshot_ids_by_table": fold_snaps,
        }
    if stage == "publish":
        return {}
    raise ValueError(f"unknown stage: {stage!r}")


def transition(
    stage: str,
    ctx_before: BatchContext,
    ctx_after: BatchContext,
    t0: datetime,
    t1: datetime,
) -> RunFact:
    """`outcome` is `skipped-guard` iff `stage` newly appears in
    `ctx_after.guard_skips` (i.e. `stage ∈ guard_skips(after) \\
    guard_skips(before)`, §7.3) -- otherwise `ok`. Counts/snapshot fields
    come from `_stage_fields`, reading only the deltas this stage produced."""
    newly_skipped = set(ctx_after.guard_skips) - set(ctx_before.guard_skips)
    outcome: Outcome = "skipped-guard" if stage in newly_skipped else "ok"
    return RunFact(
        **_common_fields(stage, ctx_after, outcome, t0, t1),
        **_stage_fields(stage, ctx_after),
    )


def _first_line(text: str, limit: int) -> str:
    first = text.splitlines()[0] if text else ""
    return first[:limit]


def _top_frame_location(exc: BaseException) -> str:
    """The top (innermost, raising) stack frame as `module:lineno` -- derived
    from the exception's own traceback object (frame introspection only, no
    `os`/`sys`/`traceback` import needed, keeping this in the `core` purity
    profile's banned-import list). `module` reads the frame's own `__name__`
    global, not a filesystem path -- no message text is included ([S-7]:
    exception text routinely embeds row values; this only names WHERE)."""
    tb = exc.__traceback__
    if tb is None:
        return "<unknown>:0"
    last = tb
    while last.tb_next is not None:
        last = last.tb_next
    frame = last.tb_frame
    module = frame.f_globals.get("__name__", "<unknown>")
    return f"{module}:{last.tb_lineno}"


def _stripped_validation_message(exc: ValidationError) -> str:
    """pydantic `ValidationError`, input stripped ([S-7]): `.errors()`'s
    default `input_value` field routinely echoes the raw (possibly row-
    derived) value that failed -- `include_input=False` (plus
    `include_context=False`/`include_url=False`) keeps only `loc`/`type`,
    which describe the violated field and rule, never the value itself."""
    parts = []
    for err in exc.errors(include_url=False, include_context=False, include_input=False):
        loc = ".".join(str(p) for p in err["loc"])
        parts.append(f"{loc}: {err['type']}" if loc else err["type"])
    return "; ".join(parts)


def _error_message(exc: BaseException) -> str | None:
    """§6.5 [S-7]: spine-raised types (`TransientError` — name-checked, see
    module docstring; pydantic `ValidationError`, input-stripped) get
    `"{type}: {first line, <=256 chars}"`; any other type gets only the top
    stack frame location, no message text."""
    if isinstance(exc, ValidationError):
        return f"{type(exc).__name__}: {_first_line(_stripped_validation_message(exc), 256)}"
    if type(exc).__name__ == "TransientError":
        return f"{type(exc).__name__}: {_first_line(str(exc), 256)}"
    return _top_frame_location(exc)


def failed(
    stage: str,
    ctx: BatchContext,
    t0: datetime,
    t1: datetime,
    exc: BaseException,
) -> RunFact:
    """`outcome = "failed"` unconditionally; `error_type` is always the
    exception's class name, `error_message` derived per `_error_message`
    (§6.5 [S-7])."""
    return RunFact(
        **_common_fields(stage, ctx, "failed", t0, t1),
        error_type=type(exc).__name__,
        error_message=_error_message(exc),
    )
