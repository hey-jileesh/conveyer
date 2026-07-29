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

`_stage_fields`' `post_check` branch is the one place a non-failed
transition's `error_message` gets populated: I-12 [H-2]'s guard-skip
read-back subset mismatch is surfaced as data, never raised, so
`ctx_after.post_check_drift` (set only by `stages/post_check.py`'s drift
branch, `None` otherwise) folds straight into the row's `error_message` when
present — `failed()` is a separate function that never calls
`_stage_fields`, so a genuinely failed attempt's `error_message` is
untouched by this.
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
    state_read_snapshot_id: int | None = None
    co_effect_snapshot_ids: Mapping[str, int] | None = None
    merge_summary: Mapping[str, str] | None = None
    error_type: str | None = None
    error_message: str | None = None


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


def _rows_merged(merge_summary: Mapping[str, str] | None) -> int:
    """`rows_merged` per §7.5's fold algorithm: `None` `merge_summary` means
    either an explicit no-op merge or a skipped (empty-facts) fold -- both
    are the "nothing changed" case the doc pins to `rows_merged = 0`. When a
    merge DID commit, `added-records` is Iceberg's own summary count of rows
    written by the MERGE (matched-and-rewritten rows surface as added
    records under copy-on-write, same as newly-inserted ones) -- recorded
    assumption, to be confirmed once `effects/spark.py`'s `fx.merge` (M2,
    bead conveyer-nvh.18) is exercised against real Spark/Iceberg summaries."""
    if merge_summary is None:
        return 0
    added = merge_summary.get("added-records")
    return int(added) if added is not None else 0


def _stage_fields(stage: str, ctx_after: BatchContext) -> dict[str, Any]:
    """Only the fields THIS stage produced (§6.5) -- every other numeric/
    snapshot field is left at `RunFact`'s `None` default."""
    if stage == "land":
        return {"raw_count": ctx_after.raw_count, "snapshot_id": ctx_after.land_snapshot_id}
    if stage == "pre_check":
        return {
            "pre_quarantined": ctx_after.pre_quarantined_count,
            "snapshot_id": ctx_after.pre_quarantine_snapshot_id,
        }
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
        return {
            "facts_appended": ctx_after.facts_appended,
            "snapshot_id": ctx_after.fact_snapshot_id,
        }
    if stage == "fold":
        return {
            "snapshot_id": ctx_after.state_snapshot_id,
            "state_read_snapshot_id": ctx_after.state_read_snapshot_id,
            "merge_summary": ctx_after.merge_summary,
            "rows_merged": _rows_merged(ctx_after.merge_summary),
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
