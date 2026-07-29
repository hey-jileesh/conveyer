"""`pre_check` stage — provisional DQ split, guarded quarantine append. LLD §7.5, I-P2.

`valid_df` and the violation set are derived from the **same predicate**
(`frames.checks.required_null_predicate`, exported precisely so callers don't
re-derive an equivalent-looking condition — see that module's own docstring):
`valid_df = raw_df.filter(~predicate)`, `viol = frames.checks.pre_violations
(raw_df, required_columns)`. Both are recomputed **unconditionally**, on
every attempt including a guard-skip rerun — this is what makes pre_check
co-effect-free (a standing constraint handed to 005, [E-5 note]): the SAME
deterministic inputs (`raw_df`, `spec.required_columns`) recompute the SAME
`valid_df`/violations every time, so a rerun's `valid_df` output stays
consistent with whatever quarantine rows a prior attempt already wrote
durably — this stage never needs to read those rows back to agree with them.

Two independent reasons a run performs no write, told apart deliberately:

- **Guard-skip** (`fx.table_has_batch(quarantine_table, batch_id,
  "pre_check")` is `True`): a PRIOR attempt already found violations and
  wrote them — this attempt accretes `guard_skips + ("pre_check",)`,
  `pre_quarantined_count = 0` (this attempt's own append count — the ledger's
  attempt-truth signature, same shape as `commit.facts_appended` on its own
  guard-skip), and resolves `pre_quarantine_snapshot_id` via
  `fx.resolve_batch_snapshot` (I-19 lineage use).
- **Zero violations** (guard absent, `viol.isEmpty()`): THIS attempt found
  nothing to quarantine — no write, no guard row, `guard_skips` untouched
  (004 §8's zero-write note: naturally idempotent because every rerun
  recomputes the identical empty violation set and again does nothing).

`stage_key = "pre_check"` throughout: the quarantine table is shared with
`post_check` (disambiguated by its own `check_stage` column), unlike land's
raw table.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from spine.core.model import LineageStamp
from spine.frames import checks, quarantine

if TYPE_CHECKING:
    from spine.context import BatchContext
    from spine.effects.records import RunnerFx

_STAGE_KEY = "pre_check"
# I-P2, provisional -- 005 owns the real contract grammar / per-row reasons.
_REASON = "null in a required column (I-P2, provisional; 005 owns the contract grammar)"


def _lineage_stamp(ctx: BatchContext) -> LineageStamp:
    return LineageStamp(
        batch_id=ctx.batch_id,
        delivery_id=ctx.delivery_id,
        feed_id=ctx.feed_id,
        received_at=ctx.received_at,
    )


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    """§7.5 pre_check: same-predicate valid/violation split, guarded quarantine append."""
    raw_df = ctx.raw_df
    if raw_df is None:  # sequencing contract: land always runs first (§7.3 STAGES order)
        raise ValueError("pre_check: ctx.raw_df is None -- land must run before pre_check")
    required_columns = ctx.spec.required_columns
    predicate = checks.required_null_predicate(required_columns)
    valid_df = raw_df.filter(~predicate)
    viol = checks.pre_violations(raw_df, required_columns)

    present = fx.table_has_batch(ctx.spec.quarantine_table, ctx.batch_id, _STAGE_KEY)
    if present:
        guard_skips = (*ctx.guard_skips, "pre_check")
        # this attempt wrote nothing -- the ledger's attempt-truth signature
        pre_quarantined_count = 0
        pre_quarantine_snapshot_id = fx.resolve_batch_snapshot(
            ctx.spec.quarantine_table, ctx.batch_id, _STAGE_KEY
        )
    elif viol.isEmpty():
        guard_skips = ctx.guard_skips
        pre_quarantined_count = 0
        pre_quarantine_snapshot_id = None
    else:
        guard_skips = ctx.guard_skips
        shaped = quarantine.shape_quarantine(viol, _lineage_stamp(ctx), _STAGE_KEY, reason=_REASON)
        pre_quarantined_count, _summary = fx.append(
            ctx.spec.quarantine_table, shaped, ctx.batch_id, _STAGE_KEY
        )
        pre_quarantine_snapshot_id = fx.resolve_batch_snapshot(
            ctx.spec.quarantine_table, ctx.batch_id, _STAGE_KEY
        )

    return replace(
        ctx,
        guard_skips=guard_skips,
        valid_df=valid_df,
        pre_quarantined_count=pre_quarantined_count,
        pre_quarantine_snapshot_id=pre_quarantine_snapshot_id,
    )
