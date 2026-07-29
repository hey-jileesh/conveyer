"""`publish` stage — `BatchCompletedV1` from durable state, unconditional emit. LLD §7.5, I-19.

Every count/id in the payload is **durable-sourced**, never attempt-local
scaffolding (I-19): `fact_count = committed_facts_df.count()` [H-1] (the
committed-facts read-back, D-3 — not the ledger's own `facts_appended`
attempt delta); `pre_quarantined`/`post_quarantined` are **read back** from
the quarantine table by `(batch_id, stage)`, never `ctx.pre_quarantined_
count`/`ctx.post_quarantined_count` (those are attempt-scaffolding values
that may differ from durable truth under I-12's cross-attempt perception
drift, [E-5]); `fact_snapshot_id`/`state_snapshot_id` are the ACCRETED
context fields, which are themselves already durable-resolved by
`commit`/`fold`'s own I-19-compliant derivation (`fx.resolve_batch_snapshot`
/ `fx.merge`'s own result) — nothing is re-derived here. `completed_at =
fx.now()` is the one deliberate attempt-truth exception (§6.6 [H-1],
matching `started_at`'s same declared carve-out).

`fx.emit` is **unconditional** on every attempt (I-7) — there is no guard on
this table-less write; a guarded emit would lose the event forever on a
kill between commit and emit, the one gap a guard cannot close.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine.core.model import BatchCompletedV1

if TYPE_CHECKING:
    from spine.context import BatchContext
    from spine.effects.records import RunnerFx

_PRE_CHECK_STAGE = "pre_check"
_POST_CHECK_STAGE = "post_check"


def _durable_quarantine_count(
    fx: RunnerFx, quarantine_table: str, batch_id: str, check_stage: str
) -> int:
    return (
        fx.read_batch(quarantine_table, batch_id)
        .filter(F.col("check_stage") == F.lit(check_stage))
        .count()
    )


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    assert ctx.committed_facts_df is not None, "publish requires committed_facts_df (commit)"
    assert ctx.raw_count is not None, "publish requires raw_count (land)"
    quarantine_table = ctx.spec.quarantine_table

    fact_count = ctx.committed_facts_df.count()
    pre_quarantined = _durable_quarantine_count(
        fx, quarantine_table, ctx.batch_id, _PRE_CHECK_STAGE
    )
    post_quarantined = _durable_quarantine_count(
        fx, quarantine_table, ctx.batch_id, _POST_CHECK_STAGE
    )

    event = BatchCompletedV1(
        pipeline=ctx.pipeline,
        feed_id=ctx.feed_id,
        batch_id=ctx.batch_id,
        delivery_id=ctx.delivery_id,
        raw_count=ctx.raw_count,
        pre_quarantined=pre_quarantined,
        post_quarantined=post_quarantined,
        fact_count=fact_count,
        fact_snapshot_id=ctx.fact_snapshot_id,
        state_snapshot_id=ctx.state_snapshot_id,
        completed_at=fx.now(),
    )
    fx.emit("batch-completed", event)

    return replace(ctx, published=True, completed_event=event)
