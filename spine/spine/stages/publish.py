"""`publish` stage — `BatchCompletedV1` from durable state, unconditional emit. LLD §7.5, I-19.

Every count/id in the payload is **durable-sourced**, never attempt-local
scaffolding (I-19): `fact_count`/`pre_quarantined`/`post_quarantined` are
all **read back** from data (`fx.read_batch`), never `ctx.pre_quarantined_
count`/`ctx.post_quarantined_count` (those are attempt-scaffolding values
that may differ from durable truth under I-12's cross-attempt perception
drift, [E-5]). `completed_at = fx.now()` is the one deliberate attempt-truth
exception (§6.6 [H-1], matching `started_at`'s same declared carve-out).

`fx.emit` is **unconditional** on every attempt (I-7) — there is no guard on
this table-less write; a guarded emit would lose the event forever on a
kill between commit and emit, the one gap a guard cannot close.

**B10 (bead conveyer-6pg.22): `fact_count`/`fact_snapshot_id`/`state_
snapshot_id` are rewritten for the N-table register.** The old singular
`BatchContext` fields `committed_facts_df`/`fact_snapshot_id`/
`state_snapshot_id` were permanently vestigial past `stages/commit.py`'s own
B9b rewrite (commit no longer set the first two; `stages/fold.py`'s B10
rewrite never set the third) and have since been deleted from
`BatchContext` outright (critique gate wf_24a3125f-ecc ruling 1, bead
conveyer-6pg.29, F4) — there is no longer one "the" fact table or "the"
state table to read a singular durable value from. This stage now derives
its own per-event singulars directly, over
`ctx.spec.fact_types` (F-4's declared order, though publish reads all N
unconditionally, no early exit):

- `fact_count` = the sum, across every declared fact type, of a durable
  `fx.read_batch(fact_table, batch_id).count()` — the SAME durable-count
  idiom the old singular `committed_facts_df.count()` used, generalized to
  N tables (D-3's read-by-name, not an attempt-local scaffolding count).
- `fact_snapshot_id` = `core.run_facts.one_snapshot(...)` over a map built
  from `fx.resolve_batch_snapshot(fact_table, batch_id, None)` per type
  (I-19's durable stamped-summary lookup — recovers a snapshot even after a
  guard-skip rerun, unlike an attempt-scoped map) — `None` when zero or more
  than one fact table produced a snapshot this batch, matching commit's own
  ledger-row "totals / one-snapshot symmetric rule" (§12, `core/run_facts.
  py`'s own docstring), reapplied here at the EVENT-payload grain rather
  than the ledger-row grain (the same rule, two independent consumers, one
  shared derivation function — never a second copy of the "exactly one
  entry" logic).
- `state_snapshot_id` = `core.run_facts.one_snapshot(ctx.fold_snapshot_ids)`
  — THIS ATTEMPT's own per-table fold map, not a durable resolution: unlike
  `append`, `effects/spark.py::_build_merge`'s own commit is never stamped
  with `conveyer.batch-id`/`conveyer.stage` (no `snapshot-property` option
  on the MERGE path), so there is no stamped-summary lookup that could ever
  recover a PRIOR attempt's fold snapshot the way `resolve_batch_snapshot`
  does for appends — `state_snapshot_id` is `None` on a fold no-op/guard-
  skip-adjacent rerun even when an earlier attempt's fold produced a real
  one. This is a pre-existing, already-documented tension with R-02's
  blanket field-equality claim (`[[spine-post-commit-fold-publish-gaps]]`
  point 4, `effects/spark.py`'s own module docstring), carried forward
  unchanged by this rewrite, not newly introduced by it.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine.core.model import BatchCompletedV1
from spine.core.run_facts import one_snapshot

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
    assert ctx.raw_count is not None, "publish requires raw_count (land)"
    quarantine_table = ctx.spec.quarantine_table

    fact_count = 0
    fact_snapshot_ids: dict[str, int] = {}
    for fact_type in ctx.spec.fact_types.values():
        fact_count += fx.read_batch(fact_type.fact_table, ctx.batch_id).count()
        snapshot_id = fx.resolve_batch_snapshot(fact_type.fact_table, ctx.batch_id, None)
        if snapshot_id is not None:
            fact_snapshot_ids[fact_type.fact_table] = snapshot_id

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
        fact_snapshot_id=one_snapshot(fact_snapshot_ids),
        state_snapshot_id=one_snapshot(ctx.fold_snapshot_ids),
        completed_at=fx.now(),
    )
    fx.emit("batch-completed", event)

    return replace(ctx, published=True, completed_event=event)
