"""`land` stage — read delivery, stamp lineage, guarded append, emit `batch-started`. §7.5.

Steps ①–⑤ of §7.5's land algorithm, one code path with two entry branches:

- **absent** (fresh): `fx.read_objects` the delivery (I-P1, provisional
  reader), stamp raw lineage (`frames.lineage.stamp_raw_lineage`, `LineageStamp`
  built from `ctx` per [C-5] — `frames/` never sees the context itself), one
  guarded append.
- **present** (guard-skip, rerun): no read, no write — the batch's raw rows
  already exist (I-3's guard reads DATA, never snapshot metadata).

Either way, `land_snapshot_id` is resolved via `fx.resolve_batch_snapshot`
(I-19's own-commit-resolution mechanism) — `fx.append`'s own return shape is
`(rows_appended, summary)`, not a snapshot id (`effects/records.py::RunnerFx.
append`'s own docstring), so this is the ONE channel that recovers the actual
snapshot id on both the fresh-write and guard-skip paths alike. `stage_key` is
`None` throughout: the raw table is written by `land` alone (never shared
with another quarantine sub-stream), so there is no `check_stage` column for
`table_has_batch`'s I-3 guard predicate to read — passing a stage key here
would name a column that does not exist on this table's schema.

`raw_count` follows [T-6]: on the fresh-write path it is the append's own
returned `added-records` count (never a `.count()` re-execution of the
upstream DAG); on guard-skip, `raw_df.count()` is the one deliberate action
this path takes (there is no write-side summary to read it from).

`raw_df` is read back by name (`fx.read_batch`) unconditionally, even on the
attempt that wrote it — 004 D-3's "one code path" rule stated once here (see
also commit's identical pattern for facts).

`batch-started` is emitted **unconditionally** (I-7) with a durable-derived
payload (I-19): `raw_count`/`land_snapshot_id` both come from the values just
resolved above, never from anything this attempt merely computed in memory
without a durable backing.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from spine.core.model import BatchStartedV1, LineageStamp
from spine.frames import lineage

if TYPE_CHECKING:
    from spine.context import BatchContext
    from spine.effects.records import RunnerFx

# The raw table has no `check_stage` column (it is not a shared quarantine
# sub-stream table) -- `None` throughout, for the guard, the append's own
# snapshot-property stamp, and its resolution alike.
_STAGE_KEY: str | None = None


def _lineage_stamp(ctx: BatchContext) -> LineageStamp:
    return LineageStamp(
        batch_id=ctx.batch_id,
        delivery_id=ctx.delivery_id,
        feed_id=ctx.feed_id,
        received_at=ctx.received_at,
    )


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    """§7.5 land, steps ①–⑤."""
    present = fx.table_has_batch(ctx.spec.raw_table, ctx.batch_id, _STAGE_KEY)
    if present:
        guard_skips = (*ctx.guard_skips, "land")
        raw_count_from_append: int | None = None
    else:
        object_df = fx.read_objects(ctx.object_uris, ctx.spec.read)
        raw = lineage.stamp_raw_lineage(object_df, _lineage_stamp(ctx))
        raw_count_from_append, _summary = fx.append(
            ctx.spec.raw_table, raw, ctx.batch_id, _STAGE_KEY
        )
        guard_skips = ctx.guard_skips

    land_snapshot_id = fx.resolve_batch_snapshot(ctx.spec.raw_table, ctx.batch_id, _STAGE_KEY)
    # D-3: read back by name, one code path, even on the attempt that just wrote it.
    raw_df = fx.read_batch(ctx.spec.raw_table, ctx.batch_id)
    # [T-6]: added-records from the write's own summary, never a re-.count() of raw.
    raw_count = raw_count_from_append if raw_count_from_append is not None else raw_df.count()

    after_write = replace(
        ctx,
        guard_skips=guard_skips,
        raw_df=raw_df,
        raw_count=raw_count,
        land_snapshot_id=land_snapshot_id,
    )

    fx.emit(
        "batch-started",
        BatchStartedV1(
            pipeline=ctx.pipeline,
            feed_id=ctx.feed_id,
            batch_id=ctx.batch_id,
            delivery_id=ctx.delivery_id,
            raw_count=raw_count,
            land_snapshot_id=land_snapshot_id,
            started_at=fx.now(),
        ),
    )
    return replace(after_write, started_emitted=True)
