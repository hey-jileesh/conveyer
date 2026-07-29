"""`fold` stage — slice-fold + conditional MERGE upsert. LLD §7.5, I-11, [E-14][C-7][T-8].

Empty `committed_facts_df` skips the merge entirely (§7.5: "Empty
`committed_facts_df` ⇒ skip the merge entirely") — `state_read_snapshot_id`,
`state_snapshot_id`, and `merge_summary` all accrete `None`, matching R-09's
pinned empty-batch payload. Otherwise: `touched` = distinct `domain_id`s in
this batch's committed facts; `(state_df, state_read_snapshot_id) =
fx.read_table(spec.state_table)` (I-6 pinned read, `-1` zero-snapshot
sentinel — [E-14]); `state_slice` = `state_df` left-semi-joined to
`touched`; `new_rows = ctx.transforms.fold(state_slice, committed_facts_df)`;
`core.merge.merge_spec(...)` shapes + identifier-validates the `MergeSpec`;
`fx.merge` renders and executes exactly one `MERGE INTO`. **No presence
guard** — idempotence is the fold contract (004 §8): a healthy rerun's MERGE
updates nothing (strict ordering inequality, I-11) and `fx.merge` reports
that as an explicit logical no-op (`MergeResult(None, None)`), which this
stage passes straight through as `state_snapshot_id = None`, `merge_summary
= None` ([C-7][T-8]).

**Ordering columns — Phase 1 default-lww path is what matters, custom-fold
resolution is an open gap (task ask, recorded here):** `core.merge.
merge_spec`'s docstring is explicit that its `ordering_cols` argument comes
from "`frames.default_lww_fold`'s hardcoded key today" and is deliberately
NOT derived inside `core/` (`core/` may not depend on `frames/`) — the
CALLING stage is the intended place to know it. The only Phase 1 fold
implementation is `frames.folds.default_lww_fold`, whose ordering key
`(event_time, source_ts, content_hash)` is `frames.folds.
LWW_ORDERING_COLUMNS` — a public export (single-sourced: `frames.folds` is
still the one place the literal tuple is written) this module imports
directly rather than re-deriving the same three column names as a second
local constant, which would risk silent drift if 007 ever changes the
hardcoded key without this stage being updated in lockstep.
**`PipelineSpecModel.fold == "custom"` has no ordering-key resolution story
in Phase 1 at all** — there is no field, contract, or convention yet for a
custom fold to declare its own ordering columns; this stage falls back to
the SAME default-lww ordering key regardless of `spec.fold`, which is only
correct by accident for a genuinely custom fold. Flagged in the handoff
report as 007's owed obligation (already named in LLD §15.2: "the
state-table schema must carry the ordering columns... a requirement the
state-schema owner inherits" — this stage's own gap is the mirror image, on
the ordering-cols SOURCE side).
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from spine.core import merge as core_merge
from spine.frames.folds import LWW_ORDERING_COLUMNS as _DEFAULT_LWW_ORDERING_COLUMNS

if TYPE_CHECKING:
    from spine.context import BatchContext
    from spine.effects.records import RunnerFx


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    assert ctx.committed_facts_df is not None, "fold requires committed_facts_df (commit)"
    committed = ctx.committed_facts_df

    if committed.isEmpty():
        return replace(
            ctx,
            state_read_snapshot_id=None,
            state_snapshot_id=None,
            merge_summary=None,
        )

    domain_id_col = ctx.spec.domain_id_col
    touched = committed.select(domain_id_col).distinct()
    state_df, state_read_snapshot_id = fx.read_table(ctx.spec.state_table)
    state_slice = state_df.join(touched, on=domain_id_col, how="left_semi")

    new_rows = ctx.transforms.fold(state_slice, committed)

    # See module docstring: Phase 1's only fold is default-lww; custom-fold
    # ordering-key resolution is an open 007 gap, not resolved here.
    spec_m = core_merge.merge_spec(
        ctx.spec, new_rows.schema.fieldNames(), _DEFAULT_LWW_ORDERING_COLUMNS
    )
    result = fx.merge(spec_m, new_rows)

    return replace(
        ctx,
        state_read_snapshot_id=state_read_snapshot_id,
        state_snapshot_id=result.snapshot_id,
        merge_summary=result.summary,
    )
