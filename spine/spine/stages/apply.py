"""`apply` stage — `ctx.transforms.apply(ctx.valid_df, ctx.co_effects)`. Pure. LLD §7.5.

Nothing else: no guard, no I/O, no logging. `fx` is accepted only to match
every other stage's `run(ctx, fx) -> BatchContext` shape (§7.3's uniform
`StageFn`) and is never called. The df handed to `apply` is `valid_df` --
001 §5's `raw_df` argument *post-admission* (pre_check's split), stated once
here per §7.5.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spine.context import BatchContext
    from spine.effects.records import RunnerFx


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    """§7.5 apply: pure transform, nothing else."""
    del fx
    valid_df = ctx.valid_df
    co_effects = ctx.co_effects
    if valid_df is None or co_effects is None:  # sequencing: pre_check/pull run first
        raise ValueError(
            "apply: ctx.valid_df/ctx.co_effects must be populated -- "
            "pre_check and pull must run before apply"
        )
    candidate = ctx.transforms.apply(valid_df, co_effects)
    return replace(ctx, candidate_facts_df=candidate)
