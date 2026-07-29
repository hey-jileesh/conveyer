"""`pull` stage — pinned co-effect reads (I-6). LLD §7.5.

For each declared `(name, decl)` in `spec.co_effects`: one pinned read
(`fx.read_table`, I-6 — current snapshot resolved first, then read AT that
snapshot; `sid = -1` is the documented zero-snapshot sentinel [T-19]).
Reads only — rerun drift across attempts is harmless (D-3).

**Zero instrumentation (§7.3, critique F4, bead conveyer-nvh.43):** this
stage used to log a WARNING here, once per pull, when `decl.own_state and
not spec.serialize` (Phase 1 does not honor `serialize`, 004 §16.2). That
check is pure spec inspection — it needs nothing `pull` reads or produces —
so it moved to `spine.binding.bind_transforms`, which fires exactly ONCE
per run, pre-land (this stage would otherwise re-log the same warning on
every attempt, including guard-skipped reruns). Every stage, including this
one, now carries zero instrumentation; `core/run_facts.py` derives every
`RunFact` field purely from `(stage, ctx_before, ctx_after, t0, t1)`.

`co_effects`/`co_effect_snapshot_ids` accrete as `MappingProxyType`-wrapped
dicts (the construction-site contract `context.py`'s own docstring
describes) — built once here, never mutated after.
"""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from spine.context import BatchContext
    from spine.effects.records import RunnerFx


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    """§7.5 pull: one pinned read per declared co-effect."""
    co_effects: dict[str, DataFrame] = {}
    co_effect_snapshot_ids: dict[str, int] = {}
    for name, decl in ctx.spec.co_effects.items():
        df, sid = fx.read_table(decl.table)
        co_effects[name] = df
        co_effect_snapshot_ids[name] = sid

    return replace(
        ctx,
        co_effects=MappingProxyType(co_effects),
        co_effect_snapshot_ids=MappingProxyType(co_effect_snapshot_ids),
    )
