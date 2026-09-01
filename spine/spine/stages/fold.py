"""`fold` stage — F-4's per-table loop, one conditional `MERGE INTO` per
declared fact type. LLD 007.1 §7.1 (F-4, "fold is N MERGEs"), §8.1 (F-6),
§8.2 (the per-type MERGE plan); B10, bead `conveyer-6pg.22`. Supersedes the
single-fact-type v1 mechanism this file used to carry (v1's `ctx.spec.
state_table`/`ctx.committed_facts_df`/`ctx.transforms.fold` — none of which
survive the 006.1 P-1 hard cut: `PipelineSpecModel.fact_table`/`.state_table`
are gone, `stages/commit.py`'s own B9b rewrite never sets `ctx.committed_
facts_df` any more, having replaced it with the per-table `facts_appended_
by_table`/`commit_snapshot_ids` maps, §4.2).

**Per declared fact type, in F-4's iteration order (`ctx.spec.fact_types`,
insertion-ordered = deploy-pinned), §8.2's normative plan:**

1. **Read** the batch's committed facts for the type: `fx.read_batch
   (fact_type.fact_table, ctx.batch_id)` — a single-partition read under
   §6.4's `identity(batch_id)` (I-6's own idiom, extended: this is the
   SAME `read_batch` shape `stages/publish.py`'s durable-count read and
   `core/delta.py`'s predecessor projection already use).
2. **Empty facts ⇒ skip the merge entirely for this type** (v1's own "R-09
   pinned empty-batch payload" carried forward at PER-TABLE grain, never
   batch grain): `rows_merged_by_table[state_table] = 0`, no key in
   `fold_snapshot_ids` — F-4's zero-fact corollary applied to fold, mirroring
   `stages/commit.py`'s own `facts_appended_by_table[t] = 0` convention for
   its own zero-novel case. No `fx.merge` call, no state read.
3. **The intra-batch reduce** (§8.2 step 2, D-3(a)'s first application):
   `core.merge.merge_spec(fact_type)` derives ONE `MergeSpec` purely from
   the bound `FactTypeModel` (§4.1 — no field authored here); `frames.fold.
   reduce_batch_winners(facts_t, spec_m)` keeps at most one row per
   `domain_id_col`, per §8.1/[T-11]'s null-ranks-lowest ordering (K-14:
   differentially verified against the plain-Python reference, 0 mismatches
   over 9219 generated cases incl. 7922 null-bearing — see `frames/fold.py`'s
   own docstring).
4. **The conditional MERGE**: `fx.merge(spec_m, winners)` renders and
   executes exactly one `MERGE INTO` (`effects/spark.py::_build_merge`/
   `render_merge`, consuming `core.merge.ordering_predicate`'s explicit
   field-wise boolean — §8.2's rendering decision, never native struct
   comparison). **No presence guard** — fold has no guard by design (§11's
   kill-matrix row): idempotence is the MERGE condition's own job (strict
   ordering inequality, [T-11]) and `fx.merge` reports a healthy rerun's
   full-tie as an explicit logical no-op (`MergeResult(None, None)`,
   detected off `changed-partition-count = "0"` under merge-on-read,
   errata #9 — never by diffing snapshot ids, since a MERGE always
   physically snapshots).
5. **Per-table projection** (§4.2/§8.2's "absent key = no-op" rule):
   `rows_merged_by_table[state_table]` is ALWAYS present (0 on skip or
   no-op, `added-records` on a real merge); `fold_snapshot_ids
   [state_table]` carries a key ONLY when `result.snapshot_id` is non-`None`
   (a real, attributable, non-no-op commit) — a rerun that no-ops
   everywhere is legible from the maps alone, matching commit's own
   per-table map convention exactly.

**Erratum (critique gate wf_24a3125f-ecc ruling 1, bead conveyer-6pg.29,
F4):** this section originally described the three now-superseded SINGULAR
`BatchContext` fields (`state_read_snapshot_id`, `state_snapshot_id`,
`merge_summary`) as "deliberately left unset, permanently ... stay on
`BatchContext` only because L-4/the additive-field class never removes a
field." That justification misapplied L-4: L-4 governs the **ledger
schema** (`RunFact`/the Iceberg `run_facts` table's `NestedField`s), not the
in-memory `BatchContext` dataclass — nothing in the LLD requires an
attempt-scoped context field to survive once every consumer has moved off
it. Since no consumer read any of the three past `stages/commit.py`'s own
B9b rewrite (`core/run_facts.py`'s `_stage_fields` derives the LEDGER row's
own singular projections purely from the per-table maps, via the "totals /
one-snapshot symmetric rule", §12) or `stages/publish.py`'s own N-table
event derivation (`state_snapshot_id` from `ctx.fold_snapshot_ids`,
`publish.py`'s own module docstring has the account), F4 deleted all three
from `BatchContext` outright — they no longer exist as fields at all, not
merely as unset ones. `RunFact`'s own equivalently-named ledger columns
(`state_read_snapshot_id`/`merge_summary`, `effects/ledger.py`'s
`RUN_LEDGER_SCHEMA`) are unaffected by this: those ARE governed by L-4 and
stay, permanently `None` on every fold row, exactly as before.
`state_read_snapshot_id` in particular has no per-table successor at all
([E-14]'s old "the pinned snapshot the fold's state slice was read at"):
the mechanical §8.2 plan never reads the state table as a separate step
(`reduce_batch_winners` takes no state input at all, unlike v1's `ctx.
transforms.fold(state_slice, facts_df)` — the touched-domain semi-join
optimization was v1's own device for feeding a CUSTOM fold function's
`state_slice_df` parameter, which the mechanical §8.2 design has no
equivalent hook for: `spec.fold == "custom"` is refused at
`PipelineSpecModel` parse, 007 D-3(e), so `Transforms.fold` is never
actually invoked by this stage). The MERGE statement's own `USING`/`ON`
clause reads the target table live, inside the effect, never through a
plan-visible pinned frame this stage could report a snapshot id for.
"""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import TYPE_CHECKING

from spine.core import merge as core_merge
from spine.frames import fold as frames_fold

if TYPE_CHECKING:
    from collections.abc import Mapping

    from spine.context import BatchContext
    from spine.effects.records import RunnerFx

_ADDED_RECORDS_KEY = "added-records"


def _rows_merged(summary: Mapping[str, str] | None) -> int:
    """A `None` summary (skip OR logical no-op, per `fx.merge`'s own
    `MergeResult` contract) is `0` rows merged for that table this attempt
    — mirrors `stages/commit.py`'s own `fx.append`-returned count, applied
    per table here instead of per fact append."""
    if summary is None:
        return 0
    added = summary.get(_ADDED_RECORDS_KEY)
    return int(added) if added is not None else 0


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    rows_merged_by_table: dict[str, int] = {}
    fold_snapshot_ids: dict[str, int] = {}

    for fact_type in ctx.spec.fact_types.values():
        state_table = fact_type.state_table
        facts_t = fx.read_batch(fact_type.fact_table, ctx.batch_id)

        if facts_t.isEmpty():
            # F-4's zero-fact corollary at fold grain (module docstring
            # step 2): no MERGE, no state read.
            rows_merged_by_table[state_table] = 0
            continue

        spec_m = core_merge.merge_spec(fact_type)
        winners = frames_fold.reduce_batch_winners(facts_t, spec_m)
        result = fx.merge(spec_m, winners)

        rows_merged_by_table[state_table] = _rows_merged(result.summary)
        if result.snapshot_id is not None:
            fold_snapshot_ids[state_table] = result.snapshot_id

    return replace(
        ctx,
        rows_merged_by_table=MappingProxyType(rows_merged_by_table),
        fold_snapshot_ids=MappingProxyType(fold_snapshot_ids),
    )
