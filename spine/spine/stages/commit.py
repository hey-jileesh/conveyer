"""`commit` stage — structural fact check, delta filter (007 seam), fact append. LLD §7.5, I-24.

`present = fx.table_has_batch(spec.fact_table, batch_id, None)` (the fact
table has no `check_stage` disambiguation column — the quarantine table's
own convention, unused here); on the fresh path: lineage-stamp the admitted
facts, run the pure structural check (I-24: non-null `domain_id`, column-set
diff vs the target table), fail fast (named defect, no append) on either
violation, then run through `frames.folds.delta_filter` (the named 007
dedup/delta seam, identity in Phase 1) before the one `fx.append`.

`facts_appended` on the fresh path is the append's own summary count
(`added-records`, I-19/[T-6] — never a re-`.count()` of the just-written
DataFrame); on guard-skip it is `0` (the ledger's healthy-rerun signature).
`fact_snapshot_id` is resolved via `fx.resolve_batch_snapshot` on EITHER
path — `append`'s actual return shape (`tuple[int, Mapping[str, str]]`,
`effects/records.py`'s documented deviation) carries a rows-added count and
a summary, not a snapshot id itself, so the stamped-summary lookup is the
one mechanism that recovers it regardless of which attempt wrote.

`committed_facts_df = fx.read_batch(fact_table, batch_id)` runs
unconditionally, either way (D-3 — `candidate_facts_df`/`admitted_facts_df`
are dead past this stage).
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine.core import checks as core_checks
from spine.core import guards
from spine.core.model import LineageStamp
from spine.frames import folds, lineage

if TYPE_CHECKING:
    from spine.context import BatchContext
    from spine.effects.records import RunnerFx

_STAGE_KEY = None  # fact table has no check_stage column -- only quarantine needs one


def _lineage_stamp(ctx: BatchContext) -> LineageStamp:
    return LineageStamp(
        batch_id=ctx.batch_id,
        delivery_id=ctx.delivery_id,
        feed_id=ctx.feed_id,
        received_at=ctx.received_at,
    )


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    assert ctx.admitted_facts_df is not None, "commit requires admitted_facts_df (post_check)"
    fact_table = ctx.spec.fact_table
    present = fx.table_has_batch(fact_table, ctx.batch_id, _STAGE_KEY)
    plan = guards.plan_append(fact_table, _STAGE_KEY, present)

    if plan.do_append:
        stamped = lineage.stamp_fact_lineage(ctx.admitted_facts_df, _lineage_stamp(ctx))

        # I-24 structural check: a pure column-set diff vs the target
        # table's CURRENT schema, plus one aggregate over domain_id_col.
        existing_df, _existing_snapshot_id = fx.read_table(fact_table)
        domain_id_col = ctx.spec.domain_id_col
        domain_id_null_count = stamped.filter(F.col(domain_id_col).isNull()).count()
        verdict = core_checks.structural_fact_check(
            present_columns=stamped.columns,
            expected_columns=existing_df.columns,
            domain_id_col=domain_id_col,
            domain_id_null_count=domain_id_null_count,
        )
        if isinstance(verdict, core_checks.StructuralFactCheckDefect):
            # I-24: fail fast BEFORE any append -- a NULL domain_id or a
            # schema drift is a named defect, never a quarantine row (MERGE's
            # ON clause never matches NULL, so it would silently break the
            # rerun-is-a-no-op invariant every fold rerun after it).
            raise ValueError(
                f"I-24 structural fact check failed (batch_id={ctx.batch_id!r}): "
                f"{'; '.join(verdict.reasons)}"
            )

        to_append = folds.delta_filter(stamped)  # 007 seam, identity in Phase 1
        rows_appended, _summary = fx.append(fact_table, to_append, ctx.batch_id, _STAGE_KEY)
        facts_appended = rows_appended
        guard_skips = ctx.guard_skips
    else:
        facts_appended = 0  # guard-skip: the ledger's healthy-rerun signature
        # §6.3 [E-13]: `guard_skips` accretes on ANY effectful stage's
        # guard-skip, not just land/pre_check's -- this is what lets
        # `core.run_facts.transition` derive a `skipped-guard` ledger outcome
        # for `commit` (R-02/R-05), the same mechanism land.py/pre_check.py
        # already use.
        guard_skips = (*ctx.guard_skips, "commit")

    fact_snapshot_id = fx.resolve_batch_snapshot(fact_table, ctx.batch_id, _STAGE_KEY)
    committed_facts_df = fx.read_batch(fact_table, ctx.batch_id)  # D-3 -- read back by name

    return replace(
        ctx,
        guard_skips=guard_skips,
        facts_appended=facts_appended,
        fact_snapshot_id=fact_snapshot_id,
        committed_facts_df=committed_facts_df,
    )
