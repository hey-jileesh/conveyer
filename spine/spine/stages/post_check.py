"""`post_check` stage — I-12 violation subtraction, drift recorded as data. LLD §7.5, I-12, [H-2].

`viol = ctx.transforms.post_check(ctx.candidate_facts_df, ctx.co_effects)` is
the only pure-transform call; everything else here is guard/append/read-back
plumbing plus the [H-2] fresh-vs-guard-skip branch I-12 pins:

- **Fresh compute** (guard absent): `admitted = frames.checks.
  violation_subtraction(candidate, viol)` (a multiplicity-preserving bag
  subtraction, [C-8]); the count identity `candidate == admitted +
  violations` is **asserted** here — a non-conforming `post_check` that
  fabricates or drops rows is a review defect this stage fails loudly on,
  not something 006's eventual keyed-violation-identity should have to catch
  first.
- **Guard-skip** (rerun): the subtraction's violation side is the **read-back
  durable quarantine rows** for `(batch_id, "post_check")`
  (`fx.read_batch(...).filter(check_stage == "post_check")`), never the
  freshly recomputed `viol` — facts and quarantine stay complementary across
  attempts even when co-effects/perception drifted between them (I-12
  [E-5]). The SAME count identity is checked, but a mismatch here is
  **recorded as data** (`ctx.post_check_drift`), never raised ([H-2]) — the
  assertion's diagnostic value was already spent on whichever attempt
  computed both sides fresh. `post_check_drift` is set to a short,
  counts-only message (`"post-check drift: durable=<N> recomputed=<M>
  subset=<bool>"` — no row values, [S-7]); `core/run_facts.py::_stage_fields`
  folds it into the ledger row's `error_message` for this transition, and
  `effects/ledger.py::record_run` (not this stage — critique F4, bead
  conveyer-nvh.43) derives the WARNING log + EMF `PostCheckDrift` emission
  from that `RunFact` field. This stage carries ZERO instrumentation of its
  own (§7.3) — it only ever sets the `ctx.post_check_drift` value.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine.core import guards
from spine.core.model import LineageStamp
from spine.frames import checks as frame_checks
from spine.frames import quarantine

if TYPE_CHECKING:
    from spine.context import BatchContext
    from spine.effects.records import RunnerFx

_STAGE = "post_check"


def _lineage_stamp(ctx: BatchContext) -> LineageStamp:
    return LineageStamp(
        batch_id=ctx.batch_id,
        delivery_id=ctx.delivery_id,
        feed_id=ctx.feed_id,
        received_at=ctx.received_at,
    )


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    assert ctx.candidate_facts_df is not None, "post_check requires candidate_facts_df (apply)"
    assert ctx.co_effects is not None, "post_check requires co_effects (pull)"
    candidate = ctx.candidate_facts_df
    viol = ctx.transforms.post_check(candidate, ctx.co_effects)

    quarantine_table = ctx.spec.quarantine_table
    present = fx.table_has_batch(quarantine_table, ctx.batch_id, _STAGE)
    plan = guards.plan_append(quarantine_table, _STAGE, present)

    if plan.do_append:
        # Fresh-compute path (I-12): identity asserted, loud failure on
        # mismatch -- until 006's keyed violation identity, this is what
        # keeps a non-conforming post_check from silently dropping rows.
        admitted = frame_checks.violation_subtraction(candidate, viol)
        candidate_count = candidate.count()
        admitted_count = admitted.count()
        violations_count = viol.count()
        identity = frame_checks.check_count_identity(
            candidate_count, admitted_count, violations_count
        )
        if not identity.ok:
            raise AssertionError(
                "I-12 count identity violated on post_check fresh-compute path "
                f"(batch_id={ctx.batch_id!r}): candidate={identity.candidate_count} "
                f"admitted={identity.admitted_count} violations={identity.violations_count}"
            )
        if violations_count > 0:
            shaped = quarantine.shape_quarantine(viol, _lineage_stamp(ctx), check_stage=_STAGE)
            rows_appended, _summary = fx.append(quarantine_table, shaped, ctx.batch_id, _STAGE)
            post_quarantined_count = rows_appended
        else:
            post_quarantined_count = 0  # zero violations -- no write, no guard row
        # zero-violations is a data-driven skip, not a guard-skip (mirrors
        # pre_check's own "zero violations" branch) -- guard_skips untouched
        # on this whole fresh-compute path either way.
        guard_skips = ctx.guard_skips
        post_check_drift = None
    else:
        # Guard-skip (rerun) path, I-12 [E-5][H-2]: subtract against the
        # READ-BACK durable quarantine rows for (batch_id, "post_check"),
        # not the recomputed `viol` -- facts/quarantine stay complementary
        # across attempts even under cross-attempt perception drift.
        durable_viol = fx.read_batch(quarantine_table, ctx.batch_id).filter(
            F.col("check_stage") == F.lit(_STAGE)
        )
        admitted = frame_checks.violation_subtraction(candidate, durable_viol)
        candidate_count = candidate.count()
        admitted_count = admitted.count()
        durable_violations_count = durable_viol.count()
        identity = frame_checks.check_count_identity(
            candidate_count, admitted_count, durable_violations_count
        )
        if not identity.ok:
            # [H-2]: recorded as data, never raised -- this attempt's
            # recomputed candidate may legitimately disagree with another
            # attempt's durable quarantine rows (004 §8's named trade-off).
            # Counts only, no row values [S-7] -- `core/run_facts.py::
            # _stage_fields` folds this into the ledger row's
            # `error_message` for this (non-failed) transition, and
            # `effects/ledger.py::record_run` derives the WARNING log + EMF
            # `PostCheckDrift` emission from that field (critique F4, bead
            # conveyer-nvh.43) -- this stage itself logs/emits nothing.
            post_check_drift = (
                f"post-check drift: durable={durable_violations_count} "
                f"recomputed={candidate_count} subset={identity.ok}"
            )
        else:
            post_check_drift = None
        post_quarantined_count = durable_violations_count  # durable count, not recomputed
        # §6.3 [E-13]: `guard_skips` accretes on ANY effectful stage's
        # guard-skip -- the same mechanism land.py/pre_check.py already use,
        # letting `core.run_facts.transition` derive a `skipped-guard`
        # ledger outcome for `post_check` too (R-02/R-05).
        guard_skips = (*ctx.guard_skips, "post_check")

    post_quarantine_snapshot_id = fx.resolve_batch_snapshot(quarantine_table, ctx.batch_id, _STAGE)

    return replace(
        ctx,
        guard_skips=guard_skips,
        admitted_facts_df=admitted,
        post_quarantined_count=post_quarantined_count,
        post_quarantine_snapshot_id=post_quarantine_snapshot_id,
        post_check_drift=post_check_drift,
    )
