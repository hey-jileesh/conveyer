"""`post_check` stage — I-12 violation subtraction, drift recorded as data. LLD §7.5, I-12, [H-2];
005.1 §8.2 (interim, 006-registered).

`viol = ctx.transforms.post_check(ctx.candidate_facts_df, ctx.co_effects)` is
the only pure-transform call; everything else here is guard/append/read-back
plumbing. Path selection mirrors pre_check's own §6.5/§6.6 two-probe
decision exactly [R2-1] — the quarantine guard `(batch_id, "post_check")`
and the fact-table presence probe `table_has_batch(fact_table, batch_id,
None)` ([DC-1]):

- **Path 4 — quarantine guard present** (§8.2.4, the rerun door): durable
  quarantine rows are authoritative, but a durable post_check row no longer
  carries candidate columns at all (only `row_hash`, §4.2) — the all-column
  bag-subtract path 3 uses is impossible here. Recomputed candidates are
  hashed (`frames.quarantine.candidate_row_hash`, the §7.3 UDF — the ONE
  admit-path UDF cost, paid only on this rerun) and anti-joined against the
  durable `row_hash` column (`frames.checks.hash_subtraction`, [DC-5]'s
  third, hash-keyed mechanism). The SAME count identity is checked, but a
  mismatch is **recorded as data** (`ctx.post_check_drift`), never raised
  ([H-2]) — the durable count, not the recomputed one, is this door's
  `post_quarantined_count` (the ledger's attempt-truth signature).
  `guard_skips` accretes `"post_check"` — a genuine guard-skip.
- **Path 2 — facts present, quarantine guard absent** ([DC-1]/[R2-1], a
  deliberate `--rN` after commit under a tightened business rule): the
  batch's committed facts already embody an empty post_check violation set
  — recomputing under CURRENT business rules/co-effects and appending would
  quarantine candidates already committed as facts, the same silent
  complementarity break one stage later (§8.2.2). So: **no append**,
  `admitted = candidate` (durable state authoritative), recompute demoted
  to the SAME `post_check_drift` channel (comparing the freshly recomputed
  `viol` against an empty durable set), no count-identity assertion.
  `guard_skips` does NOT accrete `"post_check"` here (outcome `"ok"` — the
  quarantine table's OWN guard was never present).
- **Path 3 — genuinely fresh** (neither present, §8.2.3 [DC-5], unchanged
  mechanism): `admitted = frame_checks.violation_subtraction(candidate,
  viol)` (multiplicity-preserving bag subtraction, [C-8]) — both sets are
  candidate-shaped in memory, so nothing forces hashing here, and this admit
  path stays UDF-free (A-7's cost claim). The count identity
  `candidate == admitted + violations` is **asserted** here — a
  non-conforming `post_check` that fabricates or drops rows is a review
  defect this stage fails loudly on, not something 006's eventual keyed
  violation identity should have to catch first.

`quarantine.shape_post_quarantine` (§8.1/A-14) is the one quarantine writer,
on path 3's append only. The A-14 business-reason-grammar check (a named
`ValueError`, A-14a, on any `reason` value not fullmatching
`^business/[a-z0-9][a-z0-9-]*$` — free text cannot be mechanically verified
value-free, so the pipeline author fixes their `post_check`, never a
framework guess or a silent drop) now lives HERE, not inside
`shape_post_quarantine` (critique F1, bead conveyer-azr.30): this stage
materializes `quarantine.nonconforming_reasons(viol)` (the pure filter) and
raises the same named `ValueError` itself, reusing the `.count()` this
stage already pays on the fresh path — `shape_post_quarantine`/`frames/`
stay DataFrame-in/DataFrame-out plan builders throughout, never executing a
job or raising mid-composition (`frames/checks.py:5-13`'s own contract: "no
`.count()` … never turns a count into control flow itself").

This stage carries ZERO instrumentation of its own (§7.3) — it only ever
sets `ctx.post_check_drift`; `core/run_facts.py::_stage_fields` folds it into
the ledger row's `error_message`, and `effects/ledger.py::record_run`
derives the WARNING log + EMF `PostCheckDrift` emission from that `RunFact`
field (critique F4, bead conveyer-nvh.43).
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine.core.model import LineageStamp
from spine.frames import checks as frame_checks
from spine.frames import quarantine

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

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


def _drift_message(durable_count: int, recomputed_count: int, subset_ok: bool) -> str:
    """One grammar, one meaning per field, across BOTH doors that call this
    (critique nit a, bead conveyer-azr.30 -- previously `recomputed_count`
    carried the full recomputed CANDIDATE count on path 4 but the recomputed
    VIOLATIONS count on path 2, two different quantities sharing one label):

    * `durable_count`: the durable `post_check` quarantine row count for
      this batch (0 on path 2 -- no durable rows exist on that door by
      construction; `durable_violations_count` on path 4).
    * `recomputed_count`: THIS attempt's recomputed violation count under
      the current business rules/co-effects (`violations_count` on path 2,
      unchanged; `candidate_count - admitted_count` on path 4 -- algebraically
      identical to path 4's own `identity.ok` boolean below, since
      `admitted_count = candidate_count - recomputed_count` by
      `hash_subtraction`'s own construction).
    * `subset_ok`: whether `recomputed_count == durable_count` -- always
      `False` when this function is actually called (both call sites only
      build the message on a mismatch), kept as an explicit field for
      symmetry with the count-identity check that gates the call, not as
      new information a reader should expect to vary.
    """
    return (
        f"post-check drift: durable={durable_count} recomputed={recomputed_count} "
        f"subset={subset_ok}"
    )


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    assert ctx.candidate_facts_df is not None, "post_check requires candidate_facts_df (apply)"
    assert ctx.co_effects is not None, "post_check requires co_effects (pull)"
    candidate = ctx.candidate_facts_df

    quarantine_table = ctx.spec.quarantine_table
    q_present = fx.table_has_batch(quarantine_table, ctx.batch_id, _STAGE)
    f_present = fx.table_has_batch(ctx.spec.fact_table, ctx.batch_id, None)  # [DC-1][R2-1]

    admitted: DataFrame
    if q_present:  # Path 4: §8.2.4 hash-keyed rerun subtraction
        durable_viol = fx.read_batch(quarantine_table, ctx.batch_id).filter(
            F.col("check_stage") == F.lit(_STAGE)
        )
        hashed_candidate = quarantine.candidate_row_hash(candidate)
        admitted = frame_checks.hash_subtraction(hashed_candidate, durable_viol)
        candidate_count = candidate.count()
        admitted_count = admitted.count()
        durable_violations_count = durable_viol.count()
        identity = frame_checks.check_count_identity(
            candidate_count, admitted_count, durable_violations_count
        )
        # [H-2]: recorded as data, never raised -- this attempt's recomputed
        # candidate may legitimately disagree with another attempt's durable
        # quarantine rows. Counts only, no row values [S-7]. `recomputed_
        # violations_count` (not the full `candidate_count`) is `_drift_
        # message`'s unified "recomputed" grammar (nit a) -- the count of
        # THIS attempt's candidates that hash-matched a durable violation.
        recomputed_violations_count = candidate_count - admitted_count
        post_check_drift = (
            None
            if identity.ok
            else _drift_message(durable_violations_count, recomputed_violations_count, identity.ok)
        )
        post_quarantined_count = durable_violations_count  # durable count, not recomputed
        guard_skips = (*ctx.guard_skips, "post_check")
    elif f_present:  # Path 2: [DC-1][R2-1] fact-presence demotion
        admitted = candidate
        viol = ctx.transforms.post_check(candidate, ctx.co_effects)
        violations_count = viol.count()  # `_drift_message`'s "recomputed" grammar (nit a)
        # `subset_ok=False` is definitional on this branch, not a
        # recomputed comparison: `durable_count` is always 0 on this door
        # (no durable rows exist here by construction) and this branch
        # only runs when `violations_count > 0`, so `recomputed_count ==
        # durable_count` can never hold -- literal `False`, matching
        # `_drift_message`'s own "always False when called" note.
        post_check_drift = (
            None if violations_count == 0 else _drift_message(0, violations_count, False)
        )
        post_quarantined_count = 0  # durable state authoritative -- no append
        guard_skips = ctx.guard_skips  # quarantine guard never present -- outcome "ok"
    else:  # Path 3: genuinely fresh -- unchanged mechanism [DC-5]
        viol = ctx.transforms.post_check(candidate, ctx.co_effects)
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
            # A-14/§8.2.1 (critique F1, bead conveyer-azr.30): the business-
            # reason-grammar check now lives HERE, materialized by THIS
            # stage (which already counts on every path) -- never inside
            # `frames/quarantine.py`, which stays a pure plan builder.
            nonconforming_count = quarantine.nonconforming_reasons(viol).count()
            if nonconforming_count > 0:
                raise ValueError(
                    f"post_check: {nonconforming_count} row(s) have a 'reason' value "
                    "not fullmatching ^business/[a-z0-9][a-z0-9-]*$ (005.1 A-14/§8.2.1)"
                )
            shaped = quarantine.shape_post_quarantine(
                viol, _lineage_stamp(ctx), ctx.check_version, fx.now(), ctx.spec.domain_id_col
            )
            rows_appended, _summary = fx.append(quarantine_table, shaped, ctx.batch_id, _STAGE)
            post_quarantined_count = rows_appended
        else:
            post_quarantined_count = 0  # zero violations -- no write, no guard row
        # zero-violations is a data-driven skip, not a guard-skip (mirrors
        # pre_check's own "zero violations" branch) -- guard_skips untouched
        # on this whole fresh-compute path either way.
        guard_skips = ctx.guard_skips
        post_check_drift = None

    post_quarantine_snapshot_id = fx.resolve_batch_snapshot(quarantine_table, ctx.batch_id, _STAGE)

    return replace(
        ctx,
        guard_skips=guard_skips,
        admitted_facts_df=admitted,
        post_quarantined_count=post_quarantined_count,
        post_quarantine_snapshot_id=post_quarantine_snapshot_id,
        post_check_drift=post_check_drift,
    )
