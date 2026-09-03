"""`pre_check` stage — 005.1 §6.5/§6.6 compiled-contract flow, guarded quarantine append.

Three paths, decided by two guard-read probes (I-3: data, never snapshot
metadata) — the quarantine guard `(batch_id, "pre_check")` and the
fact-presence probe ([DC-1]):

**006.1 §8.2's ANY-door delta (bead conveyer-6pg.13, B3).** [DC-1]'s
`f_present` probe is no longer a single `table_has_batch(spec.fact_table,
...)` call (that singular field is gone, P-1) — it is now the SAME
ANY-declared-fact-table composition `core/doors.py::any_fact_present`
gives `stages/post_check.py`'s own doors (§8.2's own text: "pre_check's
[DC-1] door migrates identically"): one `table_has_batch` probe per
declared fact type (P-1's `fact_types` enumeration), ORed together. Nothing
else in this file's own door logic changes — the two-probe DECISION shape,
and everything each door does once decided, are unchanged.

- **Door 1 — quarantine guard present** (§6.5's A-9 subtraction path, a
  rerun after a prior attempt found violations and wrote them): durable
  quarantine rows are authoritative. `valid_df` = the current contract's
  `typed_projection` over `raw_df` minus malformed rows minus the durable
  locators (`checks.locator_subtraction`) — rows whose cell now fails the
  CURRENT contract's cast stay in `valid_df` with a NULL cell, "recorded,
  never dropped" (§6.5's letter). The recompute demotes to a drift PROBE
  (`ctx.pre_check_drift`, a value-free counts-only summary string, [S-7]) —
  never raised. No count-identity assertion on this door (the identity can
  fail deterministically here and would wedge a batch whose only remaining
  job is to re-announce, [H-2]'s own logic). `guard_skips` accretes
  `"pre_check"` — a genuine guard-skip.
- **Door 2 — facts present, quarantine guard absent** ([DC-1], 005.1 §7.2/
  §10 errata: the zero-violation-batch-rerun-after-a-contract-change case):
  the batch's committed facts already embody an empty pre_check violation
  set — recomputing under the CURRENT contract and appending would
  quarantine rows already committed as facts, a silent complementarity
  break through the one door the quarantine guard cannot see (a
  zero-violation attempt writes no guard row at all). So: **no append**,
  `valid_df` = the current contract's `typed_projection` over raw minus
  malformed rows (the "minus flagged" filter is provably vacuous here — a
  flagged row would have quarantined, forcing door 1 — kept as
  defense-in-depth, [R2-5c]), recompute demoted to the SAME drift probe
  (against an EMPTY durable-locator set, since none exist on this door).
  `guard_skips` does NOT accrete `"pre_check"` here (the ledger's own
  `outcome="ok"` signature for this door — the quarantine table's OWN guard
  was never present).
- **Door 3 — genuinely fresh** (neither present): one evaluation, two
  outputs (§6.3). `valid_df` = `typed_projection` over the zero-failure
  subset of `evaluate(raw_df, compiled)` (`checks.zero_failures` — the
  stage-level filter `typed_projection`'s own docstring names);
  `viol = checks.violations(evaluated)`; a non-empty `viol` is shaped
  (`quarantine.shape_pre_quarantine`) and appended, guarded
  `(batch_id, "pre_check")`; zero violations ⇒ no write (naturally
  idempotent, unchanged). The count identity
  `raw_count == |valid_df| + pre_quarantined_count` is asserted HERE ONLY
  (§6.3/§6.6) — the one door where both sides are fresh, in-memory
  computations with nothing durable to defer to.

`pre_quarantined_count = 0` **before any branch** ([DC-13][R2-5a]) — only
door 3's non-empty-violations branch overwrites it with the append's own
returned count; doors 1/2 (no write) and door 3's zero-violations branch all
leave it at this initial value, matching §6.5(4)'s "attempt delta — the
ledger's healthy-rerun signature".

`stage_key = "pre_check"` for the quarantine table throughout (shared with
`post_check`, disambiguated by its own `check_stage` column, unlike land's
raw table); `stage_key = None` for the fact-table presence probe (that table
has no `check_stage` column, commit.py's own convention).
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine.core import doors
from spine.core.model import LineageStamp
from spine.frames import checks, quarantine

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from spine.context import BatchContext
    from spine.effects.records import RunnerFx

_STAGE_KEY = "pre_check"
# 005.1 §6.5/D-9: pre_check quarantine rows always carry this non-null,
# unique locator triple -- the drift probe's own comparison key.
_LOCATOR_COLUMNS: tuple[str, str, str] = ("source_uri", "object_seq", "row_index")


def _lineage_stamp(ctx: BatchContext) -> LineageStamp:
    return LineageStamp(
        batch_id=ctx.batch_id,
        delivery_id=ctx.delivery_id,
        feed_id=ctx.feed_id,
        received_at=ctx.received_at,
    )


def _locators(df: DataFrame) -> DataFrame:
    return df.select(*_LOCATOR_COLUMNS)


def _admitted_cast_failure_count(admitted_df: DataFrame, compiled: checks.CompiledContract) -> int:
    """§6.5's "no admitted-row cast failures" drift condition: a row that
    stays admitted (not durably quarantined) yet whose recomputed cast,
    under the CURRENT contract, is NULL where the raw cell was not --
    exactly check 3's own predicate, via `frames.checks.cast_failure_
    predicate` (critique nit c, bead conveyer-azr.30 -- previously
    re-derived inline here from `compiled.typed_exprs`, a second copy of
    `frames/checks.py::_cast_check`'s own logic that could silently diverge
    from the check it probes; now the single authored predicate, exported)."""
    return admitted_df.filter(checks.cast_failure_predicate(compiled)).count()


def _drift_probe(
    raw_df: DataFrame,
    compiled: checks.CompiledContract,
    durable_locators: DataFrame,
    check_version: str,
) -> str | None:
    """§6.5 doors 1/2's shared drift probe: recompute §6.3's violation
    locator set over the CURRENT contract and compare it against
    `durable_locators` (door 2 passes an empty, correctly-shaped frame --
    no durable rows exist there). `None` only when the two locator sets are
    identical AND no row admitted into `valid_df` shows a retained cast
    failure under the current contract; else a short, value-free
    counts-only summary string ([S-7]) -- recorded as data, never raised,
    an exact mirror of post_check's own `post_check_drift` [H-2]."""
    evaluated = checks.evaluate(raw_df, compiled)
    recomputed_locators = _locators(checks.violations(evaluated))
    durable = _locators(durable_locators)
    only_durable = durable.join(recomputed_locators, on=list(_LOCATOR_COLUMNS), how="left_anti")
    only_recomputed = recomputed_locators.join(durable, on=list(_LOCATOR_COLUMNS), how="left_anti")
    d = durable.count()
    r = recomputed_locators.count()
    a = only_durable.count()
    b = only_recomputed.count()
    admitted = raw_df.filter(F.col("malformed_text").isNull()).join(
        durable, on=list(_LOCATOR_COLUMNS), how="left_anti"
    )
    c = _admitted_cast_failure_count(admitted, compiled)
    if a == 0 and b == 0 and c == 0:
        return None
    return (
        f"pre_check drift: durable={d} recomputed={r} only_durable={a} only_recomputed={b} "
        f"admitted_cast_failures={c} check_version={check_version[:16]}"
    )


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    """§6.6 pre_check flow: compile the contract, decide by the two presence
    probes, shape+append only on the genuinely-fresh door."""
    raw_df = ctx.raw_df
    if raw_df is None:  # sequencing contract: land always runs first (§7.3 STAGES order)
        raise ValueError("pre_check: ctx.raw_df is None -- land must run before pre_check")
    if ctx.raw_count is None:
        raise ValueError("pre_check: ctx.raw_count is None -- land must run before pre_check")

    compiled = checks.compile_contract(ctx.spec.raw_contract)
    quarantine_table = ctx.spec.quarantine_table
    q_present = fx.table_has_batch(quarantine_table, ctx.batch_id, _STAGE_KEY)
    # 006.1 §8.2's ANY-door delta: one probe per declared fact type, ORed
    # (module docstring) -- replaces the pre-006.1 singular `spec.fact_table`
    # probe.
    fact_presence = {
        fact_type: fx.table_has_batch(fact_type_decl.fact_table, ctx.batch_id, None)
        for fact_type, fact_type_decl in ctx.spec.fact_types.items()
    }
    f_present = doors.any_fact_present(fact_presence)  # [DC-1]
    pre_quarantined_count = 0  # [DC-13][R2-5a]: explicit before any branch

    if q_present:  # Door 1: A-9 read-back subtraction (§6.5)
        durable_q = fx.read_batch(quarantine_table, ctx.batch_id).filter(
            F.col("check_stage") == F.lit(_STAGE_KEY)
        )
        valid_df = checks.typed_projection(checks.locator_subtraction(raw_df, durable_q), compiled)
        pre_check_drift = _drift_probe(raw_df, compiled, durable_q, ctx.check_version)
        guard_skips = (*ctx.guard_skips, "pre_check")
    elif f_present:  # Door 2: [DC-1] fact-presence demotion
        minus_flagged = raw_df.filter(F.col("malformed_text").isNull())
        valid_df = checks.typed_projection(minus_flagged, compiled)
        empty_locators = _locators(raw_df).filter(F.lit(False))
        pre_check_drift = _drift_probe(raw_df, compiled, empty_locators, ctx.check_version)
        guard_skips = ctx.guard_skips  # the quarantine guard was never present -- outcome "ok"
    else:  # Door 3: genuinely fresh -- one evaluation, two outputs (§6.3)
        evaluated = checks.evaluate(raw_df, compiled)
        valid_df = checks.typed_projection(checks.zero_failures(evaluated), compiled)
        viol = checks.violations(evaluated)
        pre_check_drift = None
        guard_skips = ctx.guard_skips
        if not viol.isEmpty():
            shaped = quarantine.shape_pre_quarantine(
                viol, _lineage_stamp(ctx), ctx.check_version, fx.now()
            )
            pre_quarantined_count, _summary = fx.append(
                quarantine_table, shaped, ctx.batch_id, _STAGE_KEY
            )
        valid_count = valid_df.count()  # one count job, fresh path only (§6.3)
        assert ctx.raw_count == valid_count + pre_quarantined_count, (
            "005.1 §6.3 count identity violated on pre_check's genuinely-fresh path "
            f"(batch_id={ctx.batch_id!r}): raw_count={ctx.raw_count} "
            f"valid_count={valid_count} pre_quarantined_count={pre_quarantined_count}"
        )

    # I-19: the stamped-summary lookup, unconditional, after the branch --
    # naturally resolves to `None` on any door/branch that never wrote a
    # quarantine row for `(batch_id, "pre_check")` (doors 2, and door 3's
    # zero-violations case), the same idiom `land.py`/`commit.py` use.
    pre_quarantine_snapshot_id = fx.resolve_batch_snapshot(
        quarantine_table, ctx.batch_id, _STAGE_KEY
    )

    return replace(
        ctx,
        guard_skips=guard_skips,
        valid_df=valid_df,
        pre_quarantined_count=pre_quarantined_count,
        pre_quarantine_snapshot_id=pre_quarantine_snapshot_id,
        pre_check_drift=pre_check_drift,
    )
