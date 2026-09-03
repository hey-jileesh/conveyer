"""`post_check` stage — the framework's own interpreter, per fact type. LLD 006.1 §7-§8 (P-1/P-5/
P-7/P-8), §4.5; 007.1 §7.1's door paragraph (semantics are 006.1's, cited only).

**Full rewrite (bead conveyer-6pg.13, B3) — supersedes the 005.1/single-
fact-type mechanism this file used to carry.** Pipelines contribute ZERO
check code and ZERO attribution code (D-1/D-6): this stage composes `frames/
business_checks.py` (compile once per type, evaluate per type) + `frames/
quarantine.py::shape_post_quarantine` (the one writer) entirely from
declared data (`ctx.spec.checks`, `ctx.spec.fact_types`) — there is no
`ctx.transforms.post_check` any more (§4.4's hard cut; `Transforms` itself
no longer carries the attribute).

**Path selection** mirrors pre_check's own two-probe decision, generalized
to the ANY-table composition (§8.2's `core/doors.py::post_check_path`) —
the quarantine guard `(batch_id, "post_check")` and the ANY-declared-fact-
table presence probe (one `table_has_batch` per declared fact type, P-1's
enumeration):

- **FRESH** (§8.1): per type, compile once (`business_checks.
  compile_business_checks`), evaluate (`business_checks.evaluate`), split
  into `admitted_candidates`/`business_violations` — the complementary-
  filter split itself IS the bag subtraction ([DC-5], `violation_
  subtraction` retired, `frames/checks.py`'s own docstring). Per-type count
  identity (`|candidate_t| == |admitted_t| + |violations_t|`) is asserted
  here only (§8.1's "fresh-only rule"). Every type's violations are shaped
  (`quarantine.shape_post_quarantine`, tagging `_conveyer_fact_type`,
  P-7(b)) and UNION-ed into ONE guarded append `(batch_id, "post_check")` —
  quarantine stays one table, one post-stage writer, I-4's one-commit
  invariant untouched by multi-type pipelines. Zero violations across ALL
  types ⇒ no write, no guard row (naturally idempotent).
- **DURABLE_SUBTRACT** (§8.3, quarantine guard present — a rerun after a
  prior attempt found violations and wrote them): per type, recompute
  candidates, hash them WITH the `_conveyer_fact_type` tag through the §10
  UDF (`quarantine.candidate_row_hash`, the one admit-path UDF cost this
  bead pays only on this rerun path, A-7's claim intact), anti-join the
  SAME durable `row_hash` set (`frames.checks.hash_subtraction`) —
  `admitted_facts[t]` = survivors. Type-discrimination is what the tag
  buys (G-03): a durable type-A hash is inert against a value-identical
  type-B candidate, so cross-type rows never cross-subtract. No count
  identity on this path ([H-2]) — a SEPARATE recompute (evaluate + hash
  the recomputed VIOLATIONS, per type, unioned) is compared against the
  durable set to derive `post_check_drift`, recorded as data, never
  raised. `guard_skips` accretes `"post_check"` (a genuine guard-skip).
- **DURABLE_AUTHORITY** (§8.2 [R2-1], quarantine guard absent + ANY declared
  fact table already carries this batch — a deliberate `--rN` after commit
  under a tightened business rule): the batch's committed facts already
  embody an empty post_check violation set for EVERY type; recomputing
  under CURRENT business rules and appending would quarantine candidates
  whose siblings already stand as facts (§8.2's [H-2] wedge argument) — so
  `admitted_facts[t] = candidate_facts[t]` for every type, unconditionally,
  **no check evaluation gates admission on this path**. Drift is still
  recorded (the SAME recompute-and-hash-compare mechanism as
  DURABLE_SUBTRACT, against an EMPTY durable set), demoted to the same
  channel, never raised; no count identity. `guard_skips` does NOT accrete
  `"post_check"` (the quarantine table's OWN guard was never present —
  outcome `"ok"`).

**`batch_check` (§7.5/§7.6, P-6) is structurally unreachable through this
stage's `run()` in this milestone.** `core/model.py`'s `ChecksModel` (K7)
refuses ANY `batch_check` entry at spec-PARSE time, unconditionally, until
the 005 v1.x member grammar lands — no validly-bound `PipelineSpecModel`
can carry one, so `business_checks.compile_business_checks`'s own
`isinstance` filter already excludes `BatchCheckModel` structurally (its
own docstring). `run()` itself therefore has no live call site for
`batch_check` evaluation (G-05(a)'s bind-defect coverage is the whole
story at the SPEC grain; G-05(b-h) are 005 v1.x's own named wait, 006.1
§14's B4 row).

**The §7.5/§8.4 MECHANICS land now anyway (conveyer-swb.15, D006-1's "build
now, dormant behind K7" ruling) — `_evaluate_aggregate`/`_extract_control_
value`/`_apply_batch_check_verdict` below.** Each is exercised directly by
its own unit tests, never wired into `run()`'s live control flow — the
SAME precedent `entrypoints/glue_main.py::_assert_check_expressions_
compile`'s own module docstring already established for its (also-dormant)
`BatchCheckModel` branch ("Implemented anyway, never guarded behind 'if
batch checks were ever reachable'..."). `_evaluate_aggregate` needs no
member accessor (the aggregate's subject is simply `candidate_facts[fact_
type]`, already at hand); `_extract_control_value` takes an ALREADY-
RESOLVED `member_rows` frame as a parameter — resolving WHICH frame that
is is §9's own named wait (the member-scoped accessor, the parsed member
declarations, the `required:`-coherence check), deliberately not invented
here.

**F-5 fix (security gate `wf_c9aadeb2-8eb`, LOW).** `_extract_control_value`
used to accept an already-`ValidatedExpr` and execute its `authored_text`
via `F.expr` with no re-derivation — the control position's executed-text
guarantee rested on a type annotation, not a gate (`F.expr` admits
`reflect()`/`java_method()`, arbitrary static JVM calls). It now takes the
RAW authored control-expression text plus the bound fact type's family map
and re-derives a `ValidatedExpr` through gate 1 itself
(`check_grammar.validate_expression(text, "scalar", family_map)`) — the
SAME re-derive-never-trust idiom `frames/business_checks.py::_compiled_expr`
already established for row checks. A `GrammarDefect` here is a framework
defect (bind should already have refused this spec) — raises, never
quarantines, mirroring `_compiled_expr`'s own precedent.

This stage carries ZERO instrumentation of its own (§7.3) — it only ever
sets `ctx.post_check_drift`; `core/run_facts.py::_stage_fields` folds it into
the ledger row's `error_message`, and `effects/ledger.py::record_run`
derives the WARNING log + EMF `PostCheckDrift` emission from that `RunFact`
field (unchanged from the pre-006.1 mechanism, critique F4, bead
conveyer-nvh.43).
"""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine.core import doors
from spine.core.check_grammar import GrammarDefect, validate_expression
from spine.core.doors import PostCheckPath
from spine.core.model import LineageStamp
from spine.frames import business_checks, quarantine
from spine.frames import checks as frame_checks

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyspark.sql import DataFrame

    from spine.context import BatchContext
    from spine.core.check_grammar import CompiledAggregate, Family
    from spine.core.model import ChecksModel, FactSchemaModel, FactTypeModel
    from spine.effects.records import RunnerFx

_STAGE = "post_check"


def _lineage_stamp(ctx: BatchContext) -> LineageStamp:
    return LineageStamp(
        batch_id=ctx.batch_id,
        delivery_id=ctx.delivery_id,
        feed_id=ctx.feed_id,
        received_at=ctx.received_at,
    )


def _drift_message(
    durable_count: int,
    recomputed_count: int,
    only_durable: int,
    only_recomputed: int,
    checks_version: str,
) -> str:
    """§8.3's value-free drift grammar -- the exact `pre_check.py::
    _drift_probe` shape, reused: counts only, no row values [S-7]."""
    return (
        f"post_check drift: durable={durable_count} recomputed={recomputed_count} "
        f"only_durable={only_durable} only_recomputed={only_recomputed} "
        f"checks_version={checks_version[:16]}"
    )


def _recompute_violation_hashes(
    candidate_facts: Mapping[str, DataFrame],
    fact_types: Mapping[str, FactTypeModel],
    checks: ChecksModel,
    co_effects: Mapping[str, DataFrame],
) -> DataFrame:
    """Per type: compile + evaluate + `business_violations`, hashed (tagged,
    declared-columns-only, §10's UDF) via `quarantine.candidate_row_hash` --
    the SAME hash a durable quarantine row for this type carries. Unioned
    into one `row_hash`-only frame across every declared type."""
    hashed_frames = []
    for fact_type, candidate_df in candidate_facts.items():
        schema: FactSchemaModel = fact_types[fact_type].schema_
        compiled = business_checks.compile_business_checks(checks, fact_type, schema)
        evaluated = business_checks.evaluate(candidate_df, compiled, co_effects)
        viol = business_checks.business_violations(evaluated)
        hashed = quarantine.candidate_row_hash(viol, fact_type, schema)
        hashed_frames.append(hashed.select("row_hash"))
    recomputed = hashed_frames[0]
    for frame in hashed_frames[1:]:
        recomputed = recomputed.unionByName(frame)
    return recomputed


def _drift_probe(
    candidate_facts: Mapping[str, DataFrame],
    fact_types: Mapping[str, FactTypeModel],
    checks: ChecksModel,
    co_effects: Mapping[str, DataFrame],
    durable_hashes: DataFrame,
    checks_version: str,
) -> str | None:
    """§8.3/§8.2's shared drift probe (called on BOTH non-FRESH doors, an
    exact mirror of `pre_check.py::_drift_probe`'s door-1/door-2 reuse):
    recompute every type's violations, hash them, and compare the UNION
    against `durable_hashes` (the real durable set on DURABLE_SUBTRACT; an
    empty, correctly-shaped frame on DURABLE_AUTHORITY, where no durable
    rows exist by construction). `None` iff the two hash sets are
    identical; else the value-free counts-only summary."""
    recomputed = _recompute_violation_hashes(candidate_facts, fact_types, checks, co_effects)
    durable = durable_hashes.select("row_hash")
    only_durable = durable.join(recomputed, on="row_hash", how="left_anti")
    only_recomputed = recomputed.join(durable, on="row_hash", how="left_anti")
    d, r = durable.count(), recomputed.count()
    a, b = only_durable.count(), only_recomputed.count()
    if a == 0 and b == 0:
        return None
    return _drift_message(d, r, a, b, checks_version)


def _empty_row_hash_frame(candidate_facts: Mapping[str, DataFrame]) -> DataFrame:
    """A plan-only, zero-row `row_hash: string` frame -- DURABLE_AUTHORITY's
    "no durable rows exist on this door" input to `_drift_probe`, built via
    `.limit(0)` (the pure, plan-only way to shape an empty frame, the
    identity exemplar's own `post_check` idiom) over an arbitrary candidate
    frame's own SparkSession -- never a hardcoded schema import."""
    any_frame = next(iter(candidate_facts.values()))
    return any_frame.select(F.lit(None).cast("string").alias("row_hash")).limit(0)


# =============================================================================
# §7.5/§8.4 `batch_check` mechanics — dormant behind P-6/K7 (conveyer-swb.15,
# D006-1). See this module's own docstring: no call site in `run()` below.
# =============================================================================


def _evaluate_aggregate(
    candidate_df: DataFrame, compiled: CompiledAggregate
) -> business_checks.AggregateOutcome:
    """§7.5 [EM-5]'s "one aggregation pass": `(candidate_count, aggregate)`
    computed together over ONE `.agg(...)` call. `compile_aggregate`'s own
    [EM-4] per-node coalesce-to-zero law is already baked into `aggregate_
    col` (`business_checks.aggregate_column`'s own construction); the
    candidate count rides the SAME call via `F.sum(F.lit(1))` -- never
    `F.count` (`frames/**`'s banned-attr-name rule; the `aggregate_column`
    precedent this function reuses)."""
    aggregate_col = business_checks.aggregate_column(compiled.tree, candidate_df)
    row_count_col = F.sum(F.lit(1))
    row = candidate_df.agg(aggregate_col.alias("_agg"), row_count_col.alias("_n")).collect()[0]
    return business_checks.AggregateOutcome(
        candidate_count=row["_n"] or 0, aggregate_value=row["_agg"]
    )


def _extract_control_value(
    member_rows: DataFrame,
    control_expr_text: str,
    family_map: Mapping[str, Family | None],
) -> business_checks.ControlOutcome:
    """§9's extraction plan, restated at its own post-extraction boundary:
    `member_rows` is whatever frame the (005 v1.x) member-scoped accessor
    would hand this function -- resolving WHICH frame that is is explicitly
    out of scope here (§9: "this section deliberately contains no member-
    grammar mechanics"). Given that frame: the exactly-one assertion
    (`count == 1`, §9's own subject), then a scalar extraction over that one
    row.

    Takes the RAW authored control-expression TEXT (never an already-
    `ValidatedExpr`, F-5 fix) and re-derives a `ValidatedExpr` itself via
    gate 1 (`check_grammar.validate_expression(text, "scalar", family_map)`)
    before ever reaching `F.expr` -- the SAME re-derive-never-trust idiom
    `frames/business_checks.py::_compiled_expr` already establishes for row
    checks (§6.4's executed-text rule: byte-exact, but only ONCE the text
    has itself passed the gate this function's own call re-runs). A
    `GrammarDefect` here is a framework defect (bind should already have
    refused this spec) -- raises, never quarantines, matching `_compiled_
    expr`'s own precedent."""
    admitted_row_count = member_rows.count()
    if admitted_row_count != 1:
        return business_checks.ControlOutcome(admitted_row_count=admitted_row_count)
    validated = validate_expression(control_expr_text, "scalar", family_map)
    if isinstance(validated, GrammarDefect):
        raise ValueError(
            "post_check: control expression rejected at gate 1 re-derivation (framework "
            f"defect -- bind should have refused this spec): {validated.code}: {validated.detail}"
        )
    value = member_rows.select(F.expr(validated.authored_text).alias("_v")).collect()[0]["_v"]
    return business_checks.ControlOutcome(admitted_row_count=1, value=value)


def _apply_batch_check_verdict(
    fact_presence: Mapping[str, bool],
    check_id: str,
    verdict: business_checks.BatchCheckVerdict,
    checks_version: str,
) -> str | None:
    """§8.4's demotion door -- independent of the row-path doors above,
    reusing the SAME `doors.any_fact_present` primitive (never forked):
    decide-then-do, the door decision computed exactly once. Demoted (ANY
    declared fact table already has this batch): returns the §12 drift
    segment for the caller to fold into `post_check_drift` -- the verdict
    is NEVER raised on this branch. Not demoted, and the verdict is a
    failure (`mismatch`/`control-unavailable`/`control-ambiguous`/
    `aggregate-unavailable`): raises the value-free §7.5 `batch-check-
    failed` message (deterministic, burns retries, A-10's class) after the
    quarantine append (§7.6's own sequencing -- this function's CALLER is
    responsible for calling it only after that append, per §7.6). A
    `match` verdict, not demoted, returns `None` (nothing to append,
    nothing to raise)."""
    if doors.any_fact_present(fact_presence):
        return business_checks.batch_check_drift_segment(check_id, verdict, checks_version)
    if verdict != "match":
        raise ValueError(
            business_checks.batch_check_failed_message(check_id, verdict, checks_version)
        )
    return None


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    assert ctx.candidate_facts is not None, "post_check requires candidate_facts (apply)"
    assert ctx.co_effects is not None, "post_check requires co_effects (pull)"
    candidate_facts = ctx.candidate_facts
    fact_types = ctx.spec.fact_types

    quarantine_table = ctx.spec.quarantine_table
    q_present = fx.table_has_batch(quarantine_table, ctx.batch_id, _STAGE)
    fact_presence = {
        fact_type: fx.table_has_batch(fact_type_decl.fact_table, ctx.batch_id, None)
        for fact_type, fact_type_decl in fact_types.items()
    }
    path = doors.post_check_path(q_guard_present=q_present, fact_presence=fact_presence)

    admitted: dict[str, DataFrame]
    if path is PostCheckPath.DURABLE_SUBTRACT:  # §8.3
        durable_viol = fx.read_batch(quarantine_table, ctx.batch_id).filter(
            F.col("check_stage") == F.lit(_STAGE)
        )
        admitted = {}
        for fact_type, candidate_df in candidate_facts.items():
            schema = fact_types[fact_type].schema_
            hashed_candidate = quarantine.candidate_row_hash(candidate_df, fact_type, schema)
            admitted[fact_type] = frame_checks.hash_subtraction(hashed_candidate, durable_viol)
        post_quarantined_count = durable_viol.count()  # durable count -- the ledger's own truth
        post_check_drift = _drift_probe(
            candidate_facts,
            fact_types,
            ctx.spec.checks,
            ctx.co_effects,
            durable_viol,
            ctx.checks_version,
        )
        guard_skips = (*ctx.guard_skips, "post_check")
    elif path is PostCheckPath.DURABLE_AUTHORITY:  # §8.2 [R2-1]
        admitted = dict(candidate_facts)  # durable state authoritative -- no evaluation gates it
        empty_hashes = _empty_row_hash_frame(candidate_facts)
        post_check_drift = _drift_probe(
            candidate_facts,
            fact_types,
            ctx.spec.checks,
            ctx.co_effects,
            empty_hashes,
            ctx.checks_version,
        )
        post_quarantined_count = 0
        guard_skips = ctx.guard_skips  # quarantine guard never present -- outcome "ok"
    else:  # FRESH -- §8.1
        admitted = {}
        shaped_frames: list[DataFrame] = []
        quarantined_at = fx.now()  # one clock reading, shared across every type this attempt
        for fact_type, candidate_df in candidate_facts.items():
            schema = fact_types[fact_type].schema_
            compiled = business_checks.compile_business_checks(ctx.spec.checks, fact_type, schema)
            evaluated = business_checks.evaluate(candidate_df, compiled, ctx.co_effects)
            admitted[fact_type] = business_checks.admitted_candidates(evaluated)
            viol = business_checks.business_violations(evaluated)
            candidate_count = candidate_df.count()
            admitted_count = admitted[fact_type].count()
            violations_count = viol.count()
            identity = frame_checks.check_count_identity(
                candidate_count, admitted_count, violations_count
            )
            if not identity.ok:
                raise AssertionError(
                    "006.1 §8.1 count identity violated on post_check fresh-compute path "
                    f"(batch_id={ctx.batch_id!r} fact_type={fact_type!r}): "
                    f"candidate={identity.candidate_count} admitted={identity.admitted_count} "
                    f"violations={identity.violations_count}"
                )
            if violations_count > 0:
                shaped_frames.append(
                    quarantine.shape_post_quarantine(
                        viol,
                        _lineage_stamp(ctx),
                        ctx.checks_version,
                        quarantined_at,
                        fact_type,
                        schema,
                    )
                )
        if shaped_frames:
            union = shaped_frames[0]
            for frame in shaped_frames[1:]:
                union = union.unionByName(frame)
            rows_appended, _summary = fx.append(quarantine_table, union, ctx.batch_id, _STAGE)
            post_quarantined_count = rows_appended
        else:
            post_quarantined_count = 0  # zero violations, every type -- no write, no guard row
        guard_skips = ctx.guard_skips  # zero-violations is a data-driven skip, not a guard-skip
        post_check_drift = None

    post_quarantine_snapshot_id = fx.resolve_batch_snapshot(quarantine_table, ctx.batch_id, _STAGE)

    return replace(
        ctx,
        guard_skips=guard_skips,
        admitted_facts=MappingProxyType(dict(admitted)),
        post_quarantined_count=post_quarantined_count,
        post_quarantine_snapshot_id=post_quarantine_snapshot_id,
        post_check_drift=post_check_drift,
    )
