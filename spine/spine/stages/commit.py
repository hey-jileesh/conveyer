"""`commit` stage — F-4's per-table loop, F-9's marker mechanics, F-5's delta
filter, F-8's per-table backstop. LLD 007.1 §4.3 (F-9), §7.1 (F-4), §7.2
(F-5), §7.3 (F-8); B9b, bead `conveyer-6pg.21`.

**Full rewrite (supersedes the single-fact-type mechanism this file used to
carry).** `ctx.admitted_facts` (006.1 §4.5, `Mapping[str, DataFrame]` keyed
by declared FACT-TYPE name) replaces the deleted singular `ctx.admitted_
facts_df`; this stage walks `ctx.spec.fact_types` in DECLARED order (total,
deploy-pinned) and, per type, decide-then-does its own fact table AND its
own marker rows — F-4's "N guarded appends, per-(stage, table) grain", never
a batch-grain skip.

**Resolution + probe run ONCE per batch (§7.2), before the per-table
loop.** `core/delta.py::resolve_predecessors` consumes three marker reads
(`fx.read_marker_completions`/`read_marker_presence`, feed-scoped;
`fx.read_marker_target`, batch-scoped, called only when a seed names a
Track-E target — unreachable in Phase 1, the `conveyer-kof` named wait,
§16) and yields ONE `PredecessorResolution` — the resolved predecessor
batch-id set and the probe-refusal reason (if any) — shared by every
type's own `frames/delta.py::delta_filter` call and projected once into
`ctx.delta_predecessor_batch_ids`/`ctx.delta_probe_refusal`.

**Write mechanics inside the loop, normative order (§4.3), per table *t*:**

1. `present = fx.table_has_batch(fact_table_t, batch_id, None)` (no
   `check_stage` disambiguation column on a fact table -- the quarantine
   table's own convention, unused here, matching the pre-B9b code).
2. **present** ⇒ ensure-twin (decide-then-do: `fx.marker_row_present` then,
   iff absent, `fx.append_marker_row` -- M-3's "guard-idempotent completion
   of a missing twin") and skip the append; `facts_appended_by_table[t] =
   0`, no key in `commit_snapshot_ids` (this attempt produced no snapshot
   for *t*, §4.2's own words).
3. **absent** ⇒ lineage + identity stamping (`frames/lineage.py::stamp_fact_
   lineage`, then `frames/facts.py::stamp_fact_identity` -- F-1's UDF,
   BEFORE the structural check: §6.1's DDL declares `content_hash`/
   `record_key` as framework-stamped columns, so the structural check's own
   column-set diff needs them present already); §7.3's structural check
   (fail fast, no append, no fold, on either violation -- I-24); `frames/
   delta.py::delta_filter` (F-5) against the framework's own predecessor-
   partition PROJECTION of *t* (`fx.read_table(fact_table_t)`, filtered to
   `resolution.predecessor_batch_ids` -- ONE pinned snapshot, recorded into
   `delta_read_snapshot_ids[t]`, covering both predecessor batches when
   Track E applies, §4.2); the `DivergentDuplicates` count (§12, D-2(b)
   made a number, per *t*), computed "after the within-batch collapse" over
   the stamped candidates and recorded into `divergent_duplicates_by_table
   [t]` -- **critique gate wf_24a3125f-ecc F1, bead conveyer-6pg.30: this
   stage only computes the pure count now; `effects/ledger.py::record_run`
   derives the WARNING+EMF from the ledger row alone**, mirroring
   `delta_probe_refusal`'s own channel (stages carry zero instrumentation,
   004 §13.3) -- see `_divergent_duplicate_count`'s own docstring.
4. **iff the novel set is non-empty:** ensure-twin (decide-then-do, same
   mechanism as step 2 -- the twin is decided AFTER the filter so it twins
   an actual, imminent append, §4.3), then `fx.append`.
5. A ZERO-novel table writes no marker row and no facts (F-4's zero-fact
   corollary at marker grain) -- `facts_appended_by_table[t] = 0`, no key in
   `commit_snapshot_ids`.

**Commit's LAST act, unconditionally, after all N tables:** the commit-
completion marker row (decide-then-do; rerun-idempotent) -- §6.3 answer 2's
"the zero-fact batch's durable visibility", written even when every table
was zero-fact.

**`source_ts` (§5.1 fragment 4/§6.1's framework stamp, hash-excluded,
NULLABLE) := the delivery's `received_at` -- HLD 007 D-3(b), verbatim:
"Source timestamp := delivery received_at -- framework-stamped at
registration, always present; it orders corrections correctly (a
superseding delivery arrived later, by definition); partner timestamps
REJECTED."** This is PERCEPTION time (when the framework registered the
delivery), never business/event time and never read from candidate data --
`frames/lineage.py::stamp_fact_lineage` already stamps this exact same
`LineageStamp.received_at` value onto every candidate's `received_at`
column (that function's own enumerated set is `batch_id`/`delivery_id`/
`feed_id`/`received_at`/`source_uri` only, unchanged -- `stamp_fact_lineage`
is not extended, §5.1 fragment 4 enumerates exactly its own five columns);
`_stamp_candidates` below stamps `source_ts` from the SAME `LineageStamp`
value, one clock reading shared by both columns. The DDL column stays
NULLABLE (§6.1 -- a type declaration, not a stamping instruction: a batch
carries exactly one `received_at`, so `source_ts` is non-null for every
fact this stage ever commits, but the column's own nullability is
unaffected by that fact) -- [T-11]'s null-ranks-lowest law (§8.1: "null
ranks lowest in the order") remains the fold's ordering semantics for any
future null this column might carry, framework-owned either way. (Bug
history: a prior revision of this function misread §6.1's NULLABLE
declaration as "no data source" and stamped a literal `NULL` on every fact,
silently deadening the ordering struct's second element -- fixed, U-1.)

**Iceberg write-time nullability quirk, empirically verified this bead
(recorded on `effects/spark.py::_build_append`'s own option, not here):**
the framework-stamped lineage columns and the UDF-derived `content_hash`/
`record_key` land NULLABLE at Iceberg's own write-time schema analysis
(through `stamp_fact_identity`'s UDF and `delta_filter`'s joins) even
though every actual value is non-null and the DDL declares them required --
`fx.append`'s own `check-nullability=false` write option (a real Iceberg
Spark option, structural only, never a per-row value relaxation) is the
fix; this stage need not, and does not, work around it itself.
"""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from spine.core import checks as core_checks
from spine.core import naming
from spine.core.delta import MarkerRowWrite, SeedAttrs, resolve_predecessors
from spine.core.model import LineageStamp
from spine.frames import delta as frames_delta
from spine.frames import facts as frames_facts
from spine.frames import lineage

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from spine.context import BatchContext
    from spine.core.model import FactSchemaModel
    from spine.effects.records import RunnerFx

_STAGE = "commit"


def _lineage_stamp(ctx: BatchContext) -> LineageStamp:
    return LineageStamp(
        batch_id=ctx.batch_id,
        delivery_id=ctx.delivery_id,
        feed_id=ctx.feed_id,
        received_at=ctx.received_at,
    )


def _ensure_marker_row(fx: RunnerFx, markers_table: str, write: MarkerRowWrite) -> None:
    """M-3's decide-then-do: a rerun re-affirms, never duplicates a row
    (§4.3, §6.3). The presence probe and the conditional write are both
    real `RunnerFx` effects (the SAME "guard mechanics live in the effect,
    the decide-then-do orchestration lives in the caller" split `stages/
    commit.py` already takes for `table_has_batch`/`fx.append` via `core/
    guards.py::plan_append` -- no pure `AppendPlan`-shaped value is
    introduced here since the decision is the same one-line `not present`
    `plan_append` already expresses, and threading `write` (a `MarkerRowWrite`)
    through a second dataclass would add a layer without adding a decision)."""
    present = fx.marker_row_present(markers_table, write.batch_id, write.stage, write.table_name)
    if not present:
        fx.append_marker_row(markers_table, write)


def _stamp_candidates(
    candidate_df: DataFrame, lineage_stamp: LineageStamp, schema: FactSchemaModel
) -> DataFrame:
    """Lineage stamp, then the framework `source_ts` stamp (module
    docstring: HLD 007 D-3(b), `= lineage_stamp.received_at`), then F-1's
    identity stamp -- §4.3's normative order: BOTH complete before the
    structural check ever runs. `source_ts` is stamped from the SAME
    `LineageStamp.received_at` value `frames/lineage.py::stamp_fact_lineage`
    already stamps as the `received_at` column -- one clock reading, shared
    by both columns, NEVER derived from candidate data (I-9)."""
    stamped_lineage = lineage.stamp_fact_lineage(candidate_df, lineage_stamp).withColumn(
        "source_ts", F.lit(lineage_stamp.received_at).cast(TimestampType())
    )
    declared_cols = tuple(column.name for column in schema.columns)
    key_cols = tuple(schema.record_key)
    timestamp_cols = tuple(column.name for column in schema.columns if column.type == "timestamp")
    return frames_facts.stamp_fact_identity(
        stamped_lineage, declared_cols, key_cols, timestamp_cols
    )


def _project_predecessor_facts(
    existing_df: DataFrame, predecessor_batch_ids: tuple[str, ...]
) -> DataFrame:
    """The framework's own predecessor-partition projection (§7.2: "the
    resolved batches' partitions of fact table *t*, projected to
    `(record_key, content_hash)`") -- single-partition reads under §6.4's
    `identity(batch_id)`; `existing_df` is already read at ONE pinned
    snapshot (the caller's own `fx.read_table` call, shared with the
    structural check), so both predecessor batches (Track E) share that one
    pin. An empty `predecessor_batch_ids` (every §7.2 path 1-3/5-9 outcome)
    projects to zero rows via an always-false filter -- `Column.isin()`
    unpacked over an empty tuple is verified (this bead's own kernel probe)
    to behave identically (zero matches), but the explicit branch below
    documents the intent rather than relying on that arity behavior."""
    if not predecessor_batch_ids:
        return existing_df.where(F.lit(False)).select("record_key", "content_hash")
    return existing_df.where(F.col("batch_id").isin(*predecessor_batch_ids)).select(
        "record_key", "content_hash"
    )


def _divergent_duplicate_count(stamped: DataFrame) -> int:
    """§12's `DivergentDuplicates` count -- D-2(b)'s observable-data
    condition made a number: the count of `record_key`s carrying more than
    one distinct `content_hash` within THIS batch's own stamped candidates
    for one fact table. Pure, no I/O (critique gate wf_24a3125f-ecc F1, bead
    conveyer-6pg.30): this used to also EMIT the metric directly from this
    stage, the ONLY `observability.*` call in any `stages/*.py` module (004
    §13.3 breach) and outside `record_run`'s never-raise envelope. The count
    now only feeds `ctx.divergent_duplicates_by_table`; `effects/ledger.py::
    record_run` derives the WARNING+EMF from the `RunFact` alone, mirroring
    `_emit_delta_probe_refusal`."""
    pairs = stamped.select("record_key", "content_hash").distinct()
    return pairs.groupBy("record_key").count().where(F.col("count") > 1).count()


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    assert ctx.admitted_facts is not None, "commit requires admitted_facts (post_check)"
    spec = ctx.spec
    markers_table = naming.markers_table(spec.raw_table, spec.pipeline)
    lineage_stamp = _lineage_stamp(ctx)
    committed_at = fx.now()  # one clock reading, shared across every marker write this attempt

    # --- §7.2: resolution + probe run ONCE per batch, before the loop -----
    completion_rows = fx.read_marker_completions(markers_table, ctx.feed_id)
    presence_rows = fx.read_marker_presence(markers_table, ctx.feed_id)
    seed_attrs = SeedAttrs(
        batch_id=ctx.batch_id,
        delivery_key=ctx.delivery_key,
        delivery_content_hash=ctx.content_hash,
        # `conveyer-kof`'s named wait (§16): no seed field yet -- F-5 is
        # designed against the pinned interface, "no F-5 edit occurs" once
        # it lands (§7.2).
        supersedes_batch_id=None,
    )
    target_rows = (
        fx.read_marker_target(markers_table, seed_attrs.supersedes_batch_id)
        if seed_attrs.supersedes_batch_id is not None
        else ()  # unreachable in Phase 1, see module docstring
    )
    resolution = resolve_predecessors(seed_attrs, completion_rows, presence_rows, target_rows)

    # --- F-4's per-table loop, declared order ------------------------------
    facts_appended_by_table: dict[str, int] = {}
    commit_snapshot_ids: dict[str, int] = {}
    delta_read_snapshot_ids: dict[str, int] = {}
    divergent_duplicates_by_table: dict[str, int] = {}
    any_guard_skip = False

    for fact_type_name, fact_type in spec.fact_types.items():
        fact_table = fact_type.fact_table
        present = fx.table_has_batch(fact_table, ctx.batch_id, None)

        if present:
            any_guard_skip = True
            _ensure_marker_row(
                fx,
                markers_table,
                MarkerRowWrite(
                    batch_id=ctx.batch_id,
                    feed_id=ctx.feed_id,
                    stage=_STAGE,
                    table_name=fact_table,
                    delivery_key=ctx.delivery_key,
                    delivery_content_hash=ctx.content_hash,
                    received_at=ctx.received_at,
                    committed_at=committed_at,
                ),
            )
            facts_appended_by_table[fact_table] = 0
            continue

        schema = fact_type.schema_
        candidate_df = ctx.admitted_facts[fact_type_name]
        stamped = _stamp_candidates(candidate_df, lineage_stamp, schema)

        # §7.3 (F-8): the structural check, fail fast, no append, no fold.
        existing_df, existing_snapshot_id = fx.read_table(fact_table)
        domain_id_col = schema.domain_id_col
        domain_id_null_count = stamped.filter(F.col(domain_id_col).isNull()).count()
        verdict = core_checks.structural_fact_check(
            present_columns=stamped.columns,
            expected_columns=existing_df.columns,
            domain_id_col=domain_id_col,
            domain_id_null_count=domain_id_null_count,
        )
        if isinstance(verdict, core_checks.StructuralFactCheckDefect):
            # I-24: fail fast BEFORE any append -- a NULL domain_id or a
            # schema drift is a named defect, never a quarantine row.
            raise ValueError(
                f"I-24 structural fact check failed (batch_id={ctx.batch_id!r}, "
                f"fact_type={fact_type_name!r}, fact_table={fact_table!r}): "
                f"{'; '.join(verdict.reasons)}"
            )
        delta_read_snapshot_ids[fact_table] = existing_snapshot_id

        # §7.2 (F-5): the per-type delta filter against the framework's own
        # predecessor-partition projection.
        predecessor_facts_t = _project_predecessor_facts(
            existing_df, resolution.predecessor_batch_ids
        )
        novel = frames_delta.delta_filter(stamped, predecessor_facts_t)

        divergent_duplicates_by_table[fact_table] = _divergent_duplicate_count(stamped)

        if novel.isEmpty():
            # F-4's zero-fact corollary at marker grain: no twin, no facts.
            facts_appended_by_table[fact_table] = 0
            continue

        _ensure_marker_row(
            fx,
            markers_table,
            MarkerRowWrite(
                batch_id=ctx.batch_id,
                feed_id=ctx.feed_id,
                stage=_STAGE,
                table_name=fact_table,
                delivery_key=ctx.delivery_key,
                delivery_content_hash=ctx.content_hash,
                received_at=ctx.received_at,
                committed_at=committed_at,
            ),
        )
        rows_appended, _summary = fx.append(fact_table, novel, ctx.batch_id, None)
        facts_appended_by_table[fact_table] = rows_appended
        snapshot_id = fx.resolve_batch_snapshot(fact_table, ctx.batch_id, None)
        if snapshot_id is not None:  # pragma: no branch -- always resolves right after our own
            # successful append; `append`'s own docstring treats a `None`
            # here as an infra hiccup (`TransientError`), never reaches
            # this line at all in that case.
            commit_snapshot_ids[fact_table] = snapshot_id

    # --- §4.3/§6.3 answer 2: commit's LAST act, unconditionally -----------
    _ensure_marker_row(
        fx,
        markers_table,
        MarkerRowWrite(
            batch_id=ctx.batch_id,
            feed_id=ctx.feed_id,
            stage=_STAGE,
            table_name=naming.COMMIT_COMPLETION_SENTINEL,
            delivery_key=ctx.delivery_key,
            delivery_content_hash=ctx.content_hash,
            received_at=ctx.received_at,
            committed_at=committed_at,
        ),
    )

    guard_skips = (*ctx.guard_skips, _STAGE) if any_guard_skip else ctx.guard_skips

    return replace(
        ctx,
        guard_skips=guard_skips,
        facts_appended_by_table=MappingProxyType(facts_appended_by_table),
        commit_snapshot_ids=MappingProxyType(commit_snapshot_ids),
        delta_predecessor_batch_ids=resolution.predecessor_batch_ids,
        delta_read_snapshot_ids=MappingProxyType(delta_read_snapshot_ids),
        delta_probe_refusal=resolution.probe_refusal,
        divergent_duplicates_by_table=MappingProxyType(divergent_duplicates_by_table),
    )
