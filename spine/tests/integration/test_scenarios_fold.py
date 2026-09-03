"""R-07/R-14 scenario suite (M4, bead `conveyer-nvh.25`) — LLD §12.4, I-11,
I-24, [T-11][T-12][E-6][E-18]. **Migrated for B10 (bead conveyer-6pg.22, LLD
007.1 §8.2): the mechanical per-type MERGE plan** (`stages/fold.py` rewrite
+ `frames/fold.py::reduce_batch_winners`, addendum 1's own "residual"
migration list names this file).

Reuses `scenario_helpers.py`'s table-DDL/spec/seed/batch-id helpers by import
(bare top-level module, no `__init__.py` chain in `tests/` — see that
module's own docstring and this repo's own recorded convention) rather than
re-deriving them; this file adds only the fixtures/assertions specific to
R-07/R-14. Promoted (critique F7, bead conveyer-nvh.43) from
`test_scenarios_core.py`, which this file used to import (`import
test_scenarios_core as tsc`) directly -- a test file importing another test
file's helpers -- into a dedicated non-test module.

**R-07 (out-of-order fold convergence, I-11 [T-11]):** two full `run()`
attempts over the SAME raw/quarantine/fact/state tables (same pipeline),
different `batch_id`s, run **out of event-time order** (the batch with the
NEWER `event_time` — "B" — runs first; the batch with the OLDER
`event_time` — "A" — runs second). `IDENTITY_FACT_SCHEMA` now declares
`ordering=["event_time"]` (B10's own `scenario_helpers.py` migration, that
module's own docstring), so `core.merge.merge_spec`'s per-type `MergeSpec.
ordering_cols` = `(event_time, source_ts, content_hash)` — the SAME triple
v1's hardcoded `frames.folds.LWW_ORDERING_COLUMNS` used, now sourced from
the declaration. Since `stages/fold.py`'s MERGE condition is `core.merge.
ordering_predicate`'s explicit field-wise boolean (strict `>`, null fields
rank lowest, §8.1/[T-11], K-14-verified), A's older-`event_time` facts must
never overwrite B's already-committed, newer values, and a NULL-`event_time`
fact in A must never displace B's non-null row either. A domain present ONLY
in A still inserts normally (no state row to lose to). Both batches use the
real, unmodified `pipelines.identity.transforms` module (byte-identical
projection) — no custom transforms needed for this half; B10 dropped the
custom-fold hook entirely (`stages/fold.py`'s own docstring: `Transforms.
fold` is never invoked by the mechanical §8.2 design, `spec.fold ==
"custom"` refused at parse, 007 D-3(e)).

**R-07b (fold-cardinality violation, I-11 [T-12]) — REDESIGNED for B10.**
v1's mechanism (a hand-built `Transforms.fold` emitting two rows for one
`domain_id`) is now **structurally unreachable through the full `run_
sequence` path**: `stages/fold.py` unconditionally reduces every type's
committed facts via `frames.fold.reduce_batch_winners` BEFORE calling `fx.
merge` — the custom-fold hook it used to call is simply never invoked, so
there is no seam left for a non-conforming custom fold to inject a
duplicate-domain source. Empirically confirmed too (this bead's own scratch
validation, `t6_cardinality_via_broken_target.py`): pre-corrupting the
STATE table with two rows for one `domain_id` and touching that domain with
an ordinary, unique-per-domain source does NOT trigger `MERGE_CARDINALITY_
VIOLATION` — Spark's own validator only fires when ONE TARGET row is
matched by MULTIPLE SOURCE rows (the reverse direction updates every
matched target row without error). The only way left to reach the defect is
therefore to call `effects/spark.py`'s `fx.merge` directly with a hand-built,
deliberately non-reduced (duplicate-domain) source DataFrame — bypassing
`reduce_batch_winners`, simulating exactly the "the source is unique by
construction; a defect here can only be the target's own broken grain"
diagnosis §8.2 names — the SAME mechanics K-15's own dedicated golden
exercises (`test_k_suite_fold.py`), kept here too as this file's own
end-to-end confirmation that a real `run_sequence` commit succeeds and only
the direct-`fx.merge` fold call fails.

**R-14 (structural fact check at commit, I-24):**

1. A NULL-`domain_id` fixture row that survives PAST `pre_check` (the spec's
   `raw_contract` deliberately leaves `domain_id` at its all-nullable
   default -- `nullable: true, required: false`, NOT the usual
   `required: true, nullable: false` most other scenario tests in this
   suite declare (005.1 §3.4/A-12, bead conveyer-azr.13's migration off the
   old `required_columns: []` provisional default) — so `pre_check`'s
   TEMPORARY-SHIM-derived "null in a required column" predicate never fires
   and the row reaches `commit` unquarantined) hits `commit`'s OWN structural
   check (`core.checks.structural_fact_check`, I-24's "fail-fast defect, not
   a quarantine row" designed specifically for a NULL `domain_id` that
   `pre_check`'s provisional contract didn't happen to catch) — a named
   `ValueError` defect, zero fact-table snapshots (no append), no fold (the
   sequence fails before `fold` ever runs), one `"failed"` run-ledger row for
   `stage="commit"`.
2. A drifted column set — a hand-built `Transforms` whose `apply` emits an
   extra column vs. the pre-created fact table's schema (a plain rename/add,
   simplest possible drift) — hits the SAME structural check's column-set
   diff half [E-18], same fail-fast shape: named `ValueError`, zero fact
   append, one `"failed"` ledger row for `stage="commit"`.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import scenario_helpers as sh
from pyspark.sql import Row
from pyspark.sql import functions as F
from snapshot_asserts import snapshot_ids
from spine.core import merge as core_merge
from spine.run import run as run_sequence
from spine.stages import commit as stages_commit

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import SparkSession
    from spine.effects.records import RunnerFx
    from tests.conftest import LedgerCatalogFixture

_CSV_HEADER = "domain_id,event_time,source_ts,content_hash,payload"


def _write_csv(path: Path, rows: list[tuple[str, str, str, str, str]]) -> Path:
    lines = [_CSV_HEADER, *(",".join(row) for row in rows)]
    path.write_text("\n".join(lines) + "\n")
    return path


def _ledger_rows_for(
    ledger_catalog: LedgerCatalogFixture, batch_id: str, stage: str
) -> list[dict[str, object]]:
    return [
        row
        for row in ledger_catalog.rows()
        if row["batch_id"] == batch_id and row["stage"] == stage
    ]


# --- R-07a: out-of-order folds converge on event-time order, null never wins,
# a domain new-only-in-A still inserts (I-11 [T-11]) -------------------------


def test_r07_out_of_order_batches_converge_on_event_time(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("r07_raw")
    qtn_qt = unique_table("r07_qtn")
    fact_qt = unique_table("r07_fact")
    state_qt = unique_table("r07_state")
    sh.create_raw_table(spark, raw_qt)
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)
    sh.create_state_table(spark, state_qt)
    spec = sh.make_spec(
        transforms_module="pipelines.identity.transforms",
        raw_table=sh.bare(raw_qt),
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table=sh.bare(state_qt),
        pipeline=sh.unique_pipeline("r07a"),
    )
    sh.create_markers_table_for(spark, spec)

    # Batch B: the NEWER-event_time delivery, run FIRST.
    csv_b = _write_csv(
        tmp_path / "batch_b.csv",
        [
            ("dom-x1", "2026-03-02T00:00:00Z", "2026-03-02T00:00:00Z", "hb-x1", "B-X1"),
            ("dom-x2", "2026-03-02T00:00:00Z", "2026-03-02T00:00:00Z", "hb-x2", "B-X2"),
        ],
    )
    batch_id_b = sh.batch_id(9070001)
    seed_b = sh.make_seed(spec=spec, batch_id=batch_id_b, object_uris=(str(csv_b),))
    result_b = run_sequence(seed_b, local_runner_fx)
    assert sh.facts_appended_total(result_b) == 2

    # Batch A: the OLDER-event_time delivery, run SECOND (out of order).
    #   dom-x1: older event_time than B's -> must NOT overwrite (I-11 strict >)
    #   dom-x2: NULL event_time -> must NEVER displace B's non-null row [T-11]
    #   dom-y:  present ONLY in A -> inserted normally (no state row to lose to)
    csv_a = _write_csv(
        tmp_path / "batch_a.csv",
        [
            ("dom-x1", "2026-03-01T00:00:00Z", "2026-03-01T00:00:00Z", "ha-x1", "A-X1-older"),
            ("dom-x2", "", "2026-03-01T00:00:00Z", "ha-x2", "A-X2-null-event-time"),
            ("dom-y", "2026-03-01T00:00:00Z", "2026-03-01T00:00:00Z", "ha-y", "A-Y-new"),
        ],
    )
    batch_id_a = sh.batch_id(9070002)
    seed_a = sh.make_seed(spec=spec, batch_id=batch_id_a, object_uris=(str(csv_a),))
    result_a = run_sequence(seed_a, local_runner_fx)

    # All three of A's rows are admitted as FACTS (append-only, no dedup
    # against state) -- only the FOLD's MERGE condition decides winners.
    assert sh.facts_appended_total(result_a) == 3

    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == [
        ("dom-x1", "B-X1"),  # A's older event_time never displaced B
        ("dom-x2", "B-X2"),  # A's NULL event_time never displaced B [T-11]
        ("dom-y", "A-Y-new"),  # new domain, only in A -> inserted normally
    ]


# --- R-07b: fold-cardinality violation is a named defect, target's own grain
# (I-11 [T-12], B10-redesigned -- module docstring has the full account) ----


def test_r07_fold_cardinality_violation_is_a_named_defect_at_fold(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    ledger_catalog: LedgerCatalogFixture,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("r07card_raw")
    qtn_qt = unique_table("r07card_qtn")
    fact_qt = unique_table("r07card_fact")
    state_qt = unique_table("r07card_state")
    sh.create_raw_table(spark, raw_qt)
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)
    sh.create_state_table(spark, state_qt)
    spec = sh.make_spec(
        transforms_module="pipelines.identity.transforms",
        raw_table=sh.bare(raw_qt),
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table=sh.bare(state_qt),
        pipeline=sh.unique_pipeline("r07card"),
    )
    sh.create_markers_table_for(spark, spec)

    # First batch: a NORMAL run through the real `run_sequence` (real bind_
    # transforms, mechanical §8.2 fold) -- seeds BOTH the fact table and the
    # state table with one row for "dom-c" via the real commit+fold path,
    # then commits a SECOND real batch touching "dom-c" too (both real
    # facts, both reduced to one winner each) -- this proves the ordinary
    # path stays entirely healthy (real cardinality, real fold, no defect)
    # BEFORE the direct-`fx.merge` half deliberately bypasses the reduce.
    csv_1 = _write_csv(
        tmp_path / "seed.csv",
        [("dom-c", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "h1", "seed-payload")],
    )
    batch_id_1 = sh.batch_id(9070101)
    seed_1 = sh.make_seed(spec=spec, batch_id=batch_id_1, object_uris=(str(csv_1),))
    result_1 = run_sequence(seed_1, local_runner_fx)
    assert sh.facts_appended_total(result_1) == 1
    assert result_1.completed_event.state_snapshot_id is not None
    state_before = snapshot_ids(spark, state_qt)
    batch_id_2 = sh.batch_id(9070102)

    # Direct `fx.merge` call, bypassing `frames.fold.reduce_batch_winners`
    # entirely (structurally unreachable via `run_sequence` under the B10
    # mechanical design, module docstring) -- a hand-built, deliberately
    # non-reduced source with TWO rows for "dom-c" violates I-11's one-row-
    # per-domain_id cardinality PRECONDITION [T-12] that a real fold's own
    # reduce step exists specifically to guarantee.
    fact_type = spec.fact_types["identity"]
    merge_spec = core_merge.merge_spec(fact_type)
    schema = spark.table(fact_qt).schema  # named-column construction -- see
    # `[[spine-b3-atomic-flip-apply-postcheck-rewrite]]`'s own "candidate_row_
    # hash tag mechanics" note on why a positional tuple against a stamp-
    # column-prefixed schema is a standing footgun; `spark.createDataFrame`
    # over `Row(**kwargs)` binds by NAME, immune to column-order drift.
    received_at = seed_1.received_at
    dup_rows = spark.createDataFrame(
        [
            Row(
                batch_id=batch_id_2,
                delivery_id="dup-delivery",
                feed_id=spec.pipeline,
                received_at=received_at,
                source_ts=None,
                content_hash="h2",
                record_key="rk-dom-c",
                domain_id="dom-c",
                event_time="2026-01-02T00:00:00Z",
                payload="dup-payload-a",
            ),
            Row(
                batch_id=batch_id_2,
                delivery_id="dup-delivery",
                feed_id=spec.pipeline,
                received_at=received_at,
                source_ts=None,
                content_hash="h3",
                record_key="rk-dom-c",
                domain_id="dom-c",
                event_time="2026-01-02T00:00:00Z",
                payload="dup-payload-b",
            ),
        ],
        schema=schema,
    )

    # Named defect (bead conveyer-nvh.36's original fix; B10 sharpened the
    # message to indict the TARGET's grain): effects/spark.py::merge detects
    # Spark's own MERGE_CARDINALITY_VIOLATION (via the wrapped Java
    # exception's own getMessage(), never str(exc)) and re-raises a plain
    # ValueError -- is_transient_iceberg_failure only recognizes Iceberg
    # CommitFailedException/CommitStateUnknownException/ValidationException
    # FQCNs, so this deterministic, non-Iceberg Spark validator was never at
    # risk of being wrapped as TransientError either.
    with pytest.raises(ValueError, match="fold cardinality defect") as exc_info:
        local_runner_fx.merge(merge_spec, dup_rows)

    assert "I-11" in str(exc_info.value)
    assert fact_type.state_table in str(exc_info.value)  # per-state-table indictment (§8.2)
    assert type(exc_info.value.__cause__).__name__ == "Py4JJavaError"

    # commit's own real facts (batch 1) are unaffected -- only the direct,
    # deliberately-bypassing `fx.merge` call above ever failed.
    fact_rows = {
        r["domain_id"] for r in spark.table(fact_qt).where(f"batch_id = '{batch_id_1}'").collect()
    }
    assert fact_rows == {"dom-c"}  # batch 1's real commit stands untouched

    # the failed MERGE commits zero new snapshots (empirically confirmed,
    # module docstring) -- state still shows only batch 1's real winner.
    assert snapshot_ids(spark, state_qt) == state_before
    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == [("dom-c", "seed-payload")]


# --- R-14a: NULL domain_id reaching commit fails fast (I-24) -- REDESIGNED for
# B10: post_check's own framework-reserved implicit check (`business/missing-
# domain-id`, `pipelines/identity/transforms.py`'s own docstring) now catches
# EVERY null-domain_id candidate unconditionally, regardless of the raw
# contract's own `required`/`nullable` declaration -- the SAME "a later
# framework mechanism supersedes this exact path" shape `test_scenarios_core.
# py`'s own "K6 supersedes A-14a" skip already documents for a sibling
# scenario. Scratch-validated (this bead): running the FULL `run_sequence`
# over a deliberately all-nullable `domain_id` raw_contract no longer reaches
# commit's structural check at all -- `post_quarantined=1`, the row never
# becomes a candidate fact. I-24's own backstop purpose (§7.3's "a durable-
# authority door under drift/moved pin admits an unevaluated candidate set")
# is for exactly this kind of bypass -- so this test now drives `stages.
# commit.run` DIRECTLY with a hand-built `admitted_facts` mapping carrying a
# NULL-domain_id row, simulating the scenario I-24 exists to catch (a
# candidate that should never have reached commit, by construction here
# rather than by exploiting a pre_check contract gap that no longer exists) --
# never through `run_sequence`/post_check at all. -----------------------------


def test_r14_null_domain_id_reaching_commit_fails_fast_no_append(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    raw_qt = unique_table("r14null_raw")
    qtn_qt = unique_table("r14null_qtn")
    fact_qt = unique_table("r14null_fact")
    state_qt = unique_table("r14null_state")
    sh.create_raw_table(spark, raw_qt)
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)
    sh.create_state_table(spark, state_qt)
    spec = sh.make_spec(
        transforms_module="pipelines.identity.transforms",
        raw_table=sh.bare(raw_qt),
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table=sh.bare(state_qt),
        pipeline=sh.unique_pipeline("r14null"),
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = sh.batch_id(9140001)
    seed = sh.make_seed(spec=spec, batch_id=batch_id, object_uris=())
    schema = spark.table(fact_qt).select("domain_id", "event_time", "payload").schema
    null_domain_df = spark.createDataFrame(
        [(None, "2026-04-01T00:00:00Z", "orphan-payload")], schema
    )
    ctx = replace(seed, admitted_facts={"identity": null_domain_df})
    fact_before = snapshot_ids(spark, fact_qt)

    with pytest.raises(ValueError, match="I-24"):
        stages_commit.run(ctx, local_runner_fx)

    # fail fast, before any append -- zero snapshots on the fact table.
    assert snapshot_ids(spark, fact_qt) == fact_before == frozenset()


# --- R-14b: drifted column set fails fast before append (I-24 [E-18]) --
# REDESIGNED for B10, the SAME shape as R-14a above: `stages/apply.py`'s own
# runtime return-shape law (P-1, 006.1 §4.4) now validates every candidate
# frame's column set against the DECLARED `FactSchemaModel` the moment
# `Transforms.apply` returns -- an extra column is caught THERE
# (`transform-defect/candidate-schema`) long before commit's I-24 structural
# check (which diffs against the EXISTING TABLE's own columns, a distinct
# check with a distinct purpose: catching a table that drifted independently
# of the declaration, [E-18]) ever gets a chance to run. Scratch-validated
# (this bead): a hand-built `Transforms.apply` returning an extra column
# never reaches `commit` via `run_sequence` any more -- `stages/apply.py`
# raises first. `stages.commit.run` driven DIRECTLY, mirroring R-14a. --------


def test_r14_drifted_column_set_fails_fast_before_append(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    raw_qt = unique_table("r14drift_raw")
    qtn_qt = unique_table("r14drift_qtn")
    fact_qt = unique_table("r14drift_fact")
    state_qt = unique_table("r14drift_state")
    sh.create_raw_table(spark, raw_qt)
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)  # the PRE-CREATED, undrifted schema
    sh.create_state_table(spark, state_qt)
    spec = sh.make_spec(
        transforms_module="pipelines.identity.transforms",
        raw_table=sh.bare(raw_qt),
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table=sh.bare(state_qt),
        pipeline=sh.unique_pipeline("r14drift"),
    )
    sh.create_markers_table_for(spark, spec)
    batch_id = sh.batch_id(9140002)
    seed = sh.make_seed(spec=spec, batch_id=batch_id, object_uris=())

    # A candidate frame carrying an EXTRA column ("extra_col") beyond the
    # pre-created fact table's own schema -- the simplest possible drift,
    # constructed directly (bypassing `apply`'s own now-earlier catch,
    # module docstring) so commit's OWN column-set diff [E-18] is what's
    # actually exercised here.
    schema = spark.table(fact_qt).select("domain_id", "event_time", "payload").schema
    drifted_df = spark.createDataFrame(
        [("dom-h", "2026-05-01T00:00:00Z", "payload-h")], schema
    ).withColumn("extra_col", F.lit("unexpected"))
    ctx = replace(seed, admitted_facts={"identity": drifted_df})
    fact_before = snapshot_ids(spark, fact_qt)

    with pytest.raises(ValueError, match="schema drift"):
        stages_commit.run(ctx, local_runner_fx)

    assert snapshot_ids(spark, fact_qt) == fact_before == frozenset()
