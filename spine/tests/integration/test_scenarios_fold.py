"""R-07/R-14 scenario suite (M4, bead `conveyer-nvh.25`) — LLD §12.4, I-11,
I-24, [T-11][T-12][E-6][E-18].

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
`event_time` — "A" — runs second). Since `stages/fold.py`'s MERGE condition
is `src ordering struct > tgt ordering struct` (strict `>`, null fields rank
lowest, `frames.folds.LWW_ORDERING_COLUMNS = (event_time, source_ts,
content_hash)`), A's older-`event_time` facts must never overwrite B's
already-committed, newer values, and a NULL-`event_time` fact in A must
never displace B's non-null row either (a null-keyed ordering struct always
ranks lowest, LLD I-11's own pinned semantics). A domain present ONLY in A
still inserts normally (no state row to lose to). Both batches use the real,
unmodified `pipelines.identity.transforms` module (byte-identical
projection, default-lww fold via `bind_transforms`) — no custom transforms
needed for this half.

**R-07 (fold-cardinality violation, I-11 [T-12]):** a hand-built
`Transforms` (bypassing `bind_transforms` entirely, per the bead's own ask —
`pipelines/**` is frozen this wave, so a tests-local pipeline package
extension is not an option) whose `fold` emits TWO rows for one
`domain_id` — `winners_per_domain`'s one-row-per-domain cardinality
precondition is `default_lww_fold`'s OWN job (§7.5), so only a
non-conforming custom fold can violate it. Empirically confirmed (this
bead's own scratch validation, local Iceberg/Spark 3.5.9/1.6.1) that Spark's
`MERGE_CARDINALITY_VIOLATION` only fires on the **MATCHED** branch (a target
row already present, hit by >1 source row for the same key) — an
INSERT-only duplicate-key MERGE against an absent target row raises
nothing and silently inserts both duplicates. The test therefore seeds the
domain into state via a normal FIRST batch, then triggers the duplicate-fold
on a SECOND batch touching the same domain. Spark itself raises
`py4j.protocol.Py4JJavaError` (Java class `org.apache.spark.SparkException`,
its own `getMessage()` containing the condition marker
`MERGE_CARDINALITY_VIOLATION`) — `effects/spark.py::merge` (bead
conveyer-nvh.36's fix) recognizes this via `is_merge_cardinality_violation`
(inspects the wrapped Java exception's own `getMessage()`, never `str(exc)`)
and re-raises it as a **named** `ValueError` ("fold cardinality defect: ...
I-11 [T-12]") — NOT `TransientError`: `effects/spark.py`'s
`is_transient_iceberg_failure` predicate only recognizes `org.apache.
iceberg.exceptions.{CommitFailedException,CommitStateUnknownException,
ValidationException}` FQCNs, and this is a Spark-side MERGE validator, not
an Iceberg commit conflict (I-11's own text: "a deterministic MERGE
cardinality error surfaced as a named defect" — deterministic, so retrying
via `TransientError`'s SFN-retry path would never help). Also empirically
confirmed: a FAILED `MERGE INTO` commits **zero** new snapshots to the
target table (the Spark job aborts before Iceberg's commit), so the state
table's snapshot log after the failed second-batch fold is unchanged from
what the first batch's successful fold already left behind.

**R-14 (structural fact check at commit, I-24):**

1. A NULL-`domain_id` fixture row that survives PAST `pre_check` (the spec's
   `required_columns` is deliberately `[]`, the provisional I-P2 default —
   NOT `["domain_id"]`, unlike most other scenario tests in this suite —
   so `pre_check`'s "null in a required column" predicate never fires and
   the row reaches `commit` unquarantined) hits `commit`'s OWN structural
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
from pyspark.sql import functions as F
from snapshot_asserts import snapshot_ids
from spine.binding import Transforms
from spine.core.model import PipelineSpecModel
from spine.run import run as run_sequence

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import DataFrame, SparkSession
    from spine.effects.records import RunnerFx
    from tests.conftest import LedgerCatalogFixture

import pipelines.identity.transforms as identity_transforms

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
    )

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
    assert result_b.facts_appended == 2

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
    assert result_a.facts_appended == 3

    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == [
        ("dom-x1", "B-X1"),  # A's older event_time never displaced B
        ("dom-x2", "B-X2"),  # A's NULL event_time never displaced B [T-11]
        ("dom-y", "A-Y-new"),  # new domain, only in A -> inserted normally
    ]


# --- R-07b: fold-cardinality violation is a named defect at fold (I-11 [T-12]) --


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
    )

    # First batch: a NORMAL run (real bind_transforms, default-lww fold) --
    # seeds state with one row for "dom-c" so the SECOND batch's duplicate
    # fold rows have an existing target row to MATCH against (empirically,
    # Spark's MERGE_CARDINALITY_VIOLATION only fires on the MATCHED branch --
    # an insert-only duplicate-key MERGE against an absent target raises
    # nothing at all, see module docstring).
    csv_1 = _write_csv(
        tmp_path / "seed.csv",
        [("dom-c", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "h1", "seed-payload")],
    )
    batch_id_1 = sh.batch_id(9070101)
    seed_1 = sh.make_seed(spec=spec, batch_id=batch_id_1, object_uris=(str(csv_1),))
    result_1 = run_sequence(seed_1, local_runner_fx)
    assert result_1.facts_appended == 1
    assert result_1.state_snapshot_id is not None
    state_before = snapshot_ids(spark, state_qt)

    # Second batch: hand-built Transforms (bypassing bind_transforms, per the
    # bead's own ask -- pipelines/** is frozen this wave) whose fold emits
    # TWO rows for "dom-c" -- violates I-11's one-row-per-domain_id
    # cardinality precondition [T-12].
    def _duplicate_per_domain_fold(state_slice: DataFrame, facts_df: DataFrame) -> DataFrame:
        del state_slice
        return facts_df.union(facts_df)

    bad_transforms = Transforms(
        apply=identity_transforms.apply,
        post_check=identity_transforms.post_check,
        fold=_duplicate_per_domain_fold,
    )
    csv_2 = _write_csv(
        tmp_path / "dup.csv",
        [("dom-c", "2026-01-02T00:00:00Z", "2026-01-02T00:00:00Z", "h2", "dup-payload")],
    )
    batch_id_2 = sh.batch_id(9070102)
    seed_2 = sh.make_seed(spec=spec, batch_id=batch_id_2, object_uris=(str(csv_2),))
    seed_2 = replace(seed_2, transforms=bad_transforms)

    # Named defect (bead conveyer-nvh.36 fix), NOT TransientError and NOT the
    # raw Py4JJavaError Spark itself raises: effects/spark.py::merge detects
    # Spark's own MERGE_CARDINALITY_VIOLATION (via the wrapped Java
    # exception's own getMessage(), never str(exc)) and re-raises a plain
    # ValueError -- is_transient_iceberg_failure only recognizes Iceberg
    # CommitFailedException/CommitStateUnknownException/ValidationException
    # FQCNs, so this deterministic, non-Iceberg Spark validator was never at
    # risk of being wrapped as TransientError either.
    with pytest.raises(ValueError, match="fold cardinality defect") as exc_info:
        run_sequence(seed_2, local_runner_fx)

    assert "I-11" in str(exc_info.value)
    assert type(exc_info.value.__cause__).__name__ == "Py4JJavaError"

    # commit itself succeeded (batch 2's single admitted row IS a fact) --
    # the job fails AT fold, not before it.
    assert result_1.fact_snapshot_id is not None
    fact_rows = {
        r["domain_id"] for r in spark.table(fact_qt).where(f"batch_id = '{batch_id_2}'").collect()
    }
    assert fact_rows == {"dom-c"}  # committed despite the later fold failure

    # the failed MERGE commits zero new snapshots (empirically confirmed,
    # module docstring) -- state still shows only batch 1's seeded value.
    assert snapshot_ids(spark, state_qt) == state_before
    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == [("dom-c", "seed-payload")]

    failed_fold_rows = _ledger_rows_for(ledger_catalog, batch_id_2, "fold")
    assert len(failed_fold_rows) == 1
    assert failed_fold_rows[0]["outcome"] == "failed"
    assert failed_fold_rows[0]["error_type"] == "ValueError"
    commit_rows = _ledger_rows_for(ledger_catalog, batch_id_2, "commit")
    assert len(commit_rows) == 1
    assert commit_rows[0]["outcome"] == "ok"  # commit succeeded; only fold failed


# --- R-14a: NULL domain_id survives pre_check, fails fast at commit (I-24) --


def test_r14_null_domain_id_survives_to_commit_fails_fast_no_append(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    ledger_catalog: LedgerCatalogFixture,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("r14null_raw")
    qtn_qt = unique_table("r14null_qtn")
    fact_qt = unique_table("r14null_fact")
    state_qt = unique_table("r14null_state")
    sh.create_raw_table(spark, raw_qt)
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)
    sh.create_state_table(spark, state_qt)
    # required_columns deliberately [] (the I-P2 provisional default) -- NOT
    # ["domain_id"] -- so pre_check's "null in a required column" predicate
    # never fires and this row survives, unquarantined, all the way to
    # commit's OWN structural check (I-24's whole reason to exist: catching
    # exactly the NULL domain_id case pre_check's provisional contract
    # doesn't happen to cover).
    spec = PipelineSpecModel(
        pipeline="pipelines/identity",
        transforms_module="pipelines.identity.transforms",
        raw_table=sh.bare(raw_qt),
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table=sh.bare(state_qt),
        required_columns=[],
    )
    csv_path = _write_csv(
        tmp_path / "null_domain.csv",
        [("", "2026-04-01T00:00:00Z", "2026-04-01T00:00:00Z", "h-null", "orphan-payload")],
    )
    batch_id = sh.batch_id(9140001)
    seed = sh.make_seed(spec=spec, batch_id=batch_id, object_uris=(str(csv_path),))
    fact_before = snapshot_ids(spark, fact_qt)

    with pytest.raises(ValueError, match="I-24"):
        run_sequence(seed, local_runner_fx)

    # fail fast, before any append -- zero snapshots on the fact table.
    assert snapshot_ids(spark, fact_qt) == fact_before == frozenset()
    # no fold either -- the sequence fails at commit, fold never runs.
    assert _ledger_rows_for(ledger_catalog, batch_id, "fold") == []

    commit_rows = _ledger_rows_for(ledger_catalog, batch_id, "commit")
    assert len(commit_rows) == 1
    assert commit_rows[0]["outcome"] == "failed"
    assert commit_rows[0]["error_type"] == "ValueError"
    # upstream stages (land/pre_check/pull/apply/post_check) all ran fine --
    # only commit's OWN structural check caught this.
    post_check_rows = _ledger_rows_for(ledger_catalog, batch_id, "post_check")
    assert len(post_check_rows) == 1
    assert post_check_rows[0]["outcome"] == "ok"


# --- R-14b: drifted column set fails fast before append (I-24 [E-18]) ------


def test_r14_drifted_column_set_fails_fast_before_append(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    ledger_catalog: LedgerCatalogFixture,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("r14drift_raw")
    qtn_qt = unique_table("r14drift_qtn")
    fact_qt = unique_table("r14drift_fact")
    state_qt = unique_table("r14drift_state")
    sh.create_raw_table(spark, raw_qt)
    sh.create_quarantine_table(spark, qtn_qt)
    sh.create_fact_table(spark, fact_qt)  # the PRE-CREATED, undrifted 9-col shape
    sh.create_state_table(spark, state_qt)
    spec = sh.make_spec(
        transforms_module="pipelines.identity.transforms",
        raw_table=sh.bare(raw_qt),
        quarantine_table=sh.bare(qtn_qt),
        fact_table=sh.bare(fact_qt),
        state_table=sh.bare(state_qt),
    )

    # Hand-built Transforms (bypassing bind_transforms) whose `apply` emits
    # an EXTRA column ("extra_col") beyond the pre-created fact table's
    # schema -- the simplest possible drift, caught by commit's pure
    # column-set diff [E-18] before a single row is appended.
    def _drifted_apply(valid_df: DataFrame, co_effects: object) -> DataFrame:
        del co_effects
        projected = identity_transforms.apply(valid_df, {})
        return projected.withColumn("extra_col", F.lit("unexpected"))

    drifted_transforms = Transforms(
        apply=_drifted_apply,
        post_check=identity_transforms.post_check,
        fold=lambda state_slice, facts_df: facts_df,  # never reached
    )
    csv_path = _write_csv(
        tmp_path / "drifted.csv",
        [("dom-h", "2026-05-01T00:00:00Z", "2026-05-01T00:00:00Z", "h-drift", "payload-h")],
    )
    batch_id = sh.batch_id(9140002)
    seed = sh.make_seed(spec=spec, batch_id=batch_id, object_uris=(str(csv_path),))
    seed = replace(seed, transforms=drifted_transforms)
    fact_before = snapshot_ids(spark, fact_qt)

    with pytest.raises(ValueError, match="schema drift"):
        run_sequence(seed, local_runner_fx)

    assert snapshot_ids(spark, fact_qt) == fact_before == frozenset()
    assert _ledger_rows_for(ledger_catalog, batch_id, "fold") == []

    commit_rows = _ledger_rows_for(ledger_catalog, batch_id, "commit")
    assert len(commit_rows) == 1
    assert commit_rows[0]["outcome"] == "failed"
    assert commit_rows[0]["error_type"] == "ValueError"
