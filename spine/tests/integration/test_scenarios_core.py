"""Standing scenario suite (M3/N3 gates) — R-01, R-02, R-04, R-09;
A-01/A-02/A-03/A-04/A-05/A-08/A-11 (005.1 §12.3, bead conveyer-azr.19,
n3-admission-cut). LLD §12.4, §7.5, §6.5/§6.6, I-7, I-12, I-19, [H-1][E-1].

Drives `spine.run.run(seed, fx)` end-to-end over `local_runner_fx` (the real
production assembly, §12.1) with NO explicit `stages` argument, so this suite
is also `spine/stages/__init__.py::SEQUENCE`'s own wiring test — the
entrypoint (M5) is bypassed entirely: the seed `BatchContext` is built
directly, and `transforms` is bound via the real `spine.binding.
bind_transforms` against the real `pipelines.identity`/
`pipelines.identity_violations` modules (not an inline test double), matching
the bead's own ask.

Table shapes are pinned once here (`_create_raw_table` et al.) rather than
re-derived per test: `identity`'s `apply` is a plain column projection (see
`pipelines/identity/transforms.py`'s own docstring), so candidate/admitted/
fact/state all share the SAME 9-column shape (`domain_id, event_time,
source_ts, content_hash, payload, batch_id, delivery_id, feed_id,
received_at`); the raw and quarantine tables are 005.1's own admission
shapes (§4.1/§4.2 — `scenario_helpers.py`'s `create_raw_table`/
`create_quarantine_table`, the bootstrap DDL builders), independent of the
candidate/fact shape. The state table additionally carries
`write.merge.mode = merge-on-read` (the fold no-op detection precondition,
`effects/spark.py`'s own documented empirical finding).

Goldens are reconstructed in-test from literals (sorted tuples of the columns
under test), not stored parquet: the fixture data is tiny and fully spelled
out just above each test, so a literal expected-rows tuple is both the
easier-to-read AND the easier-to-maintain form here — a schema/column-order
change in a stored parquet golden would be a silent, unreviewable diff,
whereas a literal tuple change shows up in the PR text itself.

**A-suite tests (this bead)** exercise the FULL stage wiring (§6.5/§6.6's
real reader/compiled-contract/quarantine mechanics) end-to-end; several of
the underlying mechanics (the reader's own defect surface, `compile_
contract`'s full multi-check matrix + truncation, bootstrap's promotion
diff rules) are already exhaustively unit/frame-tested by earlier N1/N2
beads (`tests/integration/test_spark_fx.py`'s n2-reader section, `tests/
frames/test_checks.py`, `tests/integration/test_create_admission_tables.
py`) — these tests deliberately stay lean, confirming the WIRING holds
rather than re-deriving that coverage.

**N4 additions (bead conveyer-azr.20, n4-rerun-matrix)**: A-07's legs (a),
(c), (d) (the non-KillFx legs of the §12.3 rerun matrix — leg (b) needs
KillFx and lives in `test_scenarios_kill.py` alongside its own A-10 hash-
keyed-rerun leg), A-10's one-shape read-back + nonconforming-reason-defect
halves, A-16, the §9 late-stream-corruption residual, and a small ledger-row
addition to `test_r09` (the "alarm-visible counts" half of §9's
all-rows-quarantined row — `pre_quarantined` on the ledger's own `RunFact`
row is exactly what `effects/ledger.py::_stage_metrics` derives the
`QuarantinedRows` CloudWatch metric from). Every new leg was scratch-
validated against a real local Spark/Iceberg session (the sanctioned
fallback, `uv run -p 3.11 --package conveyer-spine python <script>`) before
being written here — the exact drift-string shapes, cast-failure-retention
behavior, and guard-blind rerun snapshot counts below are all
probe-confirmed, not derived from the LLD text alone.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

import pipelines.identity.transforms as identity_transforms
import pytest
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from scenario_helpers import (
    EXEMPLAR_DIR as _EXEMPLAR_DIR,
)
from scenario_helpers import (
    FIXTURES_DIR as _FIXTURES_DIR,
)
from scenario_helpers import (
    IDENTITY_FACT_SCHEMA as _IDENTITY_FACT_SCHEMA,
)
from scenario_helpers import (
    IDENTITY_RAW_CONTRACT as _IDENTITY_RAW_CONTRACT,
)
from scenario_helpers import (
    VIOLATIONS_CHECKS as _VIOLATIONS_CHECKS,
)
from scenario_helpers import (
    bare as _bare,
)
from scenario_helpers import (
    batch_id as _batch_id,
)
from scenario_helpers import (
    create_fact_table as _create_fact_table,
)
from scenario_helpers import (
    create_markers_table_for as _create_markers_table_for,
)
from scenario_helpers import (
    create_quarantine_table as _create_quarantine_table,
)
from scenario_helpers import (
    create_raw_table as _create_raw_table,
)
from scenario_helpers import (
    create_state_table as _create_state_table,
)
from scenario_helpers import (
    facts_appended_total as _facts_appended_total,
)
from scenario_helpers import (
    make_seed as _make_seed,
)
from scenario_helpers import (
    make_spec as _make_spec,
)
from scenario_helpers import (
    quarantine_rows as _quarantine_rows,
)
from scenario_helpers import (
    rows_merged_total as _rows_merged_total,
)
from scenario_helpers import (
    unique_pipeline as _unique_pipeline,
)
from snapshot_asserts import assert_no_new_snapshot, snapshot_ids
from spine.binding import Transforms
from spine.bootstrap.create_admission_tables import (
    bootstrap_raw_table,
    render_raw_create_table_sql,
)
from spine.core.model import (
    ColumnSpec,
    DialectModel,
    FactTypeModel,
    PipelineSpecModel,
    RawContractModel,
    ReadSpecModel,
)
from spine.core.naming import qualified
from spine.effects.records import RunnerFx
from spine.run import run as run_sequence
from spine.stages import land, pre_check

if TYPE_CHECKING:
    from tests.conftest import LedgerCatalogFixture, MotoEventsBus


def _fact_types(fact_table: str, state_table: str) -> dict[str, FactTypeModel]:
    """006.1 P-1 (bead conveyer-6pg.13, B3): the per-type `fact_types`
    mapping every direct `PipelineSpecModel(...)` construction in this file
    now needs, in place of the deleted singular `fact_table`/`state_table`
    fields -- one `identity` type, `scenario_helpers.IDENTITY_FACT_SCHEMA`'s
    own declared shape (matching `pipelines.identity.transforms.apply`'s
    real candidate columns byte-exact)."""
    return {
        "identity": FactTypeModel(
            fact_table=fact_table, state_table=state_table, schema=_IDENTITY_FACT_SCHEMA
        )
    }


# --- a test asserts the deployed-shape yaml parses into PipelineSpecModel ---


def test_pipeline_yaml_parses_into_pipeline_spec_model() -> None:
    data = yaml.safe_load((_EXEMPLAR_DIR / "pipeline.yaml").read_text())
    spec = PipelineSpecModel(**data)
    assert spec.pipeline == "pipelines/identity"
    assert spec.transforms_module == "pipelines.identity.transforms"
    assert spec.raw_table == "conveyer_dev_lake.identity__raw"
    assert spec.quarantine_table == "conveyer_dev_lake.identity__quarantine"
    assert set(spec.fact_types) == {"identity"}
    assert spec.fact_types["identity"].fact_table == "conveyer_dev_lake.identity__facts"
    assert spec.fact_types["identity"].state_table == "conveyer_dev_lake.identity__state"
    assert spec.raw_contract.columns[0].name == "domain_id"
    assert spec.raw_contract.columns[0].required is True
    assert spec.raw_contract.columns[0].nullable is False
    assert spec.read.dialect.format == "csv"
    assert spec.fold == "default-lww"


# --- R-01: fixture delivery -> run; goldens; both events on the bus --------


def test_r01_identity_e2e_matches_goldens_and_emits_both_events(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("r01_raw")
    qtn_qt = unique_table("r01_qtn")
    fact_qt = unique_table("r01_fact")
    state_qt = unique_table("r01_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec1"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(101)
    object_uris = (
        str(_FIXTURES_DIR / "clean" / "object_1.csv"),
        str(_FIXTURES_DIR / "clean" / "object_2.csv"),
    )
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)

    result = run_sequence(seed, local_runner_fx)

    assert result.raw_count == 3
    assert result.pre_quarantined_count == 0
    assert result.post_quarantined_count == 0
    assert _facts_appended_total(result) == 3
    assert result.guard_skips == ()
    assert result.published is True

    fact_golden = [
        ("id-001", "alpha"),
        ("id-002", "bravo"),
        ("id-003", "charlie"),
    ]
    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == fact_golden

    state_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert state_rows == fact_golden  # distinct domain_ids -- one winner each, all 3 kept

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert len(envelopes) == 2
    by_type = {e["detail-type"]: e["detail"] for e in envelopes}
    assert set(by_type) == {"batch-started", "batch-completed"}
    started = by_type["batch-started"]
    assert started["raw_count"] == 3
    assert started["land_snapshot_id"] == result.land_snapshot_id
    completed = by_type["batch-completed"]
    assert completed["raw_count"] == 3
    assert completed["pre_quarantined"] == 0
    assert completed["post_quarantined"] == 0
    assert completed["fact_count"] == 3
    assert completed["fact_snapshot_id"] == result.completed_event.fact_snapshot_id
    assert completed["state_snapshot_id"] == result.completed_event.state_snapshot_id
    assert completed["state_snapshot_id"] is not None


# --- R-02: rerun same seed, fresh context -----------------------------------


def test_r02_rerun_same_batch_id_zero_new_rows_and_guard_skips(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("r02_raw")
    qtn_qt = unique_table("r02_qtn")
    fact_qt = unique_table("r02_fact")
    state_qt = unique_table("r02_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec2"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(102)
    object_uris = (
        str(_FIXTURES_DIR / "clean" / "object_1.csv"),
        str(_FIXTURES_DIR / "clean" / "object_2.csv"),
    )

    first_seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    first = run_sequence(first_seed, local_runner_fx)
    moto_events_bus.read_events()  # drain the first attempt's events

    raw_before = snapshot_ids(spark, raw_qt)
    qtn_before = snapshot_ids(spark, qtn_qt)
    fact_before = snapshot_ids(spark, fact_qt)

    # A FRESH seed context, same batch_id -- R-02's own precondition (a real
    # rerun never reuses the first attempt's in-memory BatchContext).
    second_seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    second = run_sequence(second_seed, local_runner_fx)

    assert_no_new_snapshot(spark, raw_qt, raw_before)
    assert_no_new_snapshot(spark, qtn_qt, qtn_before)  # no violations either attempt
    assert_no_new_snapshot(spark, fact_qt, fact_before)
    # NOT a snapshot_ids equality check on state_qt: a healthy-rerun MERGE
    # still commits a harmless PHYSICAL no-op snapshot (effects/spark.py's
    # own documented empirical finding; see test_stages_post_commit_fold_
    # publish.py's identical carve-out) -- state_snapshot_id is the LOGICAL
    # no-op signal this test cares about, asserted below.

    assert second.guard_skips == ("land", "commit")  # pre_check/post_check: zero-violation
    # branch every attempt (never a guard-skip, §7.5) -- fold has no presence
    # guard at all (idempotence is the fold's own contract).
    assert second.raw_count == first.raw_count == 3
    assert _facts_appended_total(second) == 0  # this attempt's own delta -- the ledger signature
    # I-19 own-commit resolution: `ctx.commit_snapshot_ids` is ATTEMPT-scoped
    # (absent key on a guard-skip, §4.2) -- the DURABLE resolution that
    # survives a guard-skip rerun is `completed_event.fact_snapshot_id`
    # (`stages/publish.py`'s own `fx.resolve_batch_snapshot`-sourced
    # derivation), asserted below via each attempt's own completed event.
    assert second.land_snapshot_id == first.land_snapshot_id
    # Known erratum (LLD R-02 vs. [C-7][T-8], recorded in the handoff report):
    # a fold no-op rerun's state_snapshot_id is None even though the ORIGINAL
    # attempt's fold produced a real snapshot id -- MERGE commits carry no
    # conveyer.batch-id/stage stamp (only `append` stamps), so there is no
    # channel to recover the original id on this path. Do not assert equality
    # here; assert the documented None explicitly instead.
    assert first.completed_event.state_snapshot_id is not None
    assert second.completed_event.state_snapshot_id is None

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert len(envelopes) == 2  # unconditional re-emit, I-7 -- guard-skip never skips events
    by_type = {e["detail-type"]: e["detail"] for e in envelopes}
    completed = by_type["batch-completed"]
    first_completed = first.completed_event
    assert completed["raw_count"] == first_completed.raw_count
    assert completed["pre_quarantined"] == first_completed.pre_quarantined
    assert completed["post_quarantined"] == first_completed.post_quarantined
    assert completed["fact_count"] == first_completed.fact_count
    assert completed["fact_snapshot_id"] == first_completed.fact_snapshot_id
    # state_snapshot_id excluded from the field-equality claim -- see the
    # erratum note above; R-09 pins this same None for the empty-batch case.
    assert completed["state_snapshot_id"] is None
    assert first_completed.state_snapshot_id is not None


# --- R-04: quarantine never drops --------------------------------------------


def test_r04_violations_variant_quarantine_never_drops(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    raw_qt = unique_table("r04_raw")
    qtn_qt = unique_table("r04_qtn")
    fact_qt = unique_table("r04_fact")
    state_qt = unique_table("r04_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec3"),
        transforms_module="pipelines.identity_violations.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
        checks=_VIOLATIONS_CHECKS,
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(104)
    object_uris = (str(_FIXTURES_DIR / "violations" / "object_1.csv"),)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)

    result = run_sequence(seed, local_runner_fx)

    assert result.raw_count == 4
    assert result.pre_quarantined_count == 1  # the null-domain_id row
    valid_count = result.raw_count - result.pre_quarantined_count
    assert valid_count == 3
    # 006.1 §4.5: `apply` returns a one-entry MAPPING (P-1) -- `candidate_
    # facts["identity"]` replaces the deleted singular `candidate_facts_df`.
    assert result.candidate_facts["identity"].count() == valid_count  # apply is a pure projection
    assert result.post_quarantined_count == 1  # the payload == "INVALID" row
    admitted_count = result.candidate_facts["identity"].count() - result.post_quarantined_count
    assert admitted_count == 2
    assert _facts_appended_total(result) == 2

    # I-12's own count identities, both stages:
    assert result.raw_count == valid_count + result.pre_quarantined_count
    assert (
        result.candidate_facts["identity"].count() == admitted_count + result.post_quarantined_count
    )

    pre_rows = _quarantine_rows(spark, qtn_qt, batch_id, "pre_check")
    assert len(pre_rows) == 1
    assert pre_rows[0]["domain_id"] is None
    # 005.1 §12.2: the null-domain_id row now quarantines at pre_check under
    # its real contract-grammar reason code (A-14), not I-P2's blanket text.
    assert pre_rows[0]["reason_code"] == "contract/null-violation"

    post_rows = _quarantine_rows(spark, qtn_qt, batch_id, "post_check")
    assert len(post_rows) == 1
    assert post_rows[0]["domain_id"] == "id-102"
    # A-14: the exemplar's violations variant migrated to a governed
    # `business/…` reason code; the row's own candidate columns (incl.
    # `payload`) now live inside the JSON `row_snapshot`, not as a table
    # column (§4.2's fixed, candidate-independent quarantine shape).
    assert post_rows[0]["reason_code"] == "business/negative-amount"
    assert '"payload":"INVALID"' in post_rows[0]["row_snapshot"]

    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == [("id-101", "delta"), ("id-103", "foxtrot")]


# --- R-09: empty/all-quarantined batch completes, fold skipped --------------


def test_r09_all_quarantined_batch_completes_with_zero_counts(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    ledger_catalog: LedgerCatalogFixture,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("r09_raw")
    qtn_qt = unique_table("r09_qtn")
    fact_qt = unique_table("r09_fact")
    state_qt = unique_table("r09_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec4"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(109)
    object_uris = (str(_FIXTURES_DIR / "all_quarantined" / "object_1.csv"),)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    state_before = snapshot_ids(spark, state_qt)

    result = run_sequence(seed, local_runner_fx)

    assert result.raw_count == 2
    assert result.pre_quarantined_count == 2  # every row -- all-quarantined at pre_check
    assert result.candidate_facts["identity"].count() == 0
    assert result.post_quarantined_count == 0  # nothing reached post_check
    assert _facts_appended_total(result) == 0
    assert result.commit_snapshot_ids == {}  # zero-fact corollary: no key at all (§4.2)

    # fold skipped entirely (§8.2's per-type "empty facts -> skip the merge
    # entirely", `stages/fold.py`'s own step 2) -- no fx.merge call at all,
    # so a bare snapshot-log equality check IS valid here (unlike R-02's
    # fold no-op case, which makes a real fx.merge call that leaves a
    # harmless physical snapshot).
    assert_no_new_snapshot(spark, state_qt, state_before)
    assert _rows_merged_total(result) == 0
    assert result.fold_snapshot_ids == {}
    assert result.published is True

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    completed = next(e["detail"] for e in envelopes if e["detail-type"] == "batch-completed")
    assert completed["raw_count"] == 2
    assert completed["pre_quarantined"] == 2
    assert completed["post_quarantined"] == 0
    assert completed["fact_count"] == 0
    assert completed["state_snapshot_id"] is None

    # §9's "alarm-visible counts" half (bead conveyer-azr.20): the run
    # ledger's own `pre_check` `RunFact` row -- not just the completed
    # event's in-memory payload above -- is what `effects/ledger.py::
    # _stage_metrics` derives the `QuarantinedRows` CloudWatch EMF metric
    # from (dim `stage=pre_check`); a batch whose quarantine rate would
    # trip an alarm must leave that count on the durable, Athena/CloudWatch
    # -queried ledger, not only on the transient event payload.
    pre_check_rows = [
        r for r in ledger_catalog.rows() if r["batch_id"] == batch_id and r["stage"] == "pre_check"
    ]
    assert len(pre_check_rows) == 1
    assert pre_check_rows[0]["pre_quarantined"] == 2


# =============================================================================
# 005.1 §12.3 A-suite (bead conveyer-azr.19, n3-admission-cut)
# =============================================================================


# --- A-01: clean multi-object GZIP delivery e2e ------------------------------


def test_a01_clean_multi_object_gzip_delivery_e2e(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("a01_raw")
    qtn_qt = unique_table("a01_qtn")
    fact_qt = unique_table("a01_fact")
    state_qt = unique_table("a01_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    gzip_read = ReadSpecModel(compression="gzip", dialect=DialectModel(format="csv", header=True))
    spec = PipelineSpecModel(
        pipeline=_unique_pipeline("core1"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_types=_fact_types(_bare(fact_qt), _bare(state_qt)),
        read=gzip_read,
        raw_contract=_IDENTITY_RAW_CONTRACT,
    )
    _create_markers_table_for(spark, spec)
    obj1 = tmp_path / "object_1.csv.gz"
    obj2 = tmp_path / "object_2.csv.gz"
    obj1.write_bytes(gzip.compress((_FIXTURES_DIR / "clean" / "object_1.csv").read_bytes()))
    obj2.write_bytes(gzip.compress((_FIXTURES_DIR / "clean" / "object_2.csv").read_bytes()))
    object_uris = (str(obj1), str(obj2))
    batch_id = _batch_id(1101)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)

    result = run_sequence(seed, local_runner_fx)

    assert result.raw_count == 3
    assert result.pre_quarantined_count == 0
    assert result.post_quarantined_count == 0
    assert _facts_appended_total(result) == 3
    # counts reconcile (§6.3's identity, externally confirmed too)
    assert (
        result.raw_count
        == result.candidate_facts["identity"].count() + result.pre_quarantined_count
    )

    raw_rows = spark.table(raw_qt).where(f"batch_id = '{batch_id}'").collect()
    assert len(raw_rows) == 3
    # A-3: sorted-URI codepoint order assigns object_seq.
    expected_seq = {uri: i + 1 for i, uri in enumerate(sorted(object_uris))}
    for row in raw_rows:
        assert row["object_seq"] == expected_seq[row["source_uri"]]
        assert row["extras"] == {}  # no undeclared columns on this fixture
        assert row["read_spec_version"] == seed.read_spec_version
        assert row["malformed_text"] is None
    obj1_rows = sorted(r["row_index"] for r in raw_rows if r["source_uri"] == str(obj1))
    obj2_rows = sorted(r["row_index"] for r in raw_rows if r["source_uri"] == str(obj2))
    assert obj1_rows == [1, 2]  # object_1.csv has 2 data rows
    assert obj2_rows == [1]  # object_2.csv has 1 data row

    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == [("id-001", "alpha"), ("id-002", "bravo"), ("id-003", "charlie")]


# --- A-02: ragged / unterminated-quote / junk-after-quote rows --------------


def test_a02_ragged_and_unterminated_quote_rows_flag_into_raw_and_quarantine(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("a02_raw")
    qtn_qt = unique_table("a02_qtn")
    fact_qt = unique_table("a02_fact")
    state_qt = unique_table("a02_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec5"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    _create_markers_table_for(spark, spec)
    path = tmp_path / "object_1.csv"
    path.write_text(
        "domain_id,event_time,source_ts,content_hash,payload\n"
        "id-1,2026-01-01T00:00:00Z,2026-01-01T00:00:00Z,h-1,ok\n"
        "id-2,2026-01-01T00:00:01Z,2026-01-01T00:00:01Z,h-2,ragged,extra\n"
        'id-3,2026-01-01T00:00:02Z,2026-01-01T00:00:02Z,h-3,"unterminated\n'
        'id-4,2026-01-01T00:00:03Z,2026-01-01T00:00:03Z,h-4,"ok"junk\n'
    )
    batch_id = _batch_id(1102)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=(str(path),))

    result = run_sequence(seed, local_runner_fx)

    assert result.raw_count == 4  # the ragged row/two junk rows count too -- flagged into raw
    assert result.pre_quarantined_count == 3
    assert _facts_appended_total(result) == 1  # only the well-formed row-1 admitted

    malformed_rows = spark.table(raw_qt).where(
        f"batch_id = '{batch_id}' AND malformed_text IS NOT NULL"
    )
    assert malformed_rows.count() == 3
    for row in malformed_rows.collect():
        assert row["domain_id"] is None  # every declared column NULL
        assert row["event_time"] is None
        assert row["extras"] == {}

    pre_rows = _quarantine_rows(spark, qtn_qt, batch_id, "pre_check")
    assert len(pre_rows) == 3
    assert all(r["reason_code"] == "unreadable/malformed-row" for r in pre_rows)

    fact_rows = [r["domain_id"] for r in spark.table(fact_qt).collect()]
    assert fact_rows == ["id-1"]


# --- A-03: U+FFFD rows; opt-out variant --------------------------------------


def test_a03_replacement_char_rows_quarantine_unless_opted_out(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    header = b"domain_id,event_time,source_ts,content_hash,payload\n"
    good_row = b"id-1,2026-01-01T00:00:00Z,2026-01-01T00:00:00Z,h-1,ok\n"
    # \xff\xfe are invalid UTF-8 continuation bytes -- Hadoop `Text` decodes
    # each malformed byte to its own U+FFFD (module docstring / effects/
    # spark.py's own probe-verified finding).
    bad_row = b"id-2,2026-01-01T00:00:01Z,2026-01-01T00:00:01Z,h-2," + b"\xff\xfe" + b"\n"
    path = tmp_path / "object_1.csv"
    path.write_bytes(header + good_row + bad_row)

    def _run(*, forbid_replacement_chars: bool, batch_num: int):
        raw_qt = unique_table(f"a03_{batch_num}_raw")
        qtn_qt = unique_table(f"a03_{batch_num}_qtn")
        fact_qt = unique_table(f"a03_{batch_num}_fact")
        state_qt = unique_table(f"a03_{batch_num}_state")
        _create_raw_table(spark, raw_qt)
        _create_quarantine_table(spark, qtn_qt)
        _create_fact_table(spark, fact_qt)
        _create_state_table(spark, state_qt)
        contract = RawContractModel(
            columns=[
                ColumnSpec(name=c.name, required=c.required, nullable=c.nullable)
                for c in _IDENTITY_RAW_CONTRACT.columns
            ],
            forbid_replacement_chars=forbid_replacement_chars,
        )
        spec = PipelineSpecModel(
            pipeline=_unique_pipeline("core2"),
            transforms_module="pipelines.identity.transforms",
            raw_table=_bare(raw_qt),
            quarantine_table=_bare(qtn_qt),
            fact_types=_fact_types(_bare(fact_qt), _bare(state_qt)),
            read=ReadSpecModel(dialect=DialectModel(format="csv", header=True)),
            raw_contract=contract,
        )
        _create_markers_table_for(spark, spec)
        batch_id = _batch_id(1103_0 + batch_num)
        seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=(str(path),))
        return run_sequence(seed, local_runner_fx), qtn_qt, batch_id

    default_result, default_qtn, default_batch = _run(forbid_replacement_chars=True, batch_num=1)
    assert default_result.raw_count == 2
    assert default_result.pre_quarantined_count == 1
    assert _facts_appended_total(default_result) == 1
    default_pre = _quarantine_rows(spark, default_qtn, default_batch, "pre_check")
    assert len(default_pre) == 1
    assert default_pre[0]["reason_code"] == "unreadable/encoding-suspect"

    opted_out_result, _opted_out_qtn, _opted_out_batch = _run(
        forbid_replacement_chars=False, batch_num=2
    )
    assert opted_out_result.raw_count == 2
    assert opted_out_result.pre_quarantined_count == 0  # admitted, U+FFFD kept in the cell
    assert _facts_appended_total(opted_out_result) == 2


# --- A-04: tier-3 matrix — one row per check type + one multi-failure row ---


def test_a04_tier3_matrix_one_row_per_check_type_plus_multi_failure(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("a04_raw")
    qtn_qt = unique_table("a04_qtn")
    fact_qt = unique_table("a04_fact")  # pre_check's [DC-1] presence probe only -- never written
    _create_raw_table_a04(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    contract = RawContractModel(
        columns=[
            ColumnSpec(name="id", required=True, nullable=False),
            ColumnSpec(name="amount", type="int", min="1", max="100"),
            ColumnSpec(name="status", allowed_values=["open", "closed"]),
            ColumnSpec(name="code", pattern="[A-Z]{3}"),
        ]
    )
    read = ReadSpecModel(dialect=DialectModel(format="csv", header=True))
    spec = PipelineSpecModel(
        pipeline="pipelines/a04-matrix",
        transforms_module="pipelines.identity.transforms",  # never called (land+pre_check only)
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_types=_fact_types(_bare(fact_qt), "spine_test_tables.unused_state"),
        read=read,
        raw_contract=contract,
    )
    # No `_create_markers_table_for` here -- this scenario drives `land`/
    # `pre_check` directly (never `commit`, see below), so no marker table
    # is ever read or written; `pipeline="pipelines/a04-matrix"` (a legal
    # PIPELINE-grammar slug, `_PIPELINE_SEGMENT` permits single dashes) is
    # therefore never fed through `naming.markers_table`'s own IDENTIFIER-
    # grammar check, which forbids `-` (`[[spine-commit-b9b-marker-
    # mechanics]]` point 2's dash trap, confirmed here the OTHER direction:
    # harmless as long as the pipeline never reaches a marker-deriving
    # stage).
    path = tmp_path / "object_1.csv"
    path.write_text(
        "id,amount,status,code\n"
        ",10,open,ABC\n"  # not-nullable
        "i2,notanumber,open,ABC\n"  # cast-failure
        "i3,10,pending,ABC\n"  # value-not-allowed
        "i4,10,open,abc\n"  # pattern-mismatch
        "i5,999,open,ABC\n"  # out-of-bounds
        "i6,10,badstatus,zzz\n"  # multi-failure: value-not-allowed AND pattern-mismatch
        "i7,50,open,XYZ\n"  # clean
    )
    batch_id = _batch_id(1104)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=(str(path),))

    landed = land.run(seed, local_runner_fx)
    after = pre_check.run(landed, local_runner_fx)

    assert landed.raw_count == 7
    assert after.pre_quarantined_count == 6
    assert sorted(r["id"] for r in after.valid_df.collect()) == ["i7"]

    pre_rows = _quarantine_rows(spark, qtn_qt, batch_id, "pre_check")
    reason_by_id = {}
    for row in pre_rows:
        snapshot = json.loads(row["row_snapshot"])
        reason_by_id[snapshot["id"]] = row["reason_code"]
    assert reason_by_id[None] == "contract/null-violation"  # empty token -> NULL, A-5
    assert reason_by_id["i2"] == "contract/cast-failure"
    assert reason_by_id["i3"] == "contract/value-not-allowed"
    assert reason_by_id["i4"] == "contract/pattern-mismatch"
    assert reason_by_id["i5"] == "contract/out-of-bounds"
    assert reason_by_id["i6"] == "contract/value-not-allowed"  # first in evaluation order


def _create_raw_table_a04(spark: SparkSession, qualified_table: str) -> None:
    contract = RawContractModel(
        columns=[
            ColumnSpec(name="id"),
            ColumnSpec(name="amount"),
            ColumnSpec(name="status"),
            ColumnSpec(name="code"),
        ]
    )
    spark.sql(render_raw_create_table_sql(qualified_table, contract))


# --- A-05: tier-1 defects — no raw rows, no batch-started, failed ledger ----


def test_a05_required_column_missing_raises_pre_append_no_trace(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    ledger_catalog: LedgerCatalogFixture,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("a05_raw")
    qtn_qt = unique_table("a05_qtn")
    fact_qt = unique_table("a05_fact")
    state_qt = unique_table("a05_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec6"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    _create_markers_table_for(spark, spec)
    path = tmp_path / "object_1.csv"
    # `domain_id` (required: true) is missing entirely from the header.
    path.write_text("event_time,source_ts,content_hash,payload\n2026-01-01T00:00:00Z,x,y,z\n")
    batch_id = _batch_id(1105)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=(str(path),))

    with pytest.raises(ValueError, match="admission-defect/required-column-missing") as excinfo:
        run_sequence(seed, local_runner_fx)

    message = str(excinfo.value)
    assert str(path) not in message  # A-10: no URIs in the message

    assert spark.table(raw_qt).where(f"batch_id = '{batch_id}'").count() == 0
    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert envelopes == []  # no batch-started

    rows = [r for r in ledger_catalog.rows() if r["batch_id"] == batch_id]
    assert len(rows) == 1
    assert rows[0]["stage"] == "land"
    assert rows[0]["outcome"] == "failed"
    assert rows[0]["error_type"] == "ValueError"


# --- A-08: extras + promotion ------------------------------------------------


def test_a08_undeclared_columns_land_in_extras_then_promotion_lands_natively(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("a08_raw")
    qtn_qt = unique_table("a08_qtn")
    fact_qt = unique_table("a08_fact")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec7"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    _create_markers_table_for(spark, spec)
    path1 = tmp_path / "object_1.csv"
    path1.write_text(
        "domain_id,event_time,source_ts,content_hash,payload,region\n"
        "id-1,2026-01-01T00:00:00Z,2026-01-01T00:00:00Z,h-1,ok,us-east\n"
    )
    batch1_id = _batch_id(1108)
    seed1 = _make_seed(spec=spec, batch_id=batch1_id, object_uris=(str(path1),))

    landed1 = land.run(seed1, local_runner_fx)
    after1 = pre_check.run(landed1, local_runner_fx)

    raw1 = spark.table(raw_qt).where(f"batch_id = '{batch1_id}'").collect()
    assert len(raw1) == 1
    assert raw1[0]["extras"] == {"region": "us-east"}  # undeclared column lands in extras
    assert "region" not in after1.valid_df.columns  # excluded from valid_df (declared-only)

    # Promote `region` into the contract, bootstrap the SAME raw table (D-11):
    promoted_contract = RawContractModel(
        columns=[
            *(
                ColumnSpec(name=c.name, required=c.required, nullable=c.nullable)
                for c in _IDENTITY_RAW_CONTRACT.columns
            ),
            ColumnSpec(name="region"),
        ]
    )
    bootstrap_raw_table(spark, qualified(_bare(raw_qt)), promoted_contract)
    assert "region" in {f.name for f in spark.table(raw_qt).schema.fields}

    # Batch 1's own already-committed row is untouched -- history stays in extras.
    raw1_after = spark.table(raw_qt).where(f"batch_id = '{batch1_id}'").collect()
    assert raw1_after[0]["extras"] == {"region": "us-east"}
    assert raw1_after[0]["region"] is None  # never backfilled -- additive-only, forward-looking

    # A fresh batch under the promoted contract lands `region` NATIVELY.
    promoted_spec = _make_spec(
        pipeline=_unique_pipeline("mkspec8"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table="spine_test_tables.unused_state",
    )
    _create_markers_table_for(spark, promoted_spec)
    # `model_copy(update=...)` -- unlike `model_dump()` + reconstruction,
    # preserves every OTHER field as its already-validated model instance
    # (nested `fact_types`' `FactTypeModel.schema_` field is alias-only on
    # input, `Field(alias="schema")` -- a `model_dump()` round-trip emits
    # the Python field name `schema_`, which the model then refuses on
    # reconstruction; `model_copy` sidesteps this entirely, `tests/unit/
    # test_binding.py`'s own established precedent for this exact pattern).
    promoted_spec = promoted_spec.model_copy(update={"raw_contract": promoted_contract})
    path2 = tmp_path / "object_2.csv"
    path2.write_text(
        "domain_id,event_time,source_ts,content_hash,payload,region\n"
        "id-2,2026-01-02T00:00:00Z,2026-01-02T00:00:00Z,h-2,ok,eu-west\n"
    )
    batch2_id = _batch_id(1109)
    seed2 = _make_seed(spec=promoted_spec, batch_id=batch2_id, object_uris=(str(path2),))

    landed2 = land.run(seed2, local_runner_fx)
    after2 = pre_check.run(landed2, local_runner_fx)

    raw2 = spark.table(raw_qt).where(f"batch_id = '{batch2_id}'").collect()
    assert raw2[0]["region"] == "eu-west"  # native column, not extras
    assert raw2[0]["extras"] == {}
    assert after2.valid_df.collect()[0]["region"] == "eu-west"


# --- A-11: count identities — raw_count includes flagged rows --------------


def test_a11_batch_started_raw_count_includes_flagged_rows(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("a11_raw")
    qtn_qt = unique_table("a11_qtn")
    fact_qt = unique_table("a11_fact")
    state_qt = unique_table("a11_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec9"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    _create_markers_table_for(spark, spec)
    path = tmp_path / "object_1.csv"
    path.write_text(
        "domain_id,event_time,source_ts,content_hash,payload\n"
        "id-1,2026-01-01T00:00:00Z,2026-01-01T00:00:00Z,h-1,ok\n"
        "id-2,2026-01-01T00:00:01Z,2026-01-01T00:00:01Z,h-2,ragged,extra\n"
    )
    batch_id = _batch_id(1111)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=(str(path),))

    result = run_sequence(seed, local_runner_fx)

    # land's own raw_count (§5.9) -- and batch-started's payload -- include
    # the ragged (flagged) row, not just the well-formed one.
    assert result.raw_count == 2
    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    started = next(e["detail"] for e in envelopes if e["detail-type"] == "batch-started")
    assert started["raw_count"] == 2

    # §6.3's count identity, externally confirmed (asserted internally too,
    # fresh path only, inside `stages/pre_check.py`):
    valid_count = result.candidate_facts["identity"].count()
    assert result.raw_count == valid_count + result.pre_quarantined_count


# =============================================================================
# 005.1 §12.3 A-suite, N4 (bead conveyer-azr.20, n4-rerun-matrix)
# =============================================================================


# --- A-07(a): unchanged-contract guard-skip rerun -- subtraction matches fresh


def test_a07a_pre_check_rerun_unchanged_contract_subtraction_matches_fresh(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    raw_qt = unique_table("a07a_raw")
    qtn_qt = unique_table("a07a_qtn")
    fact_qt = unique_table("a07a_fact")
    state_qt = unique_table("a07a_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec10"),
        transforms_module="pipelines.identity_violations.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
        checks=_VIOLATIONS_CHECKS,
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(1112)
    object_uris = (str(_FIXTURES_DIR / "violations" / "object_1.csv"),)

    first = run_sequence(
        _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris), local_runner_fx
    )
    assert first.pre_check_drift is None
    valid_first = sorted((r["domain_id"], r["payload"]) for r in first.valid_df.collect())

    # Rerun: fresh seed, SAME batch_id, UNCHANGED contract -- door 1's own
    # subtraction path (§6.5's A-9 mechanism). land+pre_check only: this leg
    # pins pre_check's own subtraction, not the rest of the sequence (A-10's
    # own kill-based leg already exercises post_check's rerun mechanics).
    second_seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    second = land.run(second_seed, local_runner_fx)
    second = pre_check.run(second, local_runner_fx)

    assert second.guard_skips == ("land", "pre_check")
    assert second.pre_check_drift is None
    valid_second = sorted((r["domain_id"], r["payload"]) for r in second.valid_df.collect())
    assert valid_second == valid_first
    assert second.pre_quarantined_count == 0


# --- A-07(c): the [DC-1] door -- zero-violation batch, contract tightened,
# a deliberate --rN rerun after completion --------------------------------


def test_a07c_pre_check_dc1_door_zero_violation_batch_rerun_after_contract_tightened(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("a07c_raw")
    qtn_qt = unique_table("a07c_qtn")
    fact_qt = unique_table("a07c_fact")
    state_qt = unique_table("a07c_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec11"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(1113)
    object_uris = (
        str(_FIXTURES_DIR / "clean" / "object_1.csv"),
        str(_FIXTURES_DIR / "clean" / "object_2.csv"),
    )
    first_seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    first = run_sequence(first_seed, local_runner_fx)
    assert first.pre_quarantined_count == 0
    assert _facts_appended_total(first) == 3
    moto_events_bus.read_events()  # drain attempt 1's events

    qtn_before = snapshot_ids(spark, qtn_qt)
    fact_before = snapshot_ids(spark, fact_qt)

    # Tighten the contract between attempts: `payload` must now start with
    # 'a' -- "alpha" still matches, "bravo"/"charlie" now would not (a real
    # rule change, not a synthetic probe).
    tightened = RawContractModel(
        columns=[
            *(
                ColumnSpec(name=c.name, required=c.required, nullable=c.nullable)
                for c in _IDENTITY_RAW_CONTRACT.columns
                if c.name != "payload"
            ),
            ColumnSpec(name="payload", pattern="a.*"),
        ]
    )
    # `model_copy(update=...)`, not `model_dump()` + reconstruction -- see
    # the identical note a few tests up (`test_a08_...`'s own `promoted_spec`).
    tightened_spec = spec.model_copy(update={"raw_contract": tightened})
    second_seed = _make_seed(spec=tightened_spec, batch_id=batch_id, object_uris=object_uris)
    second = run_sequence(second_seed, local_runner_fx)

    assert second.pre_quarantined_count == 0  # [DC-1]: durable authoritative, no append
    assert_no_new_snapshot(spark, qtn_qt, qtn_before)
    assert_no_new_snapshot(spark, fact_qt, fact_before)  # facts untouched by the recompute
    # pre_check's own guard was NEVER present on this door -- outcome "ok",
    # not a guard-skip (only land/commit genuinely guard-skip here).
    assert second.guard_skips == ("land", "commit")
    assert second.pre_check_drift is not None
    assert second.pre_check_drift.startswith(
        "pre_check drift: durable=0 recomputed=2 only_durable=0 only_recomputed=2 "
        "admitted_cast_failures=0 "
    )

    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == [("id-001", "alpha"), ("id-002", "bravo"), ("id-003", "charlie")]

    # Both lifecycle events re-emitted, payloads field-equal to the
    # originals' (R-02's own erratum carve-out: `state_snapshot_id` excluded
    # -- a healthy fold rerun is a LOGICAL no-op, `None`, even though the
    # first attempt's own fold produced a real snapshot id).
    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert len(envelopes) == 2
    by_type = {e["detail-type"]: e["detail"] for e in envelopes}
    completed = by_type["batch-completed"]
    first_completed = first.completed_event
    assert completed["raw_count"] == first_completed.raw_count
    assert completed["pre_quarantined"] == first_completed.pre_quarantined
    assert completed["post_quarantined"] == first_completed.post_quarantined
    assert completed["fact_count"] == first_completed.fact_count
    assert completed["fact_snapshot_id"] == first_completed.fact_snapshot_id
    assert completed["state_snapshot_id"] is None
    assert first_completed.state_snapshot_id is not None


# --- A-07(d): the post_check twin [R2-1] -- business rule tightened,
# a deliberate --rN rerun after completion ----------------------------------


def test_a07d_post_check_r2_1_door_zero_violation_batch_rerun_after_business_rule_tightened(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    raw_qt = unique_table("a07d_raw")
    qtn_qt = unique_table("a07d_qtn")
    fact_qt = unique_table("a07d_fact")
    state_qt = unique_table("a07d_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec12"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(1114)
    # The `identity` type declares zero checks (§4.2's empty default), so a
    # row carrying `payload == "INVALID"` still lands cleanly on attempt 1 --
    # it is only `pipelines.identity_violations`'s own (tightened) checks.yaml
    # rule that would ever flag it (006.1 §12.2/G-12).
    object_uris = (str(_FIXTURES_DIR / "post_check_drift" / "object_1.csv"),)
    first = run_sequence(
        _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris), local_runner_fx
    )
    assert first.post_quarantined_count == 0
    assert _facts_appended_total(first) == 2

    qtn_before = snapshot_ids(spark, qtn_qt)

    # `model_copy(update=...)`, not `model_dump()` + reconstruction -- see
    # `test_a08_...`'s own `promoted_spec` note above. `checks` ALSO swaps to
    # `VIOLATIONS_CHECKS` here (006.1 migration): tightening used to mean
    # swapping `transforms_module` alone (the old per-pipeline `post_check`);
    # the rule itself now lives in checks.yaml, so both must move together.
    tightened_spec = spec.model_copy(
        update={
            "transforms_module": "pipelines.identity_violations.transforms",
            "checks": _VIOLATIONS_CHECKS,
        }
    )
    second_seed = _make_seed(spec=tightened_spec, batch_id=batch_id, object_uris=object_uris)
    second = run_sequence(second_seed, local_runner_fx)

    assert second.post_quarantined_count == 0  # [R2-1]: durable authoritative, no append
    assert_no_new_snapshot(spark, qtn_qt, qtn_before)
    assert second.guard_skips == ("land", "commit")  # post_check's own guard never present
    # Structural, not the incidental exact string (critique nit a, bead
    # conveyer-azr.30 -- `_drift_message`'s grammar was unified, which
    # legitimately churns exact text elsewhere, e.g. path 4's own test in
    # test_stages_post_commit_fold_publish.py): durable=0 (no durable
    # post_check rows exist on this door by construction) and recomputed=1
    # (one row -- "id-302" -- recomputes as a violation under the tightened
    # business rule) are the substantive counts this door's drift asserts.
    assert second.post_check_drift is not None
    assert second.post_check_drift.startswith("post_check drift: durable=0 recomputed=1 ")

    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == [("id-301", "alpha"), ("id-302", "INVALID")]  # unchanged by the recompute


# --- A-10: one-shape quarantine -- pre and post rows through one projection


def test_a10_pre_and_post_rows_share_one_quarantine_table_one_projection(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    raw_qt = unique_table("a10_raw")
    qtn_qt = unique_table("a10_qtn")
    fact_qt = unique_table("a10_fact")
    state_qt = unique_table("a10_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec13"),
        transforms_module="pipelines.identity_violations.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
        checks=_VIOLATIONS_CHECKS,
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(1115)
    object_uris = (str(_FIXTURES_DIR / "violations" / "object_1.csv"),)
    result = run_sequence(
        _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris), local_runner_fx
    )
    assert result.pre_quarantined_count == 1
    assert result.post_quarantined_count == 1

    # §4.2: ONE quarantine table, ONE projection -- no per-check-stage query
    # is needed to read back every row a batch produced.
    rows = spark.table(qtn_qt).where(f"batch_id = '{batch_id}'").collect()
    assert len(rows) == 2
    by_stage = {r["check_stage"]: r.asDict() for r in rows}
    assert set(by_stage) == {"pre_check", "post_check"}

    pre_row = by_stage["pre_check"]
    assert pre_row["domain_id"] is None  # unknown at pre_check
    assert pre_row["source_uri"] is not None  # locators: non-null on pre_check rows (D-9)
    assert pre_row["object_seq"] is not None
    assert pre_row["row_index"] is not None
    assert pre_row["reason_code"] == "contract/null-violation"
    assert pre_row["reason_detail"] is not None
    assert len(pre_row["row_hash"]) == 64

    post_row = by_stage["post_check"]
    assert post_row["domain_id"] == "id-102"  # known at post_check
    assert post_row["source_uri"] is None  # locators: NULL on post_check rows (§8.1)
    assert post_row["object_seq"] is None
    assert post_row["row_index"] is None
    assert post_row["reason_code"] == "business/negative-amount"
    # `reason_detail` is a JSON array naming every failed check (id + its own
    # version) -- production behavior from the business-checks compilation
    # (006.1's post-check interpreter), postdating this test's original
    # `is None` assertion (a stale expectation, unrelated to B10/fold).
    # Structural, not the incidental exact version hash (this file's own
    # established convention, e.g. A-7d's drift-message assertion above).
    detail = json.loads(post_row["reason_detail"])
    assert [d["id"] for d in detail] == ["no-invalid-payload"]
    assert len(detail[0]["version"]) == 64
    assert len(post_row["row_hash"]) == 64

    # every row -- either stage -- carries the shared §4.2 lineage columns;
    # the ONE physical `check_version` column is stage-dependent in its
    # SOURCE (`frames/quarantine.py`'s own two shapers): pre_check rows
    # stamp A-11's raw-contract-derived `check_version`, post_check rows
    # stamp the business-checks-derived `checks_version` -- two different
    # version concepts sharing one column name by stage, not one shared
    # value every row must match.
    for row in by_stage.values():
        assert row["batch_id"] == batch_id
        assert row["delivery_id"] == result.delivery_id
        assert row["feed_id"] == result.feed_id
        assert row["quarantined_at"] is not None
    assert by_stage["pre_check"]["check_version"] == result.check_version
    assert by_stage["post_check"]["check_version"] == result.checks_version


# --- A-10: nonconforming post reason -> named defect, A-14's grammar -------


@pytest.mark.skip(
    reason=(
        "006.1 §5.4 K6 supersedes A-14a (bead conveyer-6pg.13, B3): a "
        "pipeline-authored `post_check` producing a free-text `reason` is "
        "structurally unreachable now -- `Transforms` no longer carries "
        "`post_check` at all, and the framework's own interpreter sources "
        "`reason_code`/`reason_detail` from `checks.yaml`'s declared, "
        "bind-time-validated `RowCheckModel.reason` field (`Field(pattern="
        "BUSINESS_REASON_RE)`, refused at spec PARSE, long before any batch "
        "runs) -- there is no code path left for a non-conforming reason to "
        "reach post_check at runtime. `frames/quarantine.py::shape_post_"
        "quarantine`'s own docstring states this explicitly: 'no runtime "
        "grammar check here.' Retained (not deleted) as the historical "
        "record of A-14a's own runtime mechanism, now fully retired."
    )
)
def test_a10_nonconforming_post_reason_is_a_named_defect_with_a14_grammar(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    ledger_catalog: LedgerCatalogFixture,
    unique_table: Callable[[str], str],
) -> None:
    raw_qt = unique_table("a10b_raw")
    qtn_qt = unique_table("a10b_qtn")
    fact_qt = unique_table("a10b_fact")
    state_qt = unique_table("a10b_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec14"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(1116)
    object_uris = (str(_FIXTURES_DIR / "clean" / "object_1.csv"),)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)

    def _bad_reason_post_check(candidate_facts_df, co_effects):
        del co_effects
        # A free-text reason -- never fullmatches A-14's `^business/…$`
        # grammar -- the pipeline author's own review defect, not a
        # framework guess or a silent drop.
        return candidate_facts_df.limit(1).withColumn("reason", F.lit("not-a-valid-reason"))

    seed = replace(
        seed,
        transforms=Transforms(
            apply=identity_transforms.apply,
            post_check=_bad_reason_post_check,
            fold=seed.transforms.fold,
        ),
    )

    qtn_before = snapshot_ids(spark, qtn_qt)
    fact_before = snapshot_ids(spark, fact_qt)
    with pytest.raises(ValueError, match=r"not fullmatching \^business/"):
        run_sequence(seed, local_runner_fx)

    # atomic: no partial commit on either table (D-4's premise)
    assert_no_new_snapshot(spark, qtn_qt, qtn_before)
    assert_no_new_snapshot(spark, fact_qt, fact_before)

    failed_rows = [
        r
        for r in ledger_catalog.rows()
        if r["batch_id"] == batch_id and r["stage"] == "post_check" and r["outcome"] == "failed"
    ]
    assert len(failed_rows) == 1
    assert failed_rows[0]["error_type"] == "ValueError"


# --- A-16: empty / header-only delivery -------------------------------------


def test_a16_header_only_delivery_completes_with_zero_counts_rerun_reappends_empty(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("a16_raw")
    qtn_qt = unique_table("a16_qtn")
    fact_qt = unique_table("a16_fact")
    state_qt = unique_table("a16_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    spec = _make_spec(
        pipeline=_unique_pipeline("mkspec15"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
    )
    _create_markers_table_for(spark, spec)
    batch_id = _batch_id(1117)
    object_uris = (str(_FIXTURES_DIR / "header_only" / "object_1.csv"),)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)

    result = run_sequence(seed, local_runner_fx)
    assert result.raw_count == 0
    assert result.pre_quarantined_count == 0
    assert result.post_quarantined_count == 0
    assert _facts_appended_total(result) == 0
    assert result.published is True
    # fold's own empty-facts skip (§8.2 step 2), same as R-09's own empty-batch pin
    assert _rows_merged_total(result) == 0
    assert result.fold_snapshot_ids == {}

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert len(envelopes) == 2

    raw_before = snapshot_ids(spark, raw_qt)
    fact_before = snapshot_ids(spark, fact_qt)
    qtn_before = snapshot_ids(spark, qtn_qt)

    # §9/[DC-12]: `land`'s `table_has_batch` guard predicate reads DATA
    # (I-3) -- a batch that only ever produced ZERO raw rows leaves no row
    # for LAND's own guard to find, on ANY attempt, so a rerun re-executes
    # `land` fresh every time, committing its own new, harmless empty
    # snapshot -- the accepted guard-blind edge, at `land` grain.
    # **`commit`'s own guard is NOT equally blind under the N-table design
    # (B10, bead conveyer-6pg.22): F-4's zero-fact corollary (§4.3 step 5,
    # `stages/commit.py`'s own docstring) means a ZERO-novel table writes NO
    # marker row and NO facts at all -- not even an empty append -- so a
    # rerun of an all-empty batch commits NO new fact-table snapshot,
    # correcting the OLD v1 comment this test used to carry (v1's `fx.
    # append` was always called, even with zero rows).**
    second_seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    second = run_sequence(second_seed, local_runner_fx)

    assert second.raw_count == 0
    assert _facts_appended_total(second) == 0
    assert second.guard_skips == ()  # land could not see the prior attempt (raw guard-blind);
    # commit has NOTHING to guard-skip either -- its own zero-novel corollary means it never
    # attempted an append on either attempt, so "commit" never enters `guard_skips` at all.
    raw_after = snapshot_ids(spark, raw_qt)
    fact_after = snapshot_ids(spark, fact_qt)
    qtn_after = snapshot_ids(spark, qtn_qt)
    assert len(raw_after - raw_before) == 1  # one NEW empty snapshot, re-appended (land only)
    assert fact_after == fact_before  # commit's zero-fact corollary: no append, ever
    assert qtn_after == qtn_before  # zero violations either attempt -- never even attempted


# --- §9 residual: late-stream corruption is an unnamed, loud failure -------


def test_late_stream_corruption_during_append_is_an_unnamed_loud_failure(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    """§5.7/§9's accepted residual: corruption at the HEAD of a stream (the
    header probe's own bounded read, §5.2) surfaces as a NAMED tier-1 defect
    (A-05 covers that case). Corruption DEEPER in the stream -- past what the
    header probe ever reads -- only surfaces once `land`'s real read
    genuinely executes, which happens lazily at the append action: a raw
    engine error, deterministic and loud, but unnamed -- no `admission-
    defect/…` grammar, no ledger-friendly message, an accepted residual
    (A-10)."""
    raw_qt = unique_table("late_corrupt_raw")
    qtn_qt = unique_table("late_corrupt_qtn")
    fact_qt = unique_table("late_corrupt_fact")
    state_qt = unique_table("late_corrupt_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    gzip_read = ReadSpecModel(compression="gzip", dialect=DialectModel(format="csv", header=True))
    spec = PipelineSpecModel(
        pipeline=_unique_pipeline("core3"),
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_types=_fact_types(_bare(fact_qt), _bare(state_qt)),
        read=gzip_read,
        raw_contract=_IDENTITY_RAW_CONTRACT,
    )
    _create_markers_table_for(spark, spec)
    plain = "domain_id,event_time,source_ts,content_hash,payload\n" + "".join(
        f"id-{i:04d},2026-07-01T00:00:{i % 60:02d}Z,2026-07-01T00:00:{i % 60:02d}Z,"
        f"h-{i},payload-{i}\n"
        for i in range(2000)
    )
    compressed = gzip.compress(plain.encode("utf-8"))
    # Truncated well past the header line's own compressed bytes (so the
    # header probe's bounded read, §5.2, succeeds) but well before the
    # object's true end -- corruption the header probe cannot see.
    truncated = compressed[: len(compressed) // 3]
    path = tmp_path / "object_1.csv.gz"
    path.write_bytes(truncated)
    batch_id = _batch_id(1118)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=(str(path),))

    raw_before = snapshot_ids(spark, raw_qt)
    with pytest.raises(Exception) as excinfo:  # noqa: PT011 -- deliberately broad, see docstring
        run_sequence(seed, local_runner_fx)

    # NOT the named A-10 tier-1 grammar -- a genuinely unnamed engine error
    # (empirically: a raw `Py4JJavaError` wrapping a Spark task failure).
    assert not isinstance(excinfo.value, ValueError)
    assert "admission-defect" not in str(excinfo.value)
    assert_no_new_snapshot(spark, raw_qt, raw_before)  # atomic: no partial raw commit
