"""Shared scenario-suite helpers — table-DDL, spec/seed builders, batch ids.

Promoted out of `test_scenarios_core.py` into this dedicated, non-test
module (critique F7 hygiene, bead conveyer-nvh.43): `test_scenarios_kill.py`,
`test_entrypoint.py`, and `test_scenarios_fold.py` all used to import these
directly from `test_scenarios_core.py` (`tests/` has no `__init__.py`
anywhere in this repo, so same-directory modules import as bare top-level
names) — a real test FILE importing another test file's private helpers,
rather than a shared, non-test helper module (the same shape `killfx.py`/
`snapshot_asserts.py` already are). `test_scenarios_core.py` itself now
imports from here too, rather than keeping its own copies.

**005.1 §4.4 DDL parity swap (bead conveyer-azr.19, n3-admission-cut):**
`create_raw_table`/`create_quarantine_table` no longer hand-roll a
9-column `ROW_DDL` — they call `bootstrap.create_admission_tables`'s own DDL
builders (`raw_columns_ordered`/`render_raw_create_table_sql`,
`render_quarantine_create_table_sql`), the SAME functions `bootstrap-
admission` issues in production ("one authored schema, both substrates",
§4.4's own words).

**007.1 §6.5 DDL parity swap (bead conveyer-6pg.21, B9b addendum 1,
routed from B7's own deviation 4): `create_fact_table`/`create_state_table`
no longer hand-roll the provisional 9-column `ROW_DDL` either.** They now
call `bootstrap.create_record_tables`'s own DDL builders
(`bootstrap_fact_table`/`bootstrap_state_table`), the SAME functions
`bootstrap-record-tables` issues in production — "one authored schema, both
substrates" extended to the record side. `ROW_DDL` itself is RETIRED (no
remaining caller) — the real shape is `core.record.FACT_STAMP_TYPES`'s
seven framework stamps (`batch_id`/`delivery_id`/`feed_id`/`received_at`
NOT NULL; `source_ts` nullable; `content_hash`/`record_key` NOT NULL, F-1/
F-2) plus `IDENTITY_FACT_SCHEMA`'s three declared columns (`domain_id`/
`event_time`/`payload`) — a DIFFERENT, wider, partially-NOT-NULL shape than
the old all-nullable 9-column stand-in (no `record_key` column existed in
the old shape at all). `create_markers_table` is new (§6.3's one marker
table per pipeline, `bootstrap_markers_table`) — every scenario exercising
`stages/commit.py`'s F-9 mechanics needs it provisioned before commit runs.
This swap is why the K-suite (`conveyer-6pg.21`'s own DONE bar) MUST land
after it, per that bead's addendum: the old provisional DDL never
exercised Iceberg's write-time required/optional schema-compatibility
check (`effects/spark.py::_build_append`'s own `check-nullability=false`
option/docstring has the full empirical account) — a golden asserting real
commit behavior against the WRONG (all-nullable) DDL would validate nothing
real.

Deliberately NOT promoted here: `test_scenarios_ledger.py`,
`test_stages_post_commit_fold_publish.py`, and
`test_stages_land_pre_pull_apply.py` each carry their OWN small, independent
copies of similarly-shaped helpers (different table shapes / different
pipelines in most cases) — a documented, deliberate choice recorded in
`test_scenarios_ledger.py`'s own module docstring, out of B9b's scope to
change (routed, not silently expanded).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from spine.binding import bind_transforms
from spine.bootstrap.create_admission_tables import (
    render_quarantine_create_table_sql,
    render_raw_create_table_sql,
)
from spine.bootstrap.create_record_tables import (
    bootstrap_fact_table,
    bootstrap_markers_table,
    bootstrap_state_table,
)
from spine.config import RunConfig
from spine.context import BatchContext
from spine.core import naming
from spine.core.checks import checks_version
from spine.core.contract import check_version, read_spec_version
from spine.core.model import (
    ChecksModel,
    ColumnSpec,
    DialectModel,
    FactColumnSpec,
    FactSchemaModel,
    FactTypeModel,
    PipelineSpecModel,
    RawContractModel,
    ReadSpecModel,
    RowCheckModel,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

CATALOG_PREFIX = "spine_cat."
EXEMPLAR_DIR = Path(__file__).resolve().parent.parent / "exemplar" / "identity"
FIXTURES_DIR = EXEMPLAR_DIR / "fixtures"


def bare(qualified_table: str) -> str:
    assert qualified_table.startswith(CATALOG_PREFIX)
    return qualified_table.removeprefix(CATALOG_PREFIX)


def create_raw_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(render_raw_create_table_sql(qualified_table, IDENTITY_RAW_CONTRACT))


def create_quarantine_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(render_quarantine_create_table_sql(qualified_table))


# 005.1 §3.4/A-12: `make_spec`'s `raw_contract` declares the exemplar's own 5
# non-framework columns (`domain_id, event_time, source_ts, content_hash,
# payload` -- this module's own docstring), all `type: string`; `domain_id`
# is `required: true, nullable: false` -- the one contract-declared
# constraint `stages/pre_check.py`'s real `compile_contract` (§6.1) compiles
# into its `not-nullable` check (`contract/null-violation`, A-14/§12.2).
IDENTITY_READ = ReadSpecModel(dialect=DialectModel(format="csv", header=True))
IDENTITY_RAW_CONTRACT = RawContractModel(
    columns=[
        ColumnSpec(name="domain_id", required=True, nullable=False),
        ColumnSpec(name="event_time"),
        ColumnSpec(name="source_ts"),
        ColumnSpec(name="content_hash"),
        ColumnSpec(name="payload"),
    ]
)

# 006.1 §4.1: the `identity` fact type's own declared `FactSchemaModel` --
# ONE shape, matching `pipelines.identity.transforms._FACT_COLUMNS`/
# `FACT_TYPE` exactly (both enumerate the same three names / the same key).
# `source_ts`/`content_hash` are excluded -- both are `core/record.py::
# FACT_STAMP_COLUMNS`, 007.1's framework-derived stamp set (F3 refuses an
# authored column of either name); see `pipelines.identity.transforms`'s
# own module docstring.
IDENTITY_FACT_SCHEMA = FactSchemaModel(
    columns=[
        FactColumnSpec(name="domain_id", type="string"),
        FactColumnSpec(name="event_time", type="string"),
        FactColumnSpec(name="payload", type="string"),
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
    # B10 (bead conveyer-6pg.22, 007.1 §4.1/§8.1): `ordering` may be empty
    # ("order = (source_ts, content_hash)", `FactSchemaModel.ordering`'s own
    # docstring) -- but `source_ts` stays NULL for every fact in Phase 1
    # (`stages/commit.py`'s own module docstring: no data source yet), so an
    # EMPTY declared ordering here would collapse the exemplar's fold order
    # to `content_hash`-only -- an arbitrary-but-deterministic winner, not
    # the event-time LWW semantics `pipelines.identity.transforms`'s own
    # module docstring and the R-07 scenario suite both document and assert
    # on. Declaring `event_time` here is what makes `core.merge.merge_spec`'s
    # per-type `MergeSpec.ordering_cols` = `(event_time, source_ts,
    # content_hash)` -- the SAME triple v1's hardcoded `frames.folds.
    # LWW_ORDERING_COLUMNS` used, now sourced from the declaration instead
    # of a framework constant, per §8.2's mechanical per-type design.
    ordering=["event_time"],
)


def create_fact_table(
    spark: SparkSession, qualified_table: str, schema: FactSchemaModel = IDENTITY_FACT_SCHEMA
) -> None:
    """007.1 §6.5 DDL parity (module docstring, B9b addendum 1): the SAME
    `bootstrap.create_record_tables.bootstrap_fact_table` production
    provisioning calls -- one authored schema, both substrates. `schema`
    defaults to `IDENTITY_FACT_SCHEMA` so every existing call site (`from
    scenario_helpers import create_fact_table as _create_fact_table`, no
    schema argument) keeps working unchanged."""
    bootstrap_fact_table(spark, qualified_table, schema)


def create_state_table(
    spark: SparkSession, qualified_table: str, schema: FactSchemaModel = IDENTITY_FACT_SCHEMA
) -> None:
    """The state-table counterpart of `create_fact_table` above --
    `bootstrap_state_table` bakes in `write.merge.mode=merge-on-read` +
    `write.merge.isolation-level=serializable` at CREATE (§6.2/§6.5 step 3,
    [DC2-3]) plus the declared sort order on `domain_id_col` (§6.4) --
    strictly more than the old hand-rolled DDL's own single TBLPROPERTY."""
    bootstrap_state_table(spark, qualified_table, schema)


def create_markers_table(spark: SparkSession, qualified_table: str) -> None:
    """§6.3's one marker table per pipeline (`bootstrap_markers_table`) --
    new (B9b addendum 1): no prior scenario-suite helper provisioned this
    table at all, since `stages/commit.py`'s F-9 marker mechanics (B9b) is
    the first consumer. Every scenario driving `stages/commit.py` end to
    end must create this table before commit runs (its own marker reads
    tolerate an absent table, `effects/spark.py`'s own guard, but a
    completely unprovisioned pipeline is not the scenario under test)."""
    bootstrap_markers_table(spark, qualified_table)


def unique_pipeline(prefix: str) -> str:
    """A per-test-unique `pipeline` slug (B10, bead conveyer-6pg.22) --
    `naming.markers_table` derives its table name from ONLY `(raw_table's
    db, pipeline)`, and every scenario-suite table lives in the SAME
    constant db (`tests/conftest.py::unique_table`'s own `spine_test_tables`
    literal) -- `make_spec`'s own former hardcoded `pipeline="pipelines/
    identity"` would therefore derive the IDENTICAL markers-table name for
    every test in a run, silently sharing one markers table's rows across
    tests (`[[spine-commit-b9b-marker-mechanics]]` point 2/3's own named
    trap). Alphanumeric-only suffix (no `-` separator) -- legal under BOTH
    the pipeline-segment grammar (`_PIPELINE_SEGMENT`, which permits single
    dashes) AND the identifier grammar the derived table name is later
    checked against (which forbids `-` entirely), the SAME dash-avoidance
    `naming.table_slug`'s own docstring already documents for table names."""
    return f"pipelines/{prefix}{uuid.uuid4().hex[:8]}"


def markers_table_for(spec: PipelineSpecModel) -> str:
    """The QUALIFIED markers-table identifier `stages/commit.py` will itself
    derive and read/write for `spec` -- `naming.markers_table(spec.raw_table,
    spec.pipeline)`, `naming.qualified`-wrapped. Never independently
    `unique_table`'d (B9b's own named trap, `[[spine-commit-b9b-marker-
    mechanics]]` point 3): a test creating an "isolated" markers table under
    its own name would silently look up the WRONG table."""
    return naming.qualified(naming.markers_table(spec.raw_table, spec.pipeline))


def create_markers_table_for(spark: SparkSession, spec: PipelineSpecModel) -> None:
    """`create_markers_table` at the REAL derived name for `spec` -- the one
    call every full-`run_sequence` scenario needs once its spec is built
    (commit's F-9 mechanics, §4.3), paired with `unique_pipeline` for
    per-test isolation."""
    create_markers_table(spark, markers_table_for(spec))


# 006.1 §12.2/G-12: the violations variant's rule now lives in `checks.yaml`
# (declared data), not a second Python `post_check` -- see `pipelines.
# identity_violations.transforms`'s own module docstring. Bound to the
# `identity` fact type; fails iff `payload` equals the fixture's own
# violation marker.
VIOLATIONS_CHECKS = ChecksModel(
    checks=[
        RowCheckModel(
            kind="row",
            id="no-invalid-payload",
            fact_type="identity",
            expr="payload != 'INVALID'",
            reason="business/negative-amount",
        )
    ]
)


def make_spec(
    *,
    transforms_module: str,
    raw_table: str,
    quarantine_table: str,
    fact_table: str,
    state_table: str,
    checks: ChecksModel | None = None,
    pipeline: str = "pipelines/identity",
) -> PipelineSpecModel:
    """`pipeline` defaults to the historical literal (every pre-B10 call
    site keeps working unchanged) -- B10 callers driving a FULL `run_
    sequence` through `stages/commit.py` should pass `pipeline=sh.
    unique_pipeline(...)` instead, per-test, to avoid the markers-table
    collision `unique_pipeline`'s own docstring names."""
    return PipelineSpecModel(
        pipeline=pipeline,
        transforms_module=transforms_module,
        raw_table=raw_table,
        quarantine_table=quarantine_table,
        fact_types={
            "identity": FactTypeModel(
                fact_table=fact_table, state_table=state_table, schema=IDENTITY_FACT_SCHEMA
            )
        },
        checks=checks if checks is not None else ChecksModel(),
        read=IDENTITY_READ,
        raw_contract=IDENTITY_RAW_CONTRACT,
    )


def make_seed(
    *, spec: PipelineSpecModel, batch_id: str, object_uris: tuple[str, ...]
) -> BatchContext:
    return BatchContext(
        pipeline=spec.pipeline,
        feed_id="feed/identity",
        delivery_id=str(uuid.UUID(int=1, version=4)),
        batch_id=batch_id,
        delivery_key="statement.csv",
        content_hash="sha256:" + "a" * 64,
        object_uris=object_uris,
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        spec=spec,
        run=RunConfig(),
        transforms=bind_transforms(spec),
        attempt_id="attempt-1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        read_spec_version=read_spec_version(spec.read),
        check_version=check_version(spec.raw_contract, spec.read),
        checks_version=checks_version(spec.checks),
    )


def batch_id(n: int) -> str:
    return str(uuid.UUID(int=n, version=5))


# --- B10 (bead conveyer-6pg.22): per-table-map derivations for tests that used
# to read the singular `BatchContext` fields `facts_appended`/
# `fact_snapshot_id`/`state_snapshot_id`/`state_read_snapshot_id`/
# `merge_summary` -- none of those were set by any stage past `stages/
# commit.py`'s own B9b rewrite and `stages/fold.py`'s B10 rewrite, and all
# six were deleted from `BatchContext` outright (critique gate
# wf_24a3125f-ecc ruling 1, bead conveyer-6pg.29, F4). Mirrors `core/
# run_facts.py::one_snapshot`'s own "totals / one-snapshot symmetric rule"
# at the TEST-assertion grain.


def facts_appended_total(ctx: BatchContext) -> int:
    """`sum(ctx.facts_appended_by_table.values())` -- this attempt's own
    delta, the SAME derivation `core/run_facts.py`'s ledger-row projection
    applies, exposed here for direct `BatchContext` assertions."""
    by_table = ctx.facts_appended_by_table
    return sum(by_table.values()) if by_table is not None else 0


def rows_merged_total(ctx: BatchContext) -> int:
    """The fold-side counterpart of `facts_appended_total` above."""
    by_table = ctx.rows_merged_by_table
    return sum(by_table.values()) if by_table is not None else 0


def quarantine_rows(
    spark: SparkSession, qualified_table: str, batch_id_value: str, check_stage: str
) -> list[dict[str, object]]:
    return [
        row.asDict()
        for row in spark.table(qualified_table)
        .where(f"batch_id = '{batch_id_value}' AND check_stage = '{check_stage}'")
        .collect()
    ]
