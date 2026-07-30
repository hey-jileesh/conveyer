"""`spine.bootstrap.create_admission_tables` — LLD 005.1 §4.4, A-14.

Tests the idempotent raw/quarantine DDL creation-and-evolution logic
directly against the shared `spark`/`unique_table` fixtures (`tests/
conftest.py`'s local Hadoop-catalog `spine_cat`) — the same "test the real
function against the real local Iceberg catalog" shape `tests/unit/
test_create_run_ledger.py` uses for `create_run_ledger` (that module's own
docstring: "`main()` — the real-AWS-Glue-Catalog entrypoint — is NOT covered
here"; this file follows the identical exclusion for THIS module's own
`main()`, never invoked below).

Deliberately does NOT touch `tests/integration/scenario_helpers.py`'s own
`ROW_DDL`/`create_raw_table`/`create_quarantine_table` — see `create_
admission_tables.py`'s own module docstring for why that swap rides
`n3-admission-cut` rather than this bead. `_bare`/`_CATALOG_PREFIX` below is
the same small local copy `test_spark_fx.py` and `test_stages_land_pre_pull_
apply.py` already each carry (`unique_table` returns a fully-qualified
`spine_cat.<db>.<table>` identifier; `PipelineSpecModel.raw_table`/
`.quarantine_table` want the bare `<db>.<table>` form).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from spine.bootstrap.create_admission_tables import (
    QUARANTINE_COLUMNS,
    bootstrap_admission_tables,
    bootstrap_quarantine_table,
    bootstrap_raw_table,
    raw_columns_ordered,
)
from spine.core.model import (
    ColumnSpec,
    DialectModel,
    PipelineSpecModel,
    RawContractModel,
    ReadSpecModel,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import SparkSession

_CATALOG_PREFIX = "spine_cat."


def _bare(qualified_table: str) -> str:
    assert qualified_table.startswith(_CATALOG_PREFIX)
    return qualified_table.removeprefix(_CATALOG_PREFIX)


def _contract(*extra_names: str) -> RawContractModel:
    """`domain_id` (required, non-null) + `event_time`, plus any `extra_names`
    — mirrors `scenario_helpers.IDENTITY_RAW_CONTRACT`'s shape without
    importing it (this file's own small, independent copy, same class as
    `test_stages_land_pre_pull_apply.py`'s own table-shape helpers)."""
    columns = [
        ColumnSpec(name="domain_id", required=True, nullable=False),
        ColumnSpec(name="event_time"),
    ]
    columns.extend(ColumnSpec(name=name) for name in extra_names)
    return RawContractModel(columns=columns)


def _columns_ordered(spark: SparkSession, qualified_table: str) -> list[tuple[str, str, bool]]:
    """`(name, spark_type, not_null)` per column, in physical schema order —
    the same shape `ColumnDDL` carries, so a test can compare directly
    against `raw_columns_ordered(...)`/`QUARANTINE_COLUMNS` tuples."""
    return [
        (f.name, f.dataType.simpleString(), not f.nullable)
        for f in spark.table(qualified_table).schema.fields
    ]


def _snapshot_count(spark: SparkSession, qualified_table: str) -> int:
    return spark.sql(f"SELECT COUNT(*) AS c FROM {qualified_table}.snapshots").collect()[0]["c"]


# --- raw table: fresh creation ------------------------------------------------


def test_bootstrap_raw_table_creates_expected_schema(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("raw")
    contract = _contract("payload")

    bootstrap_raw_table(spark, qt, contract)

    expected = [(c.name, c.spark_type, c.not_null) for c in raw_columns_ordered(contract)]
    assert _columns_ordered(spark, qt) == expected
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {qt}").collect()}
    assert props["format-version"] == "2"
    # A-8: `identity(batch_id)` -- a plain PARTITIONED BY (batch_id) reference.
    partition_rows = spark.sql(f"DESCRIBE TABLE EXTENDED {qt}").collect()
    partition_struct = next(r["data_type"] for r in partition_rows if r["col_name"] == "_partition")
    assert partition_struct == "struct<batch_id:string>"


def test_bootstrap_raw_table_never_created_with_extra_table_properties(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    """§4.1: "no other table properties" -- `write.target-file-size-bytes`
    arrives per-append (`RunConfig`), never at bootstrap. This only asserts
    what bootstrap itself never SETS; Iceberg's own automatic defaults
    (`format`, `write.parquet.compression-codec`, …) are not this module's
    concern and are not asserted against."""
    qt = unique_table("raw")
    bootstrap_raw_table(spark, qt, _contract())
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {qt}").collect()}
    assert "write.target-file-size-bytes" not in props


# --- raw table: idempotency + the [DC-11] promote-then-noop cycle (A-14) ----


def test_bootstrap_raw_table_second_run_is_a_noop(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("raw")
    contract = _contract("payload")
    bootstrap_raw_table(spark, qt, contract)
    before_cols = _columns_ordered(spark, qt)
    before_snaps = _snapshot_count(spark, qt)

    bootstrap_raw_table(spark, qt, contract)

    assert _columns_ordered(spark, qt) == before_cols
    # DDL-only operations never create a data snapshot.
    assert _snapshot_count(spark, qt) == before_snaps == 0


def test_bootstrap_raw_table_promote_column_then_bootstrap_again_is_a_noop(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    """A-14's own scenario: bootstrap -> promote a column -> bootstrap again
    no-ops [DC-11]. `payload` lands via `ADD COLUMNS`, physically appended
    AFTER `extras` (not where a fresh creation would have placed it) --
    order-insensitive diffing is exactly what makes the SECOND bootstrap a
    no-op instead of failing forever after the first promotion (§4.4)."""
    qt = unique_table("raw")
    pre_promotion = _contract()
    bootstrap_raw_table(spark, qt, pre_promotion)
    names_before = [name for name, _, _ in _columns_ordered(spark, qt)]
    assert "payload" not in names_before

    post_promotion = _contract("payload")
    bootstrap_raw_table(spark, qt, post_promotion)  # the promotion: ADD COLUMNS
    names_after_promotion = [name for name, _, _ in _columns_ordered(spark, qt)]
    assert "payload" in names_after_promotion
    assert names_after_promotion.index("payload") > names_after_promotion.index("extras")

    before_noop = _columns_ordered(spark, qt)
    bootstrap_raw_table(spark, qt, post_promotion)  # bootstrap again: no-op
    assert _columns_ordered(spark, qt) == before_noop


# --- raw table: non-additive diffs are loud failures (never applied) -------


def test_bootstrap_raw_table_missing_framework_column_raises(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("raw")
    spark.sql(f"CREATE TABLE {qt} (batch_id STRING, delivery_id STRING) USING iceberg")

    try:
        bootstrap_raw_table(spark, qt, _contract())
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "framework" in str(exc)
    # never dropped/retyped/reordered/partially-applied: still exactly the 2 columns.
    assert [name for name, _, _ in _columns_ordered(spark, qt)] == ["batch_id", "delivery_id"]


def test_bootstrap_raw_table_type_mismatch_raises(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("raw")
    bootstrap_raw_table(spark, qt, _contract())
    spark.sql(f"ALTER TABLE {qt} ALTER COLUMN object_seq TYPE BIGINT")

    try:
        bootstrap_raw_table(spark, qt, _contract())
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "type mismatch" in str(exc)
        assert "object_seq" in str(exc)


def test_bootstrap_raw_table_unexplained_column_raises(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("raw")
    bootstrap_raw_table(spark, qt, _contract())
    spark.sql(f"ALTER TABLE {qt} ADD COLUMNS (mystery_col STRING)")

    try:
        bootstrap_raw_table(spark, qt, _contract())
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "mystery_col" in str(exc)


def test_bootstrap_raw_table_non_additive_diff_does_not_apply_missing_declared(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    """A loud failure alongside a genuinely-missing declared column must not
    silently add the declared column anyway -- the whole diff is all-or-
    nothing (§4.4: never a partial apply)."""
    qt = unique_table("raw")
    bootstrap_raw_table(spark, qt, _contract())  # no "payload" yet
    spark.sql(f"ALTER TABLE {qt} ADD COLUMNS (mystery_col STRING)")

    try:
        bootstrap_raw_table(spark, qt, _contract("payload"))
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
    assert "payload" not in [name for name, _, _ in _columns_ordered(spark, qt)]


# --- quarantine table: constant DDL, exact-equality evolution rule ---------


def test_bootstrap_quarantine_table_creates_constant_schema(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("qtn")

    bootstrap_quarantine_table(spark, qt)

    expected = [(c.name, c.spark_type, c.not_null) for c in QUARANTINE_COLUMNS]
    assert _columns_ordered(spark, qt) == expected


def test_bootstrap_quarantine_table_second_run_is_a_noop(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("qtn")
    bootstrap_quarantine_table(spark, qt)
    before = _columns_ordered(spark, qt)

    bootstrap_quarantine_table(spark, qt)

    assert _columns_ordered(spark, qt) == before


def test_bootstrap_quarantine_table_schema_drift_raises(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("qtn")
    bootstrap_quarantine_table(spark, qt)
    spark.sql(f"ALTER TABLE {qt} ADD COLUMNS (status STRING)")  # §14's own named erosion example

    try:
        bootstrap_quarantine_table(spark, qt)
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "drift" in str(exc)


# --- orchestration: one spec, both tables -----------------------------------


def test_bootstrap_admission_tables_creates_raw_and_quarantine(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    raw_qt = unique_table("raw")
    qtn_qt = unique_table("qtn")
    spec = PipelineSpecModel(
        pipeline="pipelines/admission-probe",
        transforms_module="pipelines.admission_probe.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(unique_table("fact")),
        state_table=_bare(unique_table("state")),
        read=ReadSpecModel(dialect=DialectModel(format="csv", header=True)),
        raw_contract=_contract("payload"),
    )

    bootstrap_admission_tables(spark, "spine_cat", spec)

    assert spark.catalog.tableExists(raw_qt)
    assert spark.catalog.tableExists(qtn_qt)
    raw_names = [name for name, _, _ in _columns_ordered(spark, raw_qt)]
    assert "payload" in raw_names
    qtn_expected = [(c.name, c.spark_type, c.not_null) for c in QUARANTINE_COLUMNS]
    assert _columns_ordered(spark, qtn_qt) == qtn_expected

    # idempotent at the orchestration level too.
    bootstrap_admission_tables(spark, "spine_cat", spec)
    assert [name for name, _, _ in _columns_ordered(spark, raw_qt)] == raw_names
