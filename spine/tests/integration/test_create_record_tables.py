"""`spine.bootstrap.create_record_tables` — LLD 007.1 §6.1-§6.5, F-3, F-10.

Tests the idempotent fact/state/marker DDL creation-and-evolution logic, the
§6.5 step 1 prefix assertion, and the F-10 `table-classes.json` inventory
directly against the shared `spark`/`unique_table` fixtures (local Hadoop
catalog `spine_cat`, `tests/conftest.py`) — the same "test the real function
against the real local Iceberg catalog" shape `tests/integration/
test_create_admission_tables.py` uses for `create_admission_tables`; that
file's own docstring records why `main()` (the real-AWS-Glue-Catalog
entrypoint) is excluded here too.

Deliberately does NOT touch `tests/integration/scenario_helpers.py`'s own
`create_fact_table`/`create_state_table` (still the provisional 9-column
identity-exemplar shape, "007/009 territory" per that module's own
docstring, explicitly UNCHANGED by this bead) — mirrors `test_create_
admission_tables.py`'s identical stance on `create_raw_table`/`create_
quarantine_table` before its own n3-admission-cut swap.

K-08's DDL half (marker row-kind goldens: guard-twin vs. commit-completion,
discriminated by the `-completion-` sentinel alone, no `IS NULL` special
case) lives in `test_marker_table_distinguishes_guard_twin_and_commit_
completion_rows_by_sentinel_alone` below.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from spine.bootstrap import create_record_tables
from spine.bootstrap.create_record_tables import (
    MARKER_COLUMNS,
    assert_table_prefixes,
    bootstrap_fact_table,
    bootstrap_markers_table,
    bootstrap_record_tables,
    bootstrap_state_table,
    build_table_class_inventory,
    fact_and_state_columns_ordered,
    render_table_class_inventory_json,
)
from spine.core import naming
from spine.core.model import FactColumnSpec, FactSchemaModel, PipelineSpecModel

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import SparkSession

_CATALOG_PREFIX = "spine_cat."


def _bare(qualified_table: str) -> str:
    assert qualified_table.startswith(_CATALOG_PREFIX)
    return qualified_table.removeprefix(_CATALOG_PREFIX)


def _schema(*extra: tuple[str, str], domain_id_col: str = "domain_id") -> FactSchemaModel:
    columns = [FactColumnSpec(name="domain_id", type="string")]
    columns.extend(FactColumnSpec(name=name, type=type_) for name, type_ in extra)
    return FactSchemaModel(columns=columns, domain_id_col=domain_id_col, record_key=["domain_id"])


def _columns_ordered(spark: SparkSession, qualified_table: str) -> list[tuple[str, str, bool]]:
    return [
        (f.name, f.dataType.simpleString(), not f.nullable)
        for f in spark.table(qualified_table).schema.fields
    ]


def _snapshot_count(spark: SparkSession, qualified_table: str) -> int:
    return spark.sql(f"SELECT COUNT(*) AS c FROM {qualified_table}.snapshots").collect()[0]["c"]


def _tblproperties(spark: SparkSession, qualified_table: str) -> dict[str, str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {qualified_table}").collect()
    return {r["key"]: r["value"] for r in rows}


def _table_class(spark: SparkSession, qualified_table: str) -> str | None:
    return _tblproperties(spark, qualified_table).get("conveyer.table-class")


def _spec(
    *, raw_table: str, fact_table: str, state_table: str, pipeline: str = "identity"
) -> PipelineSpecModel:
    """A minimal, otherwise-valid `PipelineSpecModel` for prefix-assertion /
    orchestration tests -- `raw_table`/`quarantine_table` need not physically
    exist (only their NAMES are read: `markers_table`'s db component,
    `build_table_class_inventory`'s admission-table entries)."""
    return PipelineSpecModel(
        pipeline=pipeline,
        transforms_module="pipelines.identity.transforms",
        raw_table=raw_table,
        quarantine_table=raw_table.replace("__raw", "__quarantine"),
        fact_types={
            "detail": {
                "fact_table": fact_table,
                "state_table": state_table,
                "schema": {
                    "columns": [{"name": "domain_id", "type": "string"}],
                    "domain_id_col": "domain_id",
                    "record_key": ["domain_id"],
                },
            }
        },
        read={"dialect": {"format": "csv"}},
        raw_contract={"columns": [{"name": "domain_id", "required": True, "nullable": False}]},
    )


# --- fact table: fresh creation + idempotency + evolution -------------------


def test_bootstrap_fact_table_creates_expected_schema(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__facts")
    schema = _schema(("event_time", "timestamp"), ("amount", "decimal(10,2)"))

    bootstrap_fact_table(spark, qt, schema)

    expected = [(c.name, c.spark_type, c.not_null) for c in fact_and_state_columns_ordered(schema)]
    assert _columns_ordered(spark, qt) == expected
    # §6.1: stamps first (7 columns), then declared (contract order).
    names = [n for n, _, _ in _columns_ordered(spark, qt)]
    assert names[:7] == [
        "batch_id",
        "delivery_id",
        "feed_id",
        "received_at",
        "source_ts",
        "content_hash",
        "record_key",
    ]
    assert names[7:] == ["domain_id", "event_time", "amount"]
    # F-3: `identity(batch_id)` partition, no sort order.
    partition_rows = spark.sql(f"DESCRIBE TABLE EXTENDED {qt}").collect()
    partition_struct = next(r["data_type"] for r in partition_rows if r["col_name"] == "_partition")
    assert partition_struct == "struct<batch_id:string>"
    assert "sort-order" not in _tblproperties(spark, qt)


def test_bootstrap_fact_table_second_run_is_a_noop(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__facts")
    schema = _schema(("event_time", "timestamp"))
    bootstrap_fact_table(spark, qt, schema)
    before_cols = _columns_ordered(spark, qt)
    before_snaps = _snapshot_count(spark, qt)

    bootstrap_fact_table(spark, qt, schema)

    assert _columns_ordered(spark, qt) == before_cols
    assert _snapshot_count(spark, qt) == before_snaps == 0


def test_bootstrap_fact_table_promote_column_then_bootstrap_again_is_a_noop(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__facts")
    pre_promotion = _schema()
    bootstrap_fact_table(spark, qt, pre_promotion)
    names_before = [n for n, _, _ in _columns_ordered(spark, qt)]
    assert "amount" not in names_before

    post_promotion = _schema(("amount", "decimal(10,2)"))
    bootstrap_fact_table(spark, qt, post_promotion)  # the promotion: ADD COLUMNS
    names_after = [n for n, _, _ in _columns_ordered(spark, qt)]
    assert "amount" in names_after

    before_noop = _columns_ordered(spark, qt)
    bootstrap_fact_table(spark, qt, post_promotion)  # bootstrap again: no-op
    assert _columns_ordered(spark, qt) == before_noop


def test_bootstrap_fact_table_type_mismatch_raises(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__facts")
    bootstrap_fact_table(spark, qt, _schema(("count", "int")))
    spark.sql(f"ALTER TABLE {qt} ALTER COLUMN count TYPE BIGINT")  # Iceberg-legal widen

    with pytest.raises(ValueError, match="type mismatch"):
        bootstrap_fact_table(spark, qt, _schema(("count", "int")))


def test_bootstrap_fact_table_unexplained_column_raises(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__facts")
    bootstrap_fact_table(spark, qt, _schema())
    spark.sql(f"ALTER TABLE {qt} ADD COLUMNS (mystery_col STRING)")

    with pytest.raises(ValueError, match="mystery_col"):
        bootstrap_fact_table(spark, qt, _schema())


def test_bootstrap_fact_table_stamps_table_class_at_create(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__facts")
    bootstrap_fact_table(spark, qt, _schema())
    assert _table_class(spark, qt) == "facts"


# --- state table: unpartitioned, sort order, merge properties ---------------


def test_bootstrap_state_table_creates_expected_schema_and_properties(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__state")
    schema = _schema(("event_time", "timestamp"))

    bootstrap_state_table(spark, qt, schema)

    expected = [(c.name, c.spark_type, c.not_null) for c in fact_and_state_columns_ordered(schema)]
    assert _columns_ordered(spark, qt) == expected
    props = _tblproperties(spark, qt)
    assert props["write.merge.mode"] == "merge-on-read"
    assert props["write.merge.isolation-level"] == "serializable"
    assert props["sort-order"] == "domain_id ASC NULLS FIRST"
    # F-3/§6.4: unpartitioned.
    partition_rows = spark.sql(f"DESCRIBE TABLE EXTENDED {qt}").collect()
    partition_struct = next(r["data_type"] for r in partition_rows if r["col_name"] == "_partition")
    assert partition_struct == "struct<>"


def test_bootstrap_state_table_second_run_is_a_noop(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__state")
    schema = _schema()
    bootstrap_state_table(spark, qt, schema)
    before_cols = _columns_ordered(spark, qt)
    before_snaps = _snapshot_count(spark, qt)

    bootstrap_state_table(spark, qt, schema)

    assert _columns_ordered(spark, qt) == before_cols
    assert _snapshot_count(spark, qt) == before_snaps == 0


def test_bootstrap_state_table_repairs_drifted_merge_properties_without_a_new_snapshot(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    """§6.5 step 3 / errata #9 / [DC2-3]: a drifted `write.merge.mode`/
    `write.merge.isolation-level` is repaired (never raised, never
    tolerated) -- and the repair is metadata-only (no new snapshot)."""
    qt = unique_table("identity__state")
    schema = _schema()
    bootstrap_state_table(spark, qt, schema)
    spark.sql(
        f"ALTER TABLE {qt} SET TBLPROPERTIES "
        "('write.merge.mode'='copy-on-write', 'write.merge.isolation-level'='snapshot')"
    )
    before_snaps = _snapshot_count(spark, qt)

    bootstrap_state_table(spark, qt, schema)  # asserts-and-repairs, does not raise

    props = _tblproperties(spark, qt)
    assert props["write.merge.mode"] == "merge-on-read"
    assert props["write.merge.isolation-level"] == "serializable"
    assert _snapshot_count(spark, qt) == before_snaps == 0


def test_bootstrap_state_table_stamps_table_class_at_create(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__state")
    bootstrap_state_table(spark, qt, _schema())
    assert _table_class(spark, qt) == "state"


# --- marker table: constant DDL, exact-equality evolution rule, sentinel ----


def test_bootstrap_markers_table_creates_constant_schema(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__markers")

    bootstrap_markers_table(spark, qt)

    expected = [(c.name, c.spark_type, c.not_null) for c in MARKER_COLUMNS]
    assert _columns_ordered(spark, qt) == expected
    partition_rows = spark.sql(f"DESCRIBE TABLE EXTENDED {qt}").collect()
    partition_struct = next(r["data_type"] for r in partition_rows if r["col_name"] == "_partition")
    assert partition_struct == "struct<batch_id:string>"


def test_bootstrap_markers_table_second_run_is_a_noop(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__markers")
    bootstrap_markers_table(spark, qt)
    before = _columns_ordered(spark, qt)

    bootstrap_markers_table(spark, qt)

    assert _columns_ordered(spark, qt) == before


def test_bootstrap_markers_table_schema_drift_raises(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__markers")
    bootstrap_markers_table(spark, qt)
    spark.sql(f"ALTER TABLE {qt} ADD COLUMNS (status STRING)")

    with pytest.raises(ValueError, match="drift"):
        bootstrap_markers_table(spark, qt)


def test_bootstrap_markers_table_stamps_table_class_at_create(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("identity__markers")
    bootstrap_markers_table(spark, qt)
    assert _table_class(spark, qt) == "marker"


def test_marker_table_distinguishes_guard_twin_and_commit_completion_rows_by_sentinel_alone(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    """K-08's DDL half: guard-twin rows (`table_name` = a real fact table)
    and the commit-completion row (`table_name` = the sentinel) coexist in
    the SAME table, discriminated by `table_name` alone -- no `IS NULL`
    special case anywhere (`snapshot_id` is NULL on every Phase-1 row,
    §6.3's own resolution, regardless of row kind)."""
    qt = unique_table("identity__markers")
    bootstrap_markers_table(spark, qt)
    # The sentinel is structurally outside the identifier grammar -- proven
    # once, at import time, by `core/naming.py`'s own module-level assert
    # (never re-proven here; this test proves the DDL/data-level behavior).
    sentinel = naming.COMMIT_COMPLETION_SENTINEL

    spark.sql(
        f"""
        INSERT INTO {qt} VALUES
        ('b1', 'f1', 'commit', 'lake.identity__facts', NULL, 'dk1', 'ch1',
         TIMESTAMP'2026-01-01 00:00:00', TIMESTAMP'2026-01-01 00:00:01'),
        ('b1', 'f1', 'commit', '{sentinel}', NULL, 'dk1', 'ch1',
         TIMESTAMP'2026-01-01 00:00:00', TIMESTAMP'2026-01-01 00:00:02')
        """
    )
    rows = spark.sql(f"SELECT table_name, snapshot_id FROM {qt} ORDER BY table_name").collect()
    by_name = {r["table_name"]: r["snapshot_id"] for r in rows}
    assert set(by_name) == {"lake.identity__facts", sentinel}
    # Both row kinds carry a NULL snapshot_id (write-order necessity, §6.3) --
    # the discrimination is `table_name` alone, never `snapshot_id`.
    assert by_name["lake.identity__facts"] is None
    assert by_name[sentinel] is None

    guard_twins = spark.sql(
        f"SELECT DISTINCT table_name FROM {qt} "
        f"WHERE stage = 'commit' AND table_name != '{sentinel}'"
    ).collect()
    assert [r["table_name"] for r in guard_twins] == ["lake.identity__facts"]


# --- §6.5 step 1: prefix assertion -------------------------------------------


def test_assert_table_prefixes_passes_for_correctly_prefixed_tables(
    unique_table: Callable[[str], str],
) -> None:
    spec = _spec(
        raw_table=_bare(unique_table("identity__raw")),
        fact_table=_bare(unique_table("identity__facts")),
        state_table=_bare(unique_table("identity__state")),
    )
    assert_table_prefixes(spec)  # must not raise


def test_assert_table_prefixes_names_every_violation(unique_table: Callable[[str], str]) -> None:
    spec = _spec(
        raw_table=_bare(unique_table("identity__raw")),
        fact_table=_bare(unique_table("wrongname__facts")),
        state_table=_bare(unique_table("wrongname__state")),
    )
    with pytest.raises(ValueError) as exc_info:
        assert_table_prefixes(spec)
    message = str(exc_info.value)
    assert "fact_types.detail.fact_table" in message
    assert "fact_types.detail.state_table" in message


def test_bootstrap_record_tables_raises_on_out_of_prefix_marker(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    """The derived markers table shares the prefix violation surface too --
    a `raw_table` in a DIFFERENT slug family than `fact_table`/`state_table`
    makes the DERIVED marker name violate the SAME prefix `fact_table`/
    `state_table` must meet, since `markers_table` and `assert_table_
    prefixes` both key off `spec.pipeline`, not `spec.raw_table`."""
    spec = _spec(
        raw_table=_bare(unique_table("identity__raw")),
        fact_table=_bare(unique_table("identity__facts")),
        state_table=_bare(unique_table("identity__state")),
        pipeline="somethingelse",
    )
    with pytest.raises(ValueError, match="somethingelse__"):
        bootstrap_record_tables(spark, "spine_cat", spec)


# --- orchestration: bootstrap_record_tables, one spec, every table ----------


def test_bootstrap_record_tables_creates_every_declared_table(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    raw_qt = unique_table("identity__raw")
    fact_qt = unique_table("identity__facts")
    state_qt = unique_table("identity__state")
    spec = _spec(raw_table=_bare(raw_qt), fact_table=_bare(fact_qt), state_table=_bare(state_qt))

    bootstrap_record_tables(spark, "spine_cat", spec)

    assert spark.catalog.tableExists(fact_qt)
    assert spark.catalog.tableExists(state_qt)
    marker_qt = f"spine_cat.{naming.markers_table(spec.raw_table, spec.pipeline)}"
    assert spark.catalog.tableExists(marker_qt)
    assert _table_class(spark, fact_qt) == "facts"
    assert _table_class(spark, state_qt) == "state"
    assert _table_class(spark, marker_qt) == "marker"


def test_bootstrap_record_tables_full_rerun_is_idempotent(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    raw_qt = unique_table("identity__raw")
    fact_qt = unique_table("identity__facts")
    state_qt = unique_table("identity__state")
    spec = _spec(raw_table=_bare(raw_qt), fact_table=_bare(fact_qt), state_table=_bare(state_qt))

    bootstrap_record_tables(spark, "spine_cat", spec)
    before_facts = _snapshot_count(spark, fact_qt)
    before_state = _snapshot_count(spark, state_qt)

    bootstrap_record_tables(spark, "spine_cat", spec)  # full rerun -- must be a no-op

    assert _snapshot_count(spark, fact_qt) == before_facts == 0
    assert _snapshot_count(spark, state_qt) == before_state == 0


# --- §6.5 step 6 (F-10): table-classes.json inventory ------------------------


def test_build_table_class_inventory_covers_every_provisioned_table(
    unique_table: Callable[[str], str],
) -> None:
    raw_bare = _bare(unique_table("identity__raw"))
    fact_bare = _bare(unique_table("identity__facts"))
    state_bare = _bare(unique_table("identity__state"))
    spec = _spec(raw_table=raw_bare, fact_table=fact_bare, state_table=state_bare)

    inventory = build_table_class_inventory(spec)

    assert inventory[spec.raw_table] == "raw"
    assert inventory[spec.quarantine_table] == "quarantine"
    assert inventory[fact_bare] == "facts"
    assert inventory[state_bare] == "state"
    assert inventory[naming.markers_table(spec.raw_table, spec.pipeline)] == "marker"
    assert len(inventory) == 5


def test_render_table_class_inventory_json_is_deterministic_and_sorted() -> None:
    inventory = {"z.table": "state", "a.table": "facts"}
    rendered = render_table_class_inventory_json(inventory)
    assert rendered == render_table_class_inventory_json(dict(reversed(inventory.items())))
    assert rendered.index('"a.table"') < rendered.index('"z.table"')


# --- conveyer-6pg.35 item 2: DDL render guards (escaping/quoting/catalog) ---
# No reachable injection today (every value is a framework constant or an
# already-validated field -- `bootstrap/**` sits outside the linter's
# string-SQL sink profile, the priced residual). Defense in depth, tested
# directly against `_escape_sql_string_literal`/`_qualified` (private, this
# module's own "small deliberate independent copy" convention -- see its
# module docstring) plus one real-DDL round trip.


def test_escape_sql_string_literal_round_trips_a_quote_and_backslash_through_real_ddl(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    # Kernel-verified grammar fact (this bead): Spark SQL's `''` is NOT an
    # ANSI-style doubled-quote escape -- `'a''b'` parses as adjacent-literal
    # concatenation (`'a'` + `'b'` -> `"ab"`, the quote silently EATEN), not
    # an escaped embedded quote. Only the backslash form round-trips. This
    # test proves the fix end-to-end through a REAL `CREATE TABLE`, not just
    # that `_escape_sql_string_literal` looks right in isolation.
    qt = unique_table("hostile_prop")
    hostile = "\\o'reilly's " + chr(39) + "table"
    sql_text = create_record_tables._render_create_table_sql(
        qt,
        (create_record_tables.ColumnDDL("x", "string", False),),
        partition_by=(),
        table_class=hostile,
    )
    spark.sql(sql_text)
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {qt}").collect()}
    assert props["conveyer.table-class"] == hostile


def test_escape_sql_string_literal_is_a_noop_for_plain_values() -> None:
    assert create_record_tables._escape_sql_string_literal("facts") == "facts"
    assert create_record_tables._escape_sql_string_literal("merge-on-read") == "merge-on-read"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("plain", "plain"),
        ("o'reilly", "o\\'reilly"),
        ("a'b'c", "a\\'b\\'c"),
        ("back\\slash", "back\\\\slash"),
    ],
)
def test_escape_sql_string_literal_escapes_backslash_before_quote(raw: str, expected: str) -> None:
    assert create_record_tables._escape_sql_string_literal(raw) == expected


def test_qualified_composes_catalog_and_table() -> None:
    assert (
        create_record_tables._qualified("spine_cat", "lake.identity__facts")
        == "spine_cat.lake.identity__facts"
    )


def test_qualified_rejects_a_malformed_catalog() -> None:
    # `--catalog` is a `main()` CLI arg (default `spine_cat`) -- previously
    # composed unvalidated. A hostile value must be refused before it ever
    # reaches a `spark.sql(...)` DDL string, never merely produce a
    # differently-shaped (but still-executed) statement.
    with pytest.raises(ValueError, match="invalid identifier component"):
        create_record_tables._qualified("spine_cat; DROP TABLE x --", "lake.identity__facts")


def test_bootstrap_state_table_quotes_the_sort_column(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    # Defense in depth (`domain_id_col` is already `COLUMN_NAME_RE`-validated
    # at bind time, `FactSchemaModel`'s own membership check) -- the `WRITE
    # ORDERED BY` clause must still render through `quote_identifier`
    # without changing behavior: the fresh-CREATE branch must still succeed
    # against a real Iceberg table.
    qt = unique_table("identity__state_quoted_sort")
    bootstrap_state_table(spark, qt, _schema(("amount", "string")))
    assert spark.catalog.tableExists(qt)
