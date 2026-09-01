"""Idempotent per-type fact/state + marker Iceberg table creation, plus the
F-10 `table-classes.json` inventory emission (`make bootstrap-record-tables`).
LLD 007.1 §6.1-§6.5, F-3, F-10.

Mirrors `create_admission_tables.py`'s idiom exactly (idempotent, driver-side
Spark SQL DDL, run by the deploy principal, never the job role; `main()`
excluded from unit/integration coverage, same documented real-AWS exclusion
`create_run_ledger.main`/`create_admission_tables.main` already carry) with
one addition §6.5 asks of THIS bootstrap alone: after provisioning every
record-side table, it also emits the content-pinned table->class inventory
(§6.5 step 6, F-10) beside the deployed spec.

**Column-set construction (§6.1/§6.2's "one DDL builder, two property
sets").** `fact_and_state_columns_ordered` is the ONE builder both
`bootstrap_fact_table`/`bootstrap_state_table` call: framework fact-stamp
columns first (iterating `core.record.FACT_STAMP_TYPES` in its own declared
order -- never re-enumerated, per that module's own docstring), then the
type's declared columns (contract order, ALL nullable -- 006.1 P-1: no
per-column nullable claims in Phase 1). Only the TBLPROPERTIES differ between
the two callers (partition spec, merge-mode/isolation-level, table-class).

**Evolution diff reuse.** `RawSchemaDiff`/`diff_raw_schema`/
`describe_raw_diff` from `create_admission_tables.py` are imported and reused
AS-IS, unmodified: their algorithm (name-and-type-keyed, order-insensitive;
a missing DECLARED column is additive `ADD COLUMNS`; any other diff --
missing/wrong framework column, type mismatch, or an unexplained column -- is
a loud, all-findings-named `ValueError`, never a partial apply) is EXACTLY
§6.5 step 2/3's rule for fact/state tables too, despite the "Raw"-prefixed
names -- the "raw" in those names refers to the shape of the diff
(name+type keyed pairs), not to admission's raw table specifically. The
column-definition renderer (`ColumnDDL`/`render_column_def`) and the
idempotent re-stamp helper (`render_set_table_class_sql`) are reused for the
identical reason. Everything ELSE (the top-level `CREATE TABLE` renderer,
`_fetch_spec`/`_parse_spec`/`_catalog_conf`/`_build_session`, `main()`) is a
small, deliberate, independent copy -- the SAME "mirror the shape, don't
cross-import the CLI plumbing" choice `create_admission_tables.py`'s own
docstring already makes about `create_run_ledger.py`, applied symmetrically
here to keep the two per-pipeline bootstrap scripts independently runnable
and reviewable.

**The marker table (§6.3, §6.5 step 4).** `MARKER_COLUMNS` is a VERSIONLESS
CONSTANT (framework-owned shape, D-7's "quarantine's pattern" restated) --
`bootstrap_markers_table`'s "present" branch asserts EXACT equality (name,
type, nullability, AND order), mirroring `bootstrap_quarantine_table`
exactly, never the additive-evolution rule fact/state tables get.

**§6.5 step 3's assert-and-repair, restated precisely.** A present state
table whose `write.merge.mode`/`write.merge.isolation-level` TBLPROPERTIES
have drifted from `merge-on-read`/`serializable` is REPAIRED via `ALTER
TABLE ... SET TBLPROPERTIES` (kernel-verified this bead: metadata-only,
never bumps the snapshot log -- the SAME "no-op DDL" invariant
`render_set_table_class_sql`'s own docstring already established for
`conveyer.table-class`) with a loud `logger.warning` naming the drift --
never tolerated silently, never raised as a fatal defect either (errata #9's
failure shape is *silent* no-op-signal loss; an unrepaired drift would
reinstate exactly that, so "assert-and-repair" is deliberately NOT a raise,
unlike every other non-additive schema diff in this module).

**Fact-stamp / declared-column DDL type mapping, kernel-verified this
bead.** `core.record.FACT_STAMP_TYPES`'s own `.type` string ("string" /
"timestamptz") is an ICEBERG type name for documentation purposes (matching
007.1 §6.1's literal prose), NOT a literal Spark SQL DDL keyword -- Spark's
parser REJECTS a bare `TIMESTAMPTZ` column-type token outright
(`UNSUPPORTED_DATATYPE`). Spark's own `TIMESTAMP` keyword already maps to
Iceberg's timestamptz (instant/UTC-normalized) logical type, and
`simpleString()` reports it back as the single string `"timestamp"` --
`_stamp_ddl_type` performs this translation once. Similarly, a declared
fact-column of kind `"long"` renders as DDL keyword `bigint` (not the bare
literal `long`, though Spark's parser accepts either spelling) so create-time
rendering and post-create `simpleString()` readback always agree, matching
`create_admission_tables.py`'s own `row_index` precedent (`ColumnDDL(...,
"bigint", ...)`, never `"long"`).

**Prefix assertion (§6.5 step 1) uses `core.naming.table_slug`, NOT
`core.naming.slug` -- see `table_slug`'s own docstring for the full,
kernel-verified reasoning** (in short: `slug()`'s `--`-joined form can never
legally be a table-name component -- `check_qualified_table`'s own
`_IDENTIFIER_RE` already forbids it, and an UNQUOTED `--` inside a literal
Spark SQL identifier is parsed as a line-comment marker, silently truncating
the rest of the statement -- kernel-verified, not a theoretical concern).
`naming.markers_table` already derives the marker table's own name via
`table_slug` for the identical reason, so this assertion and the marker
table's own name are provably prefix-consistent by construction, never by
two independently-typed prefix strings that could drift apart.
"""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING

from spine import observability
from spine.bootstrap.create_admission_tables import (
    ColumnDDL,
    RawSchemaDiff,
    describe_raw_diff,
    diff_raw_schema,
    render_add_columns_sql,
    render_column_def,
    render_set_table_class_sql,
)
from spine.core import naming, record
from spine.core.model import FactSchemaModel, PipelineSpecModel, parse_pipeline_spec_yaml

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

logger = logging.getLogger(__name__)

# Re-exported for callers that only need the sentinel value (e.g. a future
# commit-stage marker writer) without importing all of `core.naming`.
COMMIT_COMPLETION_SENTINEL = naming.COMMIT_COMPLETION_SENTINEL

__all__ = [
    "COMMIT_COMPLETION_SENTINEL",
    "MARKER_COLUMNS",
    "RawSchemaDiff",
    "assert_table_prefixes",
    "bootstrap_fact_table",
    "bootstrap_markers_table",
    "bootstrap_record_tables",
    "bootstrap_state_table",
    "build_table_class_inventory",
    "fact_and_state_columns_ordered",
    "main",
    "render_fact_create_table_sql",
    "render_marker_create_table_sql",
    "render_state_create_table_sql",
    "render_table_class_inventory_json",
]


# --- fact-stamp / declared-column DDL type mapping (kernel-verified) --------

_STAMP_DDL_TYPES: Mapping[str, str] = MappingProxyType(
    {"string": "string", "timestamptz": "timestamp"}
)


def _stamp_ddl_type(type_str: str) -> str:
    return _STAMP_DDL_TYPES[type_str]


# Total over `core.model.FACT_COLUMN_TYPE_RE`'s exact seven kinds --
# `decimal(p,s)` is handled separately below (its DDL spelling IS the
# declared string, verbatim: Spark's own `simpleString()` readback of a
# `DECIMAL(p,s)` column is the identical lowercase `decimal(p,s)` form,
# kernel-verified).
_FACT_COLUMN_DDL_TYPES: Mapping[str, str] = MappingProxyType(
    {
        "string": "string",
        "int": "int",
        "long": "bigint",
        "bool": "boolean",
        "date": "date",
        "timestamp": "timestamp",
    }
)


def _fact_column_ddl_type(type_str: str) -> str:
    if type_str.startswith("decimal("):
        return type_str
    return _FACT_COLUMN_DDL_TYPES[type_str]


# Drift guard between `record.py`'s two independently-declared constants
# (mirrors `create_admission_tables.py`'s own `_RAW_DDL_COLUMN_NAMES` assert
# against `core.model.FRAMEWORK_RAW_COLUMNS`) -- both live in `record.py`
# itself (B6), but THIS module is the first consumer whose correctness
# depends on their agreement (iterating one for names+order, the other for
# types), so the guard lives at the consuming site.
assert set(record.FACT_STAMP_TYPES) == record.FACT_STAMP_COLUMNS, (
    "core.record.FACT_STAMP_TYPES/FACT_STAMP_COLUMNS drifted apart"
)


def fact_and_state_columns_ordered(schema: FactSchemaModel) -> tuple[ColumnDDL, ...]:
    """§6.1/§6.2's ONE DDL builder: framework stamps (constant order, never
    re-enumerated) then declared columns (contract order, all nullable,
    006.1 P-1) -- consumed verbatim by both `bootstrap_fact_table` and
    `bootstrap_state_table` ("Column set = §6.1's, verbatim", §6.2)."""
    stamps = tuple(
        ColumnDDL(name, _stamp_ddl_type(stamp.type), not stamp.nullable)
        for name, stamp in record.FACT_STAMP_TYPES.items()
    )
    declared = tuple(
        ColumnDDL(col.name, _fact_column_ddl_type(col.type), False) for col in schema.columns
    )
    return stamps + declared


def _render_create_table_sql(
    qualified_table: str,
    columns: tuple[ColumnDDL, ...],
    *,
    partition_by: tuple[str, ...],
    table_class: str,
    extra_properties: Mapping[str, str] = MappingProxyType({}),
) -> str:
    """The record-side `CREATE TABLE` renderer -- same grammar as
    `create_admission_tables.render_create_table_sql` (bare `PARTITIONED BY
    (...)` column references are `identity(...)` transforms, A-8; `format-
    version`/`conveyer.table-class` always stamped), extended with
    `extra_properties` for the state table's merge-mode/isolation-level
    pair (§6.2) -- a new function, not a signature change to admission's
    already-tested one."""
    cols_sql = ", ".join(render_column_def(c) for c in columns)
    partition_sql = f" PARTITIONED BY ({', '.join(partition_by)})" if partition_by else ""
    properties = {"format-version": "2", "conveyer.table-class": table_class, **extra_properties}
    props_sql = ", ".join(f"'{k}'='{v}'" for k, v in properties.items())
    return (
        f"CREATE TABLE {qualified_table} ({cols_sql}) USING iceberg{partition_sql} "
        f"TBLPROPERTIES ({props_sql})"
    )


def _actual_columns_by_name(spark: SparkSession, qualified_table: str) -> dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in spark.table(qualified_table).schema.fields}


def _actual_columns_ordered(spark: SparkSession, qualified_table: str) -> tuple[ColumnDDL, ...]:
    return tuple(
        ColumnDDL(f.name, f.dataType.simpleString(), not f.nullable)
        for f in spark.table(qualified_table).schema.fields
    )


# --- §6.1 fact table ----------------------------------------------------------


def render_fact_create_table_sql(qualified_table: str, schema: FactSchemaModel) -> str:
    return _render_create_table_sql(
        qualified_table,
        fact_and_state_columns_ordered(schema),
        partition_by=("batch_id",),
        table_class="facts",
    )


def bootstrap_fact_table(
    spark: SparkSession, qualified_table: str, schema: FactSchemaModel
) -> None:
    """Idempotent: absent -> `CREATE TABLE` per §6.1 (`identity(batch_id)`,
    no sort order, F-3); present -> the SAME additive-only diff discipline
    `bootstrap_raw_table` uses (module docstring), then unconditional
    `conveyer.table-class` re-stamp (F-10, metadata-only, never bumps the
    snapshot log)."""
    expected = fact_and_state_columns_ordered(schema)
    if not spark.catalog.tableExists(qualified_table):
        spark.sql(
            _render_create_table_sql(
                qualified_table, expected, partition_by=("batch_id",), table_class="facts"
            )
        )
        return
    actual = _actual_columns_by_name(spark, qualified_table)
    declared_names = frozenset(c.name for c in schema.columns)
    diff = diff_raw_schema(expected, actual, declared_names)
    if not diff.is_clean:
        raise ValueError(
            f"fact table {qualified_table} schema diff is not additive-only [DC-11]: "
            f"{describe_raw_diff(diff)}"
        )
    if diff.missing_declared:
        to_add = tuple(c for c in expected if c.name in diff.missing_declared)
        spark.sql(render_add_columns_sql(qualified_table, to_add))
    spark.sql(render_set_table_class_sql(qualified_table, "facts"))


# --- §6.2 state table ---------------------------------------------------------

_STATE_MERGE_PROPERTIES: Mapping[str, str] = MappingProxyType(
    {
        "write.merge.mode": "merge-on-read",
        "write.merge.isolation-level": "serializable",
    }
)


def render_state_create_table_sql(qualified_table: str, schema: FactSchemaModel) -> str:
    return _render_create_table_sql(
        qualified_table,
        fact_and_state_columns_ordered(schema),
        partition_by=(),  # unpartitioned, F-3/§6.4's own-merits state ruling
        table_class="state",
        extra_properties=_STATE_MERGE_PROPERTIES,
    )


def _current_tblproperties(spark: SparkSession, qualified_table: str) -> dict[str, str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {qualified_table}").collect()
    return {r["key"]: r["value"] for r in rows}


def _assert_and_repair_state_properties(spark: SparkSession, qualified_table: str) -> None:
    """§6.5 step 3 / errata #9 / [DC2-3]: `write.merge.mode`/`write.merge.
    isolation-level` are asserted-and-repaired on every 'present' bootstrap
    call -- a drifted property is ALTERed back (metadata-only, kernel-
    verified: never bumps the snapshot log) with a loud WARNING naming the
    drift. Deliberately NOT a raise, unlike every other non-additive schema
    diff in this module: errata #9's failure shape is *silent* no-op-signal
    loss, exactly what an unrepaired drift would reinstate -- "never
    tolerated and never failed-on" (this bead's own brief, verbatim)."""
    current = _current_tblproperties(spark, qualified_table)
    drifted = {k: v for k, v in _STATE_MERGE_PROPERTIES.items() if current.get(k) != v}
    if not drifted:
        return
    logger.warning(
        "state table %s: repairing drifted merge propert(y/ies) %s -- errata #9 / [DC2-3], "
        "asserted-and-repaired, never tolerated, never failed-on",
        qualified_table,
        {k: (current.get(k), v) for k, v in drifted.items()},
    )
    props_sql = ", ".join(f"'{k}'='{v}'" for k, v in _STATE_MERGE_PROPERTIES.items())
    spark.sql(f"ALTER TABLE {qualified_table} SET TBLPROPERTIES ({props_sql})")


def bootstrap_state_table(
    spark: SparkSession, qualified_table: str, schema: FactSchemaModel
) -> None:
    """Idempotent: absent -> `CREATE TABLE` per §6.2 (unpartitioned,
    merge-on-read + serializable isolation baked in at CREATE) plus `WRITE
    ORDERED BY <domain_id_col>` (§6.4's declared sort order, kernel-verified
    syntax -- set once, at CREATE only, never repeated on a later present-
    branch call, matching "Sort order per §6.4 at CREATE" literally); present
    -> the same additive-only column diff as fact tables, THEN the merge-
    property assert-and-repair, THEN the unconditional class re-stamp."""
    expected = fact_and_state_columns_ordered(schema)
    if not spark.catalog.tableExists(qualified_table):
        spark.sql(
            _render_create_table_sql(
                qualified_table,
                expected,
                partition_by=(),
                table_class="state",
                extra_properties=_STATE_MERGE_PROPERTIES,
            )
        )
        spark.sql(f"ALTER TABLE {qualified_table} WRITE ORDERED BY {schema.domain_id_col}")
        return
    actual = _actual_columns_by_name(spark, qualified_table)
    declared_names = frozenset(c.name for c in schema.columns)
    diff = diff_raw_schema(expected, actual, declared_names)
    if not diff.is_clean:
        raise ValueError(
            f"state table {qualified_table} schema diff is not additive-only [DC-11]: "
            f"{describe_raw_diff(diff)}"
        )
    if diff.missing_declared:
        to_add = tuple(c for c in expected if c.name in diff.missing_declared)
        spark.sql(render_add_columns_sql(qualified_table, to_add))
    _assert_and_repair_state_properties(spark, qualified_table)
    spark.sql(render_set_table_class_sql(qualified_table, "state"))


# --- §6.3 marker table (versionless constant, D-7's "quarantine's pattern") --

MARKER_COLUMNS: tuple[ColumnDDL, ...] = (
    ColumnDDL("batch_id", "string", True),
    ColumnDDL("feed_id", "string", True),
    ColumnDDL("stage", "string", True),
    ColumnDDL("table_name", "string", True),
    ColumnDDL("snapshot_id", "bigint", False),  # NULL on every Phase-1 row, §6.3's own resolution
    ColumnDDL("delivery_key", "string", True),
    ColumnDDL("delivery_content_hash", "string", True),
    ColumnDDL("received_at", "timestamp", True),
    ColumnDDL("committed_at", "timestamp", True),
)


def render_marker_create_table_sql(qualified_table: str) -> str:
    return _render_create_table_sql(
        qualified_table, MARKER_COLUMNS, partition_by=("batch_id",), table_class="marker"
    )


def bootstrap_markers_table(spark: SparkSession, qualified_table: str) -> None:
    """Idempotent: absent -> `CREATE TABLE` from the constant `MARKER_
    COLUMNS`; present -> assert EXACT equality (name, type, nullability, AND
    order) -- no evolution path exists for this table (§6.5 step 4, mirrors
    `bootstrap_quarantine_table` exactly). Present branch re-stamps
    `conveyer.table-class='marker'` AFTER the exact-equality assertion
    passes, unconditionally."""
    if not spark.catalog.tableExists(qualified_table):
        spark.sql(render_marker_create_table_sql(qualified_table))
        return
    actual = _actual_columns_ordered(spark, qualified_table)
    if actual != MARKER_COLUMNS:
        raise ValueError(
            f"marker table {qualified_table} schema drift (constant, versionless by design, "
            f"§6.3): expected {MARKER_COLUMNS!r}, actual {actual!r}"
        )
    spark.sql(render_set_table_class_sql(qualified_table, "marker"))


# --- §6.5 step 1: prefix assertion -------------------------------------------


def assert_table_prefixes(spec: PipelineSpecModel) -> None:
    """§6.5 step 1: every declared `fact_table`/`state_table` (and the
    derived `<slug>__markers`) must begin `<slug>__` -- loud failure
    otherwise, naming every offending table at once (the `describe_raw_diff`
    precedent), never just the first. Uses `naming.table_slug`, matching
    `naming.markers_table`'s own derivation exactly (module docstring) --
    the marker table's own name and this assertion can never disagree on
    what "the slug" means."""
    prefix = f"{naming.table_slug(spec.pipeline)}__"
    violations: list[str] = []
    for name, fact_type in spec.fact_types.items():
        for field, table in (
            ("fact_table", fact_type.fact_table),
            ("state_table", fact_type.state_table),
        ):
            if not table.rsplit(".", 1)[-1].startswith(prefix):
                violations.append(f"fact_types.{name}.{field}={table!r}")
    marker = naming.markers_table(spec.raw_table, spec.pipeline)
    if not marker.rsplit(".", 1)[-1].startswith(prefix):
        violations.append(f"markers={marker!r}")
    if violations:
        raise ValueError(
            f"table(s) do not begin with the required prefix {prefix!r} (I-21/S-15's "
            f"own-prefix grants -- an out-of-prefix table is unprovisionable, §6.5 step 1): "
            f"{sorted(violations)!r}"
        )


# --- orchestration: every record table for one pipeline spec -----------------


def _qualified(catalog: str, table: str) -> str:
    return f"{catalog}.{table}"


def bootstrap_record_tables(spark: SparkSession, catalog: str, spec: PipelineSpecModel) -> None:
    """§6.5 steps 1-5: prefix assertion, then per declared fact type (in
    `spec.fact_types`' own declared, deploy-pinned order) its fact + state
    tables, then the one marker table."""
    assert_table_prefixes(spec)
    for fact_type in spec.fact_types.values():
        bootstrap_fact_table(spark, _qualified(catalog, fact_type.fact_table), fact_type.schema_)
        bootstrap_state_table(spark, _qualified(catalog, fact_type.state_table), fact_type.schema_)
    marker_table = naming.markers_table(spec.raw_table, spec.pipeline)
    bootstrap_markers_table(spark, _qualified(catalog, marker_table))


# --- §6.5 step 6 (F-10): table-classes.json inventory ------------------------


def build_table_class_inventory(spec: PipelineSpecModel) -> dict[str, str]:
    """Pure (no Spark needed): every table THIS pipeline's full deploy
    provisions -- admission's raw/quarantine tables (bootstrapped
    separately, `create_admission_tables.py`/`make bootstrap-admission`;
    this function only needs their DECLARED NAMES, already in `spec`, never
    a second bootstrap call) plus every declared fact type's fact/state
    tables plus the one derived marker table. F-10: "the deploy step that
    renders the S-15 grants also emits the table->class inventory ... for
    every table this deploy provisions (admission's tables included)"."""
    inventory: dict[str, str] = {
        spec.raw_table: "raw",
        spec.quarantine_table: "quarantine",
    }
    for fact_type in spec.fact_types.values():
        inventory[fact_type.fact_table] = "facts"
        inventory[fact_type.state_table] = "state"
    inventory[naming.markers_table(spec.raw_table, spec.pipeline)] = "marker"
    return inventory


def render_table_class_inventory_json(inventory: Mapping[str, str]) -> str:
    """Deterministic (sorted-key) JSON rendering -- a content-pinned deploy
    artifact (I-23 idiom): byte-stable across repeated deploys of an
    unchanged spec."""
    return json.dumps(dict(sorted(inventory.items())), indent=2, sort_keys=True) + "\n"


# --- `main()`: real-AWS-Glue-Catalog entrypoint, NOT covered by tests/unit / --
# tests/integration (same documented real-AWS exclusion as `create_admission_
# tables.main`/`create_run_ledger.main`) ---------------------------------------


def _fetch_spec(uri: str) -> str:
    """A deliberate, small copy of `create_admission_tables.py::_fetch_spec`
    (itself a copy of `entrypoints/glue_main.py::default_fetch_spec`) --
    see that module's own docstring for why this isn't a shared import."""
    if uri.startswith("s3://"):
        import boto3  # type: ignore[import-untyped]  # local import -- s3:// branch only

        bucket, _, key = uri[len("s3://") :].partition("/")
        client = boto3.client("s3")
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        return body.read().decode("utf-8")
    path = uri[len("file://") :] if uri.startswith("file://") else uri
    from pathlib import Path  # local import -- mirrors the boto3 branch's lazy-import symmetry

    return Path(path).read_text()


def _write_table_class_inventory(uri: str, content: str) -> None:
    """The WRITE-side mirror of `_fetch_spec`'s read ladder -- `s3://` via
    boto3 `PutObject`; `file://` (or a bare local path) via a plain write."""
    if uri.startswith("s3://"):
        import boto3  # type: ignore[import-untyped]  # local import -- s3:// branch only

        bucket, _, key = uri[len("s3://") :].partition("/")
        client = boto3.client("s3")
        client.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
        return
    path = uri[len("file://") :] if uri.startswith("file://") else uri
    from pathlib import Path  # local import -- mirrors the boto3 branch's lazy-import symmetry

    Path(path).write_text(content)


def _parse_spec(spec_text: str) -> PipelineSpecModel:
    return parse_pipeline_spec_yaml(spec_text)


def _catalog_conf(catalog: str) -> dict[str, str]:
    """Same shape as `create_admission_tables.py::_catalog_conf` -- always
    `type=glue` (no `catalog_kind` flag; `main()` is the real-AWS path)."""
    return {
        "spark.sql.extensions": _ICEBERG_EXTENSIONS,
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.type": "glue",
    }


def _build_session(env: str, catalog: str) -> SparkSession:
    from pyspark.sql import SparkSession  # local import -- keeps this module importable

    # without a live JVM even being reachable (mirrors `create_admission_tables._build_session`).
    builder = SparkSession.builder.appName(f"conveyer-spine-bootstrap-record-{env}")
    for key, value in _catalog_conf(catalog).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def main() -> None:
    observability.install_json_handler()  # first, before any logging (nvh.47 precedent)

    parser = argparse.ArgumentParser(
        description=(
            "Idempotently create a pipeline's fact/state/marker Iceberg tables and emit "
            "table-classes.json beside the deployed spec (§6.5)."
        )
    )
    parser.add_argument("--spec-uri", required=True, help="Pipeline spec URI (s3:// or file://).")
    parser.add_argument("--catalog", default="spine_cat", help="Spark/Iceberg catalog name.")
    parser.add_argument("--env", required=True, help="Deploy environment (Spark app-name suffix).")
    args = parser.parse_args()

    spec = _parse_spec(_fetch_spec(args.spec_uri))
    spark = _build_session(args.env, args.catalog)
    bootstrap_record_tables(spark, args.catalog, spec)
    inventory = build_table_class_inventory(spec)
    inventory_uri = naming.table_class_inventory_uri(args.spec_uri)
    _write_table_class_inventory(inventory_uri, render_table_class_inventory_json(inventory))
    print(  # noqa: T201
        f"record tables ready for {spec.pipeline!r}: {len(inventory)} table(s), "
        f"inventory written to {inventory_uri}"
    )


if __name__ == "__main__":
    main()
