"""Idempotent raw/quarantine Iceberg table creation (`make bootstrap-admission`). LLD 005.1 §4.4.

Mirrors `create_run_ledger.py`'s shape (idempotent, driver-side, `main()`
excluded from unit coverage — see that module's own docstring for the
precedent) with one deliberate mechanism change: `create_run_ledger` calls
pyiceberg's `Catalog.create_table_if_not_exists` directly; this module
issues **driver-side Spark SQL DDL** (`CREATE TABLE`/`ALTER TABLE ... ADD
COLUMNS`) against a live `SparkSession`, per §4.4's own text. The reason is
additive evolution: raw's schema is a per-pipeline function of
`raw_contract` (§4.1) that must grow columns over a pipeline's lifetime
(D-11 promotion) without ever dropping/retyping/reordering existing ones —
Spark SQL's `ALTER TABLE ... ADD COLUMNS` is the natural, reviewable
expression of exactly that constraint; pyiceberg's schema-update API would
work too but every other table-shaping DDL in this package already goes
through `spark.sql(...)` (`effects/spark.py::render_merge`,
`tests/integration/scenario_helpers.py`'s own `CREATE TABLE` calls) — one
mechanism, not two.

**Two functions, two evolution rules (§4.4 steps 2-3):**

* `bootstrap_raw_table` — schema is `raw_columns_ordered(contract)`: the
  ten fixed framework columns (§4.1's listed order) with the *contract's*
  declared columns spliced in before `extras` on first creation only
  [DC-11]. On a second-or-later call the diff against the live table is
  **name-and-type-keyed, order-insensitive** (an order-sensitive diff would
  fail forever after the first promotion, since `ADD COLUMN` always appends
  after whatever the table's current last column is — verified empirically,
  this bead: promoting a column lands it physically after `extras`, not
  where §4.1's "declared columns before extras" table implies for a fresh
  create). Only a **missing declared column** is additive (`ALTER TABLE ...
  ADD COLUMNS (col STRING)`); a type mismatch, a missing/wrong framework
  column, or any column present in the table but absent from
  framework+declared+`extras` is a loud `ValueError` naming every such
  finding — never a partial, silently-mixed apply (all additive columns are
  added together only when NO non-additive finding exists in the same
  diff).
* `bootstrap_quarantine_table` — the schema is `QUARANTINE_COLUMNS`, a
  **constant** (§4.2: "independent of any pipeline's columns", D-7): no
  evolution path exists, so a second-or-later call asserts **exact**
  equality (name, type, nullability, AND order) against the live table and
  raises loudly on any diff whatsoever — "a diff means a framework upgrade
  forgot its own migration note" (§4.4).

**Test-parity swap — LANDED (bead conveyer-azr.19, n3-admission-cut; this
docstring corrected, critique nit b, bead conveyer-azr.30, since the
paragraph below described the PRE-swap state long after the swap itself
shipped — bead-narrative docstring rot, a recurring vector in this repo):**
§4.4's last paragraph called for `tests/integration/scenario_helpers.py` to
delete its hand-rolled `ROW_DDL` and call this module's DDL builders
instead ("one authored schema, both substrates") once the stages
(`stages/land.py`, `frames/quarantine.py`) cut over from the exemplar's
provisional 9-column shape to the real §4.1/§4.2 admission shape. Both
happened together, in n3-admission-cut: `scenario_helpers.py::
create_raw_table`/`create_quarantine_table` now call THIS module's
`render_raw_create_table_sql`/`render_quarantine_create_table_sql`
directly (its own module docstring records the swap by name) — the SAME
functions `bootstrap-admission` issues in production. `raw_columns_
ordered`, `render_raw_create_table_sql`, `QUARANTINE_COLUMNS`, and
`render_quarantine_create_table_sql` remain exposed at module level for
exactly this reuse; "one authored schema, both substrates" is now true of
both callers, not just this module's own tests (which continue to exercise
the identical functions directly against the local Iceberg test catalog,
A-14).

**CLI shape** (§4.4's own pseudocode, verbatim): `--spec-uri` (required,
any pipeline's spec — no I-23 self-scoping here: bootstrap runs as the
*deploy principal* naming any pipeline's spec by hand, never the job role
reading only its own, §4.5), `--catalog` (default `spine_cat`), `--env`
(required — carried into the Spark app name for log correlation, same
shape as `glue_main._build_session`'s `appName(f"conveyer-spine-
{config.env}")`). Deliberately NOT the full `RunnerConfig`/`from_args`
argv contract `create_run_ledger.py` uses (that module's own docstring
records the resulting "recorded gap" of an under-specified one-shot
invocation) — this bootstrap needs only these three values, so it parses
them with its own small `argparse` surface rather than pulling in the
Glue-job argv shape a one-shot deploy invocation has no natural reason to
supply. `main()`'s spec fetch (`_fetch_spec`) and parse (`_parse_spec`) are
small, deliberate copies of `entrypoints/glue_main.py`'s
`default_fetch_spec`/`_parse_spec` (same `s3://` boto3 / `file://` local
read ladder, same `yaml.safe_load` + `PipelineSpecModel(**...)` parse) —
"the same `PipelineSpecModel` path as the entrypoint" (this bead's own task
framing) means the same PARSER (one grammar: `yaml.safe_load` +
`PipelineSpecModel`), not a shared function reference; importing from
`entrypoints/glue_main.py` would drag this deploy-time CLI's import graph
through `spine.binding`/`spine.effects.build`/`spine.run`, none of which
bootstrap needs. `main()` always builds a `glue`-type catalog session (no
`catalog_kind` flag, matching §4.4's pseudocode exactly) — it is, like
`create_run_ledger.main`, NOT covered by `tests/unit`/`tests/integration`
(the same documented real-AWS exclusion); `bootstrap_raw_table`/
`bootstrap_quarantine_table` themselves ARE tested directly against the
local Iceberg test catalog (`tests/integration/test_create_admission_
tables.py`, A-14).

**Namespace creation, deliberately NOT mirrored from `create_run_ledger.py`
here:** that module always calls `catalog.create_namespace_if_not_exists`
(documented there as a harmless no-op against an already-Terraform-created
Glue database, needed only for the test `SqlCatalog`). This module issues
no equivalent `CREATE DATABASE`/`CREATE NAMESPACE` statement: §4.4/D-7's
split for THIS LLD is explicit — "Terraform creates the Glue database; the
bootstrap script owns the table" — and unlike the run ledger's namespace
call (whose own docstring frames it as convenience, not a load-bearing
part of that split), adding one here would blur that line. Empirically
verified (this bead, against the local Hadoop catalog) that `CREATE TABLE
catalog.newdb.newtable (...)` auto-creates the namespace directory with no
separate statement needed — the same reason `scenario_helpers.py`'s own
`create_raw_table`/`create_quarantine_table` never issue one either — so
the test substrate needs no accommodation for this omission.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from spine import observability
from spine.core.model import (
    FRAMEWORK_RAW_COLUMNS,
    PipelineSpecModel,
    RawContractModel,
    parse_pipeline_spec_yaml,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


# --- pure: one column-definition value + rendering --------------------------


@dataclass(frozen=True)
class ColumnDDL:
    name: str
    spark_type: str  # Spark's own `simpleString()` form, e.g. "string", "map<string,string>"
    not_null: bool


def render_column_def(col: ColumnDDL) -> str:
    suffix = " NOT NULL" if col.not_null else ""
    return f"{col.name} {col.spark_type.upper()}{suffix}"


def render_create_table_sql(
    qualified_table: str,
    columns: tuple[ColumnDDL, ...],
    *,
    partition_by: tuple[str, ...],
    table_class: str,
) -> str:
    """Iceberg v2, `partition_by` columns as plain `PARTITIONED BY (...)`
    references — Spark/Iceberg treats a bare column reference as an
    `identity(...)` transform (A-8; empirically verified this bead: the
    resulting table's `DESCRIBE TABLE EXTENDED` partition struct names the
    column directly, no transform wrapper). `table_class` stamps
    `conveyer.table-class` at CREATE (006.1 §16.3 item 5 -- the
    provisioning-time class marker C4/F-10 read at bind, §5.2/007.1 F-10:
    provenance, never protection)."""
    cols_sql = ", ".join(render_column_def(c) for c in columns)
    partition_sql = f" PARTITIONED BY ({', '.join(partition_by)})" if partition_by else ""
    return (
        f"CREATE TABLE {qualified_table} ({cols_sql}) USING iceberg{partition_sql} "
        f"TBLPROPERTIES ('format-version'='2', 'conveyer.table-class'='{table_class}')"
    )


def render_add_columns_sql(qualified_table: str, columns: tuple[ColumnDDL, ...]) -> str:
    cols_sql = ", ".join(render_column_def(c) for c in columns)
    return f"ALTER TABLE {qualified_table} ADD COLUMNS ({cols_sql})"


def render_set_table_class_sql(qualified_table: str, table_class: str) -> str:
    """Idempotent re-stamp for a table that ALREADY exists (§4.4's
    "present" branch, both raw and quarantine) -- `ALTER TABLE ... SET
    TBLPROPERTIES` is metadata-only (verified this bead: never bumps the
    table's snapshot log, `A-14`'s own no-op-DDL invariant stays intact)
    and safe to re-run unconditionally, which is also this bead's DEV
    BACKFILL RUNBOOK for a table provisioned before this stamping landed:
    re-running `bootstrap-admission` against an already-deployed pipeline's
    raw/quarantine tables backfills `conveyer.table-class` with no separate
    script or manual `ALTER TABLE` -- the same idempotent call every other
    redeploy already makes."""
    return (
        f"ALTER TABLE {qualified_table} SET TBLPROPERTIES ('conveyer.table-class'='{table_class}')"
    )


# --- §4.1 raw table -----------------------------------------------------------

# Framework columns, listed order, up to (not including) the contract's own
# declared columns -- `extras` is the fixed tail (`_RAW_EXTRAS_TAIL` below).
# Types/nullability pinned verbatim from §4.1's table.
_RAW_FRAMEWORK_HEAD: tuple[ColumnDDL, ...] = (
    ColumnDDL("batch_id", "string", True),
    ColumnDDL("delivery_id", "string", True),
    ColumnDDL("feed_id", "string", True),
    ColumnDDL("received_at", "timestamp", True),
    ColumnDDL("source_uri", "string", True),
    ColumnDDL("object_seq", "int", True),
    ColumnDDL("row_index", "bigint", True),
    ColumnDDL("read_spec_version", "string", True),
    ColumnDDL("malformed_text", "string", False),
)
_RAW_EXTRAS_TAIL = ColumnDDL("extras", "map<string,string>", True)

# `core.model.FRAMEWORK_RAW_COLUMNS` is the single source of the ten
# framework-column NAMES (D-9's disjointness check reuses it too); this
# module owns their TYPES/nullability (raw DDL is this module's concern
# alone, D-5). Asserted equal at import time so a future framework-column
# addition/removal in `core/model.py` cannot silently drift from this
# module's own DDL shape without failing loudly, immediately.
_RAW_DDL_COLUMN_NAMES = {c.name for c in _RAW_FRAMEWORK_HEAD} | {_RAW_EXTRAS_TAIL.name}
assert _RAW_DDL_COLUMN_NAMES == set(FRAMEWORK_RAW_COLUMNS), (
    "core.model.FRAMEWORK_RAW_COLUMNS drifted from this module's own raw-DDL column set"
)


def raw_columns_ordered(contract: RawContractModel) -> tuple[ColumnDDL, ...]:
    """§4.1 DDL order — **initial creation only** [DC-11]: framework head,
    then declared columns (contract order, always `STRING`/nullable — raw
    columns are never typed, D-5), then `extras` last."""
    declared = tuple(ColumnDDL(c.name, "string", False) for c in contract.columns)
    return _RAW_FRAMEWORK_HEAD + declared + (_RAW_EXTRAS_TAIL,)


def render_raw_create_table_sql(qualified_table: str, contract: RawContractModel) -> str:
    return render_create_table_sql(
        qualified_table,
        raw_columns_ordered(contract),
        partition_by=("batch_id",),
        table_class="raw",
    )


@dataclass(frozen=True)
class RawSchemaDiff:
    missing_declared: tuple[str, ...]  # additive: ALTER TABLE ... ADD COLUMNS
    missing_framework: tuple[str, ...]  # loud failure
    type_mismatches: tuple[tuple[str, str, str], ...]  # (name, expected, actual); loud failure
    unexplained: tuple[str, ...]  # in table, not framework/declared/extras; loud failure

    @property
    def is_clean(self) -> bool:
        """True iff every finding is additive (only `missing_declared`, if
        anything) — the only diff shape `bootstrap_raw_table` may apply
        without raising."""
        return not (self.missing_framework or self.type_mismatches or self.unexplained)


def diff_raw_schema(
    expected: tuple[ColumnDDL, ...], actual: Mapping[str, str], declared_names: frozenset[str]
) -> RawSchemaDiff:
    """Name-and-type-keyed, order-insensitive [DC-11] — nullability is
    deliberately not part of this comparison (§4.4 names the diff axes as
    "name-and-type-keyed"; the quarantine table's separate `assert-exact-
    equality` check below is where nullability/order both matter)."""
    expected_by_name = {c.name: c.spark_type for c in expected}
    missing_declared = tuple(
        sorted(n for n in expected_by_name if n not in actual and n in declared_names)
    )
    missing_framework = tuple(
        sorted(n for n in expected_by_name if n not in actual and n not in declared_names)
    )
    type_mismatches = tuple(
        sorted(
            (n, expected_by_name[n], actual[n])
            for n in expected_by_name
            if n in actual and actual[n] != expected_by_name[n]
        )
    )
    unexplained = tuple(sorted(n for n in actual if n not in expected_by_name))
    return RawSchemaDiff(missing_declared, missing_framework, type_mismatches, unexplained)


def describe_raw_diff(diff: RawSchemaDiff) -> str:
    """Renders every non-additive finding into one message — the loud
    failure names ALL of them at once, not just the first (§4.4: "any other
    diff ... is a loud failure naming the diff")."""
    parts = []
    if diff.missing_framework:
        parts.append(f"missing framework column(s): {list(diff.missing_framework)!r}")
    if diff.type_mismatches:
        parts.append(
            "type mismatch(es): "
            + ", ".join(f"{n}: expected {e!r}, actual {a!r}" for n, e, a in diff.type_mismatches)
        )
    if diff.unexplained:
        parts.append(
            "column(s) present in table but neither framework, declared, nor extras: "
            f"{list(diff.unexplained)!r}"
        )
    return "; ".join(parts)


def _actual_columns_by_name(spark: SparkSession, qualified_table: str) -> dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in spark.table(qualified_table).schema.fields}


def bootstrap_raw_table(
    spark: SparkSession, qualified_table: str, contract: RawContractModel
) -> None:
    """Idempotent (A-14): absent -> `CREATE TABLE` from `contract`; present
    -> diff, then EITHER apply the purely-additive `ADD COLUMNS` (missing
    declared columns only) OR raise, naming every non-additive finding —
    never a partial apply alongside a loud failure in the same call. The
    "present" branch unconditionally re-stamps `conveyer.table-class` AFTER
    the raise-guard (never on a non-additive diff, never before it) —
    `render_set_table_class_sql`'s own docstring is this bead's dev backfill
    runbook (006.1 §16.3 item 5)."""
    expected = raw_columns_ordered(contract)
    if not spark.catalog.tableExists(qualified_table):
        spark.sql(
            render_create_table_sql(
                qualified_table, expected, partition_by=("batch_id",), table_class="raw"
            )
        )
        return
    actual = _actual_columns_by_name(spark, qualified_table)
    declared_names = frozenset(c.name for c in contract.columns)
    diff = diff_raw_schema(expected, actual, declared_names)
    if not diff.is_clean:
        raise ValueError(
            f"raw table {qualified_table} schema diff is not additive-only [DC-11]: "
            f"{describe_raw_diff(diff)}"
        )
    if diff.missing_declared:
        to_add = tuple(c for c in expected if c.name in diff.missing_declared)
        spark.sql(render_add_columns_sql(qualified_table, to_add))
    spark.sql(render_set_table_class_sql(qualified_table, "raw"))


# --- §4.2 quarantine table (constant, pipeline-independent, D-7) -------------

QUARANTINE_COLUMNS: tuple[ColumnDDL, ...] = (
    ColumnDDL("batch_id", "string", True),
    ColumnDDL("delivery_id", "string", True),
    ColumnDDL("feed_id", "string", True),
    ColumnDDL("check_stage", "string", True),
    ColumnDDL("source_uri", "string", False),
    ColumnDDL("object_seq", "int", False),
    ColumnDDL("row_index", "bigint", False),
    ColumnDDL("domain_id", "string", False),
    ColumnDDL("record_key", "string", False),
    ColumnDDL("row_hash", "string", True),
    ColumnDDL("reason_code", "string", True),
    ColumnDDL("reason_detail", "string", False),
    ColumnDDL("check_version", "string", True),
    ColumnDDL("quarantined_at", "timestamp", True),
    ColumnDDL("row_snapshot", "string", True),
)


def render_quarantine_create_table_sql(qualified_table: str) -> str:
    return render_create_table_sql(
        qualified_table, QUARANTINE_COLUMNS, partition_by=("batch_id",), table_class="quarantine"
    )


def _actual_columns_ordered(spark: SparkSession, qualified_table: str) -> tuple[ColumnDDL, ...]:
    return tuple(
        ColumnDDL(f.name, f.dataType.simpleString(), not f.nullable)
        for f in spark.table(qualified_table).schema.fields
    )


def bootstrap_quarantine_table(spark: SparkSession, qualified_table: str) -> None:
    """Idempotent (A-14): absent -> `CREATE TABLE` from the constant
    `QUARANTINE_COLUMNS`; present -> assert EXACT equality (name, type,
    nullability, AND order) — no evolution path exists for this table, so
    ANY diff is a loud failure ("a framework upgrade forgot its own
    migration note", §4.4). The "present" branch unconditionally re-stamps
    `conveyer.table-class` AFTER the exact-equality assertion passes —
    006.1 §16.3 item 5: the tag is table-property content, not part of the
    column-level exact-schema comparison `QUARANTINE_COLUMNS` governs, so
    this leaves that assertion untouched."""
    if not spark.catalog.tableExists(qualified_table):
        spark.sql(render_quarantine_create_table_sql(qualified_table))
        return
    actual = _actual_columns_ordered(spark, qualified_table)
    if actual != QUARANTINE_COLUMNS:
        raise ValueError(
            f"quarantine table {qualified_table} schema drift (constant, versionless by "
            f"design, §4.2): expected {QUARANTINE_COLUMNS!r}, actual {actual!r}"
        )
    spark.sql(render_set_table_class_sql(qualified_table, "quarantine"))


# --- orchestration: both tables for one pipeline spec ------------------------


def _qualified(catalog: str, table: str) -> str:
    return f"{catalog}.{table}"


def bootstrap_admission_tables(spark: SparkSession, catalog: str, spec: PipelineSpecModel) -> None:
    """§4.4 steps 2-3 together: `spec.raw_table` from `spec.raw_contract`,
    `spec.quarantine_table` from the constant shape — both under `catalog`.
    Step 4 (D-11 promotion's coalescing view) is operator-authored, not
    machinery (§4.4); nothing here builds one."""
    bootstrap_raw_table(spark, _qualified(catalog, spec.raw_table), spec.raw_contract)
    bootstrap_quarantine_table(spark, _qualified(catalog, spec.quarantine_table))


# --- `main()`: real-AWS-Glue-Catalog entrypoint, NOT covered by tests/unit / --
# tests/integration (same documented exclusion as `create_run_ledger.main`) --


def _fetch_spec(uri: str) -> str:
    """`s3://` via boto3 `GetObject`; `file://` (or a bare local path) via a
    plain read — a deliberate, small copy of `entrypoints/glue_main.py::
    default_fetch_spec` (see module docstring for why this isn't a shared
    import)."""
    if uri.startswith("s3://"):
        import boto3  # type: ignore[import-untyped]  # local import -- s3:// branch only

        bucket, _, key = uri[len("s3://") :].partition("/")
        client = boto3.client("s3")
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        return body.read().decode("utf-8")
    path = uri[len("file://") :] if uri.startswith("file://") else uri
    from pathlib import Path  # local import -- mirrors the boto3 branch's lazy-import symmetry

    return Path(path).read_text()


def _parse_spec(spec_text: str) -> PipelineSpecModel:
    """`core.model.parse_pipeline_spec_yaml` — the same parser
    `entrypoints/glue_main.py::_parse_spec` uses (this bead's task framing:
    "the same `PipelineSpecModel` path as the entrypoint — one parser"; now
    literally the same FUNCTION, `core/model.py`'s strict duplicate-key-
    rejecting loader, 006.1 §4/S1 — both already depend on `core/model.py`
    for `PipelineSpecModel` itself, so this adds no new import-graph
    coupling between bootstrap and the entrypoint, unlike importing FROM
    `entrypoints/glue_main.py` directly, which this module's own docstring
    still avoids)."""
    return parse_pipeline_spec_yaml(spec_text)


def _catalog_conf(catalog: str) -> dict[str, str]:
    """Always `type=glue` (no `catalog_kind` flag — §4.4's CLI pseudocode
    names only `--spec-uri`/`--catalog`/`--env`; `main()` is the real-AWS
    path, matching `create_run_ledger.main`'s own documented-untested
    shape). No `spark.jars.packages` — Glue 5.0 ships the Iceberg runtime
    natively (I-1), the same reason `glue_main._catalog_conf` sets none;
    the local Hadoop-catalog jar-download conf is test-substrate-only
    (`tests/conftest.py::_iceberg_conf`)."""
    return {
        "spark.sql.extensions": _ICEBERG_EXTENSIONS,
        f"spark.sql.catalog.{catalog}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog}.type": "glue",
    }


def _build_session(env: str, catalog: str) -> SparkSession:
    from pyspark.sql import SparkSession  # local import -- keeps this module importable

    # without a live JVM even being reachable (mirrors `glue_main._build_session`).
    builder = SparkSession.builder.appName(f"conveyer-spine-bootstrap-admission-{env}")
    for key, value in _catalog_conf(catalog).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def main() -> None:
    observability.install_json_handler()  # first, before any logging (nvh.47 precedent)

    parser = argparse.ArgumentParser(
        description="Idempotently create a pipeline's raw/quarantine Iceberg tables (§4.4)."
    )
    parser.add_argument("--spec-uri", required=True, help="Pipeline spec URI (s3:// or file://).")
    parser.add_argument("--catalog", default="spine_cat", help="Spark/Iceberg catalog name.")
    parser.add_argument("--env", required=True, help="Deploy environment (Spark app-name suffix).")
    args = parser.parse_args()

    spec = _parse_spec(_fetch_spec(args.spec_uri))
    spark = _build_session(args.env, args.catalog)
    bootstrap_admission_tables(spark, args.catalog, spec)
    raw_qt = _qualified(args.catalog, spec.raw_table)
    qtn_qt = _qualified(args.catalog, spec.quarantine_table)
    print(f"admission tables ready for {spec.pipeline!r}: {raw_qt}, {qtn_qt}")  # noqa: T201


if __name__ == "__main__":
    main()
