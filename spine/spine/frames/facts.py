"""`stamp_fact_identity` — the commit-side fact-stamping plan builder. LLD
007.1 F-1 §5.1 (mechanics + completion block), K-02 (§13.1: "the commit UDF
reproduces every `contracts/fixtures/fact-hash/` vector"), K-03
(stamp-insensitivity, pinned at plain-value grade by `tests/unit/
test_fact_hash.py`), K-04 (the [DC-3] tying test at this call site).

**One UDF, one boundary, both stamps (§5.1's completion block).** `_make_
fact_stamps_udf` registers ONE scalar UDF per call — `derive_fact_stamps`,
closing over the fact type's own `key_cols` the same way `frames/
quarantine.py::_make_post_snapshot_udf` closes over its `key_columns`
(varies per fact type, bind-derived from `FactSchemaModel.record_key`, so a
per-call closure rather than a module-scope singleton like `frames/
quarantine.py::_snapshot_udf`, which needs no such per-type configuration).
Wraps `core.canonical` (F-1: `content_hash = row_hash(declared-column map)`,
applied to its fourth subject) and `core/identity.py::derive_record_key`
(F-2's one shared function — this is its commit call site, **gate: none**
per §5.2's table: every committed fact carries `record_key` unconditionally,
including over null key material, which renders canonically). Two UDFs was
explicitly rejected (§5.1's own trap): the pre-rendered declared-column
struct is identical input for both derivations by construction, so a second
UDF would only duplicate the struct-building/rendering cost, never add
anything.

**All-declared-columns, by construction (D-1).** The hashed object is the
map `{declared column name -> typed value}` over exactly `declared_cols` —
selected from the DECLARATION (bind-derived, I-9/[C-5]'s rule: DataFrames
and narrow plain values only, never authored/free-form), **never
introspected from the frame**: no exclusion list of framework stamp names
exists to drift, because the stamps are simply never in the struct in the
first place. A declared column missing from the candidate frame fails
loudly, before commit's own §7.3 table-side backstop ever runs.

**[DC-3] at this call site, normative.** Every column named in
`timestamp_cols` is rendered to its canonical string via an in-plan
`F.date_format(..., canonical.CANONICAL_TIMESTAMP_SPARK_PATTERN)` expression
(honors `spark.sql.session.timeZone`, unlike the Python UDF boundary's own
OS-local-zone conversion) **before** it enters the struct the UDF receives —
`Decimal`/`date` columns cross the boundary typed and untouched, same as
`frames/quarantine.py`'s own [DC-3] closure. Unlike that module, this
rendering is built as column EXPRESSIONS feeding the struct directly
(`_declared_struct_columns`), never materialized onto the returned frame via
`withColumn` — **the pre-rendered copy feeds the derivation struct only; the
appended frame keeps its typed columns** (§6.1's DDL unchanged — a fact
table's `timestamp`-typed columns must stay typed on disk, unlike
quarantine's snapshot columns, which are discarded after `_QUARANTINE_
COLUMNS` selection and never re-read as facts).

Composes after `frames/lineage.py::stamp_fact_lineage` and before `frames/
delta.py::delta_filter` (§4.3's step order — the filter consumes both
derived stamps and never re-derives, §7.2 cited).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spine.core import canonical, identity

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, Row

# §5.1's one UDF, both stamps: struct<content_hash string, record_key
# string> — 005.1 A-7's shape extended to commit. Neither field is ever
# NULL at this call site (record_key's gate is "none" here, §5.2's table).
_FACT_STAMPS_STRUCT_COL = "_conveyer_fact_stamps"
_FACT_STAMPS_RETURN_TYPE = StructType(
    [
        StructField("content_hash", StringType(), nullable=False),
        StructField("record_key", StringType(), nullable=False),
    ]
)


# No return-type annotation on `_make_fact_stamps_udf`: `F.udf(...)`'s own
# stub returns the stub-only, non-runtime-importable `UserDefinedFunctionLike`
# (`pyspark/sql/_typing.pyi`) — matching `frames/quarantine.py::_make_post_
# snapshot_udf`'s identical omission, same reason.
def _make_fact_stamps_udf(key_cols: tuple[str, ...]):
    """A FRESH UDF per `stamp_fact_identity` call (never a module-scope
    singleton) — `key_cols` is the fact type's own declared `record_key:`
    column names, which vary per fact type; closing over it is safe because
    it is bind-derived, never authored (module docstring).

    `row.asDict(recursive=True)` gives the plain-Python value domain `core.
    canonical.canonical_json`'s own value domain expects (probe-verified
    precedent: `frames/quarantine.py::_snapshot_and_hash`); `key_values`
    reads the declared key columns straight out of that same dict, so a
    [DC-3]-rendered timestamp key column arrives as its already-rendered
    canonical string (007.1 §5.1 fragment 2's accepted input), never a
    naive/OS-local datetime.

    **No gate here** (§5.2's table, commit row): `derive_record_key` is
    called unconditionally, including over null key material — commit's
    totality is sound for dedup/delta because a drop still requires
    `content_hash` equality over ALL columns (D-2), so a null-key
    "collision" can only drop a candidate whose full content already
    exists (fail-open holds, module docstring's own citation)."""

    def _derive_fact_stamps(row: Row | None) -> tuple[str, str]:
        value = row.asDict(recursive=True) if row is not None else {}
        content_hash = canonical.row_hash(value)
        key_values: dict[str, identity.KeyValue] = {name: value.get(name) for name in key_cols}
        record_key = identity.derive_record_key(key_values)
        return content_hash, record_key

    return F.udf(_derive_fact_stamps, _FACT_STAMPS_RETURN_TYPE)


def _declared_struct_columns(
    declared_cols: Sequence[str], timestamp_cols: Sequence[str]
) -> list[Column]:
    """[DC-3]'s in-plan rendering, as struct-feeding EXPRESSIONS rather than
    a `withColumn` mutation of the caller's frame (module docstring: "the
    appended frame keeps its typed columns"). A `declared_cols` entry named
    in `timestamp_cols` renders via `F.date_format(...)`, aliased back to its
    own column name so the struct's field names still match `declared_cols`
    exactly; every other entry passes through as `F.col(name)` untouched."""
    timestamp_col_set = set(timestamp_cols)
    columns: list[Column] = []
    for name in declared_cols:
        if name in timestamp_col_set:
            rendered = F.date_format(F.col(name), canonical.CANONICAL_TIMESTAMP_SPARK_PATTERN)
            columns.append(rendered.alias(name))
        else:
            columns.append(F.col(name))
    return columns


def stamp_fact_identity(
    df: DataFrame,
    declared_cols: Sequence[str],
    key_cols: Sequence[str],
    timestamp_cols: Sequence[str],
) -> DataFrame:
    """§5.1's completion block, the plan builder: pre-renders [DC-3]'s
    timestamp columns, applies the one `derive_fact_stamps` UDF over exactly
    `declared_cols` (selected from the declaration, never introspected from
    `df`), and attaches `content_hash`/`record_key` to the returned frame —
    every other column of `df` (framework stamps, anything else already on
    the candidate frame) passes through completely unchanged.

    `declared_cols`/`key_cols`/`timestamp_cols` are DataFrames-and-narrow-
    plain-values-only inputs (I-9/[C-5]): the caller bind-derives them from
    `FactSchemaModel`/`FactTypeModel` (`columns`, `record_key`, the
    `timestamp`-kind subset of `columns`) — this function does not itself
    know a fact type's declaration, so it cannot check `key_cols`/
    `timestamp_cols` are subsets of `declared_cols`; that is a caller bug,
    the same division of labour `core/identity.py::derive_record_key`'s own
    docstring states for its `key_values` completeness precondition.

    **The trap, guarded against (§5.1's own text):** a declared column
    missing from `df` fails plan construction loudly, HERE, before commit's
    own §7.3 table-side structural check ever runs — no exclusion list of
    framework stamp names exists anywhere in this function to drift, because
    the hashed struct is built from `declared_cols` alone."""
    missing = set(declared_cols) - set(df.columns)
    if missing:
        raise ValueError(
            "stamp_fact_identity: declared column(s) "
            f"{sorted(missing)!r} missing from the candidate frame "
            "(fact-schema/frame drift — a bind-time defect, caught before "
            "the commit-stage structural check, 007.1 §7.3)"
        )
    struct_col = F.struct(*_declared_struct_columns(declared_cols, timestamp_cols))
    fact_stamps_udf = _make_fact_stamps_udf(tuple(key_cols))
    with_stamps = df.withColumn(_FACT_STAMPS_STRUCT_COL, fact_stamps_udf(struct_col))
    return (
        with_stamps.withColumn("content_hash", F.col(f"{_FACT_STAMPS_STRUCT_COL}.content_hash"))
        .withColumn("record_key", F.col(f"{_FACT_STAMPS_STRUCT_COL}.record_key"))
        .drop(_FACT_STAMPS_STRUCT_COL)
    )
