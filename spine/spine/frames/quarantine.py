"""Quarantine shaping — the ONE output shape (§4.2), both writers. LLD 005.1
§7.1/§7.3/§8.1 (D-7, A-7).

**`shape_pre_quarantine`/`shape_post_quarantine`** (bead conveyer-azr.15,
n1-quarantine; wired into `stages/pre_check.py`/`stages/post_check.py` by
bead conveyer-azr.19, n3-admission-cut, which also deletes the legacy
`shape_quarantine` this module used to carry) are the §8.1 rewrite: two
shapers producing EXACTLY the §4.2 quarantine columns (`_QUARANTINE_COLUMNS`
below). Per §8.1:

* **Pre** (`shape_pre_quarantine`): locators (`source_uri`/`object_seq`/
  `row_index`) come straight from the violation rows (already durable raw
  columns); `domain_id`/`record_key` are NULL; `reason_code`/`reason_detail`
  come from the §6.4-shaped input (`frames/checks.py::violations`, N1's
  sibling bead n1-checks — not built here, only consumed by shape); the
  snapshot is `{<declared col>: …, "extras": {…}, "malformed_text": …}`
  (§7.1) — declared columns are derived structurally as "every `viol_df`
  column that is neither a framework raw column (`core.model.
  FRAMEWORK_RAW_COLUMNS`) nor a reason column" rather than threaded through
  as a parameter, so the shaper needs no `RawContractModel` in hand.
* **Post** (`shape_post_quarantine`, REWRITTEN by 006.1 §10 — the interim
  005.1 shape above is superseded, not merely extended): locators stay NULL
  (a candidate fact may derive from many raw rows — attribution semantics
  are 006's, unchanged ruling); `domain_id` reads `fact_schema.domain_id_col`
  unconditionally (006.1 §4.1 guarantees `domain_id_col ∈ columns`, so the
  005.1-era "where present" schema-level branch is now vacuous — the column
  always exists, only its per-row VALUE may be NULL, which casts through as
  NULL exactly as before); `record_key` is no longer permanently NULL — it
  derives through 007.1 F-2's one shared `core.identity.derive_record_key`,
  gated at this call site (006.1 §10 point 1/D-6): iff every declared
  `record_key:` column's value is non-null, else NULL (never partial-key
  derivation); `reason_code`/`reason_detail` are READ THROUGH from
  `evaluated_viol_df` (`frames/business_checks.py::business_violations`'s
  own output shape, §7.3 — the A-14 runtime reason-grammar check is
  superseded here too: business-check reasons are bind-time-validated data,
  006.1 §5.4 K6, not pipeline-authored free text); the snapshot is `{<every
  declared fact column>: …, "_conveyer_fact_type": <fact_type>}` — the
  reserved tag (P-7(b)) inside the hashed object, discriminating value-
  identical candidates of different fact types (G-03's golden). See
  `_make_post_snapshot_udf`'s own docstring for the UDF mechanics and
  [DC-3]'s extension to this site.

**The §7.3 UDF seam (A-7).** One scalar UDF, `_snapshot_udf`, registered
ONCE at module scope and wrapping `core.canonical` — applied only at
shaping (never on the admit path). **[DC-3], the timestamp hazard**: the
Python UDF boundary converts `TimestampType` values via the OS-local zone
(`fromInternal` -> `fromtimestamp`), silently ignoring the session's UTC
pin (probe-verified, bead conveyer-azr.15: with the driver's local zone
`America/Vancouver`, a `2026-01-02T03:04:05` UTC instant arrived at the UDF
as the NAIVE `datetime(2026, 1, 1, 19, 4, 5)` — wrong wall-clock value AND
missing tzinfo). `_render_timestamps_for_snapshot` closes this in-plan,
BEFORE any struct reaches the UDF boundary: every `TimestampType` field
among the columns entering the snapshot struct is rendered to its canonical
string via `F.date_format(col, canonical.CANONICAL_TIMESTAMP_SPARK_PATTERN)`
— a native SQL expression, which DOES honor `spark.sql.session.timeZone`
(probe-verified against the same instant: renders exactly
`"2026-01-02T03:04:05.000000Z"` under the UTC pin). `Decimal`/`date`
columns cross the UDF boundary typed and untouched (both environment-
independent — no OS-local conversion exists for them); `canonical_json`'s
own naive-datetime rejection ([DC-3], `core/canonical.py`) is the backstop
that would catch a `TimestampType` column this rendering step ever missed.
This one helper serves both shapers uniformly: pre_check's declared raw
columns are always `string` (D-5 — raw is all-string), so it is a
structural no-op there; post_check's candidate columns are typed, so a
`timestamp(fmt)` fact column is exactly where this rendering is load-
bearing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from spine.core import canonical, identity
from spine.core.model import FRAMEWORK_RAW_COLUMNS, FactSchemaModel, LineageStamp

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import DataFrame, Row


# --- §4.2's constant output shape, both new shapers -------------------------

# Column order matches §4.2's DDL table exactly (informational for readers;
# `fx.append`'s `DataFrameWriterV2` matches by NAME, not position, §8.1).
_QUARANTINE_COLUMNS: tuple[str, ...] = (
    "batch_id",
    "delivery_id",
    "feed_id",
    "check_stage",
    "source_uri",
    "object_seq",
    "row_index",
    "domain_id",
    "record_key",
    "row_hash",
    "reason_code",
    "reason_detail",
    "check_version",
    "quarantined_at",
    "row_snapshot",
)

# §7.1: `reason_code`/`reason_detail` ride ALONGSIDE a pre_check violation
# row's own raw columns (stamped by `frames/checks.py::violations`, N1's
# sibling bead n1-checks, per §6.4) but are never themselves part of the
# `{<declared col>…, extras, malformed_text}` snapshot -- excluded from the
# declared-column derivation below the same way `FRAMEWORK_RAW_COLUMNS` is.
_PRE_CHECK_REASON_COLUMNS: frozenset[str] = frozenset({"reason_code", "reason_detail"})

# §7.3's ONE scalar UDF, registered once at module scope, wrapping
# `core.canonical` -- applied only at shaping (this module's docstring; A-7).
_SNAPSHOT_STRUCT_COL = "_conveyer_snapshot_pair"
_SNAPSHOT_RETURN_TYPE = StructType(
    [
        StructField("row_snapshot", StringType(), nullable=False),
        StructField("row_hash", StringType(), nullable=False),
    ]
)


def _snapshot_and_hash(row: Row | None) -> tuple[str, str]:
    """The UDF body: `Row` (already rendered per [DC-3] below) -> canonical
    JSON + its SHA-256. `row.asDict(recursive=True)` turns nested structs
    into plain dicts and leaves `MapType`/`DecimalType`/`DateType`/`str`/
    `int`/`bool` values as their native Python types -- exactly
    `core.canonical.canonical_json`'s value domain (§7.1), probe-verified
    (bead conveyer-azr.15). `row_hash` recomputes `canonical_json` a second
    time via `canonical.row_hash` rather than hashing the already-serialized
    string directly -- one call through the single authored hash algorithm
    (§7.2), not a second, independently-derived one; the extra pass costs
    nothing beyond the violation-path Python-rate serialization A-7 already
    accepts. `row is None` cannot occur in practice (`F.struct(...)` never
    produces a NULL row even when every field inside it is NULL) but is
    guarded defensively rather than assumed."""
    value = row.asDict(recursive=True) if row is not None else {}
    return canonical.canonical_json(value), canonical.row_hash(value)


_snapshot_udf = F.udf(_snapshot_and_hash, _SNAPSHOT_RETURN_TYPE)


def _render_timestamps_for_snapshot(df: DataFrame, snapshot_cols: list[str]) -> DataFrame:
    """[DC-3]: every `TimestampType` column among `snapshot_cols` is
    rendered to its canonical string, IN-PLAN (`F.date_format`, a native SQL
    expression honoring `spark.sql.session.timeZone`), before the struct
    that carries it reaches the `_snapshot_udf` boundary. `Decimal`/`date`
    columns are left untouched (both cross the UDF boundary typed and
    environment-independent, module docstring). A no-op when `snapshot_cols`
    contains no `TimestampType` field (pre_check's declared raw columns,
    always `string`, D-5)."""
    rendered = df
    for field in df.schema.fields:
        if field.name in snapshot_cols and isinstance(field.dataType, TimestampType):
            rendered = rendered.withColumn(
                field.name,
                F.date_format(F.col(field.name), canonical.CANONICAL_TIMESTAMP_SPARK_PATTERN),
            )
    return rendered


def candidate_row_hash(
    candidate_df: DataFrame, fact_type: str, fact_schema: FactSchemaModel
) -> DataFrame:
    """006.1 §8.3's own admit-path UDF cost, paid ONLY on post_check's
    guard-skip rerun (`frames.checks.hash_subtraction`'s companion — that
    function anti-joins on the `row_hash` column this one adds). REWRITTEN
    for 006.1 §8.3 (bead conveyer-6pg.13, B3) — the pre-006.1 signature
    hashed ALL of `candidate_df`'s own columns, untagged, sound only for a
    single-fact-type pipeline; multi-type §8.3 needs the SAME `_conveyer_
    fact_type` tag (P-7(b)) `shape_post_quarantine` bakes into every durable
    row's hash, or a value-identical row of a DIFFERENT type would hash
    identically and cross-subtract (G-03's trap). This function now hashes
    ONLY `fact_schema`'s declared columns (never lineage or any other column
    `candidate_df` happens to carry — the same declared-columns-only
    subject `shape_post_quarantine` hashes) plus the tag, **through the
    SAME §10 UDF** (`_make_post_snapshot_udf`) `shape_post_quarantine` uses
    — never a second, parallel hashing implementation — so a durable row's
    `row_hash` and a rerun's recomputed candidate hash are directly
    comparable by construction (value-determinism, §15.2's precondition,
    now type-discriminated too). Every other column of `candidate_df`
    (declared or not) passes through unchanged; the `record_key` half of
    the UDF's triple output is computed but not surfaced here — this
    function's own contract is `row_hash` only."""
    declared_cols = [column.name for column in fact_schema.columns]
    rendered = _render_timestamps_for_snapshot(candidate_df, declared_cols)
    tagged_struct = F.struct(
        *[F.col(name) for name in declared_cols], F.lit(fact_type).alias("_conveyer_fact_type")
    )
    post_snapshot_udf = _make_post_snapshot_udf(tuple(fact_schema.record_key))
    with_snapshot = rendered.withColumn(_POST_SNAPSHOT_STRUCT_COL, post_snapshot_udf(tagged_struct))
    return with_snapshot.withColumn(
        "row_hash", F.col(f"{_POST_SNAPSHOT_STRUCT_COL}.row_hash")
    ).drop(_POST_SNAPSHOT_STRUCT_COL)


def shape_pre_quarantine(
    viol_df: DataFrame,
    stamp: LineageStamp,
    check_version: str,
    quarantined_at: datetime,
) -> DataFrame:
    """§8.1/§7.1's pre_check quarantine shape. `viol_df` is the §6.4-shaped
    violation frame (`frames/checks.py::violations`'s output, n1-checks,
    NOT built here): durable raw columns (locators, lineage, declared
    columns, `extras`, `malformed_text`) plus `reason_code`/`reason_detail`
    (§6.4's reason shaping). Declared columns are derived structurally —
    every `viol_df` column that is neither a framework raw column
    (`FRAMEWORK_RAW_COLUMNS`, [DC-6]) nor a reason column — so this shaper
    needs no `RawContractModel` in hand; the same exclusion is what makes
    `RawContractModel`'s own [DC-6] validator (a declared column may never
    collide with a framework column or a reason column) load-bearing here,
    not just at spec-parse.

    Produces EXACTLY the §4.2 columns (`_QUARANTINE_COLUMNS`): locators
    (`source_uri`/`object_seq`/`row_index`) pass through from `viol_df`
    unchanged; `domain_id`/`record_key` are NULL; `check_stage` is the
    literal `"pre_check"`; `row_snapshot`/`row_hash` come from the §7.3 UDF
    over `{<declared col>…, "extras", "malformed_text"}` (§7.1's pre
    structure, every key always present since the UDF's `Row` always
    carries every field of the struct it was built from, NULL or not).
    """
    missing = {"extras", "malformed_text", *_PRE_CHECK_REASON_COLUMNS} - set(viol_df.columns)
    if missing:
        raise ValueError(
            "shape_pre_quarantine: viol_df is missing §6.4-shaped column(s) "
            f"{sorted(missing)!r} (expected a raw-row-shaped frame plus "
            "'reason_code'/'reason_detail')"
        )
    declared_cols = [
        c
        for c in viol_df.columns
        if c not in FRAMEWORK_RAW_COLUMNS and c not in _PRE_CHECK_REASON_COLUMNS
    ]
    snapshot_cols = [*declared_cols, "extras", "malformed_text"]
    rendered = _render_timestamps_for_snapshot(viol_df, snapshot_cols)
    with_snapshot = rendered.withColumn(
        _SNAPSHOT_STRUCT_COL, _snapshot_udf(F.struct(*[F.col(c) for c in snapshot_cols]))
    )
    shaped = (
        with_snapshot.withColumn("batch_id", F.lit(stamp.batch_id))
        .withColumn("delivery_id", F.lit(stamp.delivery_id))
        .withColumn("feed_id", F.lit(stamp.feed_id))
        .withColumn("check_stage", F.lit("pre_check"))
        .withColumn("domain_id", F.lit(None).cast("string"))
        .withColumn("record_key", F.lit(None).cast("string"))
        .withColumn("row_hash", F.col(f"{_SNAPSHOT_STRUCT_COL}.row_hash"))
        .withColumn("check_version", F.lit(check_version))
        .withColumn("quarantined_at", F.lit(quarantined_at))
        .withColumn("row_snapshot", F.col(f"{_SNAPSHOT_STRUCT_COL}.row_snapshot"))
    )
    return shaped.select(*_QUARANTINE_COLUMNS)


# --- 006.1 §10's post_check writer rewrite -----------------------------------

# The struct field this UDF factory's closure produces, per call (a FRESH
# UDF per `shape_post_quarantine` invocation, unlike `_snapshot_udf` above --
# see `_make_post_snapshot_udf`'s own docstring for why it must be a
# per-shaping closure, not a module-scope singleton).
_POST_SNAPSHOT_STRUCT_COL = "_conveyer_post_snapshot_triple"
_POST_SNAPSHOT_RETURN_TYPE = StructType(
    [
        StructField("row_snapshot", StringType(), nullable=False),
        StructField("row_hash", StringType(), nullable=False),
        StructField("record_key", StringType(), nullable=True),
    ]
)


# No return-type annotation on `_make_post_snapshot_udf` below: `F.udf(...)`'s
# own stub returns the stub-only, non-runtime-importable `UserDefinedFunction
# Like` (`pyspark/sql/_typing.pyi`) -- matching `_snapshot_udf`'s own module-
# scope assignment above, which carries no annotation for the same reason.
def _make_post_snapshot_udf(key_columns: tuple[str, ...]):
    """006.1 §10 point 3: "one scalar UDF per shaping (extending A-7's)
    returns struct<row_snapshot, row_hash, record_key>" -- extends §7.3's
    `_snapshot_udf` shape with the `record_key` gate (006.1 §10 point 1/D-6)
    "implemented inside the UDF closure wrapping `core.identity.derive_
    record_key`". A FRESH UDF per call (never a module-scope singleton like
    `_snapshot_udf`) because `key_columns` — the fact type's own declared
    `record_key:` column names — varies per fact type; `key_columns` is
    bind-derived (`FactSchemaModel.record_key`), never authored/free-form
    text (007.1 §5.1's own I-9/[C-5] rule), so closing over it is safe.

    `row.asDict(recursive=True)` (the [DC-3]-rendered `Row`, per the caller's
    own `_render_timestamps_for_snapshot` pass — see `shape_post_quarantine`)
    gives the SAME plain-Python value domain `_snapshot_and_hash` already
    relies on; `key_values` reads the declared key columns' values straight
    out of that same dict (never re-derived) — a TimestampType key column
    therefore arrives as its ALREADY-RENDERED canonical string (007.1 §5.1
    fragment 2's own accepted input: "the value's canonical-string
    pre-rendering"), never a naive/OS-local datetime.

    **The gate (006.1 §10 point 1, D-6, restated at this call site):**
    derive iff EVERY declared key column's value is non-null; any null key
    column ⇒ `record_key` stays NULL (never partial-key derivation) — a
    plain Python `any(v is None for v in key_values.values())` check, since
    the [DC-3]-rendered timestamp strings and every other typed value are
    already ordinary Python objects at this point. `core.identity.derive_
    record_key` is the ONE shared function this wraps (006 D-6's law: never
    a second implementation) — this closure's only job is the gate; the
    derivation itself is untouched, unre-derived.
    """

    def _snapshot_hash_and_key(row: Row | None) -> tuple[str, str, str | None]:
        value = row.asDict(recursive=True) if row is not None else {}
        snapshot = canonical.canonical_json(value)
        row_hash_val = canonical.row_hash(value)
        key_values: dict[str, identity.KeyValue] = {name: value.get(name) for name in key_columns}
        record_key = (
            None
            if any(key_value is None for key_value in key_values.values())
            else identity.derive_record_key(key_values)
        )
        return snapshot, row_hash_val, record_key

    return F.udf(_snapshot_hash_and_key, _POST_SNAPSHOT_RETURN_TYPE)


def shape_post_quarantine(
    evaluated_viol_df: DataFrame,
    stamp: LineageStamp,
    checks_version: str,
    quarantined_at: datetime,
    fact_type: str,
    fact_schema: FactSchemaModel,
) -> DataFrame:
    """006.1 §10's REWRITE of the post_check quarantine shape (supersedes
    the 005.1-era interim mechanics this module's own header docstring still
    describes historically). `evaluated_viol_df` is `frames/business_checks.
    py::business_violations`'s own output shape: one fact type's declared
    candidate columns (D-4) plus `reason_code`/`reason_detail` (§7.3's
    shaping) — NOT built here, only consumed by this shaper, the same
    division of labour `shape_pre_quarantine` already keeps with `frames/
    checks.py::violations`.

    Produces EXACTLY the §4.2 columns (`_QUARANTINE_COLUMNS`): locators stay
    NULL (a candidate fact may derive from many raw rows — attribution
    semantics are 006's, unchanged); `domain_id` reads `fact_schema.
    domain_id_col` unconditionally (006.1 §4.1 guarantees the column is
    always declared — only its per-row VALUE may be NULL, which casts
    through as NULL exactly as before); `record_key` derives through the
    §10 gate inside `_make_post_snapshot_udf`'s closure (D-6: complete key
    ⇒ the derived hash, partial ⇒ NULL); `reason_code`/`reason_detail` pass
    through from `evaluated_viol_df` unchanged (no runtime grammar check
    here — business-check reasons are bind-time-validated data, 006.1 §5.4
    K6, not pipeline-authored free text the interim A-14 mechanism guarded
    against); `row_snapshot`/`row_hash` come from the extended §7.3 UDF over
    `{<every declared fact column>: …, "_conveyer_fact_type": <fact_type>}`
    — P-7(b)'s reserved tag, inside the hashed object (visible in
    `row_snapshot`, a `json_extract` projection for 012 — not built here).

    **[DC-3] at this site (007.1 §5.1 fragment 2, applied — 006.1 §10 point
    2 [EM-7]):** `_render_timestamps_for_snapshot` runs over EVERY declared
    fact column (not just `record_key:` columns) BEFORE the tagged struct
    reaches the UDF boundary — the UDF hashes the full `{<declared
    cols…>, "_conveyer_fact_type"}` object, so any unrendered `TimestampType`
    column would cross as a naive OS-local datetime and `canonical_json`
    would loudly reject it; the `record_key` gate's inputs are simply the
    SUBSET of that same already-rendered struct named by `fact_schema.
    record_key`. This is why fixtures exercising this function must include
    a fact type carrying a NON-KEY timestamp column (G-11 [EM-7]) — a build
    that only rendered key columns would pass every fixture with an
    all-key-timestamp schema while failing on this one.
    """
    declared_cols = [column.name for column in fact_schema.columns]
    missing = {*declared_cols, "reason_code", "reason_detail"} - set(evaluated_viol_df.columns)
    if missing:
        raise ValueError(
            "shape_post_quarantine: evaluated_viol_df is missing declared column(s)/reason "
            f"column(s) {sorted(missing)!r} — expected fact_schema's declared columns plus "
            "'reason_code'/'reason_detail' (frames/business_checks.py::business_violations's "
            "own output shape)"
        )

    rendered = _render_timestamps_for_snapshot(evaluated_viol_df, declared_cols)
    tagged_struct = F.struct(
        *[F.col(name) for name in declared_cols], F.lit(fact_type).alias("_conveyer_fact_type")
    )
    post_snapshot_udf = _make_post_snapshot_udf(tuple(fact_schema.record_key))
    with_snapshot = rendered.withColumn(_POST_SNAPSHOT_STRUCT_COL, post_snapshot_udf(tagged_struct))
    shaped = (
        with_snapshot.withColumn("batch_id", F.lit(stamp.batch_id))
        .withColumn("delivery_id", F.lit(stamp.delivery_id))
        .withColumn("feed_id", F.lit(stamp.feed_id))
        .withColumn("check_stage", F.lit("post_check"))
        .withColumn("source_uri", F.lit(None).cast("string"))
        .withColumn("object_seq", F.lit(None).cast("int"))
        .withColumn("row_index", F.lit(None).cast("long"))
        .withColumn("domain_id", F.col(fact_schema.domain_id_col).cast("string"))
        .withColumn("record_key", F.col(f"{_POST_SNAPSHOT_STRUCT_COL}.record_key"))
        .withColumn("row_hash", F.col(f"{_POST_SNAPSHOT_STRUCT_COL}.row_hash"))
        .withColumn("check_version", F.lit(checks_version))
        .withColumn("quarantined_at", F.lit(quarantined_at))
        .withColumn("row_snapshot", F.col(f"{_POST_SNAPSHOT_STRUCT_COL}.row_snapshot"))
    )
    return shaped.select(*_QUARANTINE_COLUMNS)
