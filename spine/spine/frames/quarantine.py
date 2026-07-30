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
* **Post** (`shape_post_quarantine`): locators are NULL (a candidate fact
  may derive from many raw rows — attribution semantics are 006's);
  `domain_id` is read from the caller-named `domain_id_col` WHEN that column
  is present on `viol_df` (else NULL — the "where present" §8.1 shorthand is
  a schema-level check, not a per-row one); `record_key` is NULL until 006
  rules; `reason_code` is the validated `reason` column (A-14/§8.2.1 — see
  `nonconforming_reasons` below, the PURE half of THE quarantine-writer
  mechanism §8.2 assigns to this seam, interim, 006-registered); `reason_detail` is
  NULL; the snapshot is the candidate row's own columns minus `reason`,
  sorted keys, all keys present (§7.1).

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

from spine.core import canonical
from spine.core.model import FRAMEWORK_RAW_COLUMNS, LineageStamp

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


def candidate_row_hash(candidate_df: DataFrame) -> DataFrame:
    """§8.2.4's own admit-path UDF cost, paid ONLY on post_check's guard-skip
    rerun (`frames.checks.hash_subtraction`'s companion — that function
    anti-joins on the `row_hash` column this one adds). Hashes ALL of
    `candidate_df`'s own columns (no `reason` to exclude here, unlike
    `shape_post_quarantine`'s viol-shaped input: a bare recomputed candidate
    row never carries one) via the SAME §7.3 UDF/[DC-3] timestamp-rendering
    machinery the two quarantine shapers use, so a durable row's `row_hash`
    (written by `shape_post_quarantine` from the identical candidate-minus-
    `reason` column set) and a rerun's freshly recomputed candidate hash are
    directly comparable — the value-determinism precondition §15.2 hands to
    006. Every other column of `candidate_df` passes through unchanged."""
    snapshot_cols = list(candidate_df.columns)
    rendered = _render_timestamps_for_snapshot(candidate_df, snapshot_cols)
    with_snapshot = rendered.withColumn(
        _SNAPSHOT_STRUCT_COL, _snapshot_udf(F.struct(*[F.col(c) for c in snapshot_cols]))
    )
    return with_snapshot.withColumn("row_hash", F.col(f"{_SNAPSHOT_STRUCT_COL}.row_hash")).drop(
        _SNAPSHOT_STRUCT_COL
    )


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


# A-14/§8.2.1: the reason grammar at the seam. Normative grammar
# `^business/[a-z0-9][a-z0-9-]*$`, fullmatch-anchored `\A(?:…)\z` the same
# way §6.1 check 6 anchors an authored `pattern` (lowercase `\z`, not Java's
# `\Z` — the latter permits a trailing line terminator, [DC-4]'s
# discriminator). ASCII-only grammar: no Python-regex/Java-regex divergence
# risk to hedge against here (unlike an authored contract `pattern`, [DC-4]
# does not apply).
_BUSINESS_REASON_FULLMATCH = r"\A(?:business/[a-z0-9][a-z0-9-]*)\z"


def nonconforming_reasons(viol_df: DataFrame) -> DataFrame:
    """§8.2(1)/A-14's PURE half (critique F1, bead conveyer-azr.30):
    `viol_df` filtered to rows whose `reason` value does NOT fullmatch
    `^business/[a-z0-9][a-z0-9-]*$` (a NULL `reason` counts as
    nonconforming too). A plain DataFrame-in/DataFrame-out plan — no
    `.count()`, no action, no control flow — matching this module's own
    `frames-transforms` purity profile (`tools/linter_configs/spine.py`)
    exactly the way `frames/checks.py`'s own predicates do.

    Previously this module itself materialized the count and raised the
    A-14 named `ValueError` (`_assert_business_reason_grammar`, now
    deleted) — an eager Spark action turned into control flow INSIDE
    `frames/`, the one purity-profile hole `.count()` sat in (it was, until
    this fix, the sole attribute absent from `_SPARK_BANNED_ATTR_NAMES`,
    routed through deliberately because `.take()`/`.collect()` are banned).
    The caller (`stages/post_check.py`, which already counts on every path)
    now materializes THIS function's output and raises the same named
    `ValueError` itself — `shape_post_quarantine` below stays a pure plan
    builder throughout, never executing a job or raising mid-composition.
    """
    conforms = F.col("reason").isNotNull() & F.col("reason").rlike(_BUSINESS_REASON_FULLMATCH)
    return viol_df.filter(~conforms)


def shape_post_quarantine(
    viol_df: DataFrame,
    stamp: LineageStamp,
    check_version: str,
    quarantined_at: datetime,
    domain_id_col: str,
) -> DataFrame:
    """§8.1/§7.1's post_check quarantine shape. `viol_df` is I-12's
    `Quarantined = tuple[Record, str]` shape realized as a frame: the
    candidate row's own columns plus its own `reason` column (one reason
    per row) — 006 owns the eventual keyed violation identity; this is the
    interim mechanics §8.2 registers to it.

    Produces EXACTLY the §4.2 columns (`_QUARANTINE_COLUMNS`): locators are
    NULL (a candidate fact may derive from many raw rows — attribution
    semantics are 006's); `domain_id` reads `domain_id_col` WHEN that column
    is present on `viol_df` (a schema-level "where present" — §8.1's own
    words — not a per-row one; NULL otherwise); `record_key` is NULL until
    006 rules; `check_stage` is the literal `"post_check"`; `reason_code` is
    the validated `reason` column (the caller — `stages/post_check.py` —
    materializes `nonconforming_reasons` above and raises the A-14 named
    `ValueError` BEFORE calling this function on any nonconforming row;
    `shape_post_quarantine` itself no longer asserts the grammar, critique
    F1); `reason_detail` is NULL (pre_check's `reason_detail` — the full
    ordered failures array, §6.4 — has no post_check analogue yet);
    `row_snapshot`/`row_hash` come from the §7.3 UDF over the candidate's
    own columns MINUS `reason`, sorted keys, all keys present (§7.1's post
    structure).
    """
    if "reason" not in viol_df.columns:
        raise ValueError(
            "shape_post_quarantine: viol_df has no 'reason' column (I-12's "
            "`Quarantined = tuple[Record, str]` shape, one reason per row)"
        )

    snapshot_cols = [c for c in viol_df.columns if c != "reason"]
    rendered = _render_timestamps_for_snapshot(viol_df, snapshot_cols)
    with_snapshot = rendered.withColumn(
        _SNAPSHOT_STRUCT_COL, _snapshot_udf(F.struct(*[F.col(c) for c in snapshot_cols]))
    )
    domain_id_expr = (
        F.col(domain_id_col).cast("string")
        if domain_id_col in viol_df.columns
        else F.lit(None).cast("string")
    )
    shaped = (
        with_snapshot.withColumn("batch_id", F.lit(stamp.batch_id))
        .withColumn("delivery_id", F.lit(stamp.delivery_id))
        .withColumn("feed_id", F.lit(stamp.feed_id))
        .withColumn("check_stage", F.lit("post_check"))
        .withColumn("source_uri", F.lit(None).cast("string"))
        .withColumn("object_seq", F.lit(None).cast("int"))
        .withColumn("row_index", F.lit(None).cast("long"))
        .withColumn("domain_id", domain_id_expr)
        .withColumn("record_key", F.lit(None).cast("string"))
        .withColumn("row_hash", F.col(f"{_SNAPSHOT_STRUCT_COL}.row_hash"))
        .withColumn("reason_code", F.col("reason"))
        .withColumn("reason_detail", F.lit(None).cast("string"))
        .withColumn("check_version", F.lit(check_version))
        .withColumn("quarantined_at", F.lit(quarantined_at))
        .withColumn("row_snapshot", F.col(f"{_SNAPSHOT_STRUCT_COL}.row_snapshot"))
    )
    return shaped.select(*_QUARANTINE_COLUMNS)
