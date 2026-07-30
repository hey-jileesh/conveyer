"""pre_check's compiled-checks half (005.1 §6.1-§6.6, N1+N3) + post_check's
I-12 subtraction mechanics it grows alongside. LLD 004.1 §7.5, I-12, [C-8];
005.1 §6, §8.2.

Every function here is a pure DataFrame-in/DataFrame-out plan builder (I-9):
no `.count()`, no `.collect()`, no driver materialization, no read of
`spine.context`/`spine.effects` (this module's own linter profile,
`frames-transforms`, bans both import roots and the Spark-API-surface
attribute names — `E-5`'s co-effect-freedom is enforced structurally, not by
convention). Counts a stage needs (e.g. I-12's fresh-compute count-identity
assertion, or 005.1's §6.6 count identity) are computed BY THE STAGE and
handed to `check_count_identity` as plain `int`s — this module never turns a
count into control flow itself.

**I-P2's provisional `required_null_predicate`/`pre_violations` are DELETED
by bead conveyer-azr.19 (n3-admission-cut)** — `stages/pre_check.py` now
cuts over entirely to `compile_contract`'s real grammar (005 §7.1 replaces
I-P2's literal-reason convenience); nothing in `spine/` calls the old pair
any more. `violation_subtraction` is I-12's all-column bag-subtraction —
**retained permanently**, not provisional: 005.1 §6.5's "one deletion, not
two mechanisms" sentence (discharging 004.1 errata proposal 5, §15.3(5))
refers only to pre_check's OWN guard-skip rerun subtraction, which is
`locator_subtraction` below — `violation_subtraction` keeps serving
post_check's fresh-path bag subtraction (005.1 §8.2.3 [DC-5]), a use this
LLD explicitly keeps unchanged. Do not delete it. `hash_subtraction` (this
bead) is post_check's OWN guard-skip rerun mechanism (§8.2.4 [DC-5]) — a
THIRD, distinct subtraction shape (hash-keyed, not bag/locator-keyed),
needed only because a durable post_check quarantine row no longer carries
candidate columns at all (only `row_hash`), so an all-column or
locator-keyed anti-join is impossible on that one path.

**005.1 §6.1-§6.6**: `compile_contract` turns a `RawContractModel` into a
`CompiledContract` — a frozen, ordered tuple of `CheckEntry` values, one per
§6.1 table row per applicable column (row-order major, contract-column-order
minor) — entirely at DRIVER/COMPILE time, with zero Spark execution (every
`pyspark.sql.Column` built here is a lazy, unbound expression tree; see
`_typed_expr`'s own docstring for why this module cannot force eager
execution to validate a cast even where 005.1 §6.2's own prose implies it,
and how the bounds check compensates). `evaluate` adds the one internal
`_conveyer_admission_failures` column (§6.3); `zero_failures`,
`typed_projection`, and `violations` are its outputs' shaping functions
(§6.3/§6.4); `locator_subtraction` is §6.5's pure anti-join half (the STAGE
composes it with the drift probe and the current-contract typed projection —
not this module's job).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

from spine.core.contract import ColumnType, parse_column_type
from spine.core.merge import quote_identifier

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from spine.core.model import ColumnSpec, RawContractModel

_BAG_RN_COL = "_conveyer_bag_rn"

# 005.1 §12.1/§6.2: the three session pins §6.2's cast-semantics table is
# normative under — load-bearing, not hygiene. The default
# `timeParserPolicy=EXCEPTION` THROWS on an unparseable date/timestamp cell
# (verified, probe-confirmed against Spark 3.5.x, bead conveyer-azr.14) —
# one bad cell would kill the whole batch, violating 001 §2.2 — so
# `CORRECTED` is load-bearing; a Spark 4 ANSI-default flip would silently
# change `try_cast`'s overflow/fraction-rejection semantics (§6.2), so
# `ansi.enabled=false` is pinned explicitly rather than left to the
# engine's own default; `timeZone=UTC` keeps a naive-cell interpretation
# environment-independent (matches `core/canonical.py`'s own UTC-only
# domain, [DC-3]). ONE authored constant, wired into BOTH
# `tests/conftest.py::_BASE_CONF` and `entrypoints/glue_main.py::
# _catalog_conf`'s session builds (§12.1) so they cannot drift apart — a
# plain `Mapping[str, str]`, not a `SparkSession` call of any kind, so this
# module's own `frames-transforms` purity profile (which bans
# `SparkSession`/`getOrCreate`/etc. by attribute name) is untouched by its
# presence here.
SESSION_PINS: Mapping[str, str] = MappingProxyType(
    {
        "spark.sql.ansi.enabled": "false",
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
        "spark.sql.session.timeZone": "UTC",
    }
)


def violation_subtraction(candidate_df: DataFrame, violations_df: DataFrame) -> DataFrame:
    """`candidate_df` anti-joined against `violations_df` on all shared
    columns (I-12, provisional — 006 owns a keyed violation identity).

    Multiplicity-preserving (bag subtraction, [C-8]): if a given
    shared-column value-tuple appears `k` times in `candidate_df` and `m`
    times in `violations_df`, exactly `min(k, m)` copies are removed,
    leaving `max(k - m, 0)` — never the "drop every copy the instant ANY one
    of them is a violation" a naive value-based anti-join would produce.
    Achieved by row-numbering each side within its own shared-column
    partition and anti-joining on `(shared columns, row number)`; the
    row-number pairing is an implementation artifact of computing the
    subtraction, not a published contract.

    No shared columns ⇒ nothing is determinable from this identity;
    `candidate_df` is returned unchanged.
    """
    shared_columns = [c for c in candidate_df.columns if c in violations_df.columns]
    if not shared_columns:
        return candidate_df
    window = Window.partitionBy(*shared_columns).orderBy(F.monotonically_increasing_id())
    candidate_ranked = candidate_df.withColumn(_BAG_RN_COL, F.row_number().over(window))
    violations_ranked = violations_df.select(*shared_columns).withColumn(
        _BAG_RN_COL, F.row_number().over(window)
    )
    join_keys = [*shared_columns, _BAG_RN_COL]
    return candidate_ranked.join(violations_ranked, on=join_keys, how="left_anti").drop(_BAG_RN_COL)


_HASH_COL = "row_hash"


def hash_subtraction(hashed_candidate_df: DataFrame, durable_row_hashes_df: DataFrame) -> DataFrame:
    """§8.2.4's post_check guard-skip rerun subtraction [DC-5] — the THIRD,
    hash-keyed mechanism (distinct from `violation_subtraction`'s all-column
    bag-subtract and `locator_subtraction`'s locator-keyed anti-join).
    Needed only on this ONE path: a durable post_check quarantine row no
    longer carries candidate columns at all (only `row_hash`, §4.2), so
    neither an all-column nor a locator-keyed anti-join is possible — hash
    identity is the sole surviving key.

    `hashed_candidate_df` must already carry a `row_hash` column (the
    caller's own recompute, via `frames.quarantine.candidate_row_hash` — a
    UDF cost this bead deliberately pays only on this one rerun path, per
    A-7's cost claim); `durable_row_hashes_df` is the read-back durable
    quarantine rows for `(batch_id, "post_check")` (any extra columns are
    tolerated — only `row_hash` is read). A plain anti-join, NOT
    multiplicity-preserving like `violation_subtraction`: two candidate rows
    sharing one hash value both survive or both fall together against a
    single durable hash — an accepted, registered (006) limitation of this
    interim mechanism (005.1 §8.2.4/§15.2), not a silent gap. `row_hash`
    itself is dropped from the result so admitted rows keep the candidate's
    own shape, unchanged."""
    durable_hashes = durable_row_hashes_df.select(_HASH_COL).distinct()
    return hashed_candidate_df.join(durable_hashes, on=_HASH_COL, how="left_anti").drop(_HASH_COL)


@dataclass(frozen=True)
class CountIdentityCheck:
    """I-12's fresh-compute assertion, as a value: `candidate == admitted +
    violations`. Built from plain `int`s the stage already has — this
    function never calls `.count()` itself and never raises; `ok=False` is
    data the STAGE decides what to do with (fail loudly on the fresh-compute
    path, or WARNING + EMF `PostCheckDrift` on the [H-2] guard-skip path).
    """

    ok: bool
    candidate_count: int
    admitted_count: int
    violations_count: int


def check_count_identity(
    candidate_count: int, admitted_count: int, violations_count: int
) -> CountIdentityCheck:
    return CountIdentityCheck(
        ok=candidate_count == admitted_count + violations_count,
        candidate_count=candidate_count,
        admitted_count=admitted_count,
        violations_count=violations_count,
    )


# =============================================================================
# 005.1 §6.1-§6.5 — compiled checks (bead conveyer-azr.14, n1-checks)
# =============================================================================

_UFFFD = "�"

_FAILURES_COL = "_conveyer_admission_failures"
_TRUNCATE_AT = 32  # §6.4: reason_detail truncates at 32 entries + one marker

# §6.3's per-check struct shape, verbatim: `struct(code, column, expected)`
# — no `failed` field. Rather than carry a `failed` boolean INSIDE every
# struct (which `to_json` would then render on every surviving entry, at
# odds with §6.4's stated "(code, column, expected)" shape), each check's
# per-row value is EITHER this struct (the check failed on this row) OR a
# same-typed NULL (F.when(pred, struct(...)).otherwise(NULL)) — filtered by
# `x.isNotNull()` (`F.filter`, order-preserving). Behaviorally identical to
# "filter(array(struct(...)), x -> x.failed)"'s literal recipe, without a
# redundant field that would need stripping again downstream.
_FAILURE_STRUCT_TYPE = StructType(
    [
        StructField("code", StringType(), True),
        StructField("column", StringType(), True),
        StructField("expected", StringType(), True),
    ]
)


@dataclass(frozen=True, eq=False)
class CheckEntry:
    """One compiled check (§6.1 table row, one per applicable column).
    `eq=False`: a `pyspark.sql.Column`'s own `__eq__` builds a NEW `Column`
    (an equality EXPRESSION, not a `bool`) rather than comparing identity —
    the dataclass-generated `__eq__`/`__hash__` a bare `@dataclass(frozen=
    True)` would otherwise synthesize is actively wrong for a field of this
    type (`Column` is provably unhashable too, probe-verified). `eq=False`
    falls back to default (`object`) identity comparison, which is exactly
    what a compiled, driver-side-only value needs — nothing here ever
    compares two `CheckEntry`s for equality.

    `check_id` is §6.1's own "Check (id)" column verbatim (`"malformed-row"`,
    `"encoding-suspect"`, `f"cast:{declared type string}"`, `"not-nullable"`,
    `"allowed-values"`, `"pattern"`, `"bounds"`) — distinct from
    `reason_code` (the governed `unreadable/…`/`contract/…` code, §6.4/005
    §8.2); `column` is `None` for the two row-level checks (1-2); `expected`
    is the §6.4 class string this check contributes to a violating row's
    `reason_detail` (never a cell value); `predicate` is a lazy boolean
    `Column` — `True` on a given row means "this row FAILS this check".
    """

    check_id: str
    column: str | None
    reason_code: str
    expected: str
    predicate: Column


@dataclass(frozen=True, eq=False)
class CompiledContract:
    """`compile_contract`'s output: `entries` in §6.1's normative evaluation
    order (row-order major, contract-column-order minor — see
    `compile_contract`'s own docstring); `declared_columns` is the contract's
    own column order (§4.1 DDL order, `typed_projection`'s select list);
    `typed_exprs` is column name -> its ONE cast expression (D-5: the same
    `Column` value `evaluate`'s check-3 entries and `typed_projection` both
    key off of — "no second cast to disagree", 005 §7.2). `eq=False` for the
    same reason as `CheckEntry` (both `predicate` and every `typed_exprs`
    value are `Column`s)."""

    entries: tuple[CheckEntry, ...]
    declared_columns: tuple[str, ...]
    typed_exprs: Mapping[str, Column]


def _fmt(column_type: ColumnType) -> str:
    """`ColumnType.fmt` is `str | None` (only `date`/`timestamp` carry it);
    `parse_column_type`'s own contract guarantees it is non-`None` for
    those two kinds (asserted, not re-derived, by `core/model.py::
    ColumnSpec._check_type_semantics`'s own `assert`) — this narrows that
    guarantee for mypy at each of `_typed_expr`/`_literal_cast`'s two
    `date`/`timestamp` call sites, rather than repeating the same assert
    inline four times."""
    assert column_type.fmt is not None, (
        f"{column_type.kind} ColumnType always carries fmt (parse_column_type's own contract)"
    )
    return column_type.fmt


def _typed_expr(name: str, column_type: ColumnType) -> Column:
    """§6.2: the ONE cast expression for a declared column — reused, by
    reference to this same function, by check 3's castability predicate
    (`_cast_check`) and by `typed_projection`'s actual cast (D-5: "no second
    cast to disagree" — a row admitted is a row that cast, 005 §7.2).

    PySpark 3.5's `Column` API has no `.try_cast()` method (arrives in
    4.0), so numeric/bool typed-exprs are built via `F.expr("try_cast(...)
    ")` — expr text composed ONLY from `name` (grammar-validated:
    `core/model.py::COLUMN_NAME_RE`, already enforced on every
    `ColumnSpec.name` at spec-parse time) backtick-quoted by
    `core/merge.py::quote_identifier` (the §6.7-merge-render "doubled
    backtick" precedent — belt-and-braces even though `COLUMN_NAME_RE`
    already forbids a backtick outright) and the compiler's OWN rendering of
    `column_type` (never authored/free-form text) — never a raw f-string of
    unvalidated input. `F.expr` joins the string-SQL sink list
    (`tools/linter_configs/spine.py`) with `_typed_expr` as the one named
    `(file, function)` exemption (§12.6 item 2).

    `date`/`timestamp` need no `F.expr` detour at all — `F.to_date`/
    `F.to_timestamp` accept a `fmt` argument directly as a proper PySpark
    function call, not a string-SQL sink.

    **Recorded gap (discovered this bead, not fixed here — see this
    module's own docstring's "zero Spark execution" note and
    `_bounds_check`'s docstring for the compensating mechanism)**: §6.2's
    own prose claims "the compiler validates date/timestamp fmts by
    constructing the expressions (a malformed fmt fails at bind, pre-land)".
    Empirically FALSE for bare `Column` construction under Spark 3.5
    (probe-verified, bead conveyer-azr.14): `F.to_date(F.col(name), fmt)`
    with a syntactically-invalid `fmt` builds successfully and does not
    raise even through `.select(...)`/`.schema` — only an actual execution
    (`DataFrame.explain(True)`, or any real action) triggers the JVM's
    `SparkUpgradeException`/`IllegalArgumentException`. This module's own
    `frames-transforms` purity profile bans every Spark action/session
    attribute name (`collect`, `SparkSession`, `getOrCreate`, …) file-wide,
    so `compile_contract` cannot force that validation itself without
    either violating this file's own purity contract or accepting a live
    `SparkSession` as a smuggled-in effect (rejected: against `frames/`'s
    architectural promise regardless of whether the current attribute-name
    list happens to permit the literal spelling). `core/model.py`'s own
    best-effort alphabet check (`_datetime_fmt_alphabet_ok`) is the one
    gate that exists pre-runtime; this bead corrected that alphabet's own
    9-letter defect (pinned obligation #2) but a fmt combining otherwise-
    valid letters into an unsupported SEQUENCE (Spark's `DateTimeFormatter`
    is count-sensitive, e.g. `V` needs exactly `VV`) still only surfaces the
    first time `evaluate`/`typed_projection` genuinely executes against
    real batch data — reported as a discovered scope boundary, not silently
    left undocumented.
    """
    quoted = quote_identifier(name)
    col = F.col(name)
    if column_type.kind == "string":
        return col
    if column_type.kind == "date":
        return F.to_date(col, _fmt(column_type))
    if column_type.kind == "timestamp":
        return F.to_timestamp(col, _fmt(column_type))
    if column_type.kind == "decimal":
        return F.expr(f"try_cast({quoted} AS decimal({column_type.precision},{column_type.scale}))")
    # int | long — Spark's try_cast type-name spelling matches `ColumnType.
    # kind` verbatim for both; `bool` does NOT (Spark SQL's type name is
    # `boolean` — `try_cast(... AS bool)` is a `ParseException:
    # UNSUPPORTED_DATATYPE`, probe-verified, bead conveyer-azr.14 — caught
    # by A-15's own bool-token-set test, not assumed).
    sql_type_name = "boolean" if column_type.kind == "bool" else column_type.kind
    return F.expr(f"try_cast({quoted} AS {sql_type_name})")


def _literal_cast(value: str, column_type: ColumnType) -> Column:
    """Casts a `min`/`max` BOUND LITERAL (never a cell value) through the
    same-kind authoritative semantics `_typed_expr` gives a column, without
    routing through `_typed_expr` itself (which is column-name-shaped, not
    literal-shaped) or opening a second string-SQL sink (`F.expr` is
    exempted ONLY inside `_typed_expr`, §12.6 item 2 — this function must
    not need its own exemption):

    * `date`/`timestamp`: `F.to_date`/`F.to_timestamp` applied to `F.lit
      (value)` — the IDENTICAL JVM parser, same `fmt`, as the column's own
      `_typed_expr` — genuinely authoritative (005.1's pinned obligation
      #1: temporal min/max parseability is THIS module's job, not
      `core/model.py`'s, which only checked non-emptiness — see that
      module's own docstring). No `F.expr` needed at all.
    * `int` / `long` / `decimal`: `core/model.py`'s `ColumnSpec` validators
      (`_parse_plain_int`/`_parse_plain_decimal`) ALREADY guarantee, at
      spec-parse time, that `value` parses as a plain Python literal of the
      right kind and (when both bounds are present) that `min <= max` — so
      re-parsing it here with the SAME stdlib functions into a properly-
      typed `F.lit(...)` (never truncating, never a second interpreter: the
      identical `int()`/`Decimal()` calls `model.py` already used) is
      sufficient; no cast expression, no `F.expr`, needed.
    """
    if column_type.kind == "date":
        return F.to_date(F.lit(value), _fmt(column_type))
    if column_type.kind == "timestamp":
        return F.to_timestamp(F.lit(value), _fmt(column_type))
    if column_type.kind == "decimal":
        return F.lit(Decimal(value))
    return F.lit(int(value))  # int | long — model.py already guarantees `value` parses


def _malformed_row_check(contract: RawContractModel) -> CheckEntry:
    """§6.1 check 1: `malformed_text IS NOT NULL` (D-4's tier-2 flag,
    already durable on `raw_df` — this check only reads it, never
    re-derives raggedness). `expected` names the contract's own declared
    width as a class string (§6.4's own illustrative example,
    `"record(width=7)"`, is exactly this shape) — never a cell value."""
    return CheckEntry(
        check_id="malformed-row",
        column=None,
        reason_code="unreadable/malformed-row",
        expected=f"record(width={len(contract.columns)})",
        predicate=F.col("malformed_text").isNotNull(),
    )


def _encoding_suspect_check(contract: RawContractModel) -> CheckEntry:
    """§6.1 check 2 / A-13 [DC-9]: any declared column's raw (pre-cast,
    always-string, D-5) value, any `extras` VALUE or KEY, or `malformed_text`
    containing U+FFFD. `F.coalesce(..., F.lit(False))` around every
    single-column `.contains(...)` keeps a NULL cell from propagating NULL
    into the overall OR (a NULL declared column or a NULL `malformed_text`
    trivially contains no U+FFFD); `extras` is a non-nullable
    `map<string,string>` (§4.1 DDL) so its own `F.exists` calls need no such
    guard. Only compiled at all when `contract.forbid_replacement_chars` is
    `True` (the opt-out omits this ENTRY, not just its predicate, from
    `CompiledContract.entries` — §6.1's "row (unless opted out)")."""
    declared_hit: Column = F.lit(False)
    for column in contract.columns:
        declared_hit = declared_hit | F.coalesce(
            F.col(column.name).contains(F.lit(_UFFFD)), F.lit(False)
        )
    extras_values_hit = F.exists(F.map_values(F.col("extras")), lambda v: v.contains(F.lit(_UFFFD)))
    extras_keys_hit = F.exists(F.map_keys(F.col("extras")), lambda k: k.contains(F.lit(_UFFFD)))
    extras_hit = extras_values_hit | extras_keys_hit
    malformed_hit = F.coalesce(F.col("malformed_text").contains(F.lit(_UFFFD)), F.lit(False))
    return CheckEntry(
        check_id="encoding-suspect",
        column=None,
        reason_code="unreadable/encoding-suspect",
        expected="no-replacement-char",
        predicate=declared_hit | extras_hit | malformed_hit,
    )


def _cast_check(column: ColumnSpec, typed_expr: Column) -> CheckEntry:
    """§6.1 check 3 (non-string columns only — `compile_contract` skips
    `kind == "string"` before calling this): `col IS NOT NULL AND
    <typed-expr>(col) IS NULL` — a NULL raw cell is check 4's concern, not a
    cast failure (a cast NEVER runs on a cell that isn't there)."""
    raw = F.col(column.name)
    return CheckEntry(
        check_id=f"cast:{column.type}",
        column=column.name,
        reason_code="contract/cast-failure",
        expected=column.type,
        predicate=raw.isNotNull() & typed_expr.isNull(),
    )


def _not_nullable_check(column: ColumnSpec) -> CheckEntry:
    """§6.1 check 4 (`nullable: false` columns only): `col IS NULL` on the
    RAW column — the one check that does NOT skip NULLs (it exists
    precisely to find them), and the one bounds/cast-adjacent check that
    reads the raw, uncast value (a cell claim, §3.2)."""
    return CheckEntry(
        check_id="not-nullable",
        column=column.name,
        reason_code="contract/null-violation",
        expected="non-null",
        predicate=F.col(column.name).isNull(),
    )


def _allowed_values_check(column: ColumnSpec) -> CheckEntry:
    """§6.1 check 5 (`allowed_values` columns only): `col IS NOT NULL AND
    NOT col IN (<literals>)` — raw STRING comparison (§6.1's own words),
    never cast: the declared `allowed_values` strings are themselves
    RAW-typed literals (§3.2). `compile_contract` only calls this for a
    column with `allowed_values` truthy; the assert narrows that guarantee
    for mypy (`ColumnSpec.allowed_values: list[str] | None`)."""
    assert column.allowed_values is not None, "caller only compiles this check when set"
    raw = F.col(column.name)
    return CheckEntry(
        check_id="allowed-values",
        column=column.name,
        reason_code="contract/value-not-allowed",
        expected="allowed-values",
        predicate=raw.isNotNull() & ~raw.isin(*column.allowed_values),
    )


def _pattern_check(column: ColumnSpec) -> CheckEntry:
    """§6.1 check 6 (`pattern` columns only): `col IS NOT NULL AND NOT
    rlike(col, '\\A(?:…)\\z')` — fullmatch anchoring, LOWERCASE `\\z`
    ([DC-4]: Java's `\\Z` permits a trailing line terminator, so `rlike
    '\\A(?:foo)\\Z'` wrongly matches `"foo\\n"` — probe-verified, bead
    conveyer-azr.14 — exactly the trailing-newline bug class `core/merge.py
    ::_check_identifier`'s own `.fullmatch()`-over-`.match()` choice already
    fixed once elsewhere in this codebase). `rlike` is Java-regex-backed in
    Spark, matching `pattern`'s own normative grammar ([DC-4])."""
    raw = F.col(column.name)
    fullmatch = f"\\A(?:{column.pattern})\\z"
    return CheckEntry(
        check_id="pattern",
        column=column.name,
        reason_code="contract/pattern-mismatch",
        expected="pattern",
        predicate=raw.isNotNull() & ~raw.rlike(fullmatch),
    )


def _bounds_check(column: ColumnSpec, column_type: ColumnType, typed_expr: Column) -> CheckEntry:
    """§6.1 check 7 (`min`/`max` columns only): `<typed-expr>(col) outside
    the compile-time-cast literals`; NULL-safe (`typed_expr.isNotNull()`
    gates the whole predicate — a failed cast is check 3's finding, not a
    bounds finding, 005.1 §6.1's own words).

    **Discharges 005.1's pinned obligation #1** ("temporal min/max
    authoritative parseability AND ordering is `compile_contract`'s job"):
    `_literal_cast` casts each present bound through the SAME kind of
    authoritative mechanism `_typed_expr` gives the column (JVM `to_date`/
    `to_timestamp` for temporals — the exact gap `core/model.py`'s own
    docstring flags as unchecked there, [R2-5b]).

    **How "fail... on unparseable [bound] or min>max" is realized without
    Spark execution inside `compile_contract` (see `_typed_expr`'s own
    docstring for why execution is unavailable here)** — two properties of
    the predicate below, both probe-verified (bead conveyer-azr.14) rather
    than assumed:

    1. **min > max is a TAUTOLOGY of the standard "outside bounds"
       predicate, with zero extra logic.** For any cast value `V` and
       `min_cast > max_cast`: either `V < min_cast`, or `V` sits strictly
       between the (inverted) bounds — in which case `V < min_cast` is
       STILL true — or `V >= min_cast > max_cast`, so `V > max_cast`. Every
       value flags. This is exactly what §6.1's normative comparison
       already gives for free once `min_cast`/`max_cast` are correctly
       ordered by construction (numeric/decimal min<=max is enforced at
       spec-parse, `core/model.py`; temporal min<=max is NOT — this is the
       case that actually needs it, and gets it for free).
    2. **An unparseable BOUND (`min_cast`/`max_cast` casts to NULL — only
       reachable for `date`/`timestamp`, since numeric bounds are
       pre-validated) would otherwise silently vanish via SQL's 3-valued
       NULL propagation** (`V < NULL` is NULL, not `True` — a naive
       predicate would silently stop flagging bounds violations for rows on
       the "valid" side of the OTHER bound, the exact silent gap the pinned
       obligation exists to close). Closed by `min_cast.isNull() |
       max_cast.isNull()` joining the disjunction explicitly: an
       unparseable bound makes EVERY non-null cast row a bounds violation —
       a loud, deterministic, unmistakable signal (100% quarantine of that
       column) the FIRST time this compiled check runs, in keeping with
       this codebase's "errors are data, quarantine, never a silent no-op"
       discipline (`de-developer` skill) even where a raised exception
       before any row is read is architecturally unavailable to this
       module. Recorded, not silently substituted: see `_typed_expr`'s
       docstring for the precise gap this leaves (a genuinely malformed
       FMT STRING, as opposed to an unparseable bound VALUE under an
       otherwise-valid fmt, still only surfaces at real execution).
    """
    below: Column = F.lit(False)
    above: Column = F.lit(False)
    invalid: Column = F.lit(False)
    if column.min is not None:
        min_cast = _literal_cast(column.min, column_type)
        invalid = invalid | min_cast.isNull()
        below = typed_expr < min_cast
    if column.max is not None:
        max_cast = _literal_cast(column.max, column_type)
        invalid = invalid | max_cast.isNull()
        above = typed_expr > max_cast
    predicate = typed_expr.isNotNull() & (invalid | below | above)
    return CheckEntry(
        check_id="bounds",
        column=column.name,
        reason_code="contract/out-of-bounds",
        expected="bounds",
        predicate=predicate,
    )


def compile_contract(contract: RawContractModel) -> CompiledContract:
    """§6.1: `contract` -> a frozen `CompiledContract` in NORMATIVE
    evaluation order — the §6.1 table's row order MAJOR, contract column
    order MINOR (i.e., every check-1 entry, then every check-2 entry, ...
    then every check-7 entry, each kind iterating `contract.columns` in
    its OWN declared order) — never the reverse nesting. Check 2 is omitted
    entirely when `contract.forbid_replacement_chars` is `False` (the
    opt-out, §6.1). Check 3 (cast) skips `kind == "string"` columns (never
    fails — identity typed-expr). Checks 4-7 only compile for columns that
    actually declare the relevant constraint (`nullable: false`,
    `allowed_values`, `pattern`, `min`/`max`). The `required` header check
    is NOT here (§6.1: it ran at land, 005 §7.1 — a header-presence claim,
    not a durable-row expression).

    Zero Spark execution: every `Column` built (directly or via
    `_typed_expr`/`_literal_cast`) is a lazy, unbound expression tree —
    `compile_contract` never calls `.collect()`/`.explain()`/any DataFrame
    action, consistent with this file's `frames-transforms` purity profile
    (which bans the Spark-session/action attribute-name surface file-wide)
    and with E-5 (a compiled predicate is a pure function of `(durable raw
    schema, contract@v)`).
    """
    typed_exprs: dict[str, Column] = {
        column.name: _typed_expr(column.name, parse_column_type(column.type))
        for column in contract.columns
    }
    entries: list[CheckEntry] = [_malformed_row_check(contract)]
    if contract.forbid_replacement_chars:
        entries.append(_encoding_suspect_check(contract))
    for column in contract.columns:  # check 3: cast (non-string only)
        if parse_column_type(column.type).kind != "string":
            entries.append(_cast_check(column, typed_exprs[column.name]))
    for column in contract.columns:  # check 4: not-nullable
        if not column.nullable:
            entries.append(_not_nullable_check(column))
    for column in contract.columns:  # check 5: allowed-values
        if column.allowed_values:
            entries.append(_allowed_values_check(column))
    for column in contract.columns:  # check 6: pattern
        if column.pattern:
            entries.append(_pattern_check(column))
    for column in contract.columns:  # check 7: bounds
        if column.min is not None or column.max is not None:
            column_type = parse_column_type(column.type)
            entries.append(_bounds_check(column, column_type, typed_exprs[column.name]))
    return CompiledContract(
        entries=tuple(entries),
        declared_columns=tuple(column.name for column in contract.columns),
        typed_exprs=MappingProxyType(typed_exprs),
    )


def cast_failure_predicate(compiled: CompiledContract) -> Column:
    """§6.1 check 3's own predicate (`_cast_check` above), re-derived as a
    single OR across every declared column's `typed_exprs` entry — exported
    (critique nit c, bead conveyer-azr.30) so a caller outside this module
    (`stages/pre_check.py`'s own drift probe, `_admitted_cast_failure_count`)
    reuses the SAME authored predicate rather than a second, independently
    re-derived copy of check 3's own logic (D-5: "no second cast/predicate
    to disagree" — the exact discipline `_typed_expr`'s own docstring
    already claims for the cast expression itself; this closes the same gap
    one level up, for the PREDICATE built from it). A `string`-kind column's
    `typed_exprs` entry is the identity, so its own disjunct
    (`raw.isNotNull() & raw.isNull()`) is always `False` — harmless, no
    special-casing needed, matching `_cast_check`'s own note."""
    predicate: Column = F.lit(False)
    for name, typed_expr in compiled.typed_exprs.items():
        predicate = predicate | (F.col(name).isNotNull() & typed_expr.isNull())
    return predicate


def evaluate(raw_df: DataFrame, compiled: CompiledContract) -> DataFrame:
    """§6.3: `raw_df` + the ONE internal `_conveyer_admission_failures`
    column — `array<struct<code, column, expected>>`, evaluation order,
    containing exactly the entries whose `predicate` is `True` on that row
    (see `_FAILURE_STRUCT_TYPE`'s own comment for why a `.otherwise(NULL)` +
    `isNotNull()` filter, not a stored `failed` field, realizes §6.3's
    literal "filter(array(struct(...)), x -> x.failed)" recipe).
    `F.filter` (`array_filter`) preserves element order, so the surviving
    array is still evaluation-order — `reason_code` (`violations`, below)
    can trust "first entry" without a separate sort. One compiled
    predicate; `typed_projection`/`violations` are its two projections
    (§6.3) — the internal column is dropped from BOTH before they leave
    this module (`typed_projection` never selects it; `violations` drops it
    explicitly)."""
    per_check = [
        F.when(
            entry.predicate,
            F.struct(
                F.lit(entry.reason_code).alias("code"),
                F.lit(entry.column).alias("column"),
                F.lit(entry.expected).alias("expected"),
            ),
        ).otherwise(F.lit(None).cast(_FAILURE_STRUCT_TYPE))
        for entry in compiled.entries
    ]
    failures = F.filter(F.array(*per_check), lambda x: x.isNotNull())
    return raw_df.withColumn(_FAILURES_COL, failures)


def zero_failures(evaluated_df: DataFrame) -> DataFrame:
    """§6.6's genuinely-fresh path: `evaluate()`'s output filtered to rows
    with an EMPTY `_conveyer_admission_failures` array — the stage-level
    filter `typed_projection`'s own docstring names ("the fresh-path
    zero-failures filter is the stage's job, between evaluate and
    typed_projection") but that the stage cannot express directly itself,
    since the internal failures column is private to this module. The
    internal column is left in place here (unlike `violations`, which drops
    it) — callers pass this function's output straight into
    `typed_projection`, a plain `select` that never references it anyway;
    dropping it here would just be a second, redundant `.drop()`."""
    return evaluated_df.filter(F.size(F.col(_FAILURES_COL)) == 0)


def typed_projection(df: DataFrame, compiled: CompiledContract) -> DataFrame:
    """§6.2: declared columns ONLY, each cast by the SAME `typed_exprs`
    entry `evaluate`'s check-3 predicates use (D-5, "no second cast to
    disagree") — extras, flags, locators, lineage, and the internal
    `_conveyer_admission_failures` column (if present) are all excluded by
    construction (a plain `select`, never a `select *`).

    **Deliberately does NOT filter rows by failure state — on ANY input,
    fresh-evaluated or not.** This is what makes the SAME function correct
    on every 005.1 §6.5 door: on the genuinely-fresh path (§6.6), the STAGE
    filters `evaluate`'s output to `F.size(_conveyer_admission_failures) ==
    0` BEFORE calling this function (a separate, stage-level concern — see
    this module's own header docstring: the "two projections" §6.3
    describes are `typed_projection` + `violations`, not "this function
    does its own filtering AND casting"); on the door-2/door-3 rerun paths
    (§6.5), the stage calls this function directly on `raw_df` minus
    flagged/anti-joined rows, with NO `evaluate()` step at all — and 005.1
    is explicit that a rerun row whose cell fails the CURRENT contract's
    cast must stay in `valid_df` with a NULL cell, "recorded, never
    dropped" (§6.5) — a filtering `typed_projection` would silently violate
    that letter on exactly the paths that need it most.
    """
    projected = [compiled.typed_exprs[name].alias(name) for name in compiled.declared_columns]
    return df.select(*projected)


def violations(evaluated_df: DataFrame) -> DataFrame:
    """§6.4: `evaluated_df` (an `evaluate()` output) filtered to rows with
    at least one failure, with `reason_code`/`reason_detail` columns added
    and the internal `_conveyer_admission_failures` column dropped. Every
    OTHER column `evaluated_df` carries (locators, `extras`,
    `malformed_text`, declared raw columns, lineage, …) passes through
    unchanged — this function does not shape the full §4.2 quarantine row
    (that is `frames/quarantine.py`'s job, a later bead); it only computes
    the two reason columns 005 §8.1's grain assigns to pre_check itself.

    No `CompiledContract` parameter: every value `reason_code`/
    `reason_detail` need (`code`, `column`, `expected` per surviving
    failure) is already baked into `_conveyer_admission_failures` as
    compile-time-fixed literals — nothing here re-consults the compiled
    entries.

    * `reason_code` = the first entry's `code` (evaluation order IS the
      declared order, §6.3/005 §8.1 — `F.element_at(arr, 1)`, 1-based).
    * `reason_detail` = deterministic `to_json` of the full failures array
      — no map, no sorting problem (`code`/`column`/`expected` are
      compile-time-fixed struct fields, A-7) — truncated at 32 entries with
      a final `{"truncated": <n_total>}` marker element (§6.4). The marker
      is realized by widening EVERY entry (real or marker) to a shared,
      enriched `struct<code, column, expected, truncated>` where exactly
      one of "the three real fields" or "`truncated`" is non-NULL per
      element — `to_json`'s own default null-field omission (probe-
      verified, bead conveyer-azr.14: `to_json` never emits a `null`-valued
      key) renders a real entry as `{"code":…,"column":…,"expected":…}`
      (or, for a row-level check, `{"code":…,"expected":…}` — `column`
      itself omitted when NULL, not shown as `"column":null`) and the
      marker as exactly `{"truncated": <n>}` — §6.4's literal shape, with
      zero string-surgery on the rendered JSON text.
    """
    failures = F.col(_FAILURES_COL)
    reshaped = F.transform(
        failures,
        lambda x: F.struct(
            x["code"].alias("code"),
            x["column"].alias("column"),
            x["expected"].alias("expected"),
            F.lit(None).cast(IntegerType()).alias("truncated"),
        ),
    )
    total = F.size(failures)
    truncated_slice = F.slice(reshaped, 1, _TRUNCATE_AT)
    marker = F.array(
        F.struct(
            F.lit(None).cast(StringType()).alias("code"),
            F.lit(None).cast(StringType()).alias("column"),
            F.lit(None).cast(StringType()).alias("expected"),
            total.alias("truncated"),
        )
    )
    detail_array = F.when(total > _TRUNCATE_AT, F.concat(truncated_slice, marker)).otherwise(
        reshaped
    )
    shaped = (
        evaluated_df.filter(F.size(failures) > 0)
        .withColumn("reason_code", F.element_at(failures, 1).getField("code"))
        .withColumn("reason_detail", F.to_json(detail_array))
    )
    return shaped.drop(_FAILURES_COL)


_LOCATOR_COLUMNS: tuple[str, str, str] = ("source_uri", "object_seq", "row_index")


def locator_subtraction(raw_df: DataFrame, durable_locators: DataFrame) -> DataFrame:
    """§6.5 door 3's PURE anti-join half (the stage composes the rest: the
    current-contract `typed_projection` call and the drift probe are NOT
    this function's job). `raw_df` filtered to `malformed_text IS NULL`,
    anti-joined against `durable_locators` (defensively re-projected to
    just the locator triple here, so a caller need not pre-select it) on
    `(source_uri, object_seq, row_index)` — durable rows authoritative.
    Locator-KEYED (not bag/value-keyed like `violation_subtraction`) is
    available here — unlike post_check's own rerun subtraction — because
    every pre_check quarantine row carries a non-null, unique locator
    (D-9): no multiplicity-preservation concern arises."""
    return raw_df.filter(F.col("malformed_text").isNull()).join(
        durable_locators.select(*_LOCATOR_COLUMNS), on=list(_LOCATOR_COLUMNS), how="left_anti"
    )
