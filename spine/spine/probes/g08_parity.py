"""G-08 Glue-parity probe — 006.1 LLD §13.1 (G-08) / §14 B5, the 005.1 N5
idiom (`conveyer-azr.22`'s "probe job re-runs A-15 discriminator rows")
applied to G-08's own discriminator rows (bead `conveyer-6pg.15`/`.16`).

`G08_VECTORS` below **is** G-08's normative executable allowlist semantics
table (006.1 §13.1, §6.4's CI pin of the allowlist) — the single, canonical
enumeration, since this module ships in the wheel (`pyproject.toml`'s
`packages = ["spine", "pipelines"]`). `tests/frames/test_business_checks.
py::test_g08_executable_semantics_table` (Critique gate wf_24a3125f-ecc F3,
bead conveyer-6pg.32) IMPORTS this table and this module's own `_evaluate`
directly, proving G-08's executable allowlist semantics against a LOCAL
pyspark session (`local[2]`, `frames.checks.SESSION_PINS`) — that confirms
the grammar and the compiled `Column` expressions agree with themselves
under one engine; it does **not** confirm the same semantics hold under
Glue 5.0's real Spark 3.5.x runtime (a different JVM, a different set of
installed jars, a possibly-different ICU/timezone database) — the
deploy-time claim only a live Glue job run can settle. That live run is
`conveyer-6pg.16` (B5-gate, human-supervised, LEAVE OPEN); this module is the
STANDALONE probe it runs — no AWS mutation, no table I/O, no `spine.config`/
`spine.context`/`RunnerFx` dependency, so it can execute unmodified on either
engine (it never imports FROM `tests/`, only the other direction).

`main()` builds its own `SparkSession` (`_build_session`, deliberately no
`.master(...)` — the identical idiom `entrypoints/session.py::build_
session` documents: production Glue already provides a master; a bare local
invocation falls back to pyspark's own `local[*]` default with no extra
flags needed, verified empirically in the kernel), evaluates every vector in
`G08_VECTORS`, prints one `PASS`/`FAIL` line per case plus a summary, and
raises `SystemExit(1)` on any failure — so an operator's shell (or a CI
runner, once this graduates beyond a manual gate step) can gate on the exit
code alone, matching `tests/smoke/test_smoke.py`'s own "fails loudly, never
hangs" posture.

**Prior to the F3 fix, this table was a hand-maintained byte-identical
duplicate of `test_business_checks.py`'s own `_G08_CASES`** (kept in sync
by cross-reference only, the same class of risk `entrypoints/glue_main.py::
_assert_patterns_compile_in_jvm`'s own duplicated regex template still
accepts for an unrelated pair) — the critique gate ruled that acceptable
for THAT pair (genuinely two independent artifacts, a JVM regex string and
a Python one) but not for this one (one semantic enumeration, forked into
two literal Python tuples that could silently drift apart the moment a row
was added test-side and forgotten probe-side, or vice versa) — P-1's
two-sources-for-one-enumeration class, the exact thing G-08 itself exists
to police. `tests/unit/test_g08_parity_probe.py` exercises this module's
own table and evaluator directly (session-scoped `spark` fixture), which is
this probe's local rehearsal: it proves the runner and its vectors are
self-consistent under local Spark, the closest local approximation to the
real Glue-parity claim only B5-gate can close.

Deliberately **excluded**: `test_business_checks.py::test_g08_bround_is_
not_grammar_admitted` (asserts `cg.validate_expression("bround(2.5)", ...)`
returns a `GrammarDefect`) — that is a pure-Python `sqlglot` parse outcome
with no JVM/Spark involvement at all, so it carries no cross-engine parity
signal; re-running it here would only re-prove this module's own Python
runs on whichever machine executes it, not anything about Glue's engine.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spine import observability
from spine.core import check_grammar as cg
from spine.frames.checks import SESSION_PINS

# --- the probe row/schema -- the canonical copy (F3, module docstring);
# `test_business_checks.py` imports these directly rather than keeping its
# own copy -------------------------------------------------------------

_G08_SCHEMA = StructType(
    [
        StructField("i", IntegerType(), True),
        StructField("j", IntegerType(), True),
        StructField("dec", DecimalType(10, 2), True),
        StructField("dec2", DecimalType(10, 2), True),
        StructField("s", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("ts2", TimestampType(), True),
        # A006-4: decimal(38,0), precision-CAPPED -- the sole purpose of this
        # column is `sum-decimal38-overflow-is-null-ansi-off` below (two
        # 38-nines rows sum to a 39-digit value, which cannot widen further
        # under the 38-digit cap; ANSI-off resolves that overflow to NULL
        # rather than raising). Defaulted to `Decimal("0")` in `_G08_ROW` so
        # every EXISTING scalar-position row (none of which reference this
        # column) is untouched.
        StructField("dec38", DecimalType(38, 0), True),
    ]
)
_G08_FAMILY: dict[str, cg.Family | None] = {
    "i": "numeric",
    "j": "numeric",
    "dec": "numeric",
    "dec2": "numeric",
    "s": "string",
    "ts": "temporal",
    "ts2": "temporal",
    "dec38": "numeric",
}
_G08_ROW = (
    5,
    0,
    Decimal("1.20"),
    Decimal("1.2"),
    "hello",
    datetime(2026, 1, 2, 3, 4, 5),
    datetime(2026, 1, 5, 3, 4, 5),
    Decimal("0"),
)
_G08_COLUMNS = ("i", "j", "dec", "dec2", "s", "ts", "ts2", "dec38")


def _row(**overrides: object) -> tuple[object, ...]:
    """`_G08_ROW` with named column overrides -- the aggregate vectors'
    OWN probe rows (§7.5/A006-4) need to vary exactly one or two columns
    (a NULL `i`, a 38-nines `dec38`) while leaving the rest at their
    ordinary default, without hand-repeating the whole 8-tuple per row."""
    base = dict(zip(_G08_COLUMNS, _G08_ROW, strict=True))
    base.update(overrides)
    return tuple(base[name] for name in _G08_COLUMNS)


@dataclass(frozen=True)
class ParityVector:
    """One discriminator row. `kind="dtype"` compares the compiled column's
    `dataType.typeName()` instead of its value (the int/int and decimal/
    decimal division-result-type rows). `raw=True` bypasses `check_grammar.
    validate_expression` and hands `expr` to `F.expr` directly — reserved
    for the handful of cases that are deliberately OUTSIDE the grammar
    (`bround`'s negative control; the bare `CAST(NULL AS int) <=> ...`
    null-safe-equality probe, which uses `CAST` outside the typed-literal
    shape §6.1 restricts it to). `group=True` (A006-4, §6.3's aggregate
    position): validates `expr` at `"aggregate"` position and evaluates it
    via `df.agg(...)` rather than `df.select(...)` -- the shape every
    aggregate-position member requires (a single reduced row, not one row
    per input row). `rows` overrides the probe frame this vector runs
    against: `None` (the default) is the shared single-row `_G08_ROW`
    frame every scalar-position vector uses; `()` builds a genuinely EMPTY
    frame (the empty-set aggregate discriminators); a non-empty tuple
    supplies exactly those rows (e.g. one NULL-valued row alongside one
    concrete row, for a NULL-skip discriminator)."""

    case_id: str
    expr: str
    expected: object
    kind: Literal["value", "dtype"] = "value"
    raw: bool = False
    group: bool = False
    rows: tuple[tuple[object, ...], ...] | None = None


# G-08's executable allowlist semantics table, scalar position (A-15 idiom)
# -- the canonical copy (F3, module docstring).
_VALUE_VECTORS: tuple[ParityVector, ...] = (
    ParityVector("div-by-zero-ansi-off", "i / 0", None),
    ParityVector("mod-by-zero-ansi-off", "i % 0", None),
    ParityVector("round-half-up-half", "round(2.5)", Decimal("3")),
    ParityVector("round-half-up-scale2", "round(1.245, 2)", Decimal("1.25")),
    ParityVector("decimal-scale-insensitive-eq", "dec = dec2", True),
    ParityVector("year-under-utc", "year(ts)", 2026),
    ParityVector("month", "month(ts)", 1),
    ParityVector("day", "day(ts)", 2),
    ParityVector("datediff-over-timestamps", "datediff(ts2, ts)", 3),
    ParityVector("date-add-implicit-cast-to-date", "date_add(ts, 3)", date(2026, 1, 5)),
    ParityVector("date-sub-implicit-cast-to-date", "date_sub(ts, 3)", date(2025, 12, 30)),
    ParityVector("abs", "abs(i - 10)", 5),
    ParityVector("floor", "floor(1.7)", Decimal("1")),
    ParityVector("ceil", "ceil(1.2)", Decimal("2")),
    ParityVector("greatest", "greatest(1, 2, 3)", 3),
    ParityVector("least", "least(1, 2, 3)", 1),
    ParityVector("length", "length(s)", 5),
    ParityVector("trim", "trim(concat('  ', s, '  '))", "hello"),
    ParityVector("ltrim", "ltrim(concat('  ', s))", "hello"),
    ParityVector("rtrim", "rtrim(concat(s, '  '))", "hello"),
    ParityVector("upper", "upper(s)", "HELLO"),
    ParityVector("lower", "lower(s)", "hello"),
    ParityVector("substring", "substring(s, 1, 3)", "hel"),
    ParityVector("concat", "concat(s, '!')", "hello!"),
    ParityVector("replace", "replace(s, 'l', 'L')", "heLLo"),
    ParityVector("coalesce", "coalesce(NULL, s)", "hello"),
    ParityVector("nvl", "nvl(NULL, s)", "hello"),
    ParityVector("nullif-equal-is-null", "nullif(s, 'hello')", None),
    ParityVector("case-when", "CASE WHEN i > 0 THEN 'pos' ELSE 'nonpos' END", "pos"),
    ParityVector("in-list-membership", "i IN (5, 6, 7)", True),
    ParityVector("between", "i BETWEEN 1 AND 10", True),
    ParityVector("like", "s LIKE 'hel%'", True),
    ParityVector("null-safe-eq-differ", "i <=> j", False),
    ParityVector("is-null", "j IS NULL", False),
    ParityVector("and", "i > 0 AND j >= 0", True),
    ParityVector("or", "i > 100 OR j = 0", True),
    ParityVector("not", "NOT (i > 100)", True),
    ParityVector("unary-minus", "-i < 0", True),
)

# Supplementary discriminator rows — the four standalone assertions in
# `test_business_checks.py` (dtype rows + the round/bround [EM-8] negative
# control) folded into the same vector shape.
_SUPPLEMENTARY_VECTORS: tuple[ParityVector, ...] = (
    ParityVector(
        "null-safe-eq-both-null", "CAST(NULL AS int) <=> CAST(NULL AS int)", True, raw=True
    ),
    ParityVector("row-position-int-div-int-is-double", "i / j", "double", kind="dtype"),
    ParityVector("decimal-division-stays-decimal", "dec / dec2", "decimal", kind="dtype"),
    ParityVector("round-half-up-no-scale-arg", "round(1.245)", Decimal("1")),
    ParityVector("bround-bankers-half", "bround(2.5)", Decimal("2"), raw=True),
    ParityVector("bround-bankers-scale2", "bround(1.245, 2)", Decimal("1.24"), raw=True),
    ParityVector("bround-bankers-no-scale-arg", "bround(1.245)", Decimal("1"), raw=True),
)

# A006-4: §6.3's aggregate-position engine rows -- these need neither
# `compile_aggregate` nor the 005 v1.x member grammar (P-6/K7's own
# dormancy, which blocks the STRUCTURAL-compile-vs-`F.expr` fidelity claim
# only, D006-1's own single remaining row); `_evaluate` already hands
# `authored_text` to `F.expr`, so a `group=True` vector is exactly as
# reachable as every scalar-position row above. This is also the engine
# witness `entrypoints/glue_main.py::_assert_aggregate_dtype_exact` rests
# on (K5).
_ROWS_TWO_DEFAULT: tuple[tuple[object, ...], ...] = (_row(), _row(i=7))
_ROWS_ONE_NULL_I: tuple[tuple[object, ...], ...] = (_row(i=5), _row(i=None))
_ROWS_ALL_NULL_I: tuple[tuple[object, ...], ...] = (_row(i=None), _row(i=None))
_DEC38_NINES = Decimal("9" * 38)
_ROWS_DEC38_OVERFLOW: tuple[tuple[object, ...], ...] = (
    _row(dec38=_DEC38_NINES),
    _row(dec38=_DEC38_NINES),
)

_AGGREGATE_VECTORS: tuple[ParityVector, ...] = (
    ParityVector("count-1-counts-rows", "count(1)", 2, group=True, rows=_ROWS_TWO_DEFAULT),
    ParityVector("count-col-skips-null", "count(i)", 1, group=True, rows=_ROWS_ONE_NULL_I),
    ParityVector("sum-empty-is-null", "sum(i)", None, group=True, rows=()),
    ParityVector("min-empty-is-null", "min(i)", None, group=True, rows=()),
    ParityVector("avg-decimal-stays-decimal", "avg(dec)", "decimal", kind="dtype", group=True),
    ParityVector("avg-int-is-double", "avg(i)", "double", kind="dtype", group=True),
    ParityVector(
        "sum-int-div-count-is-double", "sum(i) / count(1)", "double", kind="dtype", group=True
    ),
    ParityVector(
        "sum-decimal-div-count-is-decimal",
        "sum(dec) / count(1)",
        "decimal",
        kind="dtype",
        group=True,
    ),
    ParityVector("sum-all-null-column-is-null", "sum(i)", None, group=True, rows=_ROWS_ALL_NULL_I),
    ParityVector(
        "sum-decimal38-overflow-is-null-ansi-off",
        "sum(dec38)",
        None,
        group=True,
        rows=_ROWS_DEC38_OVERFLOW,
    ),
)

G08_VECTORS: tuple[ParityVector, ...] = _VALUE_VECTORS + _SUPPLEMENTARY_VECTORS + _AGGREGATE_VECTORS


@dataclass(frozen=True)
class ParityResult:
    case_id: str
    expr: str
    expected: object
    actual: object
    passed: bool
    error: str | None


def _build_probe_df(
    spark: SparkSession, rows: tuple[tuple[object, ...], ...] | None = None
) -> DataFrame:
    """The default (scalar-position) probe frame is the single shared
    `_G08_ROW`; `rows` (A006-4) overrides it -- `()` builds a genuinely
    EMPTY frame (`createDataFrame` accepts an empty list given an explicit
    schema), a non-empty tuple supplies exactly those rows."""
    data = [_G08_ROW] if rows is None else list(rows)
    return spark.createDataFrame(data, _G08_SCHEMA)


def _probe_df_for(spark: SparkSession, vector: ParityVector) -> DataFrame:
    """The frame `vector` should be evaluated against -- shared by
    `run_probe` and `tests/frames/test_business_checks.py`'s own local
    rehearsal, so both build the SAME per-vector frame (an aggregate vector
    naming `rows=()`/a NULL-laden override must never silently fall back to
    the shared single-row scalar frame)."""
    return _build_probe_df(spark, vector.rows)


def _authored_text(vector: ParityVector) -> str | None:
    """The text to hand `F.expr`, or `None` if grammar-gated and rejected
    (a probe-level failure in its own right — the deployed engine's grammar
    gate must reject exactly what the local one does). `group=True` vectors
    validate at `"aggregate"` position (§6.3) -- every `group=True` row in
    `G08_VECTORS` is, by construction, an aggregate-position member."""
    if vector.raw:
        return vector.expr
    position: cg.Position = "aggregate" if vector.group else "scalar"
    validated = cg.validate_expression(vector.expr, position, _G08_FAMILY)
    if not isinstance(validated, cg.ValidatedExpr):
        return None
    return validated.authored_text


def _evaluate(df: DataFrame, vector: ParityVector) -> ParityResult:
    text = _authored_text(vector)
    if text is None:
        return ParityResult(
            vector.case_id, vector.expr, vector.expected, None, False, "grammar rejected"
        )
    compiled = F.expr(text).alias("v")
    reduced = df.agg(compiled) if vector.group else df.select(compiled)
    if vector.kind == "dtype":
        actual: object = reduced.schema["v"].dataType.typeName()
    else:
        actual = reduced.collect()[0]["v"]
    return ParityResult(
        vector.case_id, vector.expr, vector.expected, actual, actual == vector.expected, None
    )


def run_probe(spark: SparkSession) -> list[ParityResult]:
    """Pure(-ish) evaluator over an already-live `SparkSession` — the seam
    `tests/unit/test_g08_parity_probe.py` calls directly against the shared
    `spark` fixture, and the same function `main()` below calls against a
    freshly built (or Glue-adopted) session. Each vector gets its OWN frame
    (`_probe_df_for`) -- most share the one default scalar-position frame,
    but the aggregate-position vectors (A006-4) each name their own `rows`
    override (including the genuinely empty frame, `rows=()`)."""
    return [_evaluate(_probe_df_for(spark, vector), vector) for vector in G08_VECTORS]


def _print_report(results: list[ParityResult]) -> bool:
    n_failed = 0
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        if not result.passed:
            n_failed += 1
        suffix = f" error={result.error}" if result.error else ""
        print(  # noqa: T201 -- operator-readable CLI/Glue-log report, not a data-path log line
            f"[{status}] {result.case_id}: {result.expr!r} -> actual={result.actual!r} "
            f"expected={result.expected!r}{suffix}"
        )
    print(  # noqa: T201
        f"g08-parity-probe: {len(results) - n_failed}/{len(results)} discriminator rows passed "
        f"on this engine ({n_failed} failed)."
    )
    return n_failed == 0


def _build_session() -> SparkSession:
    """No `.master(...)` set, deliberately — the same idiom `entrypoints/
    session.py::build_session` documents: on Glue, the master is already
    configured by the job's own bootstrap; run standalone (no active
    session/context anywhere in the process), pyspark falls back to its own
    `local[*]` default with no extra flags needed (verified in the kernel,
    `conveyer-6pg.15`)."""
    builder = SparkSession.builder.appName("conveyer-spine-g08-parity-probe")
    for key, value in SESSION_PINS.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def main() -> None:
    observability.install_json_handler()
    spark = _build_session()
    spark.sparkContext.setLogLevel("ERROR")
    results = run_probe(spark)
    all_passed = _print_report(results)
    if not all_passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
