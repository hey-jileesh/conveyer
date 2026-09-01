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
`.master(...)` — the identical idiom `entrypoints/glue_main.py::_build_
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
}
_G08_ROW = (
    5,
    0,
    Decimal("1.20"),
    Decimal("1.2"),
    "hello",
    datetime(2026, 1, 2, 3, 4, 5),
    datetime(2026, 1, 5, 3, 4, 5),
)


@dataclass(frozen=True)
class ParityVector:
    """One discriminator row. `kind="dtype"` compares the compiled column's
    `dataType.typeName()` instead of its value (the int/int and decimal/
    decimal division-result-type rows). `raw=True` bypasses `check_grammar.
    validate_expression` and hands `expr` to `F.expr` directly — reserved
    for the handful of cases that are deliberately OUTSIDE the grammar
    (`bround`'s negative control; the bare `CAST(NULL AS int) <=> ...`
    null-safe-equality probe, which uses `CAST` outside the typed-literal
    shape §6.1 restricts it to)."""

    case_id: str
    expr: str
    expected: object
    kind: Literal["value", "dtype"] = "value"
    raw: bool = False


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

G08_VECTORS: tuple[ParityVector, ...] = _VALUE_VECTORS + _SUPPLEMENTARY_VECTORS


@dataclass(frozen=True)
class ParityResult:
    case_id: str
    expr: str
    expected: object
    actual: object
    passed: bool
    error: str | None


def _build_probe_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([_G08_ROW], _G08_SCHEMA)


def _authored_text(vector: ParityVector) -> str | None:
    """The text to hand `F.expr`, or `None` if grammar-gated and rejected
    (a probe-level failure in its own right — the deployed engine's grammar
    gate must reject exactly what the local one does)."""
    if vector.raw:
        return vector.expr
    validated = cg.validate_expression(vector.expr, "scalar", _G08_FAMILY)
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
    if vector.kind == "dtype":
        actual: object = df.select(compiled).schema["v"].dataType.typeName()
    else:
        actual = df.select(compiled).collect()[0]["v"]
    return ParityResult(
        vector.case_id, vector.expr, vector.expected, actual, actual == vector.expected, None
    )


def run_probe(spark: SparkSession) -> list[ParityResult]:
    """Pure(-ish) evaluator over an already-live `SparkSession` — the seam
    `tests/unit/test_g08_parity_probe.py` calls directly against the shared
    `spark` fixture, and the same function `main()` below calls against a
    freshly built (or Glue-adopted) session."""
    df = _build_probe_df(spark)
    return [_evaluate(df, vector) for vector in G08_VECTORS]


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
    glue_main.py::_build_session` documents: on Glue, the master is already
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
