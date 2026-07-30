"""MUST-FAIL: an f-string flowing into `F.expr(...)` -- the string-SQL review
rule (§12.6 item 2: `expr` joins the `frames-transforms` profile's
`string_sql_sinks`, bead conveyer-azr.14). `frames/checks.py::_typed_expr` is
the ONE hardcoded `(file, function)` exemption for this exact shape (it
composes `F.expr` text only from a grammar-validated, backtick-quoted column
name and the compiler's own type rendering, never a raw column VALUE) --
this fixture's function is deliberately named something else, so the
exemption does NOT reach it: a hostile/malformed cell value threaded
straight into `F.expr` here would smuggle SQL into the cast expression,
exactly what the sink review rule exists to catch.

Simulated scope: `spine/frames/**` (`frames-transforms` profile applies).
"""

from __future__ import annotations

from pyspark.sql import functions as F


def cast_by_interpolated_value(df, column_name, declared_type):
    return df.withColumn("cast_result", F.expr(f"try_cast(`{column_name}` AS {declared_type})"))
