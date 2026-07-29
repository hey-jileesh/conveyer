"""MUST-FAIL: an f-string flowing into `.where(...)` -- the string-SQL review
rule [S-6] (§12.3's `effects-stages` profile `string_sql_sinks`): SQL/filter
predicates must be built from parameterized `Column` expressions (`F.col(...)
== F.lit(...)`), never string-interpolated, so a hostile/malformed
`batch_id` can't smuggle SQL into a `where`/`filter`/`selectExpr`/`spark.sql`
call. The rendered `MERGE INTO` in `effects/spark.py::render_merge` is the
one hardcoded exemption (by `(file, function)`), not a general license for
string interpolation feeding these sinks.

Simulated scope: `spine/effects/**` or `spine/stages/**` (`effects-stages`
profile applies).
"""

from __future__ import annotations


def read_batch(spark, table, batch_id):
    return spark.table(table).where(f"batch_id = '{batch_id}'")
