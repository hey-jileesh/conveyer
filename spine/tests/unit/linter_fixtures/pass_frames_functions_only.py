"""MUST-PASS: the everyday `frames/`/transform shape -- module-level functions
over `DataFrame` values built from `pyspark.sql.functions`/column
expressions only, no banned imports, no banned Spark-API attribute calls, no
`class` (idiom rule), nothing string-interpolated into a sink. Modeled on the
real `spine/frames/lineage.py` (functions taking/returning a `DataFrame`,
stamping literal columns).

Simulated scope: `spine/frames/**` or a pipeline `transforms.py`
(`frames-transforms` profile applies; trips none of its rules).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def add_domain_flag(df: DataFrame, flag_value: str) -> DataFrame:
    return df.withColumn("domain_flag", F.lit(flag_value))


def rename_column(df: DataFrame, old_name: str, new_name: str) -> DataFrame:
    return df.withColumnRenamed(old_name, new_name)
