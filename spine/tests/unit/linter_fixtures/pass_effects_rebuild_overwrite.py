"""MUST-PASS: the IDENTICAL `.overwrite(` shape as `fail_effects_state_
overwrite.py`, but simulated at the ONE blessed module's own rel_path
(`spine/effects/rebuild.py`, exactly) -- 007.1 [DC2-2]'s per-file
exemption (`banned_attr_exemption`) licenses this ONE construction site.
Proves the exemption is genuinely per-file (this exact rel_path only), not
a blanket relaxation of the `overwrite` ban across `spine/effects/**`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def attempt_state_swap(df: DataFrame, table: str, before_id: int) -> None:
    (
        df.writeTo(table)
        .option("validate-from-snapshot-id", str(before_id))
        .option("isolation-level", "serializable")
        .overwrite(F.lit(True))
    )
