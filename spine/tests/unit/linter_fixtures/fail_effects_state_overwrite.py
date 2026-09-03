"""MUST-FAIL: a bare `.overwrite(` on a state table, OUTSIDE the one blessed
rebuild/swap module (`spine/effects/rebuild.py`) -- 007.1 [DC2-2]/RB-2's
banned construction. Simulated scope: `spine/effects/**` (`effects-stages`
profile applies; `banned_attr_names` bans `overwrite`, and this file's
simulated rel_path is deliberately NOT in `banned_attr_exemption`).

Modeled on the shape `spine/effects/rebuild.py::attempt_state_swap` uses --
present here to prove the ban fires on ANY file lacking the exemption,
options or no options.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def overwrite_state_table(df: DataFrame, table: str, before_id: int) -> None:
    (
        df.writeTo(table)
        .option("validate-from-snapshot-id", str(before_id))
        .option("isolation-level", "serializable")
        .overwrite(F.lit(True))
    )
