"""MUST-FAIL: `spine/core/**` bans `pyspark`/`awsglue` on top of the
ingestion-core purity rules (§12.3's `core` profile: "ingestion-core purity
rules plus banned imports pyspark, awsglue") -- `core/` holds I-9's plain-value
pure zone, never a DataFrame plan.

Simulated scope: `spine/core/**` (`core` profile applies).
"""

from __future__ import annotations

import pyspark


def spark_version() -> str:
    return pyspark.__version__
