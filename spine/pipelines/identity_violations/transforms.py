"""`apply`/`post_check` for the identity exemplar's violations variant. LLD §12.2, R-04.

`apply` is `pipelines.identity.transforms.apply`, re-exported (not
re-implemented — a single source of truth for the column projection).
`post_check` flags every candidate row whose `payload` carries the fixture's
own violation marker (`_VIOLATION_MARKER`) — a real, if provisional, DQ rule
over the transform's OWN output (I-12: `Quarantined = tuple[Record, str]`,
one `reason` per flagged row), letting R-04's violations fixture exercise
`post_check`'s quarantine path without inventing a second co-effect.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from pipelines.identity.transforms import apply  # noqa: F401 -- re-exported, required export

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyspark.sql import DataFrame

# The fixture's own marker for a row `post_check` must flag -- see
# tests/exemplar/identity/fixtures/violations/*.csv.
_VIOLATION_MARKER = "INVALID"


def post_check(candidate_facts_df: DataFrame, co_effects: Mapping[str, DataFrame]) -> DataFrame:
    del co_effects
    return candidate_facts_df.filter(F.col("payload") == F.lit(_VIOLATION_MARKER)).withColumn(
        "reason", F.lit(f"payload == {_VIOLATION_MARKER!r} (identity_violations fixture marker)")
    )
