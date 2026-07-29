"""`apply`/`post_check` for the identity exemplar pipeline. LLD §12.2, I-10.

**`apply`** — a column-projection pass-through, deliberately not renaming,
casting, adding, or dropping a single column between `valid_df` and
`candidate_facts_df`: the fixture's own CSV columns already carry the exact
names the default LWW fold needs (`domain_id`, `event_time`, `source_ts`,
`content_hash`, `payload` — I-11: `domain_id_col` is the merge key,
`(event_time, source_ts, content_hash)` is `frames.folds`' hardcoded
ordering key), so `apply` only projects them through, alongside `land`'s
lineage stamp columns (`batch_id`, `delivery_id`, `feed_id`, `received_at`),
in a fixed, explicit order (`_FACT_COLUMNS`).

This is not merely a simplicity choice — it is a **precondition for
`post_check`'s quarantine writes and `pre_check`'s quarantine writes to
share one physical Iceberg table**, verified empirically (identity-pipeline
scratch validation, this bead): `DataFrameWriterV2.append()` requires an
EXACT column-name match against the target table, with no auto-upcast for
an incompatible type change (a `STRING` `event_time` column cannot append
into a `TIMESTAMP`-typed target column — `CANNOT_SAFELY_CAST`) and no
tolerance for a subset or superset of the target's columns
(`CANNOT_FIND_DATA` either way). `pre_check`'s violations are a filter of
`raw_df` (pre-`apply` shape); `post_check`'s violations are a filter of
`candidate_facts_df` (post-`apply` shape) — the ONLY way both writers can
target one `quarantine_table` (§7.5's shared-table, `check_stage`-
disambiguated design) is for `apply` to leave the row shape byte-identical,
so both writers' DataFrames match the SAME fixed table schema. A pipeline
whose `apply` genuinely reshapes rows (rename/cast/drop/add a column) would
need to declare a NARROWER `pre_check`/`post_check` contract or a dedicated
per-stage quarantine table — both out of scope for Phase 1's fixed 8-stage
protocol; the identity exemplar sidesteps the question by choosing fixture
columns that already match the facts shape, rather than resolving it
generally (a 005/007-owned gap, not this pipeline's to fix). `content_hash`
therefore arrives as upstream-supplied provenance data on the fixture (a
realistic shape: some sources already carry their own content hash) rather
than being derived here — deriving it in `apply` would ADD a column absent
from `pre_check`'s raw-shaped violations, breaking the same invariant.

Both `event_time`/`source_ts` stay the reader's native `string` type (I-P1:
all-string CSV) rather than being cast to `timestamp` — for the same reason:
a cast would retype the column, breaking parity with `pre_check`'s raw-typed
violations in the shared quarantine table. This still orders correctly:
the fixture's fixed-width, zero-padded, UTC (`Z`-suffixed) ISO-8601 strings
sort identically under plain string comparison and true chronological
order, so the fold's native Spark `struct(...) > struct(...)` ordering
comparison (I-11) is exactly as correct over these strings as it would be
over parsed timestamps — a fixture-format contract, not a general one.

**`post_check`** — the exemplar's clean half (§12.2): every candidate row is
admitted, unconditionally — an empty violations `DataFrame` with the
candidate's own columns plus `reason: string` (the I-12 contract),
`.limit(0)` being the pure, plan-only way to get an empty frame of the right
shape without a driver-side action. `pipelines.identity_violations` is the
paired violations-variant module (§12.2's "one variant fixture producing
violations" — R-04): same `apply`, a `post_check` that flags rows instead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyspark.sql import DataFrame

# The exact, fixed row shape `land`'s lineage stamp + the fixture's own CSV
# columns produce -- see the module docstring for why `apply` projects
# through EXACTLY these columns, in this order, with no rename/cast/add/drop.
_FACT_COLUMNS: tuple[str, ...] = (
    "domain_id",
    "event_time",
    "source_ts",
    "content_hash",
    "payload",
    "batch_id",
    "delivery_id",
    "feed_id",
    "received_at",
)


def apply(valid_df: DataFrame, co_effects: Mapping[str, DataFrame]) -> DataFrame:
    """Pure column projection: pins the exact fact-row shape by name (no
    rename/cast/add/drop -- see module docstring)."""
    del co_effects  # identity declares no co-effects
    return valid_df.select(*(F.col(name) for name in _FACT_COLUMNS))


def post_check(candidate_facts_df: DataFrame, co_effects: Mapping[str, DataFrame]) -> DataFrame:
    """Zero violations, always: the exemplar's clean half (§12.2)."""
    del co_effects
    return candidate_facts_df.limit(0).withColumn("reason", F.lit(None).cast("string"))
