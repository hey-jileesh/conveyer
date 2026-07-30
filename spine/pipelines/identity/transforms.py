"""`apply`/`post_check` for the identity exemplar pipeline. LLD §12.2, I-10.

**`apply`** — a column-projection pass-through, deliberately not renaming,
casting, adding, or dropping a single column: `valid_df` (005.1 §6.2's
declared-columns-ONLY projection, post pre_check) already carries exactly
the fixture's five declared columns (`domain_id`, `event_time`, `source_ts`,
`content_hash`, `payload` — I-11: `domain_id_col` is the merge key,
`(event_time, source_ts, content_hash)` is `frames.folds`' hardcoded
ordering key), so `apply` only re-projects them through, in a fixed,
explicit order (`_FACT_COLUMNS`) — lineage (`batch_id`, `delivery_id`,
`feed_id`, `received_at`) is NOT `apply`'s concern: `commit.py` stamps it
onto `admitted_facts_df` AFTER `post_check`, via `frames.lineage.
stamp_fact_lineage`, for every pipeline uniformly (§7.5).

**Historical note (superseded by n3-admission-cut, bead conveyer-azr.19):**
prior to 005.1's real `pre_check`/quarantine rewrite, `valid_df` was an
unfiltered projection of `raw_df` (carrying lineage columns), and `apply`'s
byte-identical projection was load-bearing for a DIFFERENT reason — I-P2's
provisional pre_check wrote raw-shaped violations and post_check wrote
candidate-shaped violations into the SAME literal quarantine table, which
`DataFrameWriterV2.append()`'s exact-column-match requirement made only
possible if `apply` never reshaped a column. 005.1 §4.2's quarantine table
is now a FIXED, versionless, candidate-independent shape (`row_snapshot` is
a JSON blob), so that constraint no longer applies — `apply` stays a plain
projection here because the exemplar has no actual transformation to do,
not because a shared-table schema forces it to.

Both `event_time`/`source_ts` stay the reader's native `string` type (raw
columns are always string, D-5) rather than being cast to `timestamp`. This
still orders correctly: the fixture's fixed-width, zero-padded, UTC
(`Z`-suffixed) ISO-8601 strings sort identically under plain string
comparison and true chronological order, so the fold's native Spark
`struct(...) > struct(...)` ordering comparison (I-11) is exactly as correct
over these strings as it would be over parsed timestamps — a fixture-format
contract, not a general one. `content_hash` arrives as upstream-supplied
provenance data on the fixture (a realistic shape: some sources already
carry their own content hash) rather than being derived here.

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

# 005.1 §6.2: `valid_df`'s own declared-columns-only shape (pre_check's
# typed projection) -- see the module docstring for why lineage columns are
# NOT here (commit.py stamps them after post_check, uniformly).
_FACT_COLUMNS: tuple[str, ...] = (
    "domain_id",
    "event_time",
    "source_ts",
    "content_hash",
    "payload",
)


def apply(valid_df: DataFrame, co_effects: Mapping[str, DataFrame]) -> DataFrame:
    """Pure column projection: pins the exact candidate-row shape by name
    (no rename/cast/add/drop -- see module docstring)."""
    del co_effects  # identity declares no co-effects
    return valid_df.select(*(F.col(name) for name in _FACT_COLUMNS))


def post_check(candidate_facts_df: DataFrame, co_effects: Mapping[str, DataFrame]) -> DataFrame:
    """Zero violations, always: the exemplar's clean half (§12.2)."""
    del co_effects
    return candidate_facts_df.limit(0).withColumn("reason", F.lit(None).cast("string"))
