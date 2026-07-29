"""`stamp_raw_lineage`, `stamp_fact_lineage` — `LineageStamp` in, stamped DataFrame out. §7.5.

Both functions add the same minimal, named column set as literals from the
`LineageStamp` value — never read from a co-effect or the context (I-9,
`frames/` never imports `BatchContext` [C-5]): `batch_id`, `delivery_id`,
`feed_id`, `received_at`, and `source_uri` **only when the stamp carries
one** (`LineageStamp.source_uri` is `str | None`; omitting the column
entirely when it is `None` avoids an all-NULL column on tables/pipelines
that never populate it, rather than writing a literal NULL every time).

Raw and fact lineage are stamped identically today — this is a single
shared, provisional column set (005/007 own the final raw/fact schemas);
if raw and fact lineage are ever meant to diverge (e.g. a fact table
dropping `source_uri` once a batch aggregates many source objects), that
is a schema decision for 005/007 to make explicitly, not something this
module should quietly assume.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine.core.model import LineageStamp

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _lineage_literals(stamp: LineageStamp) -> dict[str, object]:
    literals: dict[str, object] = {
        "batch_id": stamp.batch_id,
        "delivery_id": stamp.delivery_id,
        "feed_id": stamp.feed_id,
        "received_at": stamp.received_at,
    }
    if stamp.source_uri is not None:
        literals["source_uri"] = stamp.source_uri
    return literals


def _stamp(df: DataFrame, stamp: LineageStamp) -> DataFrame:
    stamped = df
    for column, value in _lineage_literals(stamp).items():
        stamped = stamped.withColumn(column, F.lit(value))
    return stamped


def stamp_raw_lineage(df: DataFrame, stamp: LineageStamp) -> DataFrame:
    """Land's raw-table stamp (§7.5 land ②: `raw = frames.stamp_raw_lineage(df, stamp)`)."""
    return _stamp(df, stamp)


def stamp_fact_lineage(df: DataFrame, stamp: LineageStamp) -> DataFrame:
    """Commit's fact-table stamp (§7.5 commit: `stamped = frames.stamp_fact_lineage(...)`)."""
    return _stamp(df, stamp)
