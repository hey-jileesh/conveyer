"""`default_lww_fold` (ordering key inside), `winners_per_domain`, `delta_filter`. §7.5, I-11.

`winners_per_domain`/`default_lww_fold` are pure DataFrame-in/DataFrame-out
plan builders (I-9): no `.collect()`, no per-row Python UDF. The ordering
semantics [T-11] — field-wise lexicographic, null fields rank lowest, strict
`>` — are reproduced with ordinary Spark column expressions
(`_ordering_sort_columns`) and documented once as a pure-Python reference
(`ordering_struct_gt`) that tests property-check the Spark behavior against.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

# I-11: hardcoded, deliberately NOT a `PipelineSpec` field — 007 owns the
# final ordering key and decides whether it ever becomes a knob [C-4].
# Public: `stages/fold.py` (and any other Phase 1 caller needing the
# default-lww ordering key) imports this name directly rather than reaching
# into a private constant.
LWW_ORDERING_COLUMNS: tuple[str, ...] = ("event_time", "source_ts", "content_hash")

_WINNER_RN_COL = "_conveyer_winner_rn"


def ordering_struct_gt(left: tuple[object, ...], right: tuple[object, ...]) -> bool:
    """Pure-Python mirror of the Spark ordering-struct comparison [T-11]:
    field-wise lexicographic, **null fields rank lowest**, strict `>` (a tie
    on every field is NOT `>`, making a rerun's equal-struct MERGE condition
    correctly not fire — idempotence).

    Not called by `winners_per_domain` itself (which must stay a
    DataFrame-in/DataFrame-out Spark plan, never a per-row Python UDF) — this
    is the documented reference semantics `_ordering_sort_columns` reproduces
    as Spark column expressions, and what property tests check the Spark
    behavior against via an independent pure-Python model.
    """
    for left_value, right_value in zip(left, right, strict=True):
        left_rank = (0, None) if left_value is None else (1, left_value)
        right_rank = (0, None) if right_value is None else (1, right_value)
        if left_rank == right_rank:
            continue
        return left_rank > right_rank
    return False


def _ordering_sort_columns(ordering_columns: tuple[str, ...]) -> list[Column]:
    """Per ordering column, a `(has_value, value)` descending pair —
    `has_value` (1 if not null, else 0) ranks non-null above null on its own;
    ties within it fall through to `value DESC NULLS LAST` (harmless: when
    `has_value` is 0 the value is null on both sides of that tie, so
    ordering by it changes nothing) before the next ordering column breaks
    the tie. Reproduces `ordering_struct_gt`'s null-ranks-lowest, field-wise
    semantics using ordinary Spark column expressions — no UDF.
    """
    keys: list[Column] = []
    for column in ordering_columns:
        has_value = F.when(F.col(column).isNotNull(), F.lit(1)).otherwise(F.lit(0))
        keys.append(has_value.desc())
        keys.append(F.col(column).desc_nulls_last())
    return keys


def winners_per_domain(
    facts_df: DataFrame, domain_id_col: str, ordering_columns: tuple[str, ...]
) -> DataFrame:
    """At most one row per `domain_id_col` (I-11's cardinality precondition,
    [T-12]): the row whose `ordering_columns` struct is the maximum within
    its domain, per `ordering_struct_gt`'s semantics.
    """
    window = Window.partitionBy(domain_id_col).orderBy(*_ordering_sort_columns(ordering_columns))
    ranked = facts_df.withColumn(_WINNER_RN_COL, F.row_number().over(window))
    return ranked.filter(F.col(_WINNER_RN_COL) == 1).drop(_WINNER_RN_COL)


def default_lww_fold(
    state_slice_df: DataFrame, facts_df: DataFrame, domain_id_col: str
) -> DataFrame:
    """The default `Transforms.fold` (I-11): one winner per touched domain
    from THIS BATCH's committed facts, using the hardcoded ordering key
    `(event_time, source_ts, content_hash)` [C-4].

    `state_slice_df` is accepted but not read — kept to satisfy the
    `fold(state_slice, facts_df) -> new_rows` signature (§6.3/§7.4). Whether
    a winner is actually newer than the CURRENT state is the MERGE
    condition's job (`src ordering struct > tgt ordering struct`, §7.5
    fold), not this function's: duplicating that comparison here, outside
    the atomic MERGE, would let the two drift.
    """
    return winners_per_domain(facts_df, domain_id_col, LWW_ORDERING_COLUMNS)


def delta_filter(df: DataFrame) -> DataFrame:
    """The named 007 seam (dedup + content-hash delta detection) — identity
    in Phase 1. Kept as a real function, not inlined at the call site, so
    007 has exactly one place to replace it.
    """
    return df
