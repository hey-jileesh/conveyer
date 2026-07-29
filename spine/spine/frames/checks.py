"""Provisional pre_check split; violation subtraction. LLD §7.5, I-P2, I-12, [C-8].

Both functions here are pure DataFrame-in/DataFrame-out plan builders (I-9):
no `.count()`, no `.collect()`, no driver materialization. Counts a stage
needs (e.g. I-12's fresh-compute count-identity assertion) are computed BY
THE STAGE and handed to `check_count_identity` as plain `int`s — this module
never turns a count into control flow itself.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

_BAG_RN_COL = "_conveyer_bag_rn"


def required_null_predicate(required_columns: Sequence[str]) -> Column:
    """The single "null in any required column" predicate (I-P2, provisional;
    005 owns the contract grammar).

    Exported (not private) so a caller building `valid_df` applies the
    IDENTICAL predicate's negation —
    `raw_df.filter(~required_null_predicate(required_columns))` — rather than
    re-deriving an equivalent-looking condition (§7.5: "`valid_df` and `viol`
    derived from the same predicate"). Zero `required_columns` ⇒
    `F.lit(False)`: nothing ever violates, so `valid_df` is the whole input —
    deterministic and rerun-stable.
    """
    if not required_columns:
        return F.lit(False)
    predicate = F.col(required_columns[0]).isNull()
    for column in required_columns[1:]:
        predicate = predicate | F.col(column).isNull()
    return predicate


def pre_violations(raw_df: DataFrame, required_columns: Sequence[str]) -> DataFrame:
    """Rows of `raw_df` with a NULL in any of `required_columns` (§7.5
    pre_check, I-P2, provisional). Callers derive `valid_df` as
    `raw_df.filter(~required_null_predicate(required_columns))` — the SAME
    predicate, negated (see `required_null_predicate`).
    """
    return raw_df.filter(required_null_predicate(required_columns))


def violation_subtraction(candidate_df: DataFrame, violations_df: DataFrame) -> DataFrame:
    """`candidate_df` anti-joined against `violations_df` on all shared
    columns (I-12, provisional — 006 owns a keyed violation identity).

    Multiplicity-preserving (bag subtraction, [C-8]): if a given
    shared-column value-tuple appears `k` times in `candidate_df` and `m`
    times in `violations_df`, exactly `min(k, m)` copies are removed,
    leaving `max(k - m, 0)` — never the "drop every copy the instant ANY one
    of them is a violation" a naive value-based anti-join would produce.
    Achieved by row-numbering each side within its own shared-column
    partition and anti-joining on `(shared columns, row number)`; the
    row-number pairing is an implementation artifact of computing the
    subtraction, not a published contract.

    No shared columns ⇒ nothing is determinable from this identity;
    `candidate_df` is returned unchanged.
    """
    shared_columns = [c for c in candidate_df.columns if c in violations_df.columns]
    if not shared_columns:
        return candidate_df
    window = Window.partitionBy(*shared_columns).orderBy(F.monotonically_increasing_id())
    candidate_ranked = candidate_df.withColumn(_BAG_RN_COL, F.row_number().over(window))
    violations_ranked = violations_df.select(*shared_columns).withColumn(
        _BAG_RN_COL, F.row_number().over(window)
    )
    join_keys = [*shared_columns, _BAG_RN_COL]
    return candidate_ranked.join(violations_ranked, on=join_keys, how="left_anti").drop(_BAG_RN_COL)


@dataclass(frozen=True)
class CountIdentityCheck:
    """I-12's fresh-compute assertion, as a value: `candidate == admitted +
    violations`. Built from plain `int`s the stage already has — this
    function never calls `.count()` itself and never raises; `ok=False` is
    data the STAGE decides what to do with (fail loudly on the fresh-compute
    path, or WARNING + EMF `PostCheckDrift` on the [H-2] guard-skip path).
    """

    ok: bool
    candidate_count: int
    admitted_count: int
    violations_count: int


def check_count_identity(
    candidate_count: int, admitted_count: int, violations_count: int
) -> CountIdentityCheck:
    return CountIdentityCheck(
        ok=candidate_count == admitted_count + violations_count,
        candidate_count=candidate_count,
        admitted_count=admitted_count,
        violations_count=violations_count,
    )
