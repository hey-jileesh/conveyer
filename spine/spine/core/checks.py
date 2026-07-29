"""`structural_fact_check` — the structural fact check planner. LLD §7.7, I-24.

Pure decision logic only: the Spark-side work (counting `domain_id` nulls,
diffing the candidate facts' columns against the target fact table's) happens
in `effects/`/`stages/commit.py` (M2); this module receives the already-
computed plain values and produces a verdict. A NULL `domain_id` or a column-
set drift is a **fail-fast defect**, not a quarantine row (I-24): MERGE's
`ON` clause never matches NULL, so such a row would be re-INSERTed by every
fold rerun, silently breaking the rerun-is-a-no-op invariant.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class StructuralFactCheckOk:
    pass


@dataclass(frozen=True)
class StructuralFactCheckDefect:
    reasons: tuple[str, ...]  # one entry per violated condition, non-empty


StructuralFactCheckVerdict = StructuralFactCheckOk | StructuralFactCheckDefect


def structural_fact_check(
    present_columns: Sequence[str],
    expected_columns: Sequence[str],
    domain_id_col: str,
    domain_id_null_count: int,
) -> StructuralFactCheckVerdict:
    """I-24: verify (a) `domain_id_col` is non-null on every row -- reported
    via `domain_id_null_count`, computed by the caller -- and (b) the
    candidate facts' column set matches the target fact table's (a pure
    column-set diff, order-insensitive). Either violation names a defect;
    both may fire together, in which case both reasons are reported."""
    reasons: list[str] = []
    if domain_id_null_count > 0:
        reasons.append(f"{domain_id_null_count} row(s) with NULL {domain_id_col!r} (I-24)")
    present_set = set(present_columns)
    expected_set = set(expected_columns)
    if present_set != expected_set:
        added = sorted(present_set - expected_set)
        missing = sorted(expected_set - present_set)
        reasons.append(f"schema drift vs target table: added={added!r} missing={missing!r}")
    if reasons:
        return StructuralFactCheckDefect(reasons=tuple(reasons))
    return StructuralFactCheckOk()
