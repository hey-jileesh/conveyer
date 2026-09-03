"""`structural_fact_check` — the structural fact check planner (LLD §7.7,
I-24); `checks_version`/`check_content_hash` — the checks.yaml version
identity functions (LLD 006.1 P-3 §7.4; recorded placement per this bead's
brief, "home beside the existing `core/checks.py`" — a deliberate co-
location choice: both concerns are "checks"-grained version/verdict
support, kept out of `core/contract.py` (005.1's raw-contract/read-spec
grammar module) to avoid conflating the two LLDs' concerns in one file).

Pure decision logic only: the Spark-side work (counting `domain_id` nulls,
diffing the candidate facts' columns against the target fact table's) happens
in `effects/`/`stages/commit.py` (M2); this module receives the already-
computed plain values and produces a verdict. A NULL `domain_id` or a column-
set drift is a **fail-fast defect**, not a quarantine row (I-24): MERGE's
`ON` clause never matches NULL, so such a row would be re-INSERTed by every
fold rerun, silently breaking the rerun-is-a-no-op invariant.

**`checks_version`/`check_content_hash` (P-3, §7.4).** On `check_stage =
'post_check'` rows, `check_version` is `checks_version` — sha256 of the
canonical JSON of the parsed `ChecksModel` — computed once at binding,
carried seed-adjacent on `BatchContext`, stamped by the post writer (pre
rows keep 005.1 A-11's pair-hash, unchanged). Per-check identity inside
`reason_detail` is `check_content_hash` — sha256 of the canonical JSON of
that ONE check's parsed model — so an entry survives unrelated edits to
sibling checks. Both are `canonical_json(model_dump(mode="json"))` (the
A-11 functions' idiom exactly): key-order-insensitive (the canonical
grammar sorts keys), content-sensitive, deterministic. `CHECK_GRAMMAR_
VERSION` is deliberately excluded from `checks_version`'s hashed object
(grammar releases are additive and semantics-pinned by the executable
table; the executing artifact is already content-pinned, I-23) — nothing
special is needed to exclude it: `ChecksModel.model_dump()` never includes
it in the first place, since it lives in `check_grammar.py`, not on the
model.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from spine.core.canonical import canonical_json

if TYPE_CHECKING:
    from spine.core.model import BatchCheckModel, ChecksModel, MembershipCheckModel, RowCheckModel


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


def checks_version(checks: ChecksModel) -> str:
    """P-3: SHA-256 (lowercase hex, full) of the canonical JSON of the
    PARSED `ChecksModel` -- `model_dump(mode="json")`, not the authored
    file text, so 009's re-homing of the authored surface cannot silently
    change version identity (the A-11 idiom, extended)."""
    return hashlib.sha256(
        canonical_json(checks.model_dump(mode="json")).encode("utf-8")
    ).hexdigest()


def check_content_hash(check: RowCheckModel | MembershipCheckModel | BatchCheckModel) -> str:
    """P-3: SHA-256 of the canonical JSON of ONE parsed check model -- the
    per-check content hash `reason_detail` carries (§7.3), so an entry
    survives unrelated edits to sibling checks."""
    return hashlib.sha256(canonical_json(check.model_dump(mode="json")).encode("utf-8")).hexdigest()
