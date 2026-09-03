"""Plain-Python reference comparator for LLD 007.1 §8.1's ordering-
comparability table -- K-14's oracle.

**One authority for §8.1's semantics, differentially tested against the
live engine.** §8.2's rendering decision names two engine-side artifacts
that must agree with THIS module, never with each other directly:
`core/merge.py::ordering_predicate`'s generated field-wise SQL (the MERGE
condition) and `frames/fold.py`'s reduce sort directives (§8.2: "the
MERGE-condition builder and the reduce's sort directives each ≡ one
plain-Python reference comparator implementing §8.1's table … native
struct/tuple comparison may not be substituted at either site without first
passing this same differential" -- K-14). This module ships the oracle;
wiring it into a live-Spark differential (generating cases, running them
against a real MERGE/`row_number()` sort, asserting agreement) is B10
ground (`frames/fold.py`, `effects/spark.py`) -- committed here so K-14's
consumer needs no design of its own, only a harness.

**§8.1's table, restated as code:**

| Type | Order semantics |
|---|---|
| `int`, `long` | integer order |
| `decimal(p,s)` | numeric order -- scale NOT order-significant |
| `date` | chronological |
| `timestamp` | chronological, compared as UTC instants |
| `string` | binary UTF-8 byte order (= codepoint order); no collation, no unicode normalization |

**[T-11] (D-3), restated as code:** within each element, `None` (SQL
`NULL`) ranks lowest -- less than every non-`None` value of its type;
`None == None` ties. Comparison across the struct is field-wise
lexicographic in declared order (`MergeSpec.ordering_cols`' own order, F-6);
strict inequality wins at the first non-tie element; a full tie (every
element ties, including the always-non-null final `content_hash`, F-6) is
the fold's no-op.

**[T-2]'s decimal caveat, carried here (§13):** a `DecimalType(p,s)` column
normalizes scale AT WRITE, so a single-column differential run against
Spark cannot discriminate the scale-insensitivity claim beyond what column
normalization already guarantees -- this reference comparator carries the
semantics regardless (Python's own `Decimal` equality/ordering is already
scale-insensitive, `Decimal("1.2") == Decimal("1.20")`), so it stays the
correct oracle even though a live differential's OWN decimal cases can
never exercise a scale-distinct pair.

Values are plain Python: `int` for int/long, `decimal.Decimal` for
`decimal(p,s)`, `datetime.date` for `date`, an AWARE `datetime.datetime`
for `timestamp`, `str` for `string`, and `None` for SQL `NULL` -- exactly
the shapes `hypothesis`' generators (005.1 §12.4's idiom) would draw for
B10's differential. `bool`/`float`/complex types are never legal `ordering:`
element values (F-6's own exclusions) and this module makes no claim about
them."""

from __future__ import annotations

from collections.abc import Sequence
from enum import Enum
from typing import Any


class Order(Enum):
    """The three-valued total order §8.1/[T-11] describes."""

    LESS = -1
    TIE = 0
    GREATER = 1


def compare_element(source: Any, target: Any) -> Order:
    """One §8.1 element's `(source, target)` -> `Order`. [T-11]: `None`
    ranks lowest (less than every non-`None` value; `None`/`None` ties);
    non-`None` comparison is the type's own native Python `<`/`>` -- numeric
    for `int`/`Decimal` (Decimal's own equality/ordering is already
    scale-insensitive), chronological for `date`/aware `datetime` (Python
    compares aware datetimes as absolute instants regardless of stored UTC
    offset -- matching §8.1's "compared as UTC instants" verbatim), and
    codepoint order for `str` (matching UTF-8 byte order for any valid
    Unicode string -- UTF-8 encoding is monotonic under codepoint order by
    construction, so "binary UTF-8 byte order" and Python's native `str`
    comparison are the SAME order, never approximated here)."""
    if source is None and target is None:
        return Order.TIE
    if source is None:
        return Order.LESS
    if target is None:
        return Order.GREATER
    if source < target:
        return Order.LESS
    if source > target:
        return Order.GREATER
    return Order.TIE


def compare_ordering_struct(source_row: Sequence[Any], target_row: Sequence[Any]) -> Order:
    """§8.1/[T-11]'s full struct comparison: field-wise lexicographic over
    `source_row`/`target_row` (same length, same declared element order --
    the caller's responsibility, matching `MergeSpec.ordering_cols`'
    already-suffixed shape: `(*declared ordering cols, source_ts,
    content_hash)`). The first non-tie element decides; an all-elements tie
    (including the always-non-null final `content_hash`, F-6) is `TIE` --
    the fold's no-op. Total for ANY equal-length input, including the empty
    sequence (`TIE`, vacuously) -- a real `MergeSpec`-shaped row is never
    empty (`ordering_cols` always carries at least the two framework
    elements), but this function itself carries no such assumption."""
    for source_value, target_value in zip(source_row, target_row, strict=True):
        result = compare_element(source_value, target_value)
        if result is not Order.TIE:
            return result
    return Order.TIE


def strictly_greater(source_row: Sequence[Any], target_row: Sequence[Any]) -> bool:
    """The MERGE `WHEN MATCHED AND <predicate>` boolean, per §8.2's
    rendering decision -- `core/merge.py::ordering_predicate`'s generated
    SQL text must agree with this function for every generated case (K-14's
    differential, wired against a live engine at B10); this module supplies
    only the oracle both engine-side artifacts (the MERGE predicate and the
    reduce's sort directives) are checked against."""
    return compare_ordering_struct(source_row, target_row) is Order.GREATER
