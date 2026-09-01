"""Self-tests for `tests/integration/ordering_reference.py` — LLD 007.1
§8.1's ordering-comparability table, K-14's oracle. This module lives in
`unit/` (not `integration/`) even though its subject module sits under
`tests/integration/` -- the reference comparator itself is pure Python, no
Spark, so its own correctness needs no Spark fixture; K-14's actual
differential (live-engine agreement) is B10 ground.

`tests/integration/` carries no `__init__.py` (no namespace package, per
this suite's established convention -- see `sys.path` insertion below,
matching how `tests/integration/*.py` files import each other's shared
helpers today)."""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

_INTEGRATION_DIR = Path(__file__).resolve().parents[1] / "integration"
if str(_INTEGRATION_DIR) not in sys.path:
    sys.path.insert(0, str(_INTEGRATION_DIR))

from ordering_reference import (  # noqa: E402
    Order,
    compare_element,
    compare_ordering_struct,
    strictly_greater,
)

# --- compare_element: §8.1's per-type semantics + [T-11]'s null handling ---


def test_int_order() -> None:
    assert compare_element(1, 2) is Order.LESS
    assert compare_element(2, 1) is Order.GREATER
    assert compare_element(5, 5) is Order.TIE


def test_null_ranks_lowest_both_directions_and_null_null_ties() -> None:
    assert compare_element(None, 5) is Order.LESS
    assert compare_element(5, None) is Order.GREATER
    assert compare_element(None, None) is Order.TIE


def test_decimal_scale_is_not_order_significant() -> None:
    assert compare_element(Decimal("1.2"), Decimal("1.20")) is Order.TIE
    assert compare_element(Decimal("1.3"), Decimal("1.20")) is Order.GREATER
    assert compare_element(Decimal("1.1"), Decimal("1.20")) is Order.LESS


def test_date_chronological_order() -> None:
    assert compare_element(date(2026, 1, 1), date(2026, 1, 2)) is Order.LESS
    assert compare_element(date(2026, 1, 2), date(2026, 1, 1)) is Order.GREATER
    assert compare_element(date(2026, 1, 1), date(2026, 1, 1)) is Order.TIE


def test_timestamp_compared_as_utc_instants_regardless_of_stored_offset() -> None:
    t_utc = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    t_offset = datetime(2026, 1, 1, 7, 0, tzinfo=timezone(timedelta(hours=-5)))  # same instant
    assert t_utc == t_offset  # sanity: Python already treats these as equal
    assert compare_element(t_utc, t_offset) is Order.TIE
    t_later = datetime(2026, 1, 1, 12, 0, 1, tzinfo=UTC)
    assert compare_element(t_later, t_utc) is Order.GREATER


def test_string_codepoint_order_matches_the_named_authoring_trap() -> None:
    # §8.1's own named trap: variable-width numeric strings order lexically.
    assert compare_element("10", "9") is Order.LESS


def test_string_nfc_nfd_are_byte_distinct_never_equal() -> None:
    nfc = "é"  # é precomposed
    nfd = "é"  # e + combining acute accent
    assert nfc != nfd
    assert compare_element(nfc, nfd) is not Order.TIE


# --- compare_ordering_struct: field-wise lexicographic, [T-11] ------------


def test_struct_first_non_tie_element_decides() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    source_row = (5, t, "hash-b")
    target_row = (5, t, "hash-a")
    assert compare_ordering_struct(source_row, target_row) is Order.GREATER


def test_struct_full_tie_across_every_element_is_tie() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    row_a = (5, t, "hash-x")
    row_b = (5, t, "hash-x")
    assert compare_ordering_struct(row_a, row_b) is Order.TIE


def test_struct_earlier_element_decides_even_when_later_elements_would_disagree() -> None:
    t_early = datetime(2026, 1, 1, tzinfo=UTC)
    t_late = datetime(2026, 1, 2, tzinfo=UTC)
    source_row = (10, t_early, "aaa")
    target_row = (5, t_late, "zzz")
    assert compare_ordering_struct(source_row, target_row) is Order.GREATER


def test_struct_null_in_an_early_element_loses_even_if_later_elements_favor_it() -> None:
    t_early = datetime(2026, 1, 1, tzinfo=UTC)
    t_late = datetime(2026, 1, 2, tzinfo=UTC)
    source_row = (None, t_late, "zzz")
    target_row = (5, t_early, "aaa")
    assert compare_ordering_struct(source_row, target_row) is Order.LESS


def test_struct_mismatched_lengths_raise() -> None:
    with pytest.raises(ValueError):
        compare_ordering_struct((1, 2), (1,))


def test_struct_empty_sequences_tie_vacuously() -> None:
    assert compare_ordering_struct((), ()) is Order.TIE


# --- strictly_greater: the MERGE predicate's boolean value -----------------


def test_strictly_greater_true_iff_greater() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    source_row = (5, t, "hash-b")
    target_row = (5, t, "hash-a")
    assert strictly_greater(source_row, target_row) is True
    assert strictly_greater(target_row, source_row) is False


def test_strictly_greater_false_on_a_full_tie() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    row_a = (5, t, "hash-x")
    row_b = (5, t, "hash-x")
    assert strictly_greater(row_a, row_b) is False
