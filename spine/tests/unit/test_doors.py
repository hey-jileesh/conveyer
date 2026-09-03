"""Unit tests for `spine.core.doors` — LLD 006.1 §8.2 (P-7), §8.4.

The door planner's truth table (§8.2) plus a property pin: `q_guard_present`
alone decides `DURABLE_SUBTRACT` regardless of `fact_presence`.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st
from spine.core import doors


def test_any_fact_present_false_for_empty_mapping() -> None:
    assert doors.any_fact_present({}) is False


def test_any_fact_present_false_when_all_absent() -> None:
    assert doors.any_fact_present({"a": False, "b": False}) is False


def test_any_fact_present_true_when_any_present() -> None:
    assert doors.any_fact_present({"a": False, "b": True}) is True


def test_any_fact_present_true_single_table() -> None:
    assert doors.any_fact_present({"a": True}) is True


# --- §8.2's decision table, verbatim ----------------------------------------


def test_post_check_path_fresh_when_guard_absent_and_no_facts() -> None:
    path = doors.post_check_path(q_guard_present=False, fact_presence={"a": False, "b": False})
    assert path is doors.PostCheckPath.FRESH


def test_post_check_path_durable_subtract_when_guard_present_no_facts() -> None:
    path = doors.post_check_path(q_guard_present=True, fact_presence={"a": False})
    assert path is doors.PostCheckPath.DURABLE_SUBTRACT


def test_post_check_path_durable_subtract_when_guard_present_and_facts() -> None:
    path = doors.post_check_path(q_guard_present=True, fact_presence={"a": True})
    assert path is doors.PostCheckPath.DURABLE_SUBTRACT


def test_post_check_path_durable_authority_when_guard_absent_any_facts() -> None:
    path = doors.post_check_path(q_guard_present=False, fact_presence={"a": True, "b": False})
    assert path is doors.PostCheckPath.DURABLE_AUTHORITY


def test_post_check_path_fresh_with_empty_fact_presence_and_no_guard() -> None:
    path = doors.post_check_path(q_guard_present=False, fact_presence={})
    assert path is doors.PostCheckPath.FRESH


# --- property: guard presence alone decides DURABLE_SUBTRACT ---------------


@given(fact_presence=st.dictionaries(st.text(min_size=1, max_size=8), st.booleans(), max_size=5))
def test_post_check_path_guard_present_always_durable_subtract(
    fact_presence: dict[str, bool],
) -> None:
    assert doors.post_check_path(q_guard_present=True, fact_presence=fact_presence) is (
        doors.PostCheckPath.DURABLE_SUBTRACT
    )


@given(
    fact_presence=st.dictionaries(
        st.text(min_size=1, max_size=8), st.booleans(), min_size=1, max_size=5
    )
)
def test_post_check_path_guard_absent_matches_any_fact_present(
    fact_presence: dict[str, bool],
) -> None:
    path = doors.post_check_path(q_guard_present=False, fact_presence=fact_presence)
    expected = (
        doors.PostCheckPath.DURABLE_AUTHORITY
        if any(fact_presence.values())
        else doors.PostCheckPath.FRESH
    )
    assert path is expected
