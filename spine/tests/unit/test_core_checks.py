"""Unit tests for `spine.core.checks.structural_fact_check` — LLD §7.7, I-24.

A NULL `domain_id` and a column-set drift are the two independent violation
conditions; either, neither, or both may fire (both reasons are reported
together when both fire).
"""

import dataclasses

import pytest
from hypothesis import given
from hypothesis import strategies as st
from spine.core import checks


def test_ok_when_no_nulls_and_no_drift() -> None:
    verdict = checks.structural_fact_check(
        ["domain_id", "amount"], ["domain_id", "amount"], "domain_id", 0
    )
    assert verdict == checks.StructuralFactCheckOk()


def test_defect_on_null_domain_id() -> None:
    verdict = checks.structural_fact_check(
        ["domain_id", "amount"], ["domain_id", "amount"], "domain_id", 3
    )
    assert isinstance(verdict, checks.StructuralFactCheckDefect)
    assert len(verdict.reasons) == 1
    assert "3 row(s) with NULL 'domain_id'" in verdict.reasons[0]


def test_defect_on_schema_drift_added_column() -> None:
    verdict = checks.structural_fact_check(
        ["domain_id", "amount", "extra_col"], ["domain_id", "amount"], "domain_id", 0
    )
    assert isinstance(verdict, checks.StructuralFactCheckDefect)
    assert len(verdict.reasons) == 1
    assert "added=['extra_col']" in verdict.reasons[0]
    assert "missing=[]" in verdict.reasons[0]


def test_defect_on_schema_drift_missing_column() -> None:
    verdict = checks.structural_fact_check(["domain_id"], ["domain_id", "amount"], "domain_id", 0)
    assert isinstance(verdict, checks.StructuralFactCheckDefect)
    assert "missing=['amount']" in verdict.reasons[0]


def test_defect_on_both_conditions_reports_both_reasons() -> None:
    verdict = checks.structural_fact_check(
        ["domain_id", "extra"], ["domain_id", "amount"], "domain_id", 2
    )
    assert isinstance(verdict, checks.StructuralFactCheckDefect)
    assert len(verdict.reasons) == 2


def test_schema_drift_is_order_insensitive() -> None:
    verdict = checks.structural_fact_check(
        ["amount", "domain_id"], ["domain_id", "amount"], "domain_id", 0
    )
    assert verdict == checks.StructuralFactCheckOk()


def test_custom_domain_id_col_name_used_in_reason() -> None:
    verdict = checks.structural_fact_check(["policy_id"], ["policy_id"], "policy_id", 1)
    assert isinstance(verdict, checks.StructuralFactCheckDefect)
    assert "policy_id" in verdict.reasons[0]


def test_structural_verdicts_are_frozen() -> None:
    ok = checks.StructuralFactCheckOk()
    with pytest.raises(dataclasses.FrozenInstanceError):
        ok.x = 1  # type: ignore[attr-defined]

    defect = checks.StructuralFactCheckDefect(reasons=("r",))
    with pytest.raises(dataclasses.FrozenInstanceError):
        defect.reasons = ()  # type: ignore[misc]


@given(
    present=st.lists(st.text(min_size=1, max_size=8), min_size=0, max_size=5, unique=True),
    expected=st.lists(st.text(min_size=1, max_size=8), min_size=0, max_size=5, unique=True),
    domain_id_null_count=st.integers(min_value=0, max_value=1000),
)
def test_verdict_shape_matches_conditions(
    present: list[str], expected: list[str], domain_id_null_count: int
) -> None:
    verdict = checks.structural_fact_check(present, expected, "domain_id", domain_id_null_count)
    has_nulls = domain_id_null_count > 0
    has_drift = set(present) != set(expected)
    if not has_nulls and not has_drift:
        assert verdict == checks.StructuralFactCheckOk()
    else:
        assert isinstance(verdict, checks.StructuralFactCheckDefect)
        expected_reason_count = int(has_nulls) + int(has_drift)
        assert len(verdict.reasons) == expected_reason_count
