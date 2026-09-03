"""Unit tests for `spine.core.checks` — `structural_fact_check` (LLD §7.7,
I-24) and `checks_version`/`check_content_hash` (P-3, §7.4).

A NULL `domain_id` and a column-set drift are the two independent violation
conditions; either, neither, or both may fire (both reasons are reported
together when both fire).
"""

import dataclasses
import re

import pytest
import yaml
from hypothesis import given, settings
from hypothesis import strategies as st
from spine.core import checks, model


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


# ==============================================================================
# `checks_version`/`check_content_hash` -- P-3, §7.4
# ==============================================================================

_ROW_CHECK_FIELDS: dict[str, str] = {
    "kind": "row",
    "id": "chk-a",
    "fact_type": "orders",
    "expr": "amount > 0",
    "reason": "business/negative-amount",
}


def test_checks_version_is_64_lowercase_hex() -> None:
    cm = model.ChecksModel(checks=[model.RowCheckModel(**_ROW_CHECK_FIELDS)])
    assert re.fullmatch(r"[0-9a-f]{64}", checks.checks_version(cm))


def test_check_content_hash_is_64_lowercase_hex() -> None:
    check = model.RowCheckModel(**_ROW_CHECK_FIELDS)
    assert re.fullmatch(r"[0-9a-f]{64}", checks.check_content_hash(check))


def test_checks_version_stable_across_equivalent_object_construction() -> None:
    a = model.ChecksModel(checks=[model.RowCheckModel(**_ROW_CHECK_FIELDS)])
    b = model.ChecksModel(checks=[model.RowCheckModel(**_ROW_CHECK_FIELDS)])
    assert checks.checks_version(a) == checks.checks_version(b)  # content, not object identity


def test_checks_version_changes_with_a_content_edit() -> None:
    a = model.ChecksModel(checks=[model.RowCheckModel(**_ROW_CHECK_FIELDS)])
    b = model.ChecksModel(
        checks=[model.RowCheckModel(**{**_ROW_CHECK_FIELDS, "expr": "amount >= 0"})]
    )
    assert checks.checks_version(a) != checks.checks_version(b)


def test_check_content_hash_changes_with_a_content_edit() -> None:
    a = model.RowCheckModel(**_ROW_CHECK_FIELDS)
    b = model.RowCheckModel(**{**_ROW_CHECK_FIELDS, "reason": "business/other-reason"})
    assert checks.check_content_hash(a) != checks.check_content_hash(b)


def test_checks_version_of_empty_checks_list_is_deterministic() -> None:
    a = model.ChecksModel(checks=[])
    b = model.ChecksModel(checks=[])
    assert checks.checks_version(a) == checks.checks_version(b)


def test_check_content_hash_survives_unrelated_sibling_edits() -> None:
    # §7.4: "an entry survives unrelated edits to sibling checks" -- one
    # check's own content hash is insensitive to what ELSE is in the list.
    check = model.RowCheckModel(**_ROW_CHECK_FIELDS)
    sibling_a = model.RowCheckModel(
        kind="row", id="chk-b", fact_type="orders", expr="qty > 0", reason="business/bad-qty"
    )
    sibling_b = model.RowCheckModel(
        kind="row", id="chk-c", fact_type="orders", expr="qty < 100", reason="business/bad-qty"
    )
    with_a = model.ChecksModel(checks=[check, sibling_a]).checks[0]
    with_b = model.ChecksModel(checks=[check, sibling_b]).checks[0]
    assert checks.check_content_hash(with_a) == checks.check_content_hash(with_b)


@given(perm=st.permutations(list(_ROW_CHECK_FIELDS.keys())))
@settings(max_examples=30)
def test_property_checks_version_insensitive_to_authored_key_order(perm: tuple[str, ...]) -> None:
    """§13.2: "`checks_version`/per-check hashes insensitive to authored
    key order (parsed-form property, A-11's class)" -- the SAME check,
    authored as YAML text with its keys in every generated permutation,
    always parses to the identical `checks_version`/`check_content_hash`
    (the hash is over the PARSED model's `model_dump`, never the raw
    authored text -- §7.4's own claim, exercised at the YAML-authoring
    grain rather than merely the Python-kwargs grain, since **kwargs
    unpacking is trivially order-insensitive on its own and would prove
    nothing about the parse-then-hash pipeline)."""
    ordered = {key: _ROW_CHECK_FIELDS[key] for key in perm}
    text = yaml.safe_dump({"checks": [ordered]})
    cm = model.ChecksModel(**yaml.safe_load(text))
    assert checks.checks_version(cm) == checks.checks_version(
        model.ChecksModel(checks=[model.RowCheckModel(**_ROW_CHECK_FIELDS)])
    )
    assert checks.check_content_hash(cm.checks[0]) == checks.check_content_hash(
        model.RowCheckModel(**_ROW_CHECK_FIELDS)
    )
