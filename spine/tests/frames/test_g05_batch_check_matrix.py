"""G-05 — `batch_check` (006.1 §13.1). One-stop reading of the whole
scenario: (a) is live and asserted at the full `checks.yaml`/
`PipelineSpecModel` grain (the closest thing to "any `batch_check` in
checks.yaml" a unit test can exercise); (b)–(h) are **authored but
skipped-with-reason** — they name the member-grammar wait §9 itself names
("the member-scoped admitted-rows accessor, the parsed member
declarations, the `required:`-coherence check replacing K7's blanket
refusal") — 006.1 §14's B4 row, narrowed by conveyer-swb.15/D006-1's "build
now, dormant" ruling (below).

**(a)'s coverage is not new mechanism** — K7
(`core/model.py::ChecksModel._check_batch_check_awaiting_member_grammar`)
already carries dedicated unit coverage at the bare-`ChecksModel` grain
(`tests/unit/test_model.py::
test_checks_model_rejects_any_batch_check_awaiting_member_grammar`) and at
the bind-defect-matrix grain
(`tests/unit/test_bind_defect_matrix.py::
test_k7_batch_check_awaiting_member_grammar`); the test below adds the
full-spec-YAML-level exercise ("any `batch_check` in checks.yaml", §13.1's
own words) that neither of those two already covers, so this file's own
`test_g05a_...` plus those two existing tests are ALL registered as G-05(a)'s
standing tests in the acceptance-coverage audit.

**(b)–(h)'s wait NARROWED, not closed (conveyer-swb.15/D006-1's coordinator
ruling: "build now, dormant behind K7").** `compile_aggregate` (`core/
check_grammar.py`), its Column-builder (`frames/business_checks.py::
aggregate_column`), the [DC-2] bind assertion, the §7.5 verdict/comparison/
message channel, and the §8.4 demotion door ALL now exist and are tested
DIRECTLY (`tests/unit/test_check_grammar.py`'s `compile_aggregate` units,
`tests/frames/test_business_checks.py`'s verdict/message/door/[DC-2]-
property tests) — "no aggregate-position compiler exists anywhere" (this
docstring's own prior claim) is no longer true. What STILL waits, exactly
as §9 names it: the member-scoped admitted-rows ACCESSOR (which frame is
"the control member's admitted rows" for a given `checks.yaml`), the
parsed member DECLARATIONS themselves, and the `required:`-coherence
check — none of which exist, so a `checks.yaml`-grain END-TO-END scenario
(an authored `member: summary` control resolving to a REAL co-effect
frame, run through `stages/post_check.py::run()`) still cannot be built.
Each stub below is `pytest.mark.skip`-marked citing this narrower wait,
matching the precedent `tests/frames/test_business_checks.py`'s own
(now-resolved) G-08 aggregate-position skips originally set (bead
conveyer-6pg.11, B1) — never silently omitted from the G-suite's own
enumeration.
"""

from __future__ import annotations

import pytest
import yaml
from pydantic import ValidationError
from spine.core.model import ChecksModel, parse_pipeline_spec_yaml

# --- G-05(a): any `batch_check` in checks.yaml -> the named bind defect ----

_BASE_SPEC: dict[str, object] = dict(
    pipeline="pipelines/identity",
    transforms_module="pipelines.identity.transforms",
    raw_table="lake.identity__raw",
    quarantine_table="lake.identity__quarantine",
    fact_types={
        "detail": {
            "fact_table": "lake.identity__facts",
            "state_table": "lake.identity__state",
            "schema": {
                "columns": [
                    {"name": "domain_id", "type": "string"},
                    {"name": "amount", "type": "decimal(10,2)"},
                ],
                "domain_id_col": "domain_id",
                "record_key": ["domain_id"],
            },
        }
    },
    read={"dialect": {"format": "csv"}},
    raw_contract={"columns": [{"name": "domain_id", "required": True, "nullable": False}]},
    sla_minutes=480,
)

_BATCH_CHECK: dict[str, object] = {
    "kind": "batch_check",
    "id": "chk-reconcile",
    "fact_type": "detail",
    "aggregate": "sum(amount)",
    "control": {"member": "summary", "expr": "total"},
}


def test_g05a_any_batch_check_in_checks_yaml_is_a_bind_defect() -> None:
    """§13.1 G-05(a), verbatim: "any `batch_check` in checks.yaml ->
    bind-defect/batch-check-awaiting-member-grammar" -- exercised at the
    full YAML-parse grain (`parse_pipeline_spec_yaml`), the closest a unit
    test gets to "checks.yaml" itself, complementing the bare-`ChecksModel`
    (test_model.py) and hand-built-defect-matrix (test_bind_defect_matrix.py)
    grains this same K7 rule already carries."""
    text = yaml.safe_dump({**_BASE_SPEC, "checks": {"checks": [_BATCH_CHECK]}})
    with pytest.raises(ValidationError, match="bind-defect/batch-check-awaiting-member-grammar"):
        parse_pipeline_spec_yaml(text)


def test_g05a_batch_check_alongside_a_valid_row_check_still_refuses() -> None:
    # The wait fires unconditionally -- a batch_check entry refuses the
    # WHOLE spec even when authored beside an otherwise-valid row check
    # (K7 has no partial-admission path; ChecksModel.checks is validated
    # as one list).
    row_check = {
        "kind": "row",
        "id": "positive-amount",
        "fact_type": "detail",
        "expr": "amount > 0",
        "reason": "business/negative-amount",
    }
    with pytest.raises(ValidationError, match="bind-defect/batch-check-awaiting-member-grammar"):
        ChecksModel(checks=[row_check, _BATCH_CHECK])


# --- G-05(b-h): named wait, skipped-with-reason -----------------------------

_WAIT_REASON = (
    "sec7.5/sec8.4 batch_check reconciliation semantics: `batch_check` itself is "
    "structurally dormant until the 005 v1.x member grammar lands (P-6; K7 refuses "
    "every batch_check at bind, core/model.py::ChecksModel). conveyer-swb.15/D006-1 "
    "(coordinator ruling: 'build now, dormant behind K7') built and directly unit-"
    "tested compile_aggregate (core/check_grammar.py), its Column-builder "
    "(frames/business_checks.py::aggregate_column), the [DC-2] bind assertion, the "
    "sec7.5 verdict/comparison/message channel, and the sec8.4 demotion door -- "
    "see test_check_grammar.py's compile_aggregate units and "
    "test_business_checks.py's verdict/message/door/[DC-2]-property tests. What "
    "STILL waits, exactly as sec9 names it: the member-scoped admitted-rows "
    "ACCESSOR, the parsed member DECLARATIONS, and the `required:`-coherence "
    "check -- none of which exist, so a checks.yaml-grain END-TO-END scenario "
    "(an authored `member: summary` control resolved against a real co-effect "
    "frame, run through stages/post_check.py::run()) still cannot be built. "
    "006.1 §14's B4 row, narrowed by this ruling."
)


@pytest.mark.skip(reason=_WAIT_REASON)
def test_g05b_reconciliation_pass_and_mismatch_incl_tolerance_and_decimal_widening() -> (
    None
): ...  # (b): reconciliation pass/mismatch incl. tolerance and decimal-widening rows


@pytest.mark.skip(reason=_WAIT_REASON)
def test_g05c_empty_detail_per_node_sum_reconciles_combined_expressions() -> (
    None
): ...  # (c): empty detail: per-node sum -> 0 reconciles incl. combined expressions [EM-4];


#         nonzero control fails loud


@pytest.mark.skip(reason=_WAIT_REASON)
def test_g05d_control_row_quarantined_at_admission_is_control_unavailable() -> (
    None
): ...  # (d): control row quarantined at admission -> control-unavailable


@pytest.mark.skip(reason=_WAIT_REASON)
def test_g05e_two_control_rows_is_control_ambiguous_incl_value_identical_pair() -> (
    None
): ...  # (e): two control rows -> control-ambiguous (incl. the value-identical-duplicate


#         pair [AE-11])


@pytest.mark.skip(reason=_WAIT_REASON)
def test_g05f_null_extracted_control_value_is_control_unavailable() -> (
    None
): ...  # (f): NULL extracted control value -> control-unavailable [AE-4]


@pytest.mark.skip(reason=_WAIT_REASON)
def test_g05g_header_only_summary_member_is_control_unavailable() -> (
    None
): ...  # (g): header-only summary member -- zero rows admitted, zero quarantined ->


#         control-unavailable [AE-5]


@pytest.mark.skip(reason=_WAIT_REASON)
def test_g05h_all_null_column_sum_and_near_precision_38_overflow_is_aggregate_unavailable() -> (
    None
): ...  # (h): all-NULL-column sum and near-precision-38 overflow sum -> aggregate-unavailable


#         [EM-5]


@pytest.mark.skip(reason=_WAIT_REASON)
def test_g05bh_all_demote_on_facts_present_rerun() -> (
    None
): ...  # all of b-h demote on facts-present rerun (§8.4's demotion door)
