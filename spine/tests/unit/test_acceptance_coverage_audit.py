"""Acceptance-coverage audit — 006.1's G-suite (§13.1), the build epic's own
DONE bar for B4 (`conveyer-6pg.14`): "G-suite complete (waits named)."

**The lesson this file absorbs** (bead brief's own citation, `conveyer-
4ot.31`'s G-09 finding): an LLD's milestone table can OMIT a scenario id
from its own acceptance enumeration even though the id is required
elsewhere in the same document -- a documentation-completeness gap that
silently narrows what "done" means, discovered only by manual cross-
reading. This file makes that cross-read MECHANICAL and standing: it reads
`design/006.1_pure_core_lld.md` itself (never a hand-copied snapshot of its
text, which would drift the moment the doc is edited), extracts every
`G-NN` scenario id named in §13.1's own G-suite table and in §14's
Implementation Plan acceptance column, and cross-checks both sets against a
REGISTRY of (test file, test function) pairs below. An id present in the
doc but absent from the registry, or a registered function that does not
actually exist in its claimed file (a rename/deletion drift), fails this
audit loudly -- "an orphaned scenario id fails the audit," this bead's own
words.

**What "resolves" means for a registered pair.** The function must exist
(checked via `ast`, never `importlib` -- this audit stays import-cheap and
Spark-free, so it can run first, fast, in any suite) in its claimed file.
Existence is asked, not passing status: a `pytest.mark.skip`-marked stub
naming its wait (G-05(b–h), G-06's guard-present drift-born-null variant,
`compile_aggregate` fidelity) is a FULLY VALID resolution -- "authored,
waits named" is this bead's own DONE bar, not "every scenario green" (some
genuinely cannot be, structurally, until 005 v1.x lands or the sibling
epic's commit/fold rewrite lands).

**Scope discipline**: large PRE-EXISTING corpus files (G-07's 68-case
gatekeeper corpus, G-09's 30-case bind-defect matrix, G-11's record-key
suite) are registered with a GENEROUS anchor set spanning every major
section, not exhaustively enumerated case-by-case -- their own files
already claim (and are pinned by) full coverage; this audit's job is
confirming the scenario ID resolves to a REAL, present test surface, not
re-deriving that surface's own internal completeness a second time.
Freshly-authored files for this bead (G-01…G-06, G-10, G-12, and G-05's own
matrix) are registered exhaustively, since their full content is known.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_TESTS_ROOT = Path(__file__).resolve().parents[1]  # .../spine/tests
_LLD_PATH = _REPO_ROOT / "design" / "006.1_pure_core_lld.md"

# --- registry: G-id -> (file relative to spine/tests/, function name) ------

REGISTRY: dict[str, tuple[tuple[str, str], ...]] = {
    "G-01": (
        (
            "integration/test_multi_type_scenarios.py",
            "test_g01_row_and_membership_checks_quarantine_with_ordered_reason_detail",
        ),
    ),
    "G-02": (
        (
            "integration/test_multi_type_scenarios.py",
            "test_g02_fresh_path_count_identity_and_one_append_per_batch",
        ),
        (
            "integration/test_multi_type_scenarios.py",
            "test_g02_zero_violations_across_all_types_writes_nothing",
        ),
    ),
    "G-03": (
        (
            "integration/test_multi_type_scenarios.py",
            "test_g03_guard_present_rerun_cross_type_value_identical_rows_not_cross_subtracted",
        ),
    ),
    "G-04": (
        (
            "integration/test_multi_type_scenarios.py",
            "test_g04a_durable_authority_when_any_declared_fact_table_has_the_batch",
        ),
        (
            "integration/test_multi_type_scenarios.py",
            "test_g04b_zero_violation_rerun_after_checks_tightened_drift_recorded_no_new_rows",
        ),
        (
            "integration/test_multi_type_scenarios.py",
            "test_g04c_pre_check_any_door_fires_when_only_the_second_declared_type_has_facts",
        ),
    ),
    "G-05": (
        # (a): live, three registered call sites (model grain, bind-defect-
        # matrix grain, full-spec-YAML grain).
        ("unit/test_model.py", "test_checks_model_rejects_any_batch_check_awaiting_member_grammar"),
        ("unit/test_bind_defect_matrix.py", "test_k7_batch_check_awaiting_member_grammar"),
        (
            "frames/test_g05_batch_check_matrix.py",
            "test_g05a_any_batch_check_in_checks_yaml_is_a_bind_defect",
        ),
        (
            "frames/test_g05_batch_check_matrix.py",
            "test_g05a_batch_check_alongside_a_valid_row_check_still_refuses",
        ),
        # (b-h): authored, skipped-with-reason -- the 005 v1.x named wait.
        (
            "frames/test_g05_batch_check_matrix.py",
            "test_g05b_reconciliation_pass_and_mismatch_incl_tolerance_and_decimal_widening",
        ),
        (
            "frames/test_g05_batch_check_matrix.py",
            "test_g05c_empty_detail_per_node_sum_reconciles_combined_expressions",
        ),
        (
            "frames/test_g05_batch_check_matrix.py",
            "test_g05d_control_row_quarantined_at_admission_is_control_unavailable",
        ),
        (
            "frames/test_g05_batch_check_matrix.py",
            "test_g05e_two_control_rows_is_control_ambiguous_incl_value_identical_pair",
        ),
        (
            "frames/test_g05_batch_check_matrix.py",
            "test_g05f_null_extracted_control_value_is_control_unavailable",
        ),
        (
            "frames/test_g05_batch_check_matrix.py",
            "test_g05g_header_only_summary_member_is_control_unavailable",
        ),
        (
            "frames/test_g05_batch_check_matrix.py",
            "test_g05h_all_null_column_sum_and_near_precision_38_overflow_is_aggregate_unavailable",
        ),
        ("frames/test_g05_batch_check_matrix.py", "test_g05bh_all_demote_on_facts_present_rerun"),
    ),
    "G-06": (
        (
            "integration/test_multi_type_scenarios.py",
            "test_g06_null_domain_id_implicit_check_fires_first_no_record_key",
        ),
        (
            "integration/test_multi_type_scenarios.py",
            "test_g06_null_domain_id_survives_when_domain_id_col_outside_record_key",
        ),
        # guard-present drift-born-null variant [AE-3]: authored, skipped --
        # needs commit/fold (sibling B9b/B10 territory), the K-suite twin.
        (
            "integration/test_multi_type_scenarios.py",
            "test_g06_guard_present_drift_born_null_variant_ae3",
        ),
    ),
    "G-07": (
        # Generous anchor set across every major section of the 68-case
        # gatekeeper corpus (accept / reject / DC-3 divergence points /
        # property suite / family_of_kind) -- not exhaustive, see module
        # docstring's "scope discipline."
        ("unit/test_check_grammar.py", "test_accept_column_and_comparisons"),
        ("unit/test_check_grammar.py", "test_accept_typed_literals_both_spellings"),
        ("unit/test_check_grammar.py", "test_accept_aggregate_position_members"),
        ("unit/test_check_grammar.py", "test_reject_rand_and_current_timestamp"),
        ("unit/test_check_grammar.py", "test_reject_cast_and_double_colon_and_trycast_every_shape"),
        ("unit/test_check_grammar.py", "test_reject_hidden_extra_arguments"),
        ("unit/test_check_grammar.py", "test_reject_no_nested_aggregate"),
        ("unit/test_check_grammar.py", "test_dc3_concat_argument_families"),
        ("unit/test_check_grammar.py", "test_dc3_nullif_polymorphism"),
        (
            "unit/test_check_grammar.py",
            "test_property_family_fail_closed_over_generated_cross_family_comparisons",
        ),
        (
            "unit/test_check_grammar.py",
            "test_property_authored_text_identity_over_every_accept_shape",
        ),
        (
            "unit/test_check_grammar.py",
            "test_property_gatekeeper_fail_closed_regardless_of_nesting_depth",
        ),
        ("unit/test_check_grammar.py", "test_family_of_kind_covers_every_fact_column_kind"),
    ),
    "G-08": (
        # Critique gate wf_24a3125f-ecc F3 (bead conveyer-6pg.32): single-
        # homed in `spine.probes.g08_parity` -- `test_g08_executable_
        # semantics_table` now parametrizes over the imported `G08_VECTORS`
        # (both the 38 value-kind rows and the 7 supplementary dtype/raw
        # rows), subsuming the four standalone assertion functions this
        # registry used to name separately (`test_g08_null_safe_eq_both_
        # null_is_true`, `test_g08_row_position_int_div_int_is_double_
        # intermediate`, `test_g08_decimal_division_stays_decimal`,
        # `test_g08_round_bround_negative_control_half_up_vs_bankers`) --
        # deleted, their own discriminator rows now live as
        # `ParityVector` entries in the imported table instead.
        ("frames/test_business_checks.py", "test_g08_executable_semantics_table"),
        ("frames/test_business_checks.py", "test_g08_bround_is_not_grammar_admitted"),
        # aggregate position: authored, skipped -- the 005 v1.x named wait.
        ("frames/test_business_checks.py", "test_g08_count_1_vs_count_nullable_col_null_skip"),
        ("frames/test_business_checks.py", "test_g08_empty_set_per_aggregate_node_coalesce"),
        (
            "frames/test_business_checks.py",
            "test_g08_aggregate_unavailable_all_null_and_overflow_sum",
        ),
        ("frames/test_business_checks.py", "test_g08_decimal_sum_avg_widening_stays_decimal"),
    ),
    "G-09": (
        # Exhaustive -- all 30, §5-section-ordered (S -> C -> F -> K).
        ("unit/test_bind_defect_matrix.py", "test_s1_duplicate_key_top_level"),
        ("unit/test_bind_defect_matrix.py", "test_s2_fact_table_collision"),
        ("unit/test_bind_defect_matrix.py", "test_s3_custom_fold_refused"),
        ("unit/test_bind_defect_matrix.py", "test_s4_stale_post_check_export"),
        (
            "unit/test_bind_defect_matrix.py",
            "test_c1_duplicate_alias_discharged_via_s1_duplicate_key",
        ),
        ("unit/test_bind_defect_matrix.py", "test_c2_co_effect_table_grammar_is_bare_pydantic"),
        ("unit/test_bind_defect_matrix.py", "test_c3_co_effect_missing_table"),
        ("unit/test_bind_defect_matrix.py", "test_c4_co_effect_not_current_state"),
        ("unit/test_bind_defect_matrix.py", "test_c5_co_effect_unknown_columns"),
        ("unit/test_bind_defect_matrix.py", "test_c6_own_state_refused"),
        ("unit/test_bind_defect_matrix.py", "test_c7_membership_unknown_co_effect"),
        (
            "unit/test_bind_defect_matrix.py",
            "test_c8_membership_columns_outside_declaration_bind_half",
        ),
        ("unit/test_bind_defect_matrix.py", "test_f1_fact_types_shape_is_bare_pydantic"),
        ("unit/test_bind_defect_matrix.py", "test_f2_fact_schema_unknown_column_ref"),
        ("unit/test_bind_defect_matrix.py", "test_f3_fact_column_reserved_name"),
        ("unit/test_bind_defect_matrix.py", "test_f4_float_double_structurally_unrepresentable"),
        (
            "unit/test_bind_defect_matrix.py",
            "test_f5_ordering_type_not_comparable_bool_citing_imported_constant",
        ),
        ("unit/test_bind_defect_matrix.py", "test_k1a_check_duplicate_id"),
        ("unit/test_bind_defect_matrix.py", "test_k1b_check_id_reserved"),
        ("unit/test_bind_defect_matrix.py", "test_k2_check_unknown_fact_type"),
        ("unit/test_bind_defect_matrix.py", "test_k3_check_column_outside_type"),
        ("unit/test_bind_defect_matrix.py", "test_k4_check_expression_rejected"),
        ("unit/test_bind_defect_matrix.py", "test_k5a_check_expression_uncompilable"),
        ("unit/test_bind_defect_matrix.py", "test_k5b_check_expression_not_boolean"),
        ("unit/test_bind_defect_matrix.py", "test_k5c_check_expression_inexact_type"),
        ("unit/test_bind_defect_matrix.py", "test_k6a_check_reason_grammar_is_bare_pydantic"),
        ("unit/test_bind_defect_matrix.py", "test_k6b_check_reason_reserved"),
        ("unit/test_bind_defect_matrix.py", "test_k7_batch_check_awaiting_member_grammar"),
        ("unit/test_bind_defect_matrix.py", "test_k8_tolerance_grammar_is_bare_pydantic"),
        ("unit/test_bind_defect_matrix.py", "test_k9_check_expression_mixed_types"),
    ),
    "G-10": (
        ("frames/test_check_verdicts.py", "test_check_verdict_fixtures_exist"),
        ("frames/test_check_verdicts.py", "test_check_verdicts_reproduce_every_committed_vector"),
        ("frames/test_check_verdicts.py", "test_coverage_includes_three_valued_null_expr_passes"),
        ("frames/test_check_verdicts.py", "test_coverage_includes_membership_null_key_passes"),
        ("frames/test_check_verdicts.py", "test_coverage_includes_a_decimal_comparison"),
        ("frames/test_check_verdicts.py", "test_coverage_includes_temporal_functions"),
    ),
    "G-11": (
        (
            "frames/test_quarantine.py",
            "test_g11_record_key_reproduces_a_committed_record_key_vector_through_the_gate",
        ),
        (
            "frames/test_quarantine.py",
            "test_g11_record_key_null_when_any_declared_key_column_is_null",
        ),
        (
            "frames/test_quarantine.py",
            "test_g11_record_key_gate_interplay_domain_id_not_in_record_key_still_derives",
        ),
        ("frames/test_quarantine.py", "test_g11_cross_type_tag_discriminates_value_identical_rows"),
    ),
    "G-12": (
        (
            "integration/test_multi_type_scenarios.py",
            "test_g12_identity_exemplar_apply_returns_one_entry_mapping",
        ),
        (
            "integration/test_multi_type_scenarios.py",
            "test_g12_identity_violations_variant_has_no_post_check_re_exports_apply",
        ),
    ),
}

# 006.1 §13.2's property suite -- the seven buildable families plus the
# named `compile_aggregate`-fidelity skip (B4's own eighth entry, blocked on
# the same 005 v1.x wait; registered here, not under any single G-id, since
# §13.2 is its own obligation row alongside §13.1's G-suite, §16.8).
PROPERTY_REGISTRY: tuple[tuple[str, str], ...] = (
    (
        "unit/test_check_grammar.py",
        "test_property_gatekeeper_fail_closed_regardless_of_nesting_depth",
    ),
    ("unit/test_check_grammar.py", "test_property_authored_text_identity_over_every_accept_shape"),
    (
        "unit/test_check_grammar.py",
        "test_property_family_fail_closed_over_generated_cross_family_comparisons",
    ),
    (
        "frames/test_business_checks.py",
        "test_property_evaluation_order_first_failure_over_generated_multi_violation_rows",
    ),
    ("unit/test_core_checks.py", "test_property_checks_version_insensitive_to_authored_key_order"),
    (
        "frames/test_quarantine.py",
        "test_property_tag_discrimination_hashes_collide_iff_type_and_value_collide",
    ),
    (
        "frames/test_business_checks.py",
        "test_property_three_valued_law_generated_null_rows_never_quarantine",
    ),
    # compile_aggregate fidelity [DC-2]: authored, skipped -- named wait.
    ("frames/test_business_checks.py", "test_property_compile_aggregate_fidelity_dc2"),
)


def _test_function_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    }


def _all_pairs() -> list[tuple[str, str, str]]:
    """Flattens `REGISTRY` into `(g_id, file, function)` rows for
    parametrization -- one row per registered pair, so a single broken
    registration fails ONE case, not the whole audit opaquely."""
    return [(g_id, file, func) for g_id, pairs in REGISTRY.items() for file, func in pairs]


def test_registry_covers_g01_through_g12_exactly() -> None:
    expected = {f"G-{n:02d}" for n in range(1, 13)}
    assert set(REGISTRY.keys()) == expected


@pytest.mark.parametrize(
    "g_id,rel_file,function_name", _all_pairs(), ids=[f"{g}:{f}::{fn}" for g, f, fn in _all_pairs()]
)
def test_registered_pair_resolves_to_a_real_test_function(
    g_id: str, rel_file: str, function_name: str
) -> None:
    path = _TESTS_ROOT / rel_file
    assert path.is_file(), f"{g_id}: registered file does not exist: {rel_file}"
    names = _test_function_names(path)
    assert function_name in names, (
        f"{g_id}: registered function {function_name!r} not found in {rel_file} "
        f"(renamed or deleted? orphaned registry entry)"
    )


@pytest.mark.parametrize(
    "rel_file,function_name", PROPERTY_REGISTRY, ids=[f"{f}::{fn}" for f, fn in PROPERTY_REGISTRY]
)
def test_property_registry_entry_resolves_to_a_real_test_function(
    rel_file: str, function_name: str
) -> None:
    path = _TESTS_ROOT / rel_file
    assert path.is_file(), f"§13.2: registered file does not exist: {rel_file}"
    names = _test_function_names(path)
    assert function_name in names, (
        f"§13.2: registered property {function_name!r} not found in {rel_file}"
    )


# --- the doc cross-read: every G-id the LLD itself names must resolve ------


def _lld_text() -> str:
    assert _LLD_PATH.is_file(), f"design doc moved or renamed: {_LLD_PATH}"
    return _LLD_PATH.read_text(encoding="utf-8")


def _section(text: str, start_marker: str, end_marker: str) -> str:
    start = text.index(start_marker)
    end = text.index(end_marker, start + len(start_marker))
    return text[start:end]


def _extract_g_ids(text: str) -> set[str]:
    """`G-01…G-04`-shaped ranges (the ellipsis may be `…` or `...`) expand
    to every id in the inclusive range; every other `G-NN`/`GNN` token
    (§14's own "G11" -- one row's missing-dash typo, deliberately tolerated
    rather than silently missed) normalizes to `G-NN`."""
    ids: set[str] = set()
    for match in re.finditer(r"G-(\d{2})\s*(?:…|\.\.\.)\s*G-(\d{2})", text):
        lo, hi = int(match.group(1)), int(match.group(2))
        ids.update(f"G-{n:02d}" for n in range(lo, hi + 1))
    for match in re.finditer(r"G-?(\d{2})\b", text):
        ids.add(f"G-{int(match.group(1)):02d}")
    return ids


def test_every_g_suite_id_named_in_lld_section_13_1_is_registered() -> None:
    text = _section(_lld_text(), "### 13.1 G-suite", "### 13.2 Property")
    doc_ids = _extract_g_ids(text)
    assert doc_ids, "expected at least one G-id in §13.1's own table"
    orphaned = doc_ids - set(REGISTRY.keys())
    assert not orphaned, f"§13.1 names id(s) with no registry entry: {sorted(orphaned)}"
    # symmetric direction: the registry names exactly this set, never more
    # (an invented id not actually in the LLD would be a registry bug).
    assert set(REGISTRY.keys()) == doc_ids


def test_every_g_id_named_in_lld_section_14_acceptance_column_is_registered() -> None:
    text = _section(_lld_text(), "## 14. Implementation Plan", "## 15. Invariants")
    doc_ids = _extract_g_ids(text)
    assert doc_ids, "expected at least one G-id in §14's own acceptance column"
    orphaned = doc_ids - set(REGISTRY.keys())
    assert not orphaned, f"§14 names id(s) with no registry entry: {sorted(orphaned)}"
