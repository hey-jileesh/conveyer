"""K-suite acceptance-coverage audit — 007.1's own K-suite (§13.1/§13.2),
the sibling of `test_acceptance_coverage_audit.py`'s G-suite audit (006.1),
now built for the record-side LLD. B11-local's own DONE bar (bead
`conveyer-6pg.23`): "every K-01..K-27 id -> standing test, all fourteen
§13.2 kill rows claimed."

**Same lesson, same construction (see the G-suite audit's own module
docstring for the full account).** This file reads `design/
007.1_record_lld.md` itself (never a hand-copied snapshot, which would
drift the moment the doc is edited), extracts every `K-NN` scenario id
named in §13.1's own table, §13.2's own kill-matrix table, and §14's
Implementation Plan acceptance column, and cross-checks all three sets
against a REGISTRY of `(test file, test function)` pairs below. An id
present in the doc but absent from the registry, or a registered function
that does not actually exist in its claimed file (a rename/deletion
drift), fails this audit loudly.

**"Resolves" means the function EXISTS** (checked via `ast`, never
`importlib` — import-cheap, Spark-free, so this audit runs first, fast, in
any suite) in its claimed file — existence is asked, not passing status
(the G-suite audit's own rule, restated).

**Registration granularity.** K-01 through K-13 (largely landed at earlier
B-milestones, B6/B8/B9) are registered with the SAME "generous anchor set"
convention the G-suite audit uses for its own pre-existing corpus files
(K-01/K-02/K-03's own vector-reproduction files each carry many supporting
tests beyond the one anchor registered here; K-05..K-13/K-20/K-21's own
`test_scenarios_commit.py` literally names each function `test_kNN_...`,
one per id, registered exhaustively since the 1:1 naming makes it free).
K-14..K-27 (this bead's own scope plus the sibling B10 fold bead) are
registered exhaustively.

**A same-prefix naming collision, found and deliberately excluded**:
`tests/unit/test_bind_defect_matrix.py` carries its OWN, UNRELATED
`test_k2_..`/`test_k3_..`/`test_k4_..`/`test_k7_..`/`test_k8_..`/
`test_k9_..` functions — 006.1 §5's check-kind bind-defect codes (`K1..K9`,
no dash, already registered under G-09 in the G-suite audit), a different
numbering scheme that merely happens to share the `test_k` prefix. None of
those are 007.1 K-suite ids; verified by grep and deliberately absent from
the REGISTRY below.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_TESTS_ROOT = Path(__file__).resolve().parents[1]  # .../spine/tests
_LLD_PATH = _REPO_ROOT / "design" / "007.1_record_lld.md"

# --- registry: K-id -> (file relative to spine/tests/, function name) ------

REGISTRY: dict[str, tuple[tuple[str, str], ...]] = {
    "K-01": (
        ("unit/test_identity.py", "test_derive_record_key_reproduces_every_committed_vector"),
    ),
    "K-02": (
        ("unit/test_fact_hash.py", "test_content_hash_reproduces_every_committed_vector"),
        (
            "frames/test_facts.py",
            "test_stamp_fact_identity_udf_reproduces_every_committed_vector",
        ),
    ),
    "K-03": (
        ("unit/test_fact_hash.py", "test_content_hash_is_insensitive_to_stamp_values"),
        ("frames/test_facts.py", "test_extra_frame_columns_never_move_content_hash"),
    ),
    "K-04": (
        (
            "frames/test_facts.py",
            "test_dc3_tying_property_plan_side_rendering_matches_canonical_json",
        ),
    ),
    "K-05": (
        (
            "integration/test_scenarios_commit.py",
            "test_k05_second_batch_drops_unchanged_keeps_changed_and_new",
        ),
        (
            "integration/test_scenarios_commit.py",
            "test_k05_within_batch_divergent_duplicates_both_commit",
        ),
    ),
    "K-06": (
        (
            "integration/test_scenarios_commit.py",
            "test_k06_none_with_key_match_refusal_recorded_and_emitted",
        ),
    ),
    "K-07": (
        (
            "integration/test_scenarios_commit.py",
            "test_k07_read1_disagreeing_winner_names_no_predecessor",
        ),
    ),
    "K-08": (
        (
            "integration/test_scenarios_commit.py",
            "test_k08_guard_twin_vs_completion_discriminated_by_sentinel_idempotent",
        ),
    ),
    "K-09": (
        (
            "integration/test_scenarios_commit.py",
            "test_k09_guard_twin_without_completion_names_no_predecessor",
        ),
    ),
    "K-10": (
        (
            "integration/test_scenarios_commit.py",
            "test_k10_kill_between_twin_and_append_then_rerun_converges",
        ),
    ),
    "K-11": (
        (
            "integration/test_scenarios_commit.py",
            "test_k11_zero_fact_batch_writes_completion_but_no_twin_or_facts",
        ),
    ),
    "K-12": (
        (
            "integration/test_scenarios_commit.py",
            "test_k12_null_domain_id_candidate_is_a_named_defect_never_silent",
        ),
    ),
    "K-13": (
        (
            "integration/test_scenarios_commit.py",
            "test_k13_schema_drift_candidate_is_a_named_defect",
        ),
        # the [AE-3] guard-present variant, self-labeled "007.1 §13.1's K-13"
        # in its own docstring -- the G-06 twin's second variant (also
        # registered under G-06 in the G-suite audit; cited, not
        # duplicated, per the K-suite gathering note, 007.1 §16.1).
        (
            "integration/test_multi_type_scenarios.py",
            "test_g06_guard_present_drift_born_null_variant_ae3",
        ),
    ),
    "K-14": (
        (
            "integration/test_k_suite_fold.py",
            "test_k14_ordering_predicate_agrees_with_the_reference_over_9219_generated_cases",
        ),
        (
            "integration/test_k_suite_fold.py",
            "test_k14_reduce_sort_directives_agree_with_the_reference_over_the_same_cases",
        ),
    ),
    "K-15": (
        (
            "integration/test_k_suite_fold.py",
            "test_k15_cardinality_defect_names_the_target_state_table",
        ),
    ),
    "K-16": (
        (
            "integration/test_k_suite_fold.py",
            "test_k16_fold_all_equals_incremental_fold_shuffled_arrival_nulls_included",
        ),
        (
            "integration/test_k_suite_fold.py",
            "test_k16_late_file_older_received_at_batch_folding_after_a_newer_one_converges",
        ),
        (
            "integration/test_k_suite_fold.py",
            "test_k16_concurrent_sibling_merges_converge_via_i11_retry",
        ),
        # rebuild-equivalence sub-variant -- landed B11-local (addendum 2),
        # closing the wait the sibling B10 bead's own module docstring named.
        (
            "integration/test_k_suite_fold.py",
            "test_k16_rebuild_equivalence_fold_all_equals_rebuild_swap",
        ),
    ),
    "K-17": (
        (
            "integration/test_k_suite_rebuild.py",
            "test_k17_mid_rebuild_fold_swap_refuses_then_repin_recompute_converges",
        ),
    ),
    "K-18": (
        (
            "integration/test_k_suite_rebuild.py",
            "test_k18_straddling_merge_conflicts_and_retry_converges",
        ),
    ),
    "K-19": (
        (
            "integration/test_k_suite_rebuild.py",
            "test_k19_batch_committed_before_pin_folded_after_swap_is_a_logical_noop",
        ),
    ),
    "K-20": (
        (
            "integration/test_scenarios_commit.py",
            "test_k20_kill_between_type_appends_then_rerun_converges",
        ),
    ),
    "K-21": (
        (
            "integration/test_scenarios_commit.py",
            "test_k21_kill_after_last_twin_before_completion_then_rerun_opens_predecessor_window",
        ),
    ),
    "K-22": (
        (
            "integration/test_k_suite_fold.py",
            "test_k22_kill_between_fold_merges_then_rerun_converges",
        ),
    ),
    "K-23": (
        (
            "integration/test_k_suite_fold.py",
            "test_k23_kill_after_fold_before_publish_then_rerun_emits",
        ),
    ),
    "K-24": (
        (
            "integration/test_k_suite_fold.py",
            "test_k24_stale_extra_attempt_of_old_batch_after_newer_batch_folded_never_regresses",
        ),
    ),
    "K-25": (
        (
            "integration/test_k_suite_fold.py",
            "test_k25_disjoint_domain_sibling_merge_conflict_is_efficiency_never_correctness",
        ),
    ),
    "K-26": (
        (
            "integration/test_k_suite_rebuild.py",
            "test_k26_rebuild_killed_before_its_swap_leaves_state_untouched",
        ),
    ),
    "K-27": (
        (
            "integration/test_k_suite_rebuild.py",
            "test_k27_killed_between_swap_and_announcement_completed_by_rerunning",
        ),
    ),
}

# §13.2's own fourteen kill-matrix rows, id-keyed here so "all fourteen rows
# claimed" is asserted as a literal count, not just implied by REGISTRY's
# own (larger) K-01..K-27 coverage.
KILL_MATRIX_IDS: frozenset[str] = frozenset(
    {
        "K-10",
        "K-13",
        "K-15",
        "K-17",
        "K-18",
        "K-19",
        "K-20",
        "K-21",
        "K-22",
        "K-23",
        "K-24",
        "K-25",
        "K-26",
        "K-27",
    }
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
    """Flattens `REGISTRY` into `(k_id, file, function)` rows for
    parametrization -- one row per registered pair, so a single broken
    registration fails ONE case, not the whole audit opaquely."""
    return [(k_id, file, func) for k_id, pairs in REGISTRY.items() for file, func in pairs]


def test_registry_covers_k01_through_k27_exactly() -> None:
    expected = {f"K-{n:02d}" for n in range(1, 28)}
    assert set(REGISTRY.keys()) == expected


def test_kill_matrix_ids_are_a_registered_subset_of_exactly_fourteen() -> None:
    """B11-local's own DONE bar, literally: "all fourteen §13.2 kill rows
    claimed" -- the count is asserted directly, not just implied."""
    assert len(KILL_MATRIX_IDS) == 14
    assert KILL_MATRIX_IDS <= set(REGISTRY.keys())


@pytest.mark.parametrize(
    "k_id,rel_file,function_name", _all_pairs(), ids=[f"{k}:{f}::{fn}" for k, f, fn in _all_pairs()]
)
def test_registered_pair_resolves_to_a_real_test_function(
    k_id: str, rel_file: str, function_name: str
) -> None:
    path = _TESTS_ROOT / rel_file
    assert path.is_file(), f"{k_id}: registered file does not exist: {rel_file}"
    names = _test_function_names(path)
    assert function_name in names, (
        f"{k_id}: registered function {function_name!r} not found in {rel_file} "
        f"(renamed or deleted? orphaned registry entry)"
    )


# --- the doc cross-read: every K-id the LLD itself names must resolve ------


def _lld_text() -> str:
    assert _LLD_PATH.is_file(), f"design doc moved or renamed: {_LLD_PATH}"
    return _LLD_PATH.read_text(encoding="utf-8")


def _section(text: str, start_marker: str, end_marker: str) -> str:
    start = text.index(start_marker)
    end = text.index(end_marker, start + len(start_marker))
    return text[start:end]


def _extract_k_ids(text: str) -> set[str]:
    """`K-14…K-16`-shaped ranges (the ellipsis may be `…` or `...`) expand
    to every id in the inclusive range; every other `K-NN` token (incl. one
    half of a `K-26/K-27`-shaped pair) normalizes to `K-NN` -- the exact
    mechanism `test_acceptance_coverage_audit.py::_extract_g_ids` already
    uses for the G-suite, adapted to the `K-` prefix."""
    ids: set[str] = set()
    for match in re.finditer(r"K-(\d{2})\s*(?:…|\.\.\.)\s*K-(\d{2})", text):
        lo, hi = int(match.group(1)), int(match.group(2))
        ids.update(f"K-{n:02d}" for n in range(lo, hi + 1))
    for match in re.finditer(r"K-(\d{2})\b", text):
        ids.add(f"K-{int(match.group(1)):02d}")
    return ids


def test_every_k_suite_id_named_in_lld_section_13_1_is_registered() -> None:
    text = _section(_lld_text(), "### 13.1 K-suite", "### 13.2 Kill-matrix coverage")
    doc_ids = _extract_k_ids(text)
    assert doc_ids, "expected at least one K-id in §13.1's own table"
    orphaned = doc_ids - set(REGISTRY.keys())
    assert not orphaned, f"§13.1 names id(s) with no registry entry: {sorted(orphaned)}"
    # symmetric direction: §13.1 names every K-01..K-19 id (the K-20..K-27
    # kill-matrix-only ids are §13.2's own table, checked below) -- no
    # invented id in the registry that the doc's own §13.1 doesn't name.
    assert doc_ids == {f"K-{n:02d}" for n in range(1, 20)}


def test_every_k_suite_id_named_in_lld_section_13_2_kill_matrix_is_registered() -> None:
    text = _section(_lld_text(), "### 13.2 Kill-matrix coverage", "### 13.3 CI gates")
    doc_ids = _extract_k_ids(text)
    assert doc_ids, "expected at least one K-id in §13.2's own kill-matrix table"
    orphaned = doc_ids - set(REGISTRY.keys())
    assert not orphaned, f"§13.2 names id(s) with no registry entry: {sorted(orphaned)}"
    # §13.2 is 007.1's own claim of "every §11 row carries a scenario id" --
    # the doc's own id set must be EXACTLY the fourteen this file asserts.
    assert doc_ids == KILL_MATRIX_IDS


def test_every_k_id_named_in_lld_section_14_acceptance_column_is_registered() -> None:
    text = _section(_lld_text(), "## 14. Implementation Plan", "## 15. Invariants")
    doc_ids = _extract_k_ids(text)
    assert doc_ids, "expected at least one K-id in §14's own acceptance column"
    orphaned = doc_ids - set(REGISTRY.keys())
    assert not orphaned, f"§14 names id(s) with no registry entry: {sorted(orphaned)}"
