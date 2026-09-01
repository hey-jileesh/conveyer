"""Unit tests for `spine.core.delta.resolve_predecessors` — LLD 007.1 §7.2
(F-5); the `resolve_predecessors` unit matrix over §7.2's fail-open paths
1-9 (this bead's DONE bar), plus self-exclusion and the N-1 standing audit.

Path numbering matches §7.2's own table verbatim (paths 1-9; path 10, a
marker read error, is a stage failure that never reaches this pure
function -- no test here models it, per the module's own docstring)."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime

import pytest
from spine.core.delta import (
    DELTA_PROBE_REFUSAL_REASONS,
    MarkerRow,
    PredecessorResolution,
    SeedAttrs,
    resolve_predecessors,
)

_SELF = "batch-self"


def _at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 8, 1, hour, minute, tzinfo=UTC)


def _seed(
    *,
    supersedes: str | None = None,
    delivery_key: str = "dk-self",
    content_hash: str = "ch-self",
) -> SeedAttrs:
    return SeedAttrs(
        batch_id=_SELF,
        delivery_key=delivery_key,
        delivery_content_hash=content_hash,
        supersedes_batch_id=supersedes,
    )


def _row(
    batch_id: str,
    *,
    dk: str = "dk-other",
    ch: str = "ch-other",
    at: datetime | None = None,
) -> MarkerRow:
    return MarkerRow(
        batch_id=batch_id,
        delivery_key=dk,
        delivery_content_hash=ch,
        received_at=at if at is not None else _at(10),
    )


# --- §7.2 path 1: genesis / fresh feed --------------------------------------


def test_path_1_genesis_fresh_feed_is_empty_no_refusal() -> None:
    result = resolve_predecessors(_seed(), [], [], [])
    assert result == PredecessorResolution(predecessor_batch_ids=(), probe_refusal=None)


# --- §7.2 path 2: same-feed guard-twin presence without completion ---------


def test_path_2_coherence_clause_voids_read1_for_an_orphan_sibling() -> None:
    completion = [_row("batch-A", at=_at(9))]  # a real, otherwise-legitimate winner
    presence = [_row("batch-B")]  # B: guard-twin, NO completion row anywhere
    result = resolve_predecessors(_seed(), completion, presence, [])
    # the orphan sibling voids read 1 ENTIRELY -- batch-A's legitimate
    # completion is discarded too, not just batch-B's absence.
    assert result == PredecessorResolution(predecessor_batch_ids=(), probe_refusal=None)


def test_path_2_coherence_clause_fires_with_no_completed_batch_at_all() -> None:
    presence = [_row("batch-B")]
    result = resolve_predecessors(_seed(), [], presence, [])
    assert result == PredecessorResolution(predecessor_batch_ids=(), probe_refusal=None)


# --- §7.2 path 3: marker-without-facts kill state ---------------------------
#
# "indistinguishable from path 2 -- same refusal" (§7.2): this pure function
# never reads fact tables, so a killed-before-first-append guard-twin row
# and an in-flight sibling's guard-twin row are the SAME input shape here.


def test_path_3_marker_without_facts_is_mechanically_path_2() -> None:
    presence_killed = [_row("batch-C")]
    result = resolve_predecessors(_seed(), [], presence_killed, [])
    assert result == PredecessorResolution(predecessor_batch_ids=(), probe_refusal=None)


# --- §7.2 path 4: latest completed batch is zero-fact -----------------------


def test_path_4_zero_fact_predecessor_is_still_named() -> None:
    # batch-Z wrote a completion row but NO guard-twin rows (it dropped
    # everything) -- resolve_predecessors names it anyway; the "vacuous
    # comparison" consequence is delta_filter's concern, not this function's.
    result = resolve_predecessors(_seed(), [_row("batch-Z", at=_at(9))], [], [])
    assert result == PredecessorResolution(predecessor_batch_ids=("batch-Z",), probe_refusal=None)


# --- §7.2 path 5: probe refusal `none-with-key-match` -----------------------


def test_path_5_none_with_key_match_on_a_key_hit_with_different_hash() -> None:
    other = _row("batch-D", dk="dk-self", ch="ch-different", at=_at(8))
    seed = _seed(delivery_key="dk-self", content_hash="ch-self")
    result = resolve_predecessors(seed, [other], [], [])
    assert result == PredecessorResolution(
        predecessor_batch_ids=(), probe_refusal="none-with-key-match"
    )


def test_path_5_none_with_key_match_on_internal_disagreement_of_a_key_matching_candidate() -> None:
    # batch-E's own rows disagree with each other (a write bug) -- refused
    # even though NEITHER individual row, taken alone, differs in hash from
    # the seed's own.
    completion = [MarkerRow("batch-E", "dk-self", "ch-self", _at(8, 0))]
    # same batch, different received_at -- an internal write-bug disagreement
    presence = [MarkerRow("batch-E", "dk-self", "ch-self", _at(8, 5))]
    result = resolve_predecessors(
        _seed(delivery_key="dk-self", content_hash="ch-self"), completion, presence, []
    )
    assert result == PredecessorResolution(
        predecessor_batch_ids=(), probe_refusal="none-with-key-match"
    )


def test_path_5_no_refusal_when_no_row_shares_our_delivery_key() -> None:
    unrelated = _row("batch-F", dk="dk-completely-different", ch="ch-x", at=_at(8))
    result = resolve_predecessors(_seed(delivery_key="dk-self"), [unrelated], [], [])
    assert result.probe_refusal is None


# --- §7.2 path 6: probe refusal `target-incoherent` -------------------------


def test_path_6_target_incoherent_on_delivery_key_mismatch_vs_superseding_delivery() -> None:
    target_rows = [MarkerRow("batch-T", "dk-OTHER", "ch-t", _at(7))]
    result = resolve_predecessors(
        _seed(supersedes="batch-T", delivery_key="dk-self", content_hash="ch-self"),
        [],
        [],
        target_rows,
    )
    assert result == PredecessorResolution(
        predecessor_batch_ids=(), probe_refusal="target-incoherent"
    )


def test_path_6_target_incoherent_on_internal_disagreement_among_targets_own_rows() -> None:
    target_rows = [
        MarkerRow("batch-T", "dk-self", "ch-t", _at(7, 0)),
        MarkerRow("batch-T", "dk-self", "ch-t", _at(7, 30)),  # disagreeing received_at
    ]
    result = resolve_predecessors(
        _seed(supersedes="batch-T", delivery_key="dk-self"), [], [], target_rows
    )
    assert result == PredecessorResolution(
        predecessor_batch_ids=(), probe_refusal="target-incoherent"
    )


# --- §7.2 path 7: probe refusal `target-unmarked` ---------------------------


def test_path_7_target_unmarked_when_the_named_target_has_no_marker_rows() -> None:
    result = resolve_predecessors(_seed(supersedes="batch-GHOST"), [], [], [])
    assert result == PredecessorResolution(
        predecessor_batch_ids=(), probe_refusal="target-unmarked"
    )


# --- §7.2 path 8: `horizon-exceeded` is constructed-but-unreachable --------


def test_path_8_horizon_exceeded_is_a_named_reason_never_produced_in_phase_1() -> None:
    assert "horizon-exceeded" in DELTA_PROBE_REFUSAL_REASONS
    assert DELTA_PROBE_REFUSAL_REASONS == frozenset(
        {"none-with-key-match", "target-incoherent", "target-unmarked", "horizon-exceeded"}
    )
    # Phase 1 never models retention -- no combination of this function's
    # four arguments can produce "horizon-exceeded"; this test documents the
    # reserved-but-unreachable status rather than exercising a code path.


# --- §7.2 path 9: interim -- field not yet in the seed ----------------------


def test_path_9_field_absent_lawful_no_hit_is_track_e_provably_absent() -> None:
    result = resolve_predecessors(_seed(supersedes=None, delivery_key="dk-self"), [], [], [])
    assert result == PredecessorResolution(predecessor_batch_ids=(), probe_refusal=None)


# --- combined + structural properties ---------------------------------------


def test_read1_and_read2_combine_when_both_succeed() -> None:
    completion = [_row("batch-A", at=_at(9))]
    target_rows = [MarkerRow("batch-T2", "dk-self", "ch-old", _at(7))]
    result = resolve_predecessors(
        _seed(supersedes="batch-T2", delivery_key="dk-self"), completion, [], target_rows
    )
    assert result == PredecessorResolution(
        predecessor_batch_ids=("batch-A", "batch-T2"), probe_refusal=None
    )


def test_a_read2_refusal_discards_a_legitimate_read1_finding_too() -> None:
    # batch-A is a perfectly good read-1 winner; a target-unmarked refusal
    # on read 2 must still force the WHOLE resolution empty (§7.2:
    # "delta_probe_refusal … non-null ⇒ every per-type filter short-
    # circuits to keep-all" -- batch grain, not Track-E-scoped).
    completion = [_row("batch-A", at=_at(9))]
    result = resolve_predecessors(_seed(supersedes="batch-GHOST"), completion, [], [])
    assert result == PredecessorResolution(
        predecessor_batch_ids=(), probe_refusal="target-unmarked"
    )


def test_self_exclusion_a_batchs_own_prior_attempt_rows_never_count() -> None:
    own_prior_completion = [_row(_SELF, at=_at(9))]
    own_prior_presence = [_row(_SELF)]
    result = resolve_predecessors(_seed(), own_prior_completion, own_prior_presence, [])
    assert result == PredecessorResolution(predecessor_batch_ids=(), probe_refusal=None)


def test_n1_standing_audit_never_fabricates_a_batch_id() -> None:
    # "resolve_predecessors takes the target IN and never returns a batch
    # id it was not given" -- every returned id is either a completion_rows
    # batch_id (read 1) or the caller-supplied supersedes_batch_id (read 2).
    completion = [_row("batch-A", at=_at(9)), _row("batch-Q", at=_at(9, 30))]
    result = resolve_predecessors(_seed(), completion, [], [])
    allowed = {row.batch_id for row in completion}
    assert set(result.predecessor_batch_ids) <= allowed


def test_n1_standing_audit_target_only_ever_the_given_supersedes_id() -> None:
    target_rows = [MarkerRow("batch-T3", "dk-self", "ch-old", _at(7))]
    result = resolve_predecessors(
        _seed(supersedes="batch-T3", delivery_key="dk-self"), [], [], target_rows
    )
    assert result.predecessor_batch_ids == ("batch-T3",)


def test_predecessor_resolution_is_frozen() -> None:
    result = resolve_predecessors(_seed(), [], [], [])
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.probe_refusal = "target-unmarked"  # type: ignore[misc]
