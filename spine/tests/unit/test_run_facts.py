"""Unit tests for `spine.core.run_facts.transition`/`failed` — LLD §6.5, §7.3, [S-7].

Covers: `outcome` derivation (`ok` / `skipped-guard` from the `guard_skips`
delta / `failed`); per-stage field extraction (only the fields a stage
produced are populated, per §6.5's column table); `rows_merged` derivation
from `merge_summary`; `error_message` derivation for `TransientError`
(name-checked, no import from `effects/`), pydantic `ValidationError`
(input-stripped), and an arbitrary foreign exception (location only, no
message text — the row-value-leak guard).
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError
from spine import config, context
from spine.binding import Transforms
from spine.core import run_facts
from spine.core.model import PipelineSpecModel

_T0 = datetime(2026, 7, 28, 10, 0, 0, tzinfo=UTC)
_T1 = datetime(2026, 7, 28, 10, 0, 5, tzinfo=UTC)


def _make_spec() -> PipelineSpecModel:
    return PipelineSpecModel(
        pipeline="pipelines/commissions",
        transforms_module="pipelines.commissions.transforms",
        raw_table="lake.commissions__raw",
        quarantine_table="lake.commissions__quarantine",
        fact_table="lake.commissions__facts",
        state_table="lake.commissions__state",
    )


def _make_seed(**overrides: object) -> context.BatchContext:
    base = dict(
        pipeline="pipelines/commissions",
        feed_id="carrier-x/commission-statements",
        delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
        batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
        delivery_key="statement-2026-07.csv",
        content_hash="sha256:" + "a" * 64,
        object_uris=("s3://bucket/statement.csv",),
        received_at=datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
        spec=_make_spec(),
        run=config.RunConfig(),
        transforms=Transforms(
            apply=lambda valid_df, co_effects: valid_df,
            post_check=lambda candidate_df, co_effects: candidate_df,
            fold=lambda state_slice, facts_df: facts_df,
        ),
        attempt_id="jr_abc123",
        sfn_retry_count=0,
        sfn_redrive_count=0,
    )
    base.update(overrides)
    return context.BatchContext(**base)  # type: ignore[arg-type]


# --- outcome derivation ---------------------------------------------------------


def test_transition_outcome_ok_when_not_newly_guard_skipped() -> None:
    seed = _make_seed()
    after = dataclasses.replace(seed, raw_count=10, land_snapshot_id=3, started_emitted=True)
    fact = run_facts.transition("land", seed, after, _T0, _T1)
    assert fact.outcome == "ok"


def test_transition_outcome_skipped_guard_when_stage_newly_in_guard_skips() -> None:
    seed = _make_seed()
    after = dataclasses.replace(
        seed,
        raw_count=10,
        land_snapshot_id=3,
        started_emitted=True,
        guard_skips=seed.guard_skips + ("land",),
    )
    fact = run_facts.transition("land", seed, after, _T0, _T1)
    assert fact.outcome == "skipped-guard"


def test_transition_outcome_ok_when_guard_skip_was_already_present_before() -> None:
    # a stage already in guard_skips BEFORE this transition is not "newly"
    # skipped by THIS stage -- outcome should be "ok" for a later stage.
    seed = _make_seed(guard_skips=("land",))
    after = dataclasses.replace(seed, pre_quarantined_count=0, pre_quarantine_snapshot_id=None)
    fact = run_facts.transition("pre_check", seed, after, _T0, _T1)
    assert fact.outcome == "ok"


def test_transition_common_fields_come_from_ctx_after() -> None:
    seed = _make_seed()
    after = dataclasses.replace(seed, raw_count=5, land_snapshot_id=1)
    fact = run_facts.transition("land", seed, after, _T0, _T1)
    assert fact.batch_id == seed.batch_id
    assert fact.pipeline == seed.pipeline
    assert fact.feed_id == seed.feed_id
    assert fact.attempt_id == seed.attempt_id
    assert fact.sfn_retry_count == seed.sfn_retry_count
    assert fact.sfn_redrive_count == seed.sfn_redrive_count
    assert fact.stage == "land"
    assert fact.started_at == _T0
    assert fact.finished_at == _T1


# --- per-stage field extraction: only this stage's fields are set ----------


def test_land_stage_fields() -> None:
    seed = _make_seed()
    after = dataclasses.replace(seed, raw_count=100, land_snapshot_id=7, started_emitted=True)
    fact = run_facts.transition("land", seed, after, _T0, _T1)
    assert fact.raw_count == 100
    assert fact.snapshot_id == 7
    assert fact.pre_quarantined is None
    assert fact.facts_appended is None


def test_pre_check_stage_fields() -> None:
    seed = _make_seed()
    after = dataclasses.replace(seed, pre_quarantined_count=2, pre_quarantine_snapshot_id=8)
    fact = run_facts.transition("pre_check", seed, after, _T0, _T1)
    assert fact.pre_quarantined == 2
    assert fact.snapshot_id == 8
    assert fact.raw_count is None


def test_pull_stage_fields() -> None:
    seed = _make_seed()
    after = dataclasses.replace(seed, co_effects={}, co_effect_snapshot_ids={"lookup": 4})
    fact = run_facts.transition("pull", seed, after, _T0, _T1)
    assert fact.co_effect_snapshot_ids == {"lookup": 4}
    assert fact.snapshot_id is None


def test_apply_stage_produces_no_count_fields() -> None:
    seed = _make_seed()
    after = dataclasses.replace(seed, candidate_facts_df=None)
    fact = run_facts.transition("apply", seed, after, _T0, _T1)
    assert fact.raw_count is None
    assert fact.facts_appended is None
    assert fact.snapshot_id is None


def test_post_check_stage_fields() -> None:
    seed = _make_seed()
    after = dataclasses.replace(seed, post_quarantined_count=3, post_quarantine_snapshot_id=9)
    fact = run_facts.transition("post_check", seed, after, _T0, _T1)
    assert fact.post_quarantined == 3
    assert fact.snapshot_id == 9
    assert fact.error_message is None  # no drift -- untouched


# --- post_check_drift -> ledger error_message (I-12 [H-2], task ask) -------


def test_post_check_drift_set_folds_into_error_message_on_ok_outcome() -> None:
    seed = _make_seed()
    after = dataclasses.replace(
        seed,
        post_quarantined_count=1,
        post_quarantine_snapshot_id=9,
        post_check_drift="post-check drift: durable=1 recomputed=2 subset=False",
    )
    fact = run_facts.transition("post_check", seed, after, _T0, _T1)
    assert fact.outcome == "ok"
    assert fact.error_message == "post-check drift: durable=1 recomputed=2 subset=False"


def test_post_check_drift_none_leaves_error_message_unset() -> None:
    seed = _make_seed()
    after = dataclasses.replace(
        seed, post_quarantined_count=0, post_quarantine_snapshot_id=None, post_check_drift=None
    )
    fact = run_facts.transition("post_check", seed, after, _T0, _T1)
    assert fact.error_message is None


def test_post_check_drift_set_but_failed_outcome_wins_no_drift_message() -> None:
    # `failed()` never calls `_stage_fields` -- a stage-supplied
    # `post_check_drift` on `ctx` cannot leak into a genuinely failed
    # transition's `error_message`, which is always `_error_message(exc)`.
    seed = _make_seed()
    ctx = dataclasses.replace(
        seed,
        post_quarantined_count=1,
        post_quarantine_snapshot_id=9,
        post_check_drift="post-check drift: durable=1 recomputed=2 subset=False",
    )
    try:
        raise ValueError("boom")
    except ValueError as exc:
        fact = run_facts.failed("post_check", ctx, _T0, _T1, exc)
    assert fact.outcome == "failed"
    assert fact.error_type == "ValueError"
    assert fact.error_message != ctx.post_check_drift
    assert "boom" not in (fact.error_message or "")


def test_commit_stage_fields() -> None:
    seed = _make_seed()
    after = dataclasses.replace(seed, facts_appended=42, fact_snapshot_id=11)
    fact = run_facts.transition("commit", seed, after, _T0, _T1)
    assert fact.facts_appended == 42
    assert fact.snapshot_id == 11


def test_fold_stage_fields_no_op_reports_zero_rows_merged() -> None:
    seed = _make_seed()
    after = dataclasses.replace(
        seed, state_snapshot_id=None, merge_summary=None, state_read_snapshot_id=5
    )
    fact = run_facts.transition("fold", seed, after, _T0, _T1)
    assert fact.snapshot_id is None
    assert fact.rows_merged == 0
    assert fact.state_read_snapshot_id == 5
    assert fact.merge_summary is None


def test_fold_stage_fields_real_merge_derives_rows_merged_from_summary() -> None:
    seed = _make_seed()
    after = dataclasses.replace(
        seed,
        state_snapshot_id=99,
        merge_summary={"added-records": "7"},
        state_read_snapshot_id=5,
    )
    fact = run_facts.transition("fold", seed, after, _T0, _T1)
    assert fact.snapshot_id == 99
    assert fact.rows_merged == 7
    assert fact.merge_summary == {"added-records": "7"}


def test_publish_stage_produces_no_count_fields() -> None:
    seed = _make_seed()
    after = dataclasses.replace(seed, published=True)
    fact = run_facts.transition("publish", seed, after, _T0, _T1)
    assert fact.raw_count is None
    assert fact.facts_appended is None


def test_transition_rejects_unknown_stage() -> None:
    seed = _make_seed()
    with pytest.raises(ValueError, match="unknown stage"):
        run_facts.transition("not-a-stage", seed, seed, _T0, _T1)


# --- error_message derivation [S-7] ----------------------------------------


class TransientError(Exception):
    """Stand-in for `effects.records.TransientError`, matched by NAME only
    (`core/` may not import `effects/` -- see `run_facts.py`'s module
    docstring). A distinct class object that merely shares the name -- the
    point of the name-only check under test."""


def test_failed_transient_error_by_name_gets_type_and_first_line() -> None:
    seed = _make_seed()
    try:
        raise TransientError("commit failed: CommitFailedException\nsecond line")
    except TransientError as exc:
        fact = run_facts.failed("commit", seed, _T0, _T1, exc)
    assert fact.outcome == "failed"
    assert fact.error_type == "TransientError"
    assert fact.error_message == "TransientError: commit failed: CommitFailedException"


def test_failed_transient_error_message_truncated_to_256_chars() -> None:
    seed = _make_seed()
    long_message = "x" * 500
    try:
        raise TransientError(long_message)
    except TransientError as exc:
        fact = run_facts.failed("commit", seed, _T0, _T1, exc)
    assert fact.error_message is not None
    # "TransientError: " prefix (17 chars) + first line truncated to 256
    assert fact.error_message == "TransientError: " + ("x" * 256)


def test_failed_validation_error_strips_raw_input_value() -> None:
    seed = _make_seed()
    # uppercase -> violates the lowercase-only pipeline slug grammar, so THIS
    # exact value is the one pydantic would otherwise echo in `input_value`.
    sensitive_value = "SUPER-SECRET-ROW-VALUE-SHOULD-NOT-LEAK"
    try:
        PipelineSpecModel(
            pipeline=sensitive_value,
            transforms_module="pipelines.commissions.transforms",
            raw_table="lake.x__raw",
            quarantine_table="lake.x__quarantine",
            fact_table="lake.x__facts",
            state_table="lake.x__state",
        )
    except ValidationError as exc:
        fact = run_facts.failed("commit", seed, _T0, _T1, exc)
    assert fact.error_type == "ValidationError"
    assert fact.error_message is not None
    assert sensitive_value not in fact.error_message
    assert "pipeline" in fact.error_message  # the violated field IS named


def test_failed_foreign_exception_gets_location_not_message() -> None:
    seed = _make_seed()
    sensitive_value = "row-value-12345-should-not-leak"

    def _raises() -> None:
        raise KeyError(sensitive_value)

    try:
        _raises()
    except KeyError as exc:
        fact = run_facts.failed("apply", seed, _T0, _T1, exc)
    assert fact.error_type == "KeyError"
    assert fact.error_message is not None
    assert sensitive_value not in fact.error_message
    assert ":" in fact.error_message  # "module:lineno" shape
    assert "test_run_facts" in fact.error_message


def test_failed_foreign_exception_with_no_traceback_yields_unknown_location() -> None:
    seed = _make_seed()
    exc = ValueError("never raised")
    fact = run_facts.failed("apply", seed, _T0, _T1, exc)
    assert fact.error_message == "<unknown>:0"


@given(message=st.text(min_size=0, max_size=1000).filter(lambda s: "\n" not in s))
def test_error_message_never_exceeds_type_prefix_plus_256_chars(message: str) -> None:
    try:
        raise TransientError(message)
    except TransientError as exc:
        derived = run_facts._error_message(exc)
    assert derived is not None
    assert len(derived) <= len("TransientError: ") + 256


def test_run_fact_is_frozen() -> None:
    seed = _make_seed()
    after = dataclasses.replace(seed, raw_count=1, land_snapshot_id=1)
    fact = run_facts.transition("land", seed, after, _T0, _T1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        fact.raw_count = 2  # type: ignore[misc]
