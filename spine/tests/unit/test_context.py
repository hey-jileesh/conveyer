"""Unit tests for `spine.context.BatchContext` — LLD §6.3.

M1 scope is "set-once readiness": construction with only the seed fields
supplied (every post-seed field defaults `None`/`()`/`False`), a single
`dataclasses.replace` accreting `guard_skips` alongside a normal field,
frozen-instance enforcement, and the `SET_ONCE_EXEMPT_FIELDS` convention
`run.py` (bead conveyer-nvh.19) will assert against. The set-once
*assertion itself* is out of scope here (that bead's job).
"""

import dataclasses
from datetime import UTC, datetime

import pytest
from spine import config, context
from spine.binding import Transforms
from spine.core.contract import check_version, read_spec_version
from spine.core.model import PipelineSpecModel


def _make_transforms() -> Transforms:
    return Transforms(
        apply=lambda valid_df, co_effects: valid_df,
        post_check=lambda candidate_df, co_effects: candidate_df,
        fold=lambda state_slice, facts_df: facts_df,
    )


def _make_spec() -> PipelineSpecModel:
    return PipelineSpecModel(
        pipeline="pipelines/commissions",
        transforms_module="pipelines.commissions.transforms",
        raw_table="lake.commissions__raw",
        quarantine_table="lake.commissions__quarantine",
        fact_table="lake.commissions__facts",
        state_table="lake.commissions__state",
        read={"dialect": {"format": "csv"}},
        raw_contract={"columns": [{"name": "id"}]},
    )


def _make_seed() -> context.BatchContext:
    spec = _make_spec()
    return context.BatchContext(
        pipeline="pipelines/commissions",
        feed_id="carrier-x/commission-statements",
        delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
        batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
        delivery_key="statement-2026-07.csv",
        content_hash="sha256:" + "a" * 64,
        object_uris=("s3://bucket/statement.csv",),
        received_at=datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
        spec=spec,
        run=config.RunConfig(),
        transforms=_make_transforms(),
        attempt_id="jr_abc123",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        read_spec_version=read_spec_version(spec.read),
        check_version=check_version(spec.raw_contract, spec.read),
    )


def test_seed_construction_defaults_post_seed_fields() -> None:
    seed = _make_seed()
    assert seed.guard_skips == ()
    assert seed.raw_df is None
    assert seed.raw_count is None
    assert seed.started_emitted is False
    assert seed.published is False
    assert seed.completed_event is None


def test_replace_sets_a_field_once() -> None:
    seed = _make_seed()
    after_land = dataclasses.replace(
        seed,
        raw_count=42,
        land_snapshot_id=7,
        started_emitted=True,
        guard_skips=seed.guard_skips + ("land",),
    )
    assert after_land.raw_count == 42
    assert after_land.land_snapshot_id == 7
    assert after_land.started_emitted is True
    assert after_land.guard_skips == ("land",)
    # the original seed is untouched -- replace never mutates in place
    assert seed.raw_count is None
    assert seed.guard_skips == ()


def test_guard_skips_accretes_across_multiple_replaces() -> None:
    seed = _make_seed()
    after_land = dataclasses.replace(seed, guard_skips=seed.guard_skips + ("land",))
    after_pre_check = dataclasses.replace(
        after_land, guard_skips=after_land.guard_skips + ("pre_check",)
    )
    assert after_pre_check.guard_skips == ("land", "pre_check")


def test_batch_context_is_frozen() -> None:
    seed = _make_seed()
    with pytest.raises(dataclasses.FrozenInstanceError):
        seed.raw_count = 5  # type: ignore[misc]


def test_set_once_exempt_fields_names_only_guard_skips() -> None:
    assert context.SET_ONCE_EXEMPT_FIELDS == frozenset({"guard_skips"})
