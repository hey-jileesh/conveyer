"""Unit tests for `spine.run.run` — the sequence driver, LLD §7.3.

Driver sequencing is exercised over a STUB stage tuple (plain functions
mutating context via `dataclasses.replace`), never the real `spine.stages`
package (still a stub owned by a later bead) -- `run()`'s `stages=None`
default is what would reach for `spine.stages.SEQUENCE`, and is deliberately
not exercised here (see `run.py`'s own docstring for the mechanism).
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta

import pytest
from spine import config, context, run
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


class _FakeFx:
    """Just enough of `RunnerFx`'s shape for the driver: `now` + `record_run`.

    A plain duck-typed double, not a `RunnerFx` instance -- `run()` only
    ever calls these two fields, and the driver itself does no isinstance
    checking (values, not types, per 002.1 §7.0 idiom).
    """

    def __init__(self) -> None:
        self._t = datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC)
        self.recorded: list = []

    def now(self) -> datetime:
        self._t += timedelta(seconds=1)
        return self._t

    def record_run(self, fact) -> None:
        self.recorded.append(fact)


def _stage_land(ctx: context.BatchContext, fx: _FakeFx) -> context.BatchContext:
    return dataclasses.replace(ctx, raw_count=10, land_snapshot_id=1)


def _stage_pre_check(ctx: context.BatchContext, fx: _FakeFx) -> context.BatchContext:
    return dataclasses.replace(ctx, pre_quarantined_count=0, pre_quarantine_snapshot_id=2)


def _stage_raises(ctx: context.BatchContext, fx: _FakeFx) -> context.BatchContext:
    raise ValueError("boom")


def _stage_overwrites_raw_count(ctx: context.BatchContext, fx: _FakeFx) -> context.BatchContext:
    # illegal: raw_count was already set by _stage_land -- this stage resets it
    return dataclasses.replace(ctx, raw_count=999)


def _stage_overwrites_read_spec_version(
    ctx: context.BatchContext, fx: _FakeFx
) -> context.BatchContext:
    # illegal: read_spec_version is a no-default seed field (005.1 A-11) --
    # already-set from construction onward, same as `pipeline`/`feed_id`.
    return dataclasses.replace(ctx, read_spec_version="a-different-hash")


def _stage_guard_skip(name: str):
    def _stage(ctx: context.BatchContext, fx: _FakeFx) -> context.BatchContext:
        return dataclasses.replace(ctx, guard_skips=ctx.guard_skips + (name,))

    return _stage


def test_run_threads_context_and_records_ok_transitions_in_order() -> None:
    seed = _make_seed()
    fx = _FakeFx()
    stages = (("land", _stage_land), ("pre_check", _stage_pre_check))

    final = run.run(seed, fx, stages=stages)

    assert final.raw_count == 10
    assert final.land_snapshot_id == 1
    assert final.pre_quarantined_count == 0
    assert final.pre_quarantine_snapshot_id == 2
    assert [fact.stage for fact in fx.recorded] == ["land", "pre_check"]
    assert [fact.outcome for fact in fx.recorded] == ["ok", "ok"]
    # the seed itself is untouched -- replace never mutates in place
    assert seed.raw_count is None


def test_run_records_failed_row_and_reraises_on_a_raising_stage() -> None:
    seed = _make_seed()
    fx = _FakeFx()
    stages = (("land", _stage_land), ("pull", _stage_raises))

    with pytest.raises(ValueError, match="boom"):
        run.run(seed, fx, stages=stages)

    assert [(fact.stage, fact.outcome) for fact in fx.recorded] == [
        ("land", "ok"),
        ("pull", "failed"),
    ]
    failed_fact = fx.recorded[-1]
    assert failed_fact.error_type == "ValueError"


def test_run_stops_at_the_failing_stage_never_reaching_later_stages() -> None:
    seed = _make_seed()
    fx = _FakeFx()
    stages = (("land", _stage_raises), ("pre_check", _stage_pre_check))

    with pytest.raises(ValueError):
        run.run(seed, fx, stages=stages)

    assert [fact.stage for fact in fx.recorded] == ["land"]


def test_run_trips_set_once_assertion_when_a_stage_overwrites_a_set_field() -> None:
    seed = _make_seed()
    fx = _FakeFx()
    stages = (("land", _stage_land), ("pre_check", _stage_overwrites_raw_count))

    with pytest.raises(AssertionError, match="raw_count"):
        run.run(seed, fx, stages=stages)


def test_run_trips_set_once_assertion_for_read_spec_version_no_exemption_needed() -> None:
    """005.1 A-11/§3.5 (bead conveyer-azr.18): `read_spec_version`/
    `check_version` carry no dataclass default (like `pipeline`/`feed_id`
    above them) -- `run.py::_assert_set_once`'s reflective walk already
    treats a no-default field as "already set from the very first stage
    onward" with zero exemption-list changes, so this is a plain
    reuse of the existing mechanism, not new machinery."""
    seed = _make_seed()
    fx = _FakeFx()
    stages = (("land", _stage_overwrites_read_spec_version),)

    with pytest.raises(AssertionError, match="read_spec_version"):
        run.run(seed, fx, stages=stages)


def test_run_allows_guard_skips_to_accrete_across_stages() -> None:
    seed = _make_seed()
    fx = _FakeFx()
    stages = (
        ("land", _stage_guard_skip("land")),
        ("pre_check", _stage_guard_skip("pre_check")),
    )

    final = run.run(seed, fx, stages=stages)

    assert final.guard_skips == ("land", "pre_check")
    assert [fact.outcome for fact in fx.recorded] == ["skipped-guard", "skipped-guard"]


def test_run_returns_the_seed_unchanged_when_stages_is_empty() -> None:
    seed = _make_seed()
    fx = _FakeFx()

    final = run.run(seed, fx, stages=())

    assert final is seed
    assert fx.recorded == []
