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
from types import MappingProxyType

import pytest
from spine import config, context, run
from spine.binding import Transforms
from spine.core.checks import checks_version
from spine.core.contract import check_version, read_spec_version
from spine.core.model import PipelineSpecModel


def _make_transforms() -> Transforms:
    # 006.1 §4.4 (bead conveyer-6pg.13, B3): `Transforms` drops `post_check`;
    # `apply` now returns a `Mapping[str, DataFrame]`. Critique gate
    # wf_24a3125f-ecc F2 (bead conveyer-6pg.31): `Transforms` drops `fold`
    # too -- `apply` is its ONLY field now.
    return Transforms(apply=lambda valid_df, co_effects: {"t": valid_df})


def _make_spec() -> PipelineSpecModel:
    return PipelineSpecModel(
        pipeline="pipelines/commissions",
        transforms_module="pipelines.commissions.transforms",
        raw_table="lake.commissions__raw",
        quarantine_table="lake.commissions__quarantine",
        # 006.1 P-1: singular fact_table/state_table replaced by a per-type
        # `fact_types` mapping -- this fixture just needs SOME valid spec.
        fact_types={
            "detail": {
                "fact_table": "lake.commissions__facts",
                "state_table": "lake.commissions__state",
                "schema": {
                    "columns": [{"name": "domain_id", "type": "string"}],
                    "domain_id_col": "domain_id",
                    "record_key": ["domain_id"],
                },
            }
        },
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
        checks_version=checks_version(spec.checks),
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


# --- 007.1 §4.2's seven per-type BatchContext deltas, against the REAL
# --- `_assert_set_once` (bead conveyer-6pg.17, B6) --------------------------
#
# The ledger-mappingproxy defect class has bitten before (`effects/
# ledger.py::_row_from_run_fact`'s `dataclasses.asdict` + `copy.deepcopy`
# choking on `types.MappingProxyType`, fixed bead conveyer-nvh.36) -- this
# suite deliberately exercises the REAL reflective assertion end-to-end
# through `run.run()` (never a hand-rolled `!=` comparison standing in for
# it), with the new fields wrapped in `types.MappingProxyType` exactly as
# `context.py`'s own construction-site convention documents, so a future
# regression in `_assert_set_once`'s own MappingProxyType handling (there is
# none today -- `!=` alone, no `asdict`/`deepcopy` in this code path) would
# be caught here too.


def _stage_commit(ctx: context.BatchContext, fx: _FakeFx) -> context.BatchContext:
    return dataclasses.replace(
        ctx,
        facts_appended_by_table=MappingProxyType({"detail": 3}),
        commit_snapshot_ids=MappingProxyType({"detail": 101}),
        delta_predecessor_batch_ids=("batch-prev",),
        delta_read_snapshot_ids=MappingProxyType({"detail": 55}),
        delta_probe_refusal=None,
    )


def _stage_fold(ctx: context.BatchContext, fx: _FakeFx) -> context.BatchContext:
    return dataclasses.replace(
        ctx,
        rows_merged_by_table=MappingProxyType({"detail": 3}),
        fold_snapshot_ids=MappingProxyType({"detail": 202}),
    )


def _stage_fold_overwrites_a_commit_mapping_field(
    ctx: context.BatchContext, fx: _FakeFx
) -> context.BatchContext:
    # illegal: facts_appended_by_table was already set by _stage_commit
    return dataclasses.replace(ctx, facts_appended_by_table=MappingProxyType({"detail": 999}))


def _stage_fold_overwrites_delta_predecessor_batch_ids(
    ctx: context.BatchContext, fx: _FakeFx
) -> context.BatchContext:
    # illegal: delta_predecessor_batch_ids was already set by _stage_commit,
    # to a DIFFERENT (non-default) tuple
    return dataclasses.replace(ctx, delta_predecessor_batch_ids=("batch-other",))


def test_run_allows_a_single_legitimate_set_of_all_seven_per_type_delta_fields() -> None:
    seed = _make_seed()
    fx = _FakeFx()
    stages = (("commit", _stage_commit), ("fold", _stage_fold))

    final = run.run(seed, fx, stages=stages)

    assert dict(final.facts_appended_by_table) == {"detail": 3}
    assert dict(final.commit_snapshot_ids) == {"detail": 101}
    assert final.delta_predecessor_batch_ids == ("batch-prev",)
    assert dict(final.delta_read_snapshot_ids) == {"detail": 55}
    assert final.delta_probe_refusal is None
    assert dict(final.rows_merged_by_table) == {"detail": 3}
    assert dict(final.fold_snapshot_ids) == {"detail": 202}
    assert [fact.outcome for fact in fx.recorded] == ["ok", "ok"]


def test_run_trips_set_once_on_a_mappingproxytype_field_overwrite() -> None:
    seed = _make_seed()
    fx = _FakeFx()
    stages = (("commit", _stage_commit), ("fold", _stage_fold_overwrites_a_commit_mapping_field))

    with pytest.raises(AssertionError, match="facts_appended_by_table"):
        run.run(seed, fx, stages=stages)


def test_run_trips_set_once_on_a_delta_predecessor_batch_ids_overwrite() -> None:
    seed = _make_seed()
    fx = _FakeFx()
    stages = (
        ("commit", _stage_commit),
        ("fold", _stage_fold_overwrites_delta_predecessor_batch_ids),
    )

    with pytest.raises(AssertionError, match="delta_predecessor_batch_ids"):
        run.run(seed, fx, stages=stages)
