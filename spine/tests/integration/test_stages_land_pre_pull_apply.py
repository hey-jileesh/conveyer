"""`stages/{land,pre_check,pull,apply}.py` — LLD §7.5, §6.5/§6.6, I-6, I-7, I-12, I-19, [T-6][E-5].

Uses `local_runner_fx` (the REAL production assembly) throughout — no
parallel test-only builder, matching every other integration suite in this
package (`test_spark_fx.py`, `test_substrate.py`). The seed `BatchContext` is
built directly (bypassing the entrypoint/`bind_transforms`) with a minimal
inline `Transforms` double — plain functions, no `importlib` binding needed
for these four stages.

**005.1 §4.4 DDL parity swap (bead conveyer-azr.19, n3-admission-cut):**
`_create_raw_table`/`_create_quarantine_table` call `bootstrap.create_
admission_tables`'s own DDL builders (the same functions `scenario_helpers.
py` now uses) rather than a hand-rolled shape — the raw table's physical DDL
depends only on `_CONTRACT_COLUMN_NAMES` (declared columns are always
`STRING`/nullable regardless of a contract's own `nullable`/`required`
flags, D-5), so one fixed base contract suffices for every test in this
file regardless of which columns a given test's OWN `raw_contract` (via
`_make_spec`'s `required_columns`) later declares `required`/`nullable:
false`.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import SparkSession
from spine.binding import Transforms
from spine.bootstrap.create_admission_tables import (
    render_quarantine_create_table_sql,
    render_raw_create_table_sql,
)
from spine.config import RunConfig
from spine.context import BatchContext
from spine.core.contract import check_version, read_spec_version
from spine.core.model import CoEffectDecl, ColumnSpec, PipelineSpecModel, RawContractModel
from spine.effects.records import RunnerFx
from spine.stages import apply, land, pre_check, pull

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from tests.conftest import MotoEventsBus

_CATALOG_PREFIX = "spine_cat."

# This suite's raw table is (id, amount) -- the raw DDL's own column NAMES
# (§4.1: declared columns are always STRING/nullable regardless of a
# contract's `required`/`nullable` flags, D-5), independent of whichever
# columns a given test's `raw_contract` (via `_make_spec`'s
# `required_columns`) marks `required: true, nullable: false`.
_CONTRACT_COLUMN_NAMES = ("id", "amount")
_BASE_RAW_CONTRACT = RawContractModel(columns=[ColumnSpec(name=c) for c in _CONTRACT_COLUMN_NAMES])


def _bare(qualified_table: str) -> str:
    assert qualified_table.startswith(_CATALOG_PREFIX)
    return qualified_table.removeprefix(_CATALOG_PREFIX)


def _create_raw_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(render_raw_create_table_sql(qualified_table, _BASE_RAW_CONTRACT))


def _create_quarantine_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(render_quarantine_create_table_sql(qualified_table))


def _create_fact_table(spark: SparkSession, qualified_table: str) -> None:
    """pre_check's [DC-1] fact-presence probe (`table_has_batch(fact_table,
    batch_id, None)`) needs a REAL table carrying a `batch_id` column
    (`I-3`'s guard predicate), even in tests that never populate it -- an
    absent table raises `AnalysisException`, not `False`."""
    spark.sql(f"CREATE TABLE {qualified_table} (domain_id STRING, batch_id STRING) USING iceberg")


def _create_coeff_table(spark: SparkSession, qualified_table: str, *, seed_row: bool) -> None:
    spark.sql(f"CREATE TABLE {qualified_table} (k STRING, v STRING) USING iceberg")
    if seed_row:
        spark.sql(f"INSERT INTO {qualified_table} VALUES ('k1', 'v1')")


def _passthrough_transforms() -> Transforms:
    return Transforms(
        apply=lambda valid_df, co_effects: valid_df,
        post_check=lambda candidate_df, co_effects: candidate_df,
        fold=lambda state_slice, facts_df: facts_df,
    )


# `_make_spec`'s `required_columns` param (a TEST-local name, not a model
# field: `PipelineSpecModel.required_columns` was deleted, A-12) drives
# `raw_contract`'s `nullable: false`/`required: true` columns -- the real
# contract grammar `stages/pre_check.py::compile_contract` compiles into its
# `not-nullable` check (`contract/null-violation`, §6.1).


def _raw_contract(required_columns: list[str] | None) -> RawContractModel:
    names = set(required_columns or [])
    return RawContractModel(
        columns=[
            ColumnSpec(name=c, nullable=c not in names, required=c in names)
            for c in _CONTRACT_COLUMN_NAMES
        ]
    )


def _make_spec(
    *,
    raw_table: str,
    quarantine_table: str,
    fact_table: str = "spine_test_tables.unused_fact",
    required_columns: list[str] | None = None,
    co_effects: dict[str, CoEffectDecl] | None = None,
    serialize: bool = False,
) -> PipelineSpecModel:
    return PipelineSpecModel(
        pipeline="pipelines/m3-stages",
        transforms_module="pipelines.m3_stages",
        co_effects=co_effects or {},
        raw_table=raw_table,
        quarantine_table=quarantine_table,
        fact_table=fact_table,
        state_table="spine_test_tables.unused_state",
        read={"dialect": {"format": "csv"}},
        raw_contract=_raw_contract(required_columns),
        serialize=serialize,
    )


def _make_seed(
    *,
    spec: PipelineSpecModel,
    batch_id: str,
    object_uris: tuple[str, ...],
    transforms: Transforms | None = None,
) -> BatchContext:
    return BatchContext(
        pipeline="pipelines/m3-stages",
        feed_id="feed/m3-stages",
        delivery_id=str(uuid.UUID(int=1, version=4)),
        batch_id=batch_id,
        delivery_key="statement.csv",
        content_hash="sha256:" + "a" * 64,
        object_uris=object_uris,
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        spec=spec,
        run=RunConfig(),
        transforms=transforms or _passthrough_transforms(),
        attempt_id="attempt-1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        read_spec_version=read_spec_version(spec.read),
        check_version=check_version(spec.raw_contract, spec.read),
    )


def _batch_id(n: int) -> str:
    return str(uuid.UUID(int=n, version=5))


def _write_csv(path: Path, text: str) -> tuple[str, ...]:
    path.write_text(text)
    return (str(path),)


# --- land: fresh write vs. guard-skip, both emit unconditionally (I-7) ------


def test_land_fresh_writes_raw_stamps_snapshot_and_emits_batch_started(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("land_fresh_raw")
    qtn_qt = unique_table("land_fresh_qtn")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    spec = _make_spec(raw_table=_bare(raw_qt), quarantine_table=_bare(qtn_qt))
    batch_id = _batch_id(1)
    object_uris = _write_csv(tmp_path / "batch.csv", "id,amount\n1,10.5\n2,20.0\n")
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)

    after = land.run(seed, local_runner_fx)

    assert after.raw_count == 2
    assert after.land_snapshot_id is not None
    assert after.guard_skips == ()
    assert after.started_emitted is True
    assert sorted(r["id"] for r in after.raw_df.collect()) == ["1", "2"]

    envelopes = moto_events_bus.read_events()
    assert len(envelopes) == 1
    assert envelopes[0]["detail-type"] == "batch-started"
    detail = envelopes[0]["detail"]
    assert detail["batch_id"] == batch_id
    assert detail["raw_count"] == 2
    assert detail["land_snapshot_id"] == after.land_snapshot_id


def test_land_guard_skip_reruns_without_write_and_still_emits(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus: MotoEventsBus,
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("land_skip_raw")
    qtn_qt = unique_table("land_skip_qtn")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    spec = _make_spec(raw_table=_bare(raw_qt), quarantine_table=_bare(qtn_qt))
    batch_id = _batch_id(2)
    object_uris = _write_csv(tmp_path / "batch.csv", "id,amount\n1,10.5\n")
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)

    first = land.run(seed, local_runner_fx)
    moto_events_bus.read_events()  # drain the first attempt's event

    second = land.run(first, local_runner_fx)

    assert second.guard_skips == ("land",)
    assert second.raw_count == 1  # the ONE deliberate .count() action, [T-6]
    assert second.land_snapshot_id == first.land_snapshot_id  # stamped-summary resolution, I-19
    assert second.started_emitted is True

    envelopes = moto_events_bus.read_events()
    assert len(envelopes) == 1  # unconditional re-emit, I-7 -- guard-skip never skips the event
    assert envelopes[0]["detail"]["raw_count"] == 1


# --- pre_check: zero-violation vs. with-violations vs. guard-skip rerun -----


def test_pre_check_zero_violations_writes_nothing_and_no_guard_row(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("pre_check_clean_raw")
    qtn_qt = unique_table("pre_check_clean_qtn")
    fact_qt = unique_table("pre_check_clean_fact")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    spec = _make_spec(
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        required_columns=["amount"],
    )
    batch_id = _batch_id(3)
    object_uris = _write_csv(tmp_path / "batch.csv", "id,amount\n1,10.5\n2,20.0\n")
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    landed = land.run(seed, local_runner_fx)
    before_snapshots = spark.read.format("iceberg").load(f"{qtn_qt}.snapshots").count()

    after = pre_check.run(landed, local_runner_fx)

    after_snapshots = spark.read.format("iceberg").load(f"{qtn_qt}.snapshots").count()
    assert after_snapshots == before_snapshots  # no write at all
    assert after.pre_quarantined_count == 0
    assert after.pre_quarantine_snapshot_id is None
    assert after.guard_skips == ()  # a data-driven skip is NOT a guard-skip
    assert sorted(r["id"] for r in after.valid_df.collect()) == ["1", "2"]
    assert local_runner_fx.table_has_batch(_bare(qtn_qt), batch_id, "pre_check") is False


def test_pre_check_with_violations_counts_and_valid_plus_viol_equals_raw(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("pre_check_dirty_raw")
    qtn_qt = unique_table("pre_check_dirty_qtn")
    fact_qt = unique_table("pre_check_dirty_fact")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    spec = _make_spec(
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        required_columns=["amount"],
    )
    batch_id = _batch_id(4)
    object_uris = _write_csv(tmp_path / "batch.csv", "id,amount\n1,10.5\n2,\n3,20.0\n")
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    landed = land.run(seed, local_runner_fx)

    after = pre_check.run(landed, local_runner_fx)

    assert after.pre_quarantined_count == 1
    assert after.pre_quarantine_snapshot_id is not None
    valid_ids = sorted(r["id"] for r in after.valid_df.collect())
    assert valid_ids == ["1", "3"]
    assert len(valid_ids) + after.pre_quarantined_count == landed.raw_count  # candidate identity
    assert after.guard_skips == ()
    qtn_rows = spark.table(qtn_qt).where(f"batch_id = '{batch_id}'").collect()
    assert len(qtn_rows) == 1
    # 005.1 §4.2's fixed quarantine shape: the offending row's own `id`
    # value lives inside the JSON `row_snapshot`, not as a table column.
    assert '"id":"2"' in qtn_rows[0]["row_snapshot"]
    assert qtn_rows[0]["check_stage"] == "pre_check"
    assert qtn_rows[0]["reason_code"] == "contract/null-violation"


def test_pre_check_guard_skip_rerun_recomputes_same_valid_df_without_rewriting(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    raw_qt = unique_table("pre_check_rerun_raw")
    qtn_qt = unique_table("pre_check_rerun_qtn")
    fact_qt = unique_table("pre_check_rerun_fact")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    spec = _make_spec(
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        required_columns=["amount"],
    )
    batch_id = _batch_id(5)
    object_uris = _write_csv(tmp_path / "batch.csv", "id,amount\n1,10.5\n2,\n")
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    landed = land.run(seed, local_runner_fx)
    first = pre_check.run(landed, local_runner_fx)

    second = pre_check.run(first, local_runner_fx)

    assert second.guard_skips == ("pre_check",)
    # this attempt's own append count -- the ledger signature
    assert second.pre_quarantined_count == 0
    assert second.pre_quarantine_snapshot_id == first.pre_quarantine_snapshot_id
    assert sorted(r["id"] for r in second.valid_df.collect()) == ["1"]  # recomputed identically


# --- pull: pinned reads incl. zero-snapshot sentinel; zero instrumentation -


def test_pull_reads_two_co_effects_incl_zero_snapshot_sentinel(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    seeded_qt = unique_table("pull_seeded")
    empty_qt = unique_table("pull_empty")
    _create_coeff_table(spark, seeded_qt, seed_row=True)
    _create_coeff_table(spark, empty_qt, seed_row=False)  # zero-snapshot table, I-6 [T-19]
    spec = _make_spec(
        raw_table="spine_test_tables.unused_raw",
        quarantine_table="spine_test_tables.unused_qtn",
        co_effects={
            "seeded": CoEffectDecl(table=_bare(seeded_qt)),
            "empty": CoEffectDecl(table=_bare(empty_qt)),
        },
    )
    seed = _make_seed(spec=spec, batch_id=_batch_id(6), object_uris=("s3://unused/x",))

    after = pull.run(seed, local_runner_fx)

    assert set(after.co_effects) == {"seeded", "empty"}
    assert set(after.co_effect_snapshot_ids) == {"seeded", "empty"}
    assert after.co_effect_snapshot_ids["seeded"] != -1
    assert after.co_effect_snapshot_ids["empty"] == -1
    assert [r["k"] for r in after.co_effects["seeded"].collect()] == ["k1"]
    assert after.co_effects["empty"].count() == 0


def test_pull_own_state_without_serialize_reads_but_logs_nothing(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Critique F4 (bead conveyer-nvh.43): the `decl.own_state and not spec.
    serialize` WARNING moved to `spine.binding.bind_transforms` (once per
    run, pre-land — `tests/unit/test_binding.py`'s own coverage) so that it
    fires once per RUN instead of once per ATTEMPT/`pull` call. `pull` still
    performs the pinned read correctly; it now carries zero instrumentation
    of its own (§7.3), so this scenario produces no log records here at
    all."""
    own_qt = unique_table("pull_own_state")
    _create_coeff_table(spark, own_qt, seed_row=True)
    spec = _make_spec(
        raw_table="spine_test_tables.unused_raw",
        quarantine_table="spine_test_tables.unused_qtn",
        co_effects={"self": CoEffectDecl(table=_bare(own_qt), own_state=True)},
        serialize=False,
    )
    seed = _make_seed(spec=spec, batch_id=_batch_id(7), object_uris=("s3://unused/x",))

    with caplog.at_level(logging.WARNING, logger="spine.stages.pull"):
        after = pull.run(seed, local_runner_fx)

    assert after.co_effect_snapshot_ids["self"] != -1
    assert caplog.records == []


# --- apply: pure pass-through, nothing else ---------------------------------


def test_apply_calls_transforms_apply_with_valid_df_and_co_effects_only(
    local_runner_fx: RunnerFx,
) -> None:
    seen: dict[str, object] = {}

    def _apply(valid_df: DataFrame, co_effects: object) -> DataFrame:
        seen["valid_df"] = valid_df
        seen["co_effects"] = co_effects
        return valid_df

    spec = _make_spec(
        raw_table="spine_test_tables.unused_raw", quarantine_table="spine_test_tables.unused_qtn"
    )
    transforms = Transforms(
        apply=_apply,
        post_check=lambda candidate_df, co_effects: candidate_df,
        fold=lambda state_slice, facts_df: facts_df,
    )
    seed = _make_seed(
        spec=spec, batch_id=_batch_id(8), object_uris=("s3://unused/x",), transforms=transforms
    )
    marker_valid_df = object()
    marker_co_effects = object()
    ctx = replace(seed, valid_df=marker_valid_df, co_effects=marker_co_effects)  # type: ignore[arg-type]

    after = apply.run(ctx, local_runner_fx)

    assert after.candidate_facts_df is marker_valid_df
    assert seen["valid_df"] is marker_valid_df
    assert seen["co_effects"] is marker_co_effects


def test_apply_never_calls_fx() -> None:
    class _PoisonFx:
        def __getattr__(self, name: str) -> object:
            raise AssertionError(f"apply must never call fx.{name} -- it is pure (§7.5)")

    spec = _make_spec(
        raw_table="spine_test_tables.unused_raw", quarantine_table="spine_test_tables.unused_qtn"
    )
    seed = _make_seed(spec=spec, batch_id=_batch_id(9), object_uris=("s3://unused/x",))
    ctx = replace(seed, valid_df="marker-df", co_effects={})  # type: ignore[arg-type]

    after = apply.run(ctx, _PoisonFx())  # type: ignore[arg-type]

    assert after.candidate_facts_df == "marker-df"
