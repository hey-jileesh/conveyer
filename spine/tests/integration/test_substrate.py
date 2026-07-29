"""Fixture self-tests for the M2 conftest substrate — LLD §12.1, I-2, I-6,
[T-16].

These are not pipeline-stage tests: they exist to prove the *fixtures
themselves* are trustworthy — the substrate `local_runner_fx`/`unique_table`/
`ledger_catalog` and friends will be built on top of, in R-01..R-14 (§12.4)
and nvh.18+. If any of these fail, the fixtures are the defect, not a stage.

Type annotations referencing `tests/conftest.py`'s own dataclasses
(`LedgerCatalogFixture`, `MotoEventsBus`, `TickableClock`) are
`TYPE_CHECKING`-guarded, not imported at runtime: `tests/` has no
`__init__.py` (deliberately, see `tests/conftest.py`'s own module docstring
on per-subdir collision-avoidance), so `tests.conftest` is not a runtime-
importable module path here — pytest's own conftest-loading mechanism
provides these fixtures by name injection, which needs no import at all.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel
from pyspark.sql import SparkSession
from spine.core.run_facts import RunFact
from spine.effects import events, ledger
from spine.effects.records import RunnerFx

if TYPE_CHECKING:
    from tests.conftest import LedgerCatalogFixture, MotoEventsBus, TickableClock

# --- session catalog: create + append + MERGE INTO (proves extensions + ----
# catalog live, independent of the fixture's own internal assert) ------------


def test_session_catalog_create_append_merge(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    table = unique_table("merge_probe")
    spark.sql(f"CREATE TABLE {table} (id INT, v STRING) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")

    spark.createDataFrame([(1, "a-updated"), (3, "c")], ["id", "v"]).createOrReplaceTempView(
        "merge_probe_source"
    )
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING merge_probe_source s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.v = s.v
        WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
        """
    )

    rows = sorted((row["id"], row["v"]) for row in spark.table(table).collect())
    assert rows == [(1, "a-updated"), (2, "b"), (3, "c")]


# --- unique_table: two tests, same prefix, no collision ---------------------


def test_unique_table_first_write_with_prefix(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    table = unique_table("independence")
    spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1)")
    assert [row["id"] for row in spark.table(table).collect()] == [1]


def test_unique_table_second_write_same_prefix_does_not_collide(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    table = unique_table("independence")
    spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (2)")
    assert [row["id"] for row in spark.table(table).collect()] == [2]


# --- ledger fixture: bootstrap + one pyiceberg append + read back -----------


def _run_fact(**overrides: object) -> RunFact:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    base: dict[str, object] = dict(
        batch_id="b1",
        pipeline="p1",
        feed_id="f1",
        attempt_id="a1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        stage="land",
        outcome="ok",
        started_at=now,
        finished_at=now,
    )
    base.update(overrides)
    return RunFact(**base)  # type: ignore[arg-type]


def test_ledger_catalog_round_trip(ledger_catalog: LedgerCatalogFixture) -> None:
    record_run = ledger.build_record_run(ledger_catalog.catalog, ledger_catalog.config)
    record_run(_run_fact(raw_count=10))

    rows = ledger_catalog.rows()
    assert len(rows) == 1
    assert rows[0]["batch_id"] == "b1"
    assert rows[0]["raw_count"] == 10


# --- moto bus round trip via effects.events.build_emit ----------------------


class _Probe(BaseModel):
    x: int


def test_moto_events_bus_round_trip(moto_events_bus: MotoEventsBus) -> None:
    emit = events.build_emit(moto_events_bus.client, moto_events_bus.bus_name)

    emit("batch-started", _Probe(x=1))

    envelopes = moto_events_bus.read_events()
    assert len(envelopes) == 1
    assert envelopes[0]["detail-type"] == "batch-started"
    assert envelopes[0]["detail"] == {"x": 1}


def test_moto_events_bus_is_isolated_per_test(moto_events_bus: MotoEventsBus) -> None:
    """A second test requesting the fixture sees no leftover events from the
    previous test — function scope, fresh `mock_aws()` each time."""
    assert moto_events_bus.read_events() == []


# --- wrapped-fx pass-through --------------------------------------------------


def test_make_wrapped_fx_pass_through_wraps_only_named_fields(
    local_runner_fx: RunnerFx,
    make_wrapped_fx: Callable[..., RunnerFx],
) -> None:
    calls: list[str] = []

    def _spy(original: Callable[..., object]) -> Callable[..., object]:
        def _wrapped(*args: object, **kwargs: object) -> object:
            calls.append("now")
            return original(*args, **kwargs)

        return _wrapped

    wrapped = make_wrapped_fx(local_runner_fx, {"now": _spy})

    result = wrapped.now()

    assert calls == ["now"]
    assert result == local_runner_fx.now()
    # every other field passes through unchanged (identity, no incidental
    # wrapping) -- a representative sample, not every field, is sufficient
    # to prove the mechanism only touches what it's told to.
    assert wrapped.record_run is local_runner_fx.record_run
    assert wrapped.emit is local_runner_fx.emit
    assert wrapped.config is local_runner_fx.config


# --- local_runner_fx skeleton: real emit + real record_run + controllable now,
# spark-side fields still nvh.18 stubs ---------------------------------------


def test_local_runner_fx_emit_is_real_and_moto_backed(
    local_runner_fx: RunnerFx, moto_events_bus: MotoEventsBus
) -> None:
    local_runner_fx.emit("batch-completed", _Probe(x=2))

    envelopes = moto_events_bus.read_events()
    assert len(envelopes) == 1
    assert envelopes[0]["detail-type"] == "batch-completed"
    assert envelopes[0]["detail"] == {"x": 2}


def test_local_runner_fx_record_run_is_real_and_working(
    local_runner_fx: RunnerFx, ledger_catalog: LedgerCatalogFixture
) -> None:
    local_runner_fx.record_run(_run_fact(stage="pull", facts_appended=None))

    rows = ledger_catalog.rows()
    assert len(rows) == 1
    assert rows[0]["stage"] == "pull"


def test_local_runner_fx_now_is_the_controllable_clock(
    local_runner_fx: RunnerFx, clock: TickableClock
) -> None:
    assert local_runner_fx.now() == clock.now()
    clock.tick()
    assert local_runner_fx.now() == clock.now()


def test_local_runner_fx_spark_side_fields_are_live(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    """nvh.18: the Spark-side `RunnerFx` fields are the real
    `effects/spark.py` closures over this fixture's own session/config, not
    the old `NotImplementedError("nvh.18")` stubs -- a zero-snapshot
    `read_table` call is a real, cheap, fully-local proof of liveness (I-6
    [T-19]); `tests/integration/test_spark_fx.py` covers the full behavior
    contract (reads, guards, append, merge, resolve_batch_snapshot)."""
    qualified_table = unique_table("substrate_liveness")
    spark.sql(f"CREATE TABLE {qualified_table} (id INT) USING iceberg")

    df, sid = local_runner_fx.read_table(qualified_table.removeprefix("spine_cat."))

    assert sid == -1
    assert df.count() == 0
