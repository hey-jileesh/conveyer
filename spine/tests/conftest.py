"""Local-stack fixtures — LLD §12.1.

`build_test_session` is the ONE session-scoped plain SparkSession builder for
the whole spine suite (architect decision D-3): `local[2]`, driver 2 g,
`spark.sql.shuffle.partitions = 2`, AQE on — extended (this bead,
`conveyer-nvh.17`, M2) with the Iceberg Hadoop catalog (`spine_cat`, session
tmpdir warehouse), via `extra_conf` — never a second `SparkSession.builder`
chain. `spark` is the pytest fixture every suite under `tests/` shares
(session-scoped: one JVM for the whole run, §12.1's wall-time mitigation);
it is lazily built — `pytest tests/unit` never touches Spark since no test
there requests the fixture.

This module additionally builds (§12.1, §12.4):

* `unique_table` — per-test unique `spine_cat.<db>.<name>` identifiers so
  tests stay independent without a session restart.
* `ledger_catalog` — a pyiceberg `SqlCatalog` (SQLite + its own per-test
  tmpdir warehouse) run-ledger fixture, bootstrapped via
  `effects.ledger.build_catalog` + `bootstrap.create_run_ledger` (the SAME
  functions the production path uses — no schema/creation logic duplicated
  here). Deliberately a DIFFERENT catalog client from Spark's `spine_cat`
  (I-2) — its own SQLite file, its own warehouse directory, per test.
* `moto_events_bus` — a moto-backed EventBridge bus with an SQS capture
  queue subscribed to every `conveyer.spine`-sourced event, so a test can
  read back what an `emit` call actually published.
* `clock` — a tick-able clock double (plain class, no mocking framework)
  standing in for `RunnerFx.now`.
* `make_wrapped_fx` — the KillFx/FlakyMerge hook-point mechanism (architect
  G-3): wraps NAMED `RunnerFx` fields with caller-supplied wrapper
  functions, returning a new `RunnerFx`. M4 (`conveyer-nvh.2x`+) supplies
  the actual `KillFx`/`FlakyMerge` wrapper functions and the R-03/R-08
  scenario tests that use them; this bead ships only the mechanism plus a
  trivial pass-through self-test (`tests/integration/test_substrate.py`).
* `local_runner_fx` — assembles a `RunnerFx` via the REAL
  `effects.build.make_runner_fx(spark, config)` factory (the identical
  production assembly path, per that module's own docstring: "tests/
  conftest.py assembles the identical shape ... a parallel, test-only
  builder, not a caller of this one" — this fixture is precisely that
  caller relationship made concrete: `make_runner_fx`'s own boto3 `events`
  client is constructed while `moto_events_bus`'s `mock_aws()` context is
  already active, so it is transparently moto-backed with no override;
  its deferred pyiceberg catalog factory resolves `config.ledger_catalog_
  kind == "sql"` against `ledger_catalog`'s own already-bootstrapped
  SQLite table, so `record_run` is real and working with zero overrides.
  The one field this fixture DOES override is `now` (wall clock ->
  `clock`, so scenario tests can control attempt-truth timestamps).
  Spark-side fields (`read_objects`, `read_table`, `read_batch`,
  `table_has_batch`, `append`, `merge`, `resolve_batch_snapshot`) are the
  REAL production closures (`effects/spark.py::build_spark_fx`, bead
  conveyer-nvh.18, M2 — landed since this bead (`nvh.17`) shipped the
  skeleton; this fixture's own shape has not needed to change since).

Recorded assumption on `warehouse_uri` reuse: `RunnerConfig.warehouse_uri`
is ONE field serving two test-only purposes by the existing shape of
`effects/ledger.py::build_catalog` (its `sql` branch reads
`config.warehouse_uri` for the pyiceberg `SqlCatalog`'s own storage) and of
the Spark entrypoint's `hadoop` catalog wiring (§7.6). `ledger_catalog`
deliberately gives its `RunnerConfig` a PER-TEST, Spark-independent
`warehouse_uri` (test isolation for ledger row-count assertions takes
priority over documentation-only consistency with the Spark session's own,
session-scoped warehouse conf) — `local_runner_fx.config.warehouse_uri`
therefore describes the LEDGER's storage, not the live Spark session's --
settled, not open: `effects/spark.py` (bead conveyer-nvh.18) does not read
`RunnerConfig.warehouse_uri` at all; the Spark session's own Iceberg-Hadoop
catalog wiring is this module's own `build_test_session`, entirely
independent of it.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Generator, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3  # type: ignore[import-untyped]
import pytest
from moto import mock_aws
from pyspark.sql import SparkSession
from spine.bootstrap.create_run_ledger import create_run_ledger
from spine.config import RunnerConfig
from spine.effects import build, ledger
from spine.effects.records import RunnerFx

_BASE_CONF: Mapping[str, str] = {
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "2",
    "spark.sql.adaptive.enabled": "true",
    "spark.ui.enabled": "false",
}

# LLD §7.1/§12.1: the exact Iceberg runtime jar coordinate. Resolved via
# `spark.jars.packages` against the DEFAULT ivy cache location (no
# `spark.jars.ivy` override) -- verified empirically that a plain
# `spark.jars.packages` resolution, with no ivy conf set at all, writes to
# `~/.ivy2/{cache,jars}`. CI cache key: keep it pinned to this coordinate
# string so a bump of the jar version naturally invalidates the cache, e.g.
# (GitHub Actions):
#   - uses: actions/cache@v4
#     with:
#       path: ~/.ivy2
#       key: ivy2-${{ runner.os }}-iceberg-spark-runtime-3.5_2.12-1.6.1
_ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

_UNIQUE_TABLE_DB = "spine_test_tables"
_SUBSTRATE_CHECK_DB = "__substrate_check"


def _iceberg_conf(warehouse_dir: str) -> dict[str, str]:
    """§7.6 catalog wiring note, local analogue: `spine_cat` ->
    `SparkCatalog` `type=hadoop` on a session tmpdir (prod uses `type=glue`,
    entrypoint-selected -- this conf is test-only)."""
    return {
        "spark.jars.packages": _ICEBERG_PACKAGE,
        "spark.sql.extensions": _ICEBERG_EXTENSIONS,
        "spark.sql.catalog.spine_cat": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.spine_cat.type": "hadoop",
        "spark.sql.catalog.spine_cat.warehouse": warehouse_dir,
    }


def build_test_session(extra_conf: Mapping[str, str] | None = None) -> SparkSession:
    """`local[2]` + `_BASE_CONF`, with `extra_conf` layered on top (last-wins
    on key collision) — the extension seam M2 builds the Iceberg catalog
    conf through.
    """
    builder = SparkSession.builder.master("local[2]").appName("conveyer-spine-tests")
    conf: dict[str, str] = {**_BASE_CONF, **(extra_conf or {})}
    for key, value in conf.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    return session


def _assert_iceberg_extensions_live(session: SparkSession) -> None:
    """Local analogue of [T-16]: a MERGE INTO must actually succeed against
    the wired `spine_cat` catalog BEFORE any fixture consumer relies on it —
    a missing/misconfigured extension must fail HERE, at session build, not
    seven stages into some unrelated test. Throwaway table under a private
    namespace, dropped immediately after (never reused, so no collision
    with `unique_table` or `tests/exemplar` fixtures)."""
    table = f"spine_cat.{_SUBSTRATE_CHECK_DB}.probe"
    try:
        session.sql(f"CREATE TABLE {table} (id INT, v STRING) USING iceberg")
        session.sql(f"INSERT INTO {table} VALUES (1, 'a'), (2, 'b')")
        session.createDataFrame([(1, "a-updated"), (3, "c")], ["id", "v"]).createOrReplaceTempView(
            "__substrate_check_source"
        )
        session.sql(
            f"""
            MERGE INTO {table} t
            USING __substrate_check_source s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.v = s.v
            WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)
            """
        )
        rows = sorted((row["id"], row["v"]) for row in session.table(table).collect())
        expected = [(1, "a-updated"), (2, "b"), (3, "c")]
        if rows != expected:
            raise AssertionError(
                f"Iceberg substrate check: MERGE INTO produced {rows!r}, expected {expected!r} "
                "-- spine_cat catalog/extensions are not live [T-16]"
            )
    except Exception as exc:  # noqa: BLE001 -- re-raise with substrate-check context, then propagate
        if isinstance(exc, AssertionError):
            raise
        raise AssertionError(
            "Iceberg substrate check failed before any test ran -- spine_cat catalog wiring or "
            f"spark.sql.extensions is broken [T-16]: {exc}"
        ) from exc
    finally:
        session.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.fixture(scope="session")
def _iceberg_warehouse(tmp_path_factory: pytest.TempPathFactory) -> str:
    return str(tmp_path_factory.mktemp("iceberg-warehouse"))


@pytest.fixture(scope="session")
def spark(_iceberg_warehouse: str) -> Generator[SparkSession, None, None]:
    session = build_test_session(extra_conf=_iceberg_conf(_iceberg_warehouse))
    _assert_iceberg_extensions_live(session)
    yield session
    session.stop()


@pytest.fixture
def unique_table(spark: SparkSession) -> Callable[[str], str]:
    """`unique_table("probe") -> "spine_cat.spine_test_tables.probe_<hex>"` —
    a fresh, collision-free identifier per call, in the session's Iceberg
    Hadoop catalog (§12.1). Does not create the table; callers issue their
    own DDL/DML against the returned identifier. `prefix` must itself be a
    valid unqualified SQL identifier segment (tests control it)."""

    def _make(prefix: str) -> str:
        return f"spine_cat.{_UNIQUE_TABLE_DB}.{prefix}_{uuid.uuid4().hex}"

    return _make


# --- Run ledger: pyiceberg SqlCatalog, deliberately a DIFFERENT catalog from
# Spark's (I-2) --------------------------------------------------------------


def _test_runner_config(*, warehouse_uri: str, ledger_sql_uri: str, event_bus: str) -> RunnerConfig:
    """A fully-populated `RunnerConfig` for local-stack tests. Only the
    fields that vary per fixture use are parameters; the rest are stable
    test defaults so `local_runner_fx`'s ledger-identifier fields
    (`spine_db`/`run_ledger_table`) always agree with whatever catalog
    `ledger_catalog` already bootstrapped."""
    return RunnerConfig(
        env="test",
        aws_region="us-east-1",
        catalog_kind="hadoop",
        warehouse_uri=warehouse_uri,
        ledger_catalog_kind="sql",
        ledger_sql_uri=ledger_sql_uri,
        spine_db="spine_test_db",
        run_ledger_table="run_ledger",
        event_bus=event_bus,
        landing_bucket="conveyer-test-landing",
        pipeline_spec_uri="s3://conveyer-test-specs/identity/pipeline.yaml",
        delivery_json="{}",
        attempt_id="attempt-1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        run_config_json="{}",
        sla_minutes=30,
    )


@dataclass(frozen=True)
class LedgerCatalogFixture:
    catalog: Any  # pyiceberg Catalog
    config: RunnerConfig
    identifier: str

    def rows(self) -> list[dict[str, Any]]:
        """Read back every run-ledger row via pyiceberg (not Spark) —
        proves the ledger fixture round-trips on its own catalog client."""
        return self.catalog.load_table(self.identifier).scan().to_arrow().to_pylist()


@pytest.fixture
def ledger_catalog(tmp_path: Path) -> LedgerCatalogFixture:
    """Per-test `SqlCatalog` (SQLite + its own tmpdir warehouse) with the
    run_ledger table already bootstrapped via `bootstrap.create_run_ledger`
    (reusing the production creation path, not a re-derived schema)."""
    config = _test_runner_config(
        warehouse_uri=f"file://{tmp_path / 'ledger-warehouse'}",
        ledger_sql_uri=f"sqlite:///{tmp_path / 'ledger.db'}",
        event_bus="unused-by-ledger-catalog",
    )
    catalog = ledger.build_catalog(config)
    create_run_ledger(catalog, config.spine_db, config.run_ledger_table)
    return LedgerCatalogFixture(
        catalog=catalog,
        config=config,
        identifier=f"{config.spine_db}.{config.run_ledger_table}",
    )


# --- moto EventBridge ---------------------------------------------------------


@dataclass(frozen=True)
class MotoEventsBus:
    client: Any  # boto3 "events" client, moto-backed while in scope
    bus_name: str
    queue_url: str

    def read_events(self, *, max_messages: int = 10) -> list[dict[str, Any]]:
        """Drains up to `max_messages` from the capture queue, returning
        parsed EventBridge envelopes (`source`/`detail-type`/`detail`) for
        assertions."""
        sqs = boto3.client("sqs", region_name="us-east-1")
        messages = sqs.receive_message(
            QueueUrl=self.queue_url, WaitTimeSeconds=1, MaxNumberOfMessages=max_messages
        ).get("Messages", [])
        return [json.loads(message["Body"]) for message in messages]


@pytest.fixture
def moto_events_bus() -> Generator[MotoEventsBus, None, None]:
    """A moto-backed EventBridge bus with an SQS queue subscribed to every
    `conveyer.spine`-sourced event — `effects.events.build_emit`/
    `effects.build.make_runner_fx`'s own boto3 client, constructed while
    this fixture's `mock_aws()` context is active, is transparently
    intercepted (moto patches the request layer, not per-client-instance),
    so callers needn't share the exact client object this fixture built."""
    with mock_aws():
        client = boto3.client("events", region_name="us-east-1")
        sqs = boto3.client("sqs", region_name="us-east-1")
        bus_name = "conveyer-spine-test-bus"
        client.create_event_bus(Name=bus_name)
        queue_url = sqs.create_queue(QueueName="conveyer-spine-test-capture-queue")["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        client.put_rule(
            Name="capture-all",
            EventBusName=bus_name,
            EventPattern=json.dumps({"source": ["conveyer.spine"]}),
        )
        client.put_targets(
            Rule="capture-all", EventBusName=bus_name, Targets=[{"Id": "1", "Arn": queue_arn}]
        )
        yield MotoEventsBus(client=client, bus_name=bus_name, queue_url=queue_url)


# --- Controllable clock (plain class, no mocking framework) ------------------


class TickableClock:
    """Stands in for `RunnerFx.now` (§6.6 [H-1]): tests control attempt-
    truth timestamps explicitly instead of racing the wall clock."""

    def __init__(self, start: datetime) -> None:
        self._current = start

    def now(self) -> datetime:
        return self._current

    def tick(self, delta: timedelta = timedelta(seconds=1)) -> None:
        self._current = self._current + delta

    def set(self, ts: datetime) -> None:
        self._current = ts


@pytest.fixture
def clock() -> TickableClock:
    return TickableClock(datetime(2026, 1, 1, tzinfo=UTC))


# --- Failure-wrapper hook points (architect G-3) ------------------------------


def _make_wrapped_fx(
    fx: RunnerFx, wrappers: Mapping[str, Callable[[Callable[..., Any]], Callable[..., Any]]]
) -> RunnerFx:
    """Returns a new `RunnerFx` with each NAMED field in `wrappers` replaced
    by `wrapper(original_callable)`. Every field not named in `wrappers`
    passes through unchanged (identity — no double-wrapping, no incidental
    behavior change). This is only the mechanism: M4 supplies the actual
    `KillFx` (raise after the Nth effect commit, R-03) and `FlakyMerge`
    (raise `TransientError` once then succeed, R-08) wrapper functions,
    each of shape `Callable[[Callable], Callable]`."""
    updates = {name: wrapper(getattr(fx, name)) for name, wrapper in wrappers.items()}
    return replace(fx, **updates)


@pytest.fixture
def make_wrapped_fx() -> Callable[
    [RunnerFx, Mapping[str, Callable[[Callable[..., Any]], Callable[..., Any]]]], RunnerFx
]:
    return _make_wrapped_fx


# --- local_runner_fx skeleton (nvh.18 completes it) --------------------------


@pytest.fixture
def local_runner_fx(
    spark: SparkSession,
    ledger_catalog: LedgerCatalogFixture,
    moto_events_bus: MotoEventsBus,
    clock: TickableClock,
) -> RunnerFx:
    """Assembles a `RunnerFx` via the REAL `effects.build.make_runner_fx`
    factory — the same call production wiring makes — over `ledger_
    catalog`'s already-bootstrapped SQLite ledger and `moto_events_bus`'s
    moto-backed bus (`config.event_bus` set to that bus's name). `now` is
    the only override (wall clock -> `clock`). Spark-side fields
    (`read_objects`, `read_table`, `read_batch`, `table_has_batch`,
    `append`, `merge`, `resolve_batch_snapshot`) are today's `nvh.18` stubs;
    `spark` is threaded through now so nvh.18 needs no fixture-shape change
    when it replaces them."""
    config = replace(ledger_catalog.config, event_bus=moto_events_bus.bus_name)
    fx = build.make_runner_fx(spark, config)
    return replace(fx, now=clock.now)
