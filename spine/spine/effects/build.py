"""`make_runner_fx(spark, config)` — prod closure assembly. LLD §7.6.

Assembles a real `RunnerFx` (`spine/effects/records.py`) for production: the
**driver-side** closures `conveyer-nvh.16` (M2) built — `emit`
(`effects/events.py`, a real boto3 `events` client) and `record_run`
(`effects/ledger.py`, a deferred pyiceberg catalog factory) — plus `now` and
`config`; and the **Spark-side** closures (`read_objects`, `read_table`,
`read_batch`, `table_has_batch`, `append`, `merge`,
`resolve_batch_snapshot`) `conveyer-nvh.18` (M2) builds, via
`effects/spark.py::build_spark_fx` (catalog wiring/append/merge/guards,
§7.6's implementation notes).

`tests/conftest.py` assembles the identical `RunnerFx` shape over local Spark
+ moto (no mocks, ever) — `local_runner_fx` calls this SAME function, not a
parallel test-only builder, so the Spark-side fields it gets are the real
`effects/spark.py` closures over the test session (only `now` is overridden,
by that fixture, wall clock -> a controllable clock double).
"""

from __future__ import annotations

import functools
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import boto3  # type: ignore[import-untyped]

from spine.config import RunnerConfig
from spine.effects import events, ledger
from spine.effects import spark as spark_fx
from spine.effects.records import RunnerFx

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def _now() -> datetime:
    return datetime.now(UTC)


def make_runner_fx(spark: SparkSession, config: RunnerConfig) -> RunnerFx:
    events_client = boto3.client("events", region_name=config.aws_region)
    # Deferred, not built-eagerly: `build_record_run` calls this factory on
    # every `record_run` invocation (effects/ledger.py's own `get_catalog`),
    # so a catalog construction hiccup at job start never prevents the
    # Spark-side stages from running — only the ledger channel degrades,
    # per §11.3's non-gating rule.
    catalog_factory = functools.partial(ledger.build_catalog, config)
    fx = spark_fx.build_spark_fx(spark, config)

    return RunnerFx(
        read_objects=fx.read_objects,
        read_table=fx.read_table,
        read_batch=fx.read_batch,
        table_has_batch=fx.table_has_batch,
        append=fx.append,
        merge=fx.merge,
        resolve_batch_snapshot=fx.resolve_batch_snapshot,
        record_run=ledger.build_record_run(catalog_factory, config),
        emit=events.build_emit(events_client, config.event_bus),
        now=_now,
        config=config,
    )
