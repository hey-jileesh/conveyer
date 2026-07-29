"""Unit tests for `spine.effects.build.make_runner_fx` — LLD §7.6.

Asserts the assembled `RunnerFx` carries the full §7.6 field shape; every
Spark-side callable is a real closure (`effects/spark.py::build_spark_fx`,
bead `conveyer-nvh.18`) over the given `spark` argument, not the earlier
`NotImplementedError("nvh.18")` stub -- these are unit tests (fast,
`spark=None` since the closures don't touch it until called), so behavior
coverage of the Spark-side fields themselves lives in
`tests/integration/test_spark_fx.py`, which supplies a real session; the
driver-side closures (`emit`, `record_run`) are real and working here (moto
EventBridge for `emit`; a real local `SqlCatalog` for `record_run`, which --
per its own never-raises contract -- is safe to call even before the ledger
table is bootstrapped).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import boto3
import pytest
from moto import mock_aws
from pydantic import BaseModel
from spine.effects import build
from spine.effects.records import RunnerFx

_BUS = "conveyer-spine-build-test-bus"


@dataclass(frozen=True)
class _FakeConfig:
    env: str = "test"
    aws_region: str = "us-east-1"
    catalog_kind: str = "hadoop"
    warehouse_uri: str | None = None
    ledger_catalog_kind: str = "sql"
    ledger_sql_uri: str | None = "sqlite:///:memory:"
    spine_db: str = "spine_db"
    run_ledger_table: str = "run_ledger"
    event_bus: str = _BUS
    landing_bucket: str = "bucket"
    pipeline_spec_uri: str = "s3://x"
    delivery_json: str = "{}"
    attempt_id: str = "a1"
    sfn_retry_count: int = 0
    sfn_redrive_count: int = 0
    run_config_json: str = "{}"
    sla_minutes: int = 60


class _Dummy(BaseModel):
    x: int = 1


@pytest.fixture
def bus_and_queue() -> Iterator[tuple[Any, str]]:
    with mock_aws():
        client = boto3.client("events", region_name="us-east-1")
        sqs = boto3.client("sqs", region_name="us-east-1")
        client.create_event_bus(Name=_BUS)
        queue_url = sqs.create_queue(QueueName="conveyer-spine-build-test-queue")["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        client.put_rule(
            Name="all", EventBusName=_BUS, EventPattern=json.dumps({"source": ["conveyer.spine"]})
        )
        client.put_targets(Rule="all", EventBusName=_BUS, Targets=[{"Id": "1", "Arn": queue_arn}])
        yield client, queue_url


def test_make_runner_fx_returns_a_full_runner_fx(bus_and_queue: tuple[Any, str]) -> None:
    fx = build.make_runner_fx(None, _FakeConfig())  # type: ignore[arg-type]
    assert isinstance(fx, RunnerFx)
    assert fx.config.env == "test"


def test_now_closure_returns_an_aware_utc_datetime(bus_and_queue: tuple[Any, str]) -> None:
    from datetime import UTC

    fx = build.make_runner_fx(None, _FakeConfig())  # type: ignore[arg-type]
    now = fx.now()
    assert now.tzinfo is UTC


@pytest.mark.parametrize(
    "field",
    [
        "read_objects",
        "read_table",
        "read_batch",
        "table_has_batch",
        "append",
        "merge",
        "resolve_batch_snapshot",
    ],
)
def test_spark_side_fields_are_real_callables_not_the_old_stub(
    field: str, bus_and_queue: tuple[Any, str]
) -> None:
    fx = build.make_runner_fx(None, _FakeConfig())  # type: ignore[arg-type]
    callable_field = getattr(fx, field)
    assert callable(callable_field)
    assert callable_field.__module__ == "spine.effects.spark"


def test_emit_closure_is_real_and_publishes_to_the_configured_bus(
    bus_and_queue: tuple[Any, str],
) -> None:
    _client, queue_url = bus_and_queue
    fx = build.make_runner_fx(None, _FakeConfig())  # type: ignore[arg-type]

    fx.emit("batch-started", _Dummy())

    sqs = boto3.client("sqs", region_name="us-east-1")
    messages = sqs.receive_message(
        QueueUrl=queue_url, WaitTimeSeconds=1, MaxNumberOfMessages=5
    ).get("Messages", [])
    assert len(messages) == 1
    envelope = json.loads(messages[0]["Body"])
    assert envelope["source"] == "conveyer.spine"
    assert envelope["detail-type"] == "batch-started"


def test_record_run_closure_is_real_and_never_raises(bus_and_queue: tuple[Any, str]) -> None:
    from datetime import UTC, datetime

    from spine.core.run_facts import RunFact

    fx = build.make_runner_fx(None, _FakeConfig())  # type: ignore[arg-type]
    now = datetime.now(UTC)
    run_fact = RunFact(
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
    fx.record_run(run_fact)  # table not bootstrapped -- must not raise
