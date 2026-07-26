"""Integration tests (factory level, LLD §12.1) for
`effects.events.make_event_fx` against moto EventBridge.

Captures the published event via an EventBridge rule routed to an SQS
queue (moto has no direct "read back the bus" API) and asserts `Source`,
`DetailType`, and the model's JSON `Detail` land verbatim (§6.4). Also
covers the failure -> `TransientError` path (S7.7).
"""

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import boto3
import pytest
from ingestion.core.model import DeliveryOverdueV1, DeliveryRegisteredV1
from ingestion.effects.events import make_event_fx
from ingestion.effects.records import TransientError
from moto import mock_aws

_BUS = "conveyer-test-bus"
_QUEUE = "conveyer-test-capture-queue"

_REGISTERED_EVENT = DeliveryRegisteredV1(
    feed_id="carrier-x/commission-statements",
    delivery_id="11111111-1111-4111-8111-111111111111",
    batch_id="22222222-2222-4222-8222-222222222222",
    delivery_key="manifest-1",
    content_hash="sha256:" + "a" * 64,
    size_bytes=1024,
    object_uris=["s3://lake/carrier-x/commission-statements/2026-07-25/a.csv"],
    received_at=datetime(2026, 7, 25, 9, 0, tzinfo=UTC),
    pipeline="pipelines/commissions",
)


@pytest.fixture
def bus_and_queue() -> Iterator[tuple[Any, str, str]]:
    with mock_aws():
        events = boto3.client("events", region_name="us-east-1")
        sqs = boto3.client("sqs", region_name="us-east-1")

        events.create_event_bus(Name=_BUS)
        queue_url = sqs.create_queue(QueueName=_QUEUE)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        events.put_rule(
            Name="capture-all",
            EventBusName=_BUS,
            EventPattern=json.dumps({"source": ["conveyer.ingestion"]}),
        )
        events.put_targets(
            Rule="capture-all", EventBusName=_BUS, Targets=[{"Id": "1", "Arn": queue_arn}]
        )
        yield events, _BUS, queue_url


def _receive_one(sqs_queue_url: str) -> dict[str, Any]:
    sqs = boto3.client("sqs", region_name="us-east-1")
    messages = sqs.receive_message(
        QueueUrl=sqs_queue_url, WaitTimeSeconds=1, MaxNumberOfMessages=5
    ).get("Messages", [])
    assert len(messages) == 1
    return json.loads(messages[0]["Body"])  # type: ignore[no-any-return]


def test_emit_puts_event_with_source_detail_type_and_json_detail(bus_and_queue) -> None:
    events_client, bus, queue_url = bus_and_queue
    emit = make_event_fx(events_client, bus)

    emit("delivery-registered", _REGISTERED_EVENT)

    envelope = _receive_one(queue_url)
    assert envelope["source"] == "conveyer.ingestion"
    assert envelope["detail-type"] == "delivery-registered"
    assert envelope["detail"] == json.loads(_REGISTERED_EVENT.model_dump_json())


def test_emit_overdue_event_carries_correct_detail_type(bus_and_queue) -> None:
    events_client, bus, queue_url = bus_and_queue
    emit = make_event_fx(events_client, bus)
    overdue = DeliveryOverdueV1(
        feed_id="carrier-x/commission-statements",
        expectation_date=datetime(2026, 7, 24, tzinfo=UTC).date(),
        expected_by=datetime(2026, 7, 24, 13, 0, tzinfo=UTC),
        checked_at=datetime(2026, 7, 25, 9, 0, tzinfo=UTC),
    )

    emit("delivery-overdue", overdue)

    envelope = _receive_one(queue_url)
    assert envelope["detail-type"] == "delivery-overdue"
    assert envelope["detail"]["feed_id"] == "carrier-x/commission-statements"


def test_emit_to_nonexistent_bus_raises_transient_error(bus_and_queue) -> None:
    events_client, _bus, _queue_url = bus_and_queue
    emit = make_event_fx(events_client, "no-such-bus")

    with pytest.raises(TransientError):
        emit("delivery-registered", _REGISTERED_EVENT)
