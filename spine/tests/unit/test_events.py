"""Unit tests for `spine.effects.events.build_emit` — LLD §7.6, I-7, [T-17].

Covers: success against moto EventBridge (source/`DetailType`/JSON `Detail`
land verbatim); a real `ClientError` (nonexistent bus) -> `TransientError`;
and the `FailedEntryCount > 0` path, which moto cannot fabricate (a 200
response with a per-entry failure is not a shape moto's `put_events`
produces) -- covered instead by a tiny recorded-calls fake client (no
mocking framework, ever, per the engine-wide idiom rule): a plain class
whose `put_events` records its call and returns a hand-built response with
`FailedEntryCount: 1`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from moto import mock_aws
from pydantic import BaseModel
from spine.effects import events
from spine.effects.records import TransientError

_BUS = "conveyer-spine-test-bus"
_QUEUE = "conveyer-spine-test-capture-queue"


class _Dummy(BaseModel):
    x: int


@pytest.fixture
def bus_and_queue() -> Iterator[tuple[Any, str]]:
    with mock_aws():
        client = boto3.client("events", region_name="us-east-1")
        sqs = boto3.client("sqs", region_name="us-east-1")

        client.create_event_bus(Name=_BUS)
        queue_url = sqs.create_queue(QueueName=_QUEUE)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        client.put_rule(
            Name="capture-all",
            EventBusName=_BUS,
            EventPattern=json.dumps({"source": ["conveyer.spine"]}),
        )
        client.put_targets(
            Rule="capture-all", EventBusName=_BUS, Targets=[{"Id": "1", "Arn": queue_arn}]
        )
        yield client, queue_url


def _receive_one(queue_url: str) -> dict[str, Any]:
    sqs = boto3.client("sqs", region_name="us-east-1")
    messages = sqs.receive_message(
        QueueUrl=queue_url, WaitTimeSeconds=1, MaxNumberOfMessages=5
    ).get("Messages", [])
    assert len(messages) == 1
    return json.loads(messages[0]["Body"])  # type: ignore[no-any-return]


def test_emit_puts_event_with_source_detail_type_and_json_detail(
    bus_and_queue: tuple[Any, str],
) -> None:
    client, queue_url = bus_and_queue
    emit = events.build_emit(client, _BUS)

    emit("batch-started", _Dummy(x=1))

    envelope = _receive_one(queue_url)
    assert envelope["source"] == "conveyer.spine"
    assert envelope["detail-type"] == "batch-started"
    assert envelope["detail"] == {"x": 1}


def test_emit_carries_the_callers_detail_type(bus_and_queue: tuple[Any, str]) -> None:
    client, queue_url = bus_and_queue
    emit = events.build_emit(client, _BUS)

    emit("batch-completed", _Dummy(x=2))

    envelope = _receive_one(queue_url)
    assert envelope["detail-type"] == "batch-completed"


def test_emit_to_nonexistent_bus_raises_transient_error(bus_and_queue: tuple[Any, str]) -> None:
    client, _queue_url = bus_and_queue
    emit = events.build_emit(client, "no-such-bus")

    with pytest.raises(TransientError):
        emit("batch-started", _Dummy(x=1))


# --- FailedEntryCount > 0: moto cannot fabricate this, so a tiny recorded-
# calls fake client stands in (no mocking framework, ever). ------------------


class _FailingEntryClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def put_events(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {
            "FailedEntryCount": 1,
            "Entries": [{"ErrorCode": "InternalFailure", "ErrorMessage": "simulated failure"}],
        }


def test_emit_raises_transient_error_on_failed_entry_count() -> None:
    fake_client = _FailingEntryClient()
    emit = events.build_emit(fake_client, _BUS)

    with pytest.raises(TransientError, match="simulated failure"):
        emit("batch-completed", _Dummy(x=3))

    assert len(fake_client.calls) == 1
    entry = fake_client.calls[0]["Entries"][0]
    assert entry["Source"] == "conveyer.spine"
    assert entry["DetailType"] == "batch-completed"
    assert json.loads(entry["Detail"]) == {"x": 3}


class _AllFailedNoEntriesClient:
    """`FailedEntryCount > 0` but an empty `Entries` list -- the "unknown
    error" fallback branch."""

    def put_events(self, **_kwargs: Any) -> dict[str, Any]:
        return {"FailedEntryCount": 1, "Entries": []}


def test_emit_raises_transient_error_with_unknown_reason_when_entries_empty() -> None:
    emit = events.build_emit(_AllFailedNoEntriesClient(), _BUS)

    with pytest.raises(TransientError, match="unknown error"):
        emit("batch-started", _Dummy(x=4))
