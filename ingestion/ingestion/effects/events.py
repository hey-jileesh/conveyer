"""make_event_fx (EventBridge PutEvents) -- LLD S7.7 / S6.4.

`Effects.emit` is a bare `Callable[[str, BaseModel], None]` (not a
sub-record like `StoreFx`) -- `make_event_fx` returns that function
directly, with the boto3 events client and target bus closed over.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from pydantic import BaseModel

from ingestion.effects.records import TransientError

_SOURCE = "conveyer.ingestion"


def _emit(client: Any, event_bus: str, detail_type: str, model: BaseModel) -> None:
    """PutEvents of one entry: `Source="conveyer.ingestion"`, the caller's
    `DetailType` (S6.4: `"delivery-registered"` / `"delivery-overdue"`),
    `Detail` = the model's JSON. Any failure -- a `ClientError` from the call
    itself, or a per-entry failure reported in a 200 response
    (`FailedEntryCount > 0`) -- becomes `TransientError` (S7.3).
    """
    try:
        response = client.put_events(
            Entries=[
                {
                    "Source": _SOURCE,
                    "DetailType": detail_type,
                    "EventBusName": event_bus,
                    "Detail": model.model_dump_json(),
                }
            ]
        )
    except ClientError as exc:
        raise TransientError(
            f"put_events failed for DetailType={detail_type!r} on bus {event_bus!r}: {exc}"
        ) from exc
    if response.get("FailedEntryCount", 0) > 0:
        entries = response.get("Entries", [])
        reason = entries[0].get("ErrorMessage", "unknown error") if entries else "unknown error"
        raise TransientError(
            f"put_events failed for DetailType={detail_type!r} on bus {event_bus!r}: {reason}"
        )


def make_event_fx(client: Any, event_bus: str) -> Callable[[str, BaseModel], None]:
    return functools.partial(_emit, client, event_bus)
