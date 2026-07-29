"""`emit` — `PutEvents`; raises `TransientError` on any `FailedEntryCount`. LLD §7.6, I-7.

`build_emit(events_client, bus_name) -> emit` closes over the boto3 `events`
client and the target bus name; `emit(detail_type, model)` publishes one
entry: `Source="conveyer.spine"`, the caller's `DetailType` (`"batch-started"`
/ `"batch-completed"`), `Detail` = the model's JSON (`model_dump_json()`).

I-7 / [T-17]: boto3's `put_events` does **not** raise for a per-entry
publish failure — a 200 response can still carry `FailedEntryCount > 0` with
the failure detail living inside `Entries[i]`, not the HTTP status. Both
failure shapes — a raised `ClientError` from the call itself, and a
per-entry failure inside an otherwise-200 response — become `TransientError`
here, since I-18's whole "execution `SUCCEEDED` implies `batch-completed`
emitted" reduction rests on `emit` failing the job on ANY unpublished entry.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from pydantic import BaseModel

from spine.effects.records import TransientError

_SOURCE = "conveyer.spine"


def _emit(client: Any, bus_name: str, detail_type: str, model: BaseModel) -> None:
    try:
        response = client.put_events(
            Entries=[
                {
                    "Source": _SOURCE,
                    "DetailType": detail_type,
                    "EventBusName": bus_name,
                    "Detail": model.model_dump_json(),
                }
            ]
        )
    except ClientError as exc:
        raise TransientError(
            f"put_events failed for DetailType={detail_type!r} on bus {bus_name!r}: {exc}"
        ) from exc
    if response.get("FailedEntryCount", 0) > 0:
        entries = response.get("Entries", [])
        reason = entries[0].get("ErrorMessage", "unknown error") if entries else "unknown error"
        raise TransientError(
            f"put_events failed for DetailType={detail_type!r} on bus {bus_name!r}: {reason}"
        )


def build_emit(events_client: Any, bus_name: str) -> Callable[[str, BaseModel], None]:
    return functools.partial(_emit, events_client, bus_name)
