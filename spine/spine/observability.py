"""Observability helpers — LLD §11.1 (EMF metrics) / §11.2 (structured JSON logs).

No file is named for this in the LLD's repo layout (§4); this is the
simplest consistent home for the two small, effect-side (stdout I/O)
mechanisms `effects/ledger.py` (and, later, `stages/*.py`/`run.py`) use —
mirrors `ingestion/ingestion/observability.py`'s placement and shape exactly
(one 20-line stdlib JSON formatter, hand-rolled EMF, no metrics library).

Kept out of `core/`/`frames/` on purpose — both functions here perform I/O
(`print`/`logging` to stdout), which neither pure zone may ever do (§7.0
rule 4, I-9).

[S-18]: **no log record at any level may contain DataFrame contents or row
values** — counts, ids, and snapshot ids only. Callers of `emit_metric`/
`install_json_handler` are responsible for only ever passing scalar
identifiers and counts; this module enforces nothing beyond the fixed
`_RECORD_ATTRS` allowlist (§11.2: "every record carries batch_id, pipeline,
feed_id, attempt_id, stage when known").

No `class` statement for the JSON formatter (idiom rule, engine-wide, per
`tools/linter_configs/spine.py`) — `logging.Formatter`'s `.format` method is
replaced with a plain function assigned directly on an instance (verified
live in ingestion's own module; the same technique is reused here
unchanged).
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import IO

# §11.2: every log record carries these identifiers "when known".
_RECORD_ATTRS: tuple[str, ...] = ("batch_id", "pipeline", "feed_id", "attempt_id", "stage")

_METRIC_NAMESPACE = "Conveyer/Spine"

# Marks the handler this module installs so a warm process (or a test that
# calls `install_json_handler` more than once) doesn't accumulate duplicate
# handlers -- same idiom as ingestion's own marker (that module's own
# docstring explains why "any handler at all" is the wrong check: some AWS
# runtimes pre-install a root handler before user code ever runs).
_JSON_HANDLER_NAME = "conveyer-spine-json-handler"


def _format_json(record: logging.LogRecord) -> str:
    """The §11.2 "20-line JSON formatter" — assigned as a formatter
    instance's `.format` attribute (see module docstring), never a
    `logging.Formatter` subclass.
    """
    payload: dict[str, object] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "level": record.levelname,
        "message": record.getMessage(),
    }
    for attr in _RECORD_ATTRS:
        value = getattr(record, attr, None)
        if value is not None:
            payload[attr] = value
    return json.dumps(payload)


def make_json_handler(stream: IO[str] = sys.stdout) -> logging.Handler:
    """A `logging.Handler` that writes one JSON object per line to `stream`
    (stdout by default — Glue's continuous logging ships stdout to
    CloudWatch Logs). Callers attach this to a `logging.Logger` once per
    process; every `logger.info(..., extra={"batch_id": ..., ...})` call
    then carries the §11.2 identifier fields automatically when supplied.
    """
    handler = logging.StreamHandler(stream)
    formatter = logging.Formatter()
    formatter.format = _format_json  # type: ignore[method-assign]
    handler.setFormatter(formatter)
    return handler


def install_json_handler(stream: IO[str] = sys.stdout) -> None:
    """Idempotently install `make_json_handler` on the root logger — safe to
    call repeatedly (the `_JSON_HANDLER_NAME` marker makes a second call a
    no-op). Also raises the root logger's effective level to INFO — it
    defaults to WARNING, which would otherwise silently swallow every
    `logger.info(...)` state-transition line §11.2 calls for even with the
    handler attached.
    """
    root = logging.getLogger()
    if any(h.name == _JSON_HANDLER_NAME for h in root.handlers):
        return
    handler = make_json_handler(stream)
    handler.name = _JSON_HANDLER_NAME
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def emit_metric(
    name: str,
    value: float,
    pipeline: str,
    feed_id: str,
    stage: str | None = None,
    unit: str = "Count",
    extra_dims: Mapping[str, str] | None = None,
) -> None:
    """CloudWatch EMF, hand-rolled dict, printed to stdout (§11.1) —
    namespace `Conveyer/Spine`, dimensions `pipeline`, `feed_id` (+ `stage`
    on stage metrics, when the caller supplies one). No metrics client or
    library is used anywhere in this codebase (§11.1 idiom); CloudWatch
    parses EMF straight out of Glue's continuous-logging stdout capture.

    `extra_dims` (007.1 §12, B9b: `DeltaProbeRefusals`'s `reason` dimension,
    `DivergentDuplicates`'s `table` dimension) adds further NAME-DIMENSIONED
    values, insertion order, after `stage` — every key becomes both a
    `Dimensions` entry and a top-level payload key, the same shape `stage`
    already has. `None`/omitted (every EXISTING caller) leaves the payload
    byte-identical to before this parameter existed — a strictly additive
    extension, not a reshape. **[S-7]/[S-18] is the caller's obligation, not
    this function's**: every value threaded through `extra_dims` must
    already be an enum code or a lineage identifier (table name, reason
    code) — never `delivery_key`/hash/row-derived payload material; this
    function enforces nothing beyond the fixed `emit_metric` signature
    itself, the same posture the module docstring already states.
    """
    dims = ["pipeline", "feed_id"]
    if stage is not None:
        dims.append("stage")
    if extra_dims:
        dims.extend(extra_dims.keys())
    payload: dict[str, object] = {
        "_aws": {
            "Timestamp": int(datetime.now(UTC).timestamp() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": _METRIC_NAMESPACE,
                    "Dimensions": [dims],
                    "Metrics": [{"Name": name, "Unit": unit}],
                }
            ],
        },
        "pipeline": pipeline,
        "feed_id": feed_id,
        name: value,
    }
    if stage is not None:
        payload["stage"] = stage
    if extra_dims:
        payload.update(extra_dims)
    print(json.dumps(payload))  # noqa: T201 -- the sanctioned §11.1 mechanism
