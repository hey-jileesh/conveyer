"""Observability helpers — LLD §11.1 (structured JSON logs) / §11.2 (EMF
metrics). No file is named for this in the LLD's repo layout (§4); this is
the simplest consistent home for the two small, effect-side (stdout I/O)
mechanisms `registration/registrar.py` and the drivers use. Kept out of
`core/` on purpose — both functions here perform I/O (writing to stdout via
`print`/`logging`), which the pure core may never do (§7.0 rule 4).

`ledger.py::_emit_metric` (built in an earlier bead, `effects/ledger.py`)
is a private, minimal EMF-shaped `print` predating this module — it is a
candidate first consumer of `emit_metric` below, but refactoring it is out
of this bead's file ownership; noted for a follow-up, not changed here.

No `class` statement is used for the JSON log formatter (the IDIOM rule
bans plain classes everywhere in `ingestion/**`, §12.2) — `logging.Formatter`
is normally customized by subclassing, but a formatter's `.format` method
can equally be replaced with a plain function assigned directly on a
`logging.Formatter()` instance: attribute lookup finds the instance
attribute first, and since it was assigned (not defined on the class), it
is called as a plain function, not bound with an implicit `self`. Verified
live in the kernel.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import IO

# §11.1: every log record carries these identifiers "when known".
_RECORD_ATTRS: tuple[str, ...] = ("feed_id", "delivery_id", "batch_id", "driver_run_id")

_METRIC_NAMESPACE = "Conveyer/Ingestion"

# M-9 (security-gate): `logging.Handler.name` marks the handler this module
# installs, so `install_json_handler` can recognize (and skip re-adding) its
# own handler on a warm Lambda invocation -- checking `root.handlers` for
# "any handler at all" would be wrong: AWS's own Lambda Python runtime
# frequently pre-installs its own root handler before user code ever runs.
_JSON_HANDLER_NAME = "conveyer-json-handler"


def _format_json(record: logging.LogRecord) -> str:
    """The §11.1 "20-line JSON formatter" — assigned as a formatter
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
    (stdout by default — Lambda ships stdout to CloudWatch Logs). Callers
    attach this to a `logging.Logger` once per module/container; every
    `logger.info(..., extra={"feed_id": ..., "delivery_id": ...})` call then
    carries the LLD §11.1 identifier fields automatically when supplied.
    """
    handler = logging.StreamHandler(stream)
    formatter = logging.Formatter()
    formatter.format = _format_json  # type: ignore[method-assign]
    handler.setFormatter(formatter)
    return handler


def install_json_handler(stream: IO[str] = sys.stdout) -> None:
    """Idempotently install `make_json_handler` on the root logger -- M-9
    (security-gate): `make_json_handler` was previously called from
    NOWHERE, so §11.1's structured JSON logs (feed_id/delivery_id/batch_id/
    driver_run_id correlation) were never actually in effect in any
    deployed function. Every entrypoint `handler()` calls this once, at the
    top, on EVERY invocation -- safe to call repeatedly: a warm Lambda
    container reuses the same process (and root logger) across
    invocations, so a naive unconditional `addHandler` would accumulate a
    duplicate handler (and duplicate log lines) per warm invocation; the
    `_JSON_HANDLER_NAME` marker makes this a no-op after the first call.
    Also raises the root logger's effective level to INFO -- it defaults to
    WARNING, which would otherwise silently swallow every `logger.info(...)`
    state-transition line §11.1 calls for even with the handler attached.
    """
    root = logging.getLogger()
    if any(h.name == _JSON_HANDLER_NAME for h in root.handlers):
        return
    handler = make_json_handler(stream)
    handler.name = _JSON_HANDLER_NAME
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def emit_metric(name: str, value: float, feed_id: str, unit: str = "Count") -> None:
    """CloudWatch EMF, hand-rolled dict, printed to stdout (§11.2) —
    namespace `Conveyer/Ingestion`, dimension `feed_id`. No metrics client
    or library is used anywhere in this codebase (§11.2: "no library");
    CloudWatch parses EMF straight out of Lambda's stdout-captured logs.
    """
    payload = {
        "_aws": {
            "Timestamp": int(datetime.now(UTC).timestamp() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": _METRIC_NAMESPACE,
                    "Dimensions": [["feed_id"]],
                    "Metrics": [{"Name": name, "Unit": unit}],
                }
            ],
        },
        "feed_id": feed_id,
        name: value,
    }
    print(json.dumps(payload))  # noqa: T201 -- the sanctioned §11.2 mechanism
