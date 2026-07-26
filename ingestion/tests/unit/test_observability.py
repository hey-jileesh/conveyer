"""Unit tests for `ingestion.observability` -- LLD §11.1 (structured JSON
logs) / §11.2 (EMF metrics).

Both mechanisms are pure stdout logic (no boto3, no AWS) -- `_format_json`/
`make_json_handler` are exercised directly against a `logging.LogRecord`/a
real `logging.Logger` writing to an in-memory `io.StringIO` stream, and
`emit_metric`'s EMF dict shape is asserted via `capsys` (stdout capture, no
CloudWatch involved -- §11.2: "hand-rolled dict, printed to stdout; no
library"). Closes part of the F4/F5 coverage shortfall (LLD §12.5): this
module was 46% covered (only the two factory-shaped lines that don't
require actually invoking the formatter/print were hit) before this file.
"""

from __future__ import annotations

import io
import json
import logging

import pytest
from ingestion import observability

# --- _format_json (§11.1's "20-line JSON formatter") ------------------------


def _make_record(
    *,
    level: int = logging.INFO,
    msg: str = "hello %s",
    args: tuple[object, ...] = ("world",),
    extra: dict[str, object] | None = None,
) -> logging.LogRecord:
    logger = logging.getLogger("test-observability-probe")
    record = logger.makeRecord(
        logger.name, level, "test_observability.py", 1, msg, args, None, extra=extra
    )
    return record


def test_format_json_carries_message_and_level() -> None:
    record = _make_record()
    payload = json.loads(observability._format_json(record))
    assert payload["message"] == "hello world"  # %-style interpolation applied
    assert payload["level"] == "INFO"
    assert "timestamp" in payload


def test_format_json_includes_known_identifiers_when_present() -> None:
    record = _make_record(
        extra={"feed_id": "carrier-x/commission-statements", "delivery_id": "d-1"}
    )
    payload = json.loads(observability._format_json(record))
    assert payload["feed_id"] == "carrier-x/commission-statements"
    assert payload["delivery_id"] == "d-1"
    # batch_id/driver_run_id were never set on this record -- omitted, not null.
    assert "batch_id" not in payload
    assert "driver_run_id" not in payload


def test_format_json_omits_all_identifiers_when_none_supplied() -> None:
    record = _make_record(msg="plain message", args=())
    payload = json.loads(observability._format_json(record))
    assert payload["message"] == "plain message"
    for attr in ("feed_id", "delivery_id", "batch_id", "driver_run_id"):
        assert attr not in payload


def test_format_json_carries_all_four_identifiers_when_all_present() -> None:
    record = _make_record(
        level=logging.WARNING,
        msg="state transition",
        args=(),
        extra={
            "feed_id": "f",
            "delivery_id": "d",
            "batch_id": "b",
            "driver_run_id": "r",
        },
    )
    payload = json.loads(observability._format_json(record))
    assert payload["level"] == "WARNING"
    assert payload["feed_id"] == "f"
    assert payload["delivery_id"] == "d"
    assert payload["batch_id"] == "b"
    assert payload["driver_run_id"] == "r"


# --- make_json_handler (end-to-end through a real logging.Logger) -----------


def test_make_json_handler_writes_one_json_line_per_record() -> None:
    stream = io.StringIO()
    handler = observability.make_json_handler(stream)
    logger = logging.getLogger("test-observability-handler")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers = [handler]
    try:
        logger.info(
            "delivery registered",
            extra={
                "feed_id": "carrier-y/renewal-statements",
                "delivery_id": "d-2",
                "batch_id": "b-2",
                "driver_run_id": "r-2",
            },
        )
        logger.info("second line, no identifiers")
    finally:
        logger.handlers = []

    lines = stream.getvalue().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["message"] == "delivery registered"
    assert first["feed_id"] == "carrier-y/renewal-statements"
    second = json.loads(lines[1])
    assert second["message"] == "second line, no identifiers"
    assert "feed_id" not in second


def test_make_json_handler_default_call_wires_the_json_formatter() -> None:
    # `stream` defaults to `sys.stdout` bound at module-import time (module
    # docstring: "Lambda ships stdout to CloudWatch Logs") -- asserting
    # actual output reaching a *test-time* `sys.stdout` replacement (e.g.
    # `capsys`) is unreliable: pytest's capture fixtures install a fresh
    # `sys.stdout` object per test, but this factory's default argument was
    # already bound to whatever `sys.stdout` was at collection time, so a
    # later swap is invisible to it (a standard late-binding-default-argument
    # gotcha, not a bug in `make_json_handler`). What IS meaningful to assert
    # without depending on that binding: calling with no `stream` argument
    # succeeds and wires the same custom `_format_json` formatter (§11.1's
    # "formatter.format = _format_json" technique, per the module docstring)
    # as the explicit-stream path exercised above.
    handler = observability.make_json_handler()
    assert isinstance(handler, logging.StreamHandler)
    assert handler.formatter is not None
    assert handler.formatter.format is observability._format_json


# --- emit_metric (§11.2's hand-rolled EMF dict) ------------------------------


def test_emit_metric_prints_emf_shaped_dict_with_default_unit(
    capsys: pytest.CaptureFixture[str],
) -> None:
    observability.emit_metric("DeliveriesRegistered", 1, "carrier-x/commission-statements")
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip())

    metrics_block = payload["_aws"]["CloudWatchMetrics"][0]
    assert metrics_block["Namespace"] == "Conveyer/Ingestion"
    assert metrics_block["Dimensions"] == [["feed_id"]]
    assert metrics_block["Metrics"] == [{"Name": "DeliveriesRegistered", "Unit": "Count"}]
    assert isinstance(payload["_aws"]["Timestamp"], int)
    assert payload["feed_id"] == "carrier-x/commission-statements"
    assert payload["DeliveriesRegistered"] == 1


def test_emit_metric_respects_explicit_unit_override(
    capsys: pytest.CaptureFixture[str],
) -> None:
    observability.emit_metric("RegistrationSeconds", 1.5, "carrier-y/renewal-statements", "Seconds")
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip())

    metrics_block = payload["_aws"]["CloudWatchMetrics"][0]
    assert metrics_block["Metrics"] == [{"Name": "RegistrationSeconds", "Unit": "Seconds"}]
    assert payload["RegistrationSeconds"] == 1.5
