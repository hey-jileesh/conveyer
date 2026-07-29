"""Unit tests for `spine.observability` — LLD §11.1 (EMF) / §11.2 (structured
JSON logs).

Mirrors `ingestion/tests/unit/test_observability.py`'s shape exactly: both
mechanisms are pure stdout logic (no boto3, no AWS) — `_format_json`/
`make_json_handler` are exercised directly against a `logging.LogRecord`/a
real `logging.Logger` writing to an in-memory `io.StringIO` stream, and
`emit_metric`'s EMF dict shape is asserted via `capsys` (stdout capture, no
CloudWatch involved — §11.1: "hand-rolled dict, printed to stdout; no
library"). Spine's own identifier allowlist is `batch_id`, `pipeline`,
`feed_id`, `attempt_id`, `stage` (§11.2), not ingestion's four.
"""

from __future__ import annotations

import io
import json
import logging

import pytest
from spine import observability

# --- _format_json (§11.2's "20-line JSON formatter") -------------------------


def _make_record(
    *,
    level: int = logging.INFO,
    msg: str = "hello %s",
    args: tuple[object, ...] = ("world",),
    extra: dict[str, object] | None = None,
) -> logging.LogRecord:
    logger = logging.getLogger("test-spine-observability-probe")
    return logger.makeRecord(
        logger.name, level, "test_observability.py", 1, msg, args, None, extra=extra
    )


def test_format_json_carries_message_and_level() -> None:
    record = _make_record()
    payload = json.loads(observability._format_json(record))
    assert payload["message"] == "hello world"
    assert payload["level"] == "INFO"
    assert "timestamp" in payload


def test_format_json_includes_known_identifiers_when_present() -> None:
    record = _make_record(extra={"batch_id": "b-1", "stage": "land"})
    payload = json.loads(observability._format_json(record))
    assert payload["batch_id"] == "b-1"
    assert payload["stage"] == "land"
    for attr in ("pipeline", "feed_id", "attempt_id"):
        assert attr not in payload


def test_format_json_omits_all_identifiers_when_none_supplied() -> None:
    record = _make_record(msg="plain message", args=())
    payload = json.loads(observability._format_json(record))
    assert payload["message"] == "plain message"
    for attr in ("batch_id", "pipeline", "feed_id", "attempt_id", "stage"):
        assert attr not in payload


def test_format_json_carries_all_five_identifiers_when_all_present() -> None:
    record = _make_record(
        level=logging.WARNING,
        msg="stage transition",
        args=(),
        extra={
            "batch_id": "b",
            "pipeline": "p",
            "feed_id": "f",
            "attempt_id": "a",
            "stage": "fold",
        },
    )
    payload = json.loads(observability._format_json(record))
    assert payload["level"] == "WARNING"
    assert payload["batch_id"] == "b"
    assert payload["pipeline"] == "p"
    assert payload["feed_id"] == "f"
    assert payload["attempt_id"] == "a"
    assert payload["stage"] == "fold"


# --- make_json_handler / install_json_handler --------------------------------


def test_make_json_handler_writes_one_json_line_per_record() -> None:
    stream = io.StringIO()
    handler = observability.make_json_handler(stream)
    logger = logging.getLogger("test-spine-observability-handler")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers = [handler]
    try:
        logger.info(
            "batch started",
            extra={"batch_id": "b-2", "pipeline": "p-2", "feed_id": "f-2", "stage": "land"},
        )
        logger.info("second line, no identifiers")
    finally:
        logger.handlers = []

    lines = stream.getvalue().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["message"] == "batch started"
    assert first["batch_id"] == "b-2"
    second = json.loads(lines[1])
    assert second["message"] == "second line, no identifiers"
    assert "batch_id" not in second


def test_install_json_handler_is_idempotent_and_raises_root_level() -> None:
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    try:
        root.handlers = []
        root.setLevel(logging.WARNING)
        observability.install_json_handler(io.StringIO())
        first_count = len(root.handlers)
        observability.install_json_handler(io.StringIO())  # second call: no-op
        assert len(root.handlers) == first_count == 1
        assert root.level == logging.INFO
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)


def test_make_json_handler_default_call_wires_the_json_formatter() -> None:
    handler = observability.make_json_handler()
    assert isinstance(handler, logging.StreamHandler)
    assert handler.formatter is not None
    assert handler.formatter.format is observability._format_json


# --- emit_metric (§11.1's hand-rolled EMF dict) -------------------------------


def test_emit_metric_prints_emf_shaped_dict_without_stage_dimension(
    capsys: pytest.CaptureFixture[str],
) -> None:
    observability.emit_metric("RawRows", 10.0, "pipelines/commissions", "carrier-x/a")
    payload = json.loads(capsys.readouterr().out.strip())

    metrics_block = payload["_aws"]["CloudWatchMetrics"][0]
    assert metrics_block["Namespace"] == "Conveyer/Spine"
    assert metrics_block["Dimensions"] == [["pipeline", "feed_id"]]
    assert metrics_block["Metrics"] == [{"Name": "RawRows", "Unit": "Count"}]
    assert isinstance(payload["_aws"]["Timestamp"], int)
    assert payload["pipeline"] == "pipelines/commissions"
    assert payload["feed_id"] == "carrier-x/a"
    assert payload["RawRows"] == 10.0
    assert "stage" not in payload


def test_emit_metric_adds_stage_dimension_and_respects_unit_override(
    capsys: pytest.CaptureFixture[str],
) -> None:
    observability.emit_metric(
        "StageSeconds", 1.5, "pipelines/commissions", "carrier-x/a", stage="fold", unit="Seconds"
    )
    payload = json.loads(capsys.readouterr().out.strip())

    metrics_block = payload["_aws"]["CloudWatchMetrics"][0]
    assert metrics_block["Dimensions"] == [["pipeline", "feed_id", "stage"]]
    assert metrics_block["Metrics"] == [{"Name": "StageSeconds", "Unit": "Seconds"}]
    assert payload["stage"] == "fold"
    assert payload["StageSeconds"] == 1.5
