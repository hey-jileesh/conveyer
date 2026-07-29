"""Producer-side contract-fixture exactness for lifecycle events -- LLD 004.1
I-16 / §12.5 / architect D-4 (resolves LLD gap G-4: spine is the producer of
`batch-started`/`batch-completed`, symmetric to ingestion's ownership of
`delivery-registered`).

Repo-root `contracts/fixtures/events/<detail-type>/v1-basic.json` holds the
EventBridge `Detail` payload only (envelope is substrate, §12.5). This suite
builds `BatchStartedV1`/`BatchCompletedV1` from pinned inputs -- literal
values, matching the shapes `stages/land.py`/`stages/publish.py` actually
construct -- and asserts EXACT JSON equality with each fixture file, using
`model.model_dump_json()` with no arguments: the identical call
`effects/events.py`'s `emit` makes (`"Detail": model.model_dump_json()`), so
the fixture bytes are what a real attempt would actually put on the bus.

Mirrors `ingestion/tests/contracts/test_emit_fixtures.py`'s pattern exactly
(pinned inputs, not derived from any shared builder; equality checked on
*parsed* JSON, not raw bytes, so the fixture file's own `indent=2` formatting
convention never causes a false failure). The consumer side (spine parsing
every fixture for semantic fields, including these two directories) is
`test_parse_fixtures.py`, this suite's sibling -- one fixture set, two
consumers, no new CI stage (§12.5).

Pinned identity-exemplar-shaped values (§12.2's own pipeline/feed, a
deterministic UUIDv4 delivery id and UUIDv5 batch id, per I-22) -- chosen for
readability, not reused from any other test's own batch_id (fixture JSON is
independent of any running scenario).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from spine.core.model import BatchCompletedV1, BatchStartedV1

_FIXTURES_DIR = Path(__file__).resolve().parents[3] / "contracts" / "fixtures" / "events"

_PIPELINE = "pipelines/identity"
_FEED_ID = "feed/identity"
_DELIVERY_ID = "00000000-0000-4000-8000-000000000001"  # UUIDv4 shape, [H-4]
_BATCH_ID = "00000000-0000-5000-8000-000000000001"  # UUIDv5 shape, I-22

_PINNED_STARTED: dict[str, BatchStartedV1] = {
    "v1-basic.json": BatchStartedV1(
        pipeline=_PIPELINE,
        feed_id=_FEED_ID,
        batch_id=_BATCH_ID,
        delivery_id=_DELIVERY_ID,
        raw_count=2,
        land_snapshot_id=1001,
        started_at=datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
    ),
}

_PINNED_COMPLETED: dict[str, BatchCompletedV1] = {
    "v1-basic.json": BatchCompletedV1(
        pipeline=_PIPELINE,
        feed_id=_FEED_ID,
        batch_id=_BATCH_ID,
        delivery_id=_DELIVERY_ID,
        raw_count=2,
        pre_quarantined=0,
        post_quarantined=0,
        fact_count=2,
        fact_snapshot_id=1002,
        state_snapshot_id=1003,
        completed_at=datetime(2026, 7, 25, 9, 0, 5, tzinfo=UTC),
    ),
}


def test_batch_started_fixtures_dir_is_populated() -> None:
    directory = _FIXTURES_DIR / "batch-started"
    assert directory.is_dir()
    assert sorted(p.name for p in directory.glob("*.json")) == sorted(_PINNED_STARTED)


def test_batch_completed_fixtures_dir_is_populated() -> None:
    directory = _FIXTURES_DIR / "batch-completed"
    assert directory.is_dir()
    assert sorted(p.name for p in directory.glob("*.json")) == sorted(_PINNED_COMPLETED)


@pytest.mark.parametrize("fixture_name", sorted(_PINNED_STARTED))
def test_batch_started_matches_fixture_exactly(fixture_name: str) -> None:
    """Same call `fx.emit` makes (`effects/events.py`): `model_dump_json()`,
    no args -- exact bytes-on-the-bus, checked as parsed-JSON equality."""
    event = _PINNED_STARTED[fixture_name]
    fixture_path = _FIXTURES_DIR / "batch-started" / fixture_name
    actual = json.loads(event.model_dump_json())
    expected = json.loads(fixture_path.read_text())
    assert actual == expected


@pytest.mark.parametrize("fixture_name", sorted(_PINNED_COMPLETED))
def test_batch_completed_matches_fixture_exactly(fixture_name: str) -> None:
    event = _PINNED_COMPLETED[fixture_name]
    fixture_path = _FIXTURES_DIR / "batch-completed" / fixture_name
    actual = json.loads(event.model_dump_json())
    expected = json.loads(fixture_path.read_text())
    assert actual == expected
