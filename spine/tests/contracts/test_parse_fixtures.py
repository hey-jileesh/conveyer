"""Consumer-side contract-fixture parsing — LLD 004.1 I-16, R-10.

Parses every file in each `contracts/fixtures/events/<detail-type>/` directory
with the spine's own model of that event and asserts semantic fields (never
exact JSON equality -- that's the producer suite's job, `ingestion/tests/
contracts/test_emit_fixtures.py`). A fixture change is a PR that touches both
packages' expectations or fails CI (I-16).

`delivery-registered/` exists as of M1 (bead conveyer-nvh.13); `batch-
started/`/`batch-completed/` fixture directories were authored this bead
(conveyer-nvh.28, M5, architect D-4 -- spine is the producer of these two
lifecycle events, symmetric to ingestion's `delivery-registered` ownership;
producer-side exactness for them lives in this package's own
`test_emit_fixtures.py`, unlike `delivery-registered`'s, which lives in
ingestion's suite). `_fixture_cases` still tolerates a directory's absence
rather than asserting it exists -- a defensive default, not a signal that any
of the three is currently missing -- so this suite keeps growing
automatically if a future detail type's fixtures land before its glob entry
does.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel
from spine.core.model import BatchCompletedV1, BatchStartedV1, DeliveryRegisteredV1

_EVENTS_DIR = Path(__file__).resolve().parents[3] / "contracts" / "fixtures" / "events"

_MODEL_BY_DETAIL_TYPE: dict[str, type[BaseModel]] = {
    "delivery-registered": DeliveryRegisteredV1,
    "batch-started": BatchStartedV1,
    "batch-completed": BatchCompletedV1,
}


def _fixture_cases() -> list[tuple[type[BaseModel], Path]]:
    cases: list[tuple[type[BaseModel], Path]] = []
    for detail_type, model_cls in _MODEL_BY_DETAIL_TYPE.items():
        directory = _EVENTS_DIR / detail_type
        if not directory.is_dir():
            continue
        cases.extend((model_cls, path) for path in sorted(directory.glob("*.json")))
    return cases


def test_delivery_registered_fixtures_exist() -> None:
    # The one detail type guaranteed present at M1 (§12.6) -- guards against
    # `_fixture_cases` silently collecting zero cases if the fixtures dir moves.
    directory = _EVENTS_DIR / "delivery-registered"
    assert directory.is_dir()
    assert list(directory.glob("*.json"))


@pytest.mark.parametrize("detail_type", ["batch-started", "batch-completed"])
def test_lifecycle_fixtures_exist(detail_type: str) -> None:
    # Authored this bead (conveyer-nvh.28, M5, architect D-4) -- same
    # zero-cases guard as `delivery-registered`'s, now that both lifecycle
    # directories are populated.
    directory = _EVENTS_DIR / detail_type
    assert directory.is_dir()
    assert list(directory.glob("*.json"))


@pytest.mark.parametrize(
    "model_cls,fixture_path",
    _fixture_cases(),
    ids=lambda value: value.name if isinstance(value, Path) else value.__name__,
)
def test_fixture_parses_with_semantic_fields(
    model_cls: type[BaseModel], fixture_path: Path
) -> None:
    raw: dict[str, Any] = json.loads(fixture_path.read_text())
    parsed = model_cls.model_validate(raw)
    assert parsed.schema_version == 1  # type: ignore[attr-defined]
    assert parsed.batch_id == raw["batch_id"]  # type: ignore[attr-defined]
    assert parsed.pipeline == raw["pipeline"]  # type: ignore[attr-defined]
