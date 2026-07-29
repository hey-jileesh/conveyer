"""Producer-side contract-fixture exactness -- LLD 004.1 I-16 / S12.5 / S12.6 item 2.

Repo-root `contracts/fixtures/events/<detail-type>/<case>.json` holds the
EventBridge `Detail` payload only (envelope is substrate, S12.5); this suite
builds `DeliveryRegisteredV1` from pinned inputs -- literal values, not
derived from any shared builder -- and asserts EXACT JSON equality with each
fixture file. A fixture change is a PR that touches both this suite and the
fixture; an accidental additive field on the model (or a fixture edited by
hand) fails this test loudly. The consumer side (spine, once it exists)
parses every file in the same directory for semantic fields only (R-10);
this suite owns the producer's byte-for-byte guarantee.

Formatting convention: fixtures are committed as `model_dump_json(indent=2)`
plus a trailing newline; equality is checked on the *parsed* JSON (dict
equality), not the raw bytes, so incidental whitespace never causes a false
failure -- only field/value drift does.

002.1 errata note [E-12]: `DeliveryRegisteredV1.object_uris` carries no
`min_length` in ingestion's own producer model (002.1 S6.4), but
registration only ever emits `delivery-registered` for a `"registered"`
disposition, which by construction has >= 1 data object. Both fixtures
below pin `object_uris` with >= 1 entries, and
`test_object_uris_non_empty_errata_e12` asserts this holds for every pinned
event -- the spine's own `DeliveryRegisteredV1` (004.1 S6.1) tightens this
to `Field(min_length=1)`, which is stricter than the producer model but
never violated by it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from ingestion.core.model import DeliveryRegisteredV1

_FIXTURES_DIR = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "fixtures"
    / "events"
    / "delivery-registered"
)

# Pinned inputs -- literal, not derived -- one entry per fixture file name.
_PINNED_EVENTS: dict[str, DeliveryRegisteredV1] = {
    "v1-minimal.json": DeliveryRegisteredV1(
        feed_id="carrier-x/commission-statements",
        delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
        batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
        delivery_key="statement-2026-07.csv",
        content_hash="sha256:1b57d99bac736c030171a604e3f1e6d96e6ed860f1b1aca5e58506dbfa4ee7d6",
        size_bytes=4096,
        object_uris=[
            "s3://conveyer-dev-lake/carrier-x/commission-statements/"
            "received_at=20260725T090000000000Z/"
            "dl-a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d/statement-2026-07.csv"
        ],
        received_at=datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
        pipeline="pipelines/commissions",
    ),
    "v1-multi-object.json": DeliveryRegisteredV1(
        feed_id="carrier-y/renewal-statements",
        delivery_id="c3d4e5f6-a7b8-4c3d-9e4f-5a6b7c8d9e0f",
        batch_id="d4e5f6a7-b8c9-5d4e-8f5a-6b7c8d9e0f1a",
        delivery_key="manifest-renewals-2026-08",
        content_hash="sha256:10d88ef52e770c3cb94bd3608c74b61e713b67a6043d7128af15c77476057a76",
        size_bytes=5120,
        object_uris=[
            "s3://conveyer-dev-lake/carrier-y/renewal-statements/"
            "received_at=20260803T143000000000Z/"
            "dl-c3d4e5f6-a7b8-4c3d-9e4f-5a6b7c8d9e0f/renewals-part1.csv",
            "s3://conveyer-dev-lake/carrier-y/renewal-statements/"
            "received_at=20260803T143000000000Z/"
            "dl-c3d4e5f6-a7b8-4c3d-9e4f-5a6b7c8d9e0f/renewals-part2.csv",
        ],
        received_at=datetime(2026, 8, 3, 14, 30, 0, tzinfo=UTC),
        pipeline="pipelines/renewals",
    ),
}


def test_fixtures_dir_is_populated() -> None:
    assert _FIXTURES_DIR.is_dir(), f"missing fixtures dir: {_FIXTURES_DIR}"
    assert sorted(p.name for p in _FIXTURES_DIR.glob("*.json")) == sorted(_PINNED_EVENTS)


@pytest.mark.parametrize("fixture_name", sorted(_PINNED_EVENTS))
def test_emit_matches_fixture_exactly(fixture_name: str) -> None:
    """Build the event from pinned inputs; parsed JSON must equal the fixture."""
    event = _PINNED_EVENTS[fixture_name]
    fixture_path = _FIXTURES_DIR / fixture_name
    actual = json.loads(event.model_dump_json())
    expected = json.loads(fixture_path.read_text())
    assert actual == expected


@pytest.mark.parametrize("fixture_name", sorted(_PINNED_EVENTS))
def test_object_uris_non_empty_errata_e12(fixture_name: str) -> None:
    """002.1 errata [E-12]: registration guarantees >= 1 data object."""
    assert len(_PINNED_EVENTS[fixture_name].object_uris) >= 1
