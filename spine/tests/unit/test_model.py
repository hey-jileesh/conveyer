"""Unit tests for `spine.core.model` — LLD §6.1, §6.2, §6.6, §7.5.

Covers: `DeliveryRegisteredV1`'s tolerant-reader `extra="allow"` and
`object_uris` bounds (I-22); both M0 delivery-registered fixtures parsing
(§12.6); `PipelineSpecModel`/`CoEffectDecl`'s `extra="forbid"` and table-
identifier grammar (§6.2, §6.7); `BatchStartedV1`/`BatchCompletedV1` shape
(§6.6); `LineageStamp` immutability (§7.5 [C-5]).
"""

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from spine.core import model

_FIXTURES_DIR = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "fixtures"
    / "events"
    / "delivery-registered"
)

_VALID_SEED: dict[str, Any] = {
    "schema_version": 1,
    "feed_id": "carrier-x/commission-statements",
    "delivery_id": "a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
    "batch_id": "b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
    "delivery_key": "statement-2026-07.csv",
    "content_hash": "sha256:1b57d99bac736c030171a604e3f1e6d96e6ed860f1b1aca5e58506dbfa4ee7d6",
    "size_bytes": 4096,
    "object_uris": ["s3://bucket/statement.csv"],
    "received_at": datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
    "pipeline": "pipelines/commissions",
}


# --- §6.1 DeliveryRegisteredV1: tolerant reader ------------------------------


def test_tolerant_reader_ignores_unknown_fields() -> None:
    data = dict(_VALID_SEED)
    data["some_future_field"] = "unrecognized"
    parsed = model.DeliveryRegisteredV1.model_validate(data)
    assert parsed.model_extra == {"some_future_field": "unrecognized"}


# --- both M0 fixtures parse (§12.6) ------------------------------------------


def test_fixtures_dir_has_both_m0_fixtures() -> None:
    names = sorted(p.name for p in _FIXTURES_DIR.glob("*.json"))
    assert names == ["v1-minimal.json", "v1-multi-object.json"]


@pytest.mark.parametrize("fixture_path", sorted(_FIXTURES_DIR.glob("*.json")), ids=lambda p: p.name)
def test_seed_model_parses_m0_fixtures(fixture_path: Path) -> None:
    data = json.loads(fixture_path.read_text())
    parsed = model.DeliveryRegisteredV1.model_validate(data)
    assert parsed.schema_version == 1
    assert parsed.batch_id == data["batch_id"]
    assert parsed.pipeline == data["pipeline"]


# --- object_uris bounds (I-22) -----------------------------------------------


def test_object_uris_rejects_empty_list() -> None:
    with pytest.raises(ValidationError):
        model.DeliveryRegisteredV1(**{**_VALID_SEED, "object_uris": []})


def test_object_uris_accepts_256_entries() -> None:
    uris = [f"s3://bucket/{i}" for i in range(256)]
    parsed = model.DeliveryRegisteredV1(**{**_VALID_SEED, "object_uris": uris})
    assert len(parsed.object_uris) == 256


def test_object_uris_rejects_257_entries() -> None:
    uris = [f"s3://bucket/{i}" for i in range(257)]
    with pytest.raises(ValidationError):
        model.DeliveryRegisteredV1(**{**_VALID_SEED, "object_uris": uris})


def test_object_uris_accepts_1024_char_entry() -> None:
    uri = "s3://" + "a" * 1019  # exactly 1024 chars
    parsed = model.DeliveryRegisteredV1(**{**_VALID_SEED, "object_uris": [uri]})
    assert len(parsed.object_uris[0]) == 1024


def test_object_uris_rejects_1025_char_entry() -> None:
    uri = "s3://" + "a" * 1020  # exactly 1025 chars
    with pytest.raises(ValidationError):
        model.DeliveryRegisteredV1(**{**_VALID_SEED, "object_uris": [uri]})


# --- §6.2 PipelineSpecModel / CoEffectDecl: extra="forbid" ------------------

_VALID_SPEC: dict[str, Any] = {
    "pipeline": "pipelines/commissions",
    "transforms_module": "pipelines.commissions.transforms",
    "raw_table": "lake.commissions__raw",
    "quarantine_table": "lake.commissions__quarantine",
    "fact_table": "lake.commissions__facts",
    "state_table": "lake.commissions__state",
}


def test_pipeline_spec_model_accepts_valid_shape() -> None:
    spec = model.PipelineSpecModel(**_VALID_SPEC)
    assert spec.fold == "default-lww"
    assert spec.serialize is False
    assert spec.sla_minutes == 480


def test_pipeline_spec_model_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(**{**_VALID_SPEC, "unexpected_field": True})


def test_co_effect_decl_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        model.CoEffectDecl(table="lake.rate_cards", unexpected_field=True)


@pytest.mark.parametrize(
    "table",
    [
        "badtable",  # no dot at all
        "lake.bad-table",  # dash in the table component
        "lake.",  # empty table component
        ".table",  # empty db component
        "lake.bad table",  # space
    ],
)
def test_pipeline_spec_model_rejects_bad_table_identifiers(table: str) -> None:
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(**{**_VALID_SPEC, "raw_table": table})


def test_co_effect_decl_rejects_bad_table_identifier() -> None:
    with pytest.raises(ValidationError):
        model.CoEffectDecl(table="bad-table")


def test_pipeline_spec_model_co_effects_round_trip() -> None:
    spec = model.PipelineSpecModel(
        **_VALID_SPEC,
        co_effects={"rate_cards": model.CoEffectDecl(table="lake.rate_cards", own_state=True)},
    )
    assert spec.co_effects["rate_cards"].own_state is True


# --- §6.6 lifecycle events ---------------------------------------------------


def test_batch_started_v1_shape() -> None:
    started = model.BatchStartedV1(
        pipeline="pipelines/commissions",
        feed_id="carrier-x/commission-statements",
        batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
        delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
        raw_count=10,
        land_snapshot_id=123,
        started_at=datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
    )
    assert started.schema_version == 1


def test_batch_started_v1_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        model.BatchStartedV1(
            pipeline="pipelines/commissions",
            feed_id="carrier-x/commission-statements",
            batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
            delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
            raw_count=10,
            land_snapshot_id=123,
            started_at=datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
            unexpected_field=True,
        )


def test_batch_completed_v1_naming_split_h1() -> None:
    """`fact_count` (batch truth) is a distinct field from the ledger's
    `facts_appended` (attempt truth) -- this model carries only the former,
    named `fact_count`, per [H-1]."""
    completed = model.BatchCompletedV1(
        pipeline="pipelines/commissions",
        feed_id="carrier-x/commission-statements",
        batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
        delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
        raw_count=10,
        pre_quarantined=0,
        post_quarantined=0,
        fact_count=10,
        fact_snapshot_id=456,
        state_snapshot_id=789,
        completed_at=datetime(2026, 7, 25, 9, 5, 0, tzinfo=UTC),
    )
    assert completed.fact_count == 10
    assert not hasattr(completed, "facts_appended")


# --- §7.5 [C-5] LineageStamp: frozen dataclass value ------------------------


def test_lineage_stamp_is_frozen() -> None:
    stamp = model.LineageStamp(
        batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
        delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
        feed_id="carrier-x/commission-statements",
        received_at=datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
    )
    assert stamp.source_uri is None
    with pytest.raises(dataclasses.FrozenInstanceError):
        stamp.batch_id = "different"  # type: ignore[misc]
