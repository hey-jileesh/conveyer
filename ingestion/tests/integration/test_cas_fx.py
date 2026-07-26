"""Integration tests (factory level, LLD §12.1) for `effects.cas.make_cas_fx`
against moto DynamoDB.

Covers the full turnstile state machine (§8.4): `WON`, `LOST_IN_PROGRESS`
(fresh concurrent claim), `LOST_COMPLETED`, `TAKEN_OVER` (stale claim) with
fencing (a taken-over zombie's `complete` call is silently absorbed, the
resuming owner's succeeds), the takeover-race condition-failure path (a
racing writer wins the `UpdateItem` between our `GetItem` and our own
`UpdateItem` -> `LOST_IN_PROGRESS`), `sweep_stale`, and `marker_exists`/`mark`
semantics (first writer wins, second loses the race).

The race test uses a plain `types.SimpleNamespace` wrapping the real moto
client with one method (`update_item`) replaced by a closure that performs
a genuine concurrent write before delegating to the real call -- a "record
of plain functions" test double (§7.7/§12.2), not `unittest.mock`.
"""

from __future__ import annotations

import types
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import boto3
import pytest
from ingestion.core.completeness import CompletenessResult
from ingestion.core.decisions import RegistrationRequest
from ingestion.core.model import Completeness, FeedConfig, S3PushConnection, StagedObject
from ingestion.effects.cas import _get_raw, make_cas_fx
from moto import mock_aws

_TABLE = "conveyer-test-cas"
_FEED_ID = "carrier-y/renewal-statements"


@pytest.fixture
def cas_client() -> Iterator[Any]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        client.create_table(
            TableName=_TABLE,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield client


def _request(delivery_id: str = "33333333-3333-4333-8333-333333333333") -> RegistrationRequest:
    feed = FeedConfig(
        feed_id=_FEED_ID,
        driver="s3-push",
        pipeline="pipelines/renewals",
        connection=S3PushConnection(partner_principal_arns=["arn:aws:iam::123456789012:root"]),
        completeness=Completeness(mode="manifest"),
    )
    objects = [
        StagedObject(
            name="a.csv",
            role="data",
            uri="s3://lake/carrier-y/renewal-statements/received_at=x/dl-1/a.csv",
            bytes=100,
            sha256="a" * 64,
            src_key="incoming/a.csv",
        ),
        StagedObject(
            name="m.manifest.json",
            role="manifest",
            uri="s3://lake/carrier-y/renewal-statements/received_at=x/dl-1/m.manifest.json",
            bytes=50,
            sha256="b" * 64,
            src_key="incoming/m.manifest.json",
        ),
    ]
    return RegistrationRequest(
        feed=feed,
        delivery_id=delivery_id,
        delivery_key="manifest-xyz",
        received_at=datetime(2026, 7, 25, 9, tzinfo=UTC),
        driver="s3-push",
        driver_run_id="run-a",
        completeness=CompletenessResult(
            verdict="complete", reason=None, asserted_record_count=42, data_object_names=("a.csv",)
        ),
        objects=objects,
    )


def test_claim_first_writer_wins(cas_client: Any) -> None:
    fx = make_cas_fx(cas_client, _TABLE)

    result = fx.claim(
        _request(),
        "b-won",
        "run-w",
        {"key": "incoming/a.csv"},
        datetime(2026, 7, 25, 10, tzinfo=UTC),
    )

    assert result.kind == "WON"
    assert result.item is None


def test_claim_second_writer_loses_to_fresh_in_progress_claim(cas_client: Any) -> None:
    fx = make_cas_fx(cas_client, _TABLE)
    fx.claim(_request(), "b-dup", "run-w", {}, datetime(2026, 7, 25, 10, tzinfo=UTC))

    result = fx.claim(_request(), "b-dup", "run-x", {}, datetime(2026, 7, 25, 10, 0, 5, tzinfo=UTC))

    assert result.kind == "LOST_IN_PROGRESS"
    assert result.item is not None
    assert result.item.owner_run_id == "run-w"


def test_claim_loses_to_completed_claim(cas_client: Any) -> None:
    fx = make_cas_fx(cas_client, _TABLE)
    t0 = datetime(2026, 7, 25, 12, tzinfo=UTC)
    fx.claim(_request(), "b-done", "run-c", {}, t0)
    fx.complete(_FEED_ID, "b-done", "run-c", datetime(2026, 7, 25, 12, 0, 1, tzinfo=UTC))

    result = fx.claim(
        _request(), "b-done", "run-late", {}, datetime(2026, 7, 25, 12, 0, 2, tzinfo=UTC)
    )

    assert result.kind == "LOST_COMPLETED"


def test_claim_takes_over_stale_in_progress_claim_carrying_dead_run_identity(
    cas_client: Any,
) -> None:
    fx = make_cas_fx(cas_client, _TABLE)
    t0 = datetime(2026, 7, 25, 11, tzinfo=UTC)
    dead_req = _request(delivery_id="dead-delivery")
    fx.claim(dead_req, "b-stale", "run-dead", {}, t0)

    t1 = datetime(2026, 7, 25, 11, 21, tzinfo=UTC)  # 21 min later, > the 1200 s staleness threshold
    result = fx.claim(_request(delivery_id="resumer-would-be-id"), "b-stale", "run-resumer", {}, t1)

    assert result.kind == "TAKEN_OVER"
    # The returned item carries the DEAD run's identity -- decisions.py's
    # `_plan_taken_over` builds the resumed row from exactly this, so the
    # resuming Lambda's own run_id/delivery_id must never leak into it.
    assert result.item is not None
    assert result.item.owner_run_id == "run-dead"
    assert result.item.delivery_id == "dead-delivery"
    # But the STORED item is now owned by the resuming run (fencing target).
    stored = _get_raw(cas_client, _TABLE, "batch#carrier-y/renewal-statements#b-stale")
    assert stored is not None
    assert stored["owner_run_id"] == "run-resumer"


def test_complete_is_fenced_a_taken_over_zombie_cannot_complete(cas_client: Any) -> None:
    fx = make_cas_fx(cas_client, _TABLE)
    fx.claim(_request(), "b-stale", "run-dead", {}, datetime(2026, 7, 25, 11, tzinfo=UTC))
    fx.claim(_request(), "b-stale", "run-resumer", {}, datetime(2026, 7, 25, 11, 21, tzinfo=UTC))

    fx.complete(
        _FEED_ID, "b-stale", "run-dead", datetime(2026, 7, 25, 11, 22, tzinfo=UTC)
    )  # zombie
    stored_after_zombie = _get_raw(cas_client, _TABLE, "batch#carrier-y/renewal-statements#b-stale")
    assert stored_after_zombie is not None
    assert (
        stored_after_zombie["status"] == "in_progress"
    )  # zombie's write was absorbed, not applied

    fx.complete(_FEED_ID, "b-stale", "run-resumer", datetime(2026, 7, 25, 11, 23, tzinfo=UTC))
    stored_after_real = _get_raw(cas_client, _TABLE, "batch#carrier-y/renewal-statements#b-stale")
    assert stored_after_real is not None
    assert stored_after_real["status"] == "completed"


def test_takeover_race_condition_failure_yields_lost_in_progress(cas_client: Any) -> None:
    fx = make_cas_fx(cas_client, _TABLE)
    fx.claim(_request(), "b-race", "run-dead", {}, datetime(2026, 7, 25, 14, tzinfo=UTC))
    pk = "batch#carrier-y/renewal-statements#b-race"

    def racing_update_item(**kwargs: Any) -> Any:
        # A concurrent sweep/registrar wins the takeover between our GetItem
        # and our UpdateItem attempt.
        cas_client.update_item(
            TableName=_TABLE,
            Key={"pk": {"S": pk}},
            UpdateExpression="SET owner_run_id = :o",
            ExpressionAttributeValues={":o": {"S": "run-winner"}},
        )
        return cas_client.update_item(**kwargs)

    racer_client = types.SimpleNamespace(
        put_item=cas_client.put_item,
        get_item=cas_client.get_item,
        scan=cas_client.scan,
        update_item=racing_update_item,
    )
    fx_racer = make_cas_fx(racer_client, _TABLE)

    result = fx_racer.claim(
        _request(), "b-race", "run-loser", {}, datetime(2026, 7, 25, 14, 21, tzinfo=UTC)
    )

    assert result.kind == "LOST_IN_PROGRESS"
    stored = _get_raw(cas_client, _TABLE, pk)
    assert stored is not None
    assert stored["owner_run_id"] == "run-winner"  # the racer's write stuck; ours was rejected


def test_sweep_stale_returns_only_stale_in_progress_batch_claims(cas_client: Any) -> None:
    fx = make_cas_fx(cas_client, _TABLE)
    fx.claim(_request(), "b-stale-1", "run-1", {}, datetime(2026, 7, 25, 9, tzinfo=UTC))
    fx.claim(_request(), "b-fresh", "run-2", {}, datetime(2026, 7, 25, 14, 24, tzinfo=UTC))
    fx.claim(_request(), "b-done", "run-3", {}, datetime(2026, 7, 25, 9, tzinfo=UTC))
    fx.complete(_FEED_ID, "b-done", "run-3", datetime(2026, 7, 25, 9, 1, tzinfo=UTC))

    stale = fx.sweep_stale(datetime(2026, 7, 25, 14, 25, tzinfo=UTC))

    assert {item.batch_id for item in stale} == {"b-stale-1"}


def test_marker_exists_and_mark_first_writer_wins(cas_client: Any) -> None:
    fx = make_cas_fx(cas_client, _TABLE)
    pk = "overdue#carrier-y/renewal-statements#2026-07-24"

    assert fx.marker_exists(pk) is False
    assert fx.mark(pk, datetime(2026, 7, 25, 9, tzinfo=UTC), 35) is True
    assert fx.marker_exists(pk) is True
    assert fx.mark(pk, datetime(2026, 7, 25, 9, 0, 5, tzinfo=UTC), 35) is False  # lost the race
