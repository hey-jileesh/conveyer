"""Absence detector golden suite -- LLD §12.4 (G-12/G-12b), §9.3 (stuck-claim
sweep routing).

Drives the REAL `absence.detector` module against `local_effects` (moto
S3/DynamoDB/EventBridge + a `SqlCatalog` ledger -- `tests/conftest.py`), the
same no-mocking-framework convention every other golden test file in this
suite uses (§12.2 IDIOM rule). `queue_url`/`_drain_events`/`s3_client` are
duplicated here rather than imported from a sibling test module (matching
the established per-file convention already used by `test_s3_push_
registration.py` and `test_sftp_pull.py` -- neither exports these via
`conftest.py`, and this file is not those modules' owner to refactor).

* G-12/G-12b (§12.4): a `weekdays`-expectation feed with one prior on-time
  delivery (Friday) and none for the current weekday (Monday) -- the sweep
  emits exactly one `delivery-overdue` the first time (G-12); a second
  sweep at the same `now` emits nothing, because the CAS marker from the
  first sweep persists (G-12b) -- this is what proves emit-then-mark's
  at-least-once/idempotent contract (D-14), not just that the pure
  `core.expectations.overdue_dates` function is correct (already covered
  by `tests/unit/test_expectations.py`).
* The stuck-claim sweep test pre-seeds one stale `WON` claim per driver
  (same technique `test_s3_push_registration.py::test_g11_...` and
  `test_sftp_pull.py::test_sftp_resume_batch_id_completes_a_crashed_
  delivery` each already use for THEIR OWN claims) and asserts
  `absence.detector.sweep_stuck_claims` -- not a hand-rolled fake --
  discovers both via the REAL `cas.sweep_stale` and issues the correct
  `invoke_async` call for each route. For the s3-push route this closes
  the loop G-11 (M3) opened: G-11 proved a REPLAYED "Object Created" event
  resumes a `TAKEN_OVER` claim to the dead run's identity; this test proves
  the absence sweep's own synthetic-event reconstruction (from the claim's
  recorded `trigger`, not re-derived from anything else) produces that
  EXACT same event, by actually replaying it through `drivers.s3_push
  .acquire` and asserting the same resume outcome G-11 asserts.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import pytest
from ingestion.absence import detector
from ingestion.core import folds
from ingestion.core.completeness import CompletenessResult
from ingestion.core.decisions import RegistrationRequest
from ingestion.core.hashing import canonical_content_hash
from ingestion.core.minting import mint_batch_id
from ingestion.core.model import (
    Completeness,
    DeliveryObject,
    DeliveryRecord,
    Expectation,
    FeedConfig,
    S3PushConnection,
    SftpConnection,
    StagedObject,
    TrailerSpec,
    Trigger,
)
from ingestion.core.naming import split_s3_uri
from ingestion.drivers import s3_push
from ingestion.effects.records import Effects
from ingestion.registration import registrar

_OVERDUE_FEED_ID = "carrier-z/weekday-feed"
_S3_FEED_ID = "carrier-w/trailer-feed"
_SFTP_FEED_ID = "carrier-w/sftp-trailer-feed"
_S3_MANIFEST_FEED_ID = "carrier-w/manifest-feed"
_S3_VESTIBULE_PREFIX = f"{_S3_FEED_ID}/incoming/"
_S3_MANIFEST_VESTIBULE_PREFIX = f"{_S3_MANIFEST_FEED_ID}/incoming/"
_QUEUE = "conveyer-test-absence-capture"


# --- feed builders -------------------------------------------------------------


def _s3push_connection() -> S3PushConnection:
    return S3PushConnection(partner_principal_arns=["arn:aws:iam::111111111111:role/uploader"])


def _trailer_completeness() -> Completeness:
    return Completeness(mode="trailer", trailer=TrailerSpec(pattern=r"TOTAL:\d+"))


def _overdue_feed_config() -> FeedConfig:
    return FeedConfig(
        feed_id=_OVERDUE_FEED_ID,
        driver="s3-push",
        pipeline="pipelines/weekday",
        connection=_s3push_connection(),
        completeness=_trailer_completeness(),
        expectation=Expectation(expected="weekdays", by="09:00", timezone="UTC"),
    )


def _s3_stuck_feed_config() -> FeedConfig:
    return FeedConfig(
        feed_id=_S3_FEED_ID,
        driver="s3-push",
        pipeline="pipelines/trailer",
        connection=_s3push_connection(),
        completeness=_trailer_completeness(),
    )


def _sftp_stuck_feed_config() -> FeedConfig:
    return FeedConfig(
        feed_id=_SFTP_FEED_ID,
        driver="sftp-pull",
        pipeline="pipelines/sftp-trailer",
        connection=SftpConnection(
            secret_ref="arn:aws:secretsmanager:us-east-1:000000000000:secret:conveyer-dev/sftp/w",
            remote_path="/outbound/w/",
        ),
        trigger=Trigger(schedule="cron(0 13 ? * MON-FRI *)"),
        completeness=_trailer_completeness(),
    )


def _manifest_stuck_feed_config() -> FeedConfig:
    return FeedConfig(
        feed_id=_S3_MANIFEST_FEED_ID,
        driver="s3-push",
        pipeline="pipelines/manifest",
        connection=_s3push_connection(),
        completeness=Completeness(mode="manifest"),
    )


def _seed_registry(fx: Effects, s3_client: Any, feeds: list[FeedConfig]) -> None:
    bucket, key = split_s3_uri(fx.config.registry_uri)
    payload = {"registry_version": 1, "feeds": [json.loads(f.model_dump_json()) for f in feeds]}
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode("utf-8"))


# --- ledger row seeding (overdue sweep needs no full registration flow) -------


def _record(delivery_id: str, feed_id: str, received_at: datetime) -> DeliveryRecord:
    return DeliveryRecord(
        delivery_id=delivery_id,
        feed_id=feed_id,
        delivery_key="statement.csv",
        batch_id="batch-" + delivery_id,
        content_hash="sha256:" + "a" * 64,
        size_bytes=10,
        object_uris=[f"s3://lake/{feed_id}/statement.csv"],
        objects=[
            DeliveryObject(
                name="statement.csv",
                role="data",
                uri=f"s3://lake/{feed_id}/statement.csv",
                bytes=10,
                sha256="b" * 64,
            )
        ],
        manifest_ref=None,
        asserted_record_count=1,
        completeness_mode="trailer",
        received_at=received_at,
        recorded_at=received_at,
        disposition="registered",
        supersedes=None,
        driver="s3-push",
        driver_run_id="run-seed",
        notes=None,
    )


# --- event capture (SQS behind an EventBridge rule, per test_events_fx.py) ----


@pytest.fixture
def queue_url(local_effects: Effects) -> str:
    region = local_effects.config.aws_region
    events_client = boto3.client("events", region_name=region)
    sqs = boto3.client("sqs", region_name=region)
    url = sqs.create_queue(QueueName=_QUEUE)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    events_client.put_rule(
        Name="capture-absence",
        EventBusName=local_effects.config.event_bus,
        EventPattern=json.dumps({"source": ["conveyer.ingestion"]}),
    )
    events_client.put_targets(
        Rule="capture-absence",
        EventBusName=local_effects.config.event_bus,
        Targets=[{"Id": "1", "Arn": queue_arn}],
    )
    return url


def _drain_events(fx: Effects, queue_url: str) -> list[dict[str, Any]]:
    sqs = boto3.client("sqs", region_name=fx.config.aws_region)
    messages = sqs.receive_message(
        QueueUrl=queue_url, WaitTimeSeconds=1, MaxNumberOfMessages=10
    ).get("Messages", [])
    if messages:
        sqs.delete_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(messages)
            ],
        )
    return [json.loads(m["Body"]) for m in messages]


@pytest.fixture
def s3_client(local_effects: Effects) -> Any:
    return boto3.client("s3", region_name=local_effects.config.aws_region)


# --- G-12 / G-12b: overdue sweep, emit-then-mark ------------------------------


def test_g12_weekday_feed_overdue_by_deadline_emits_one_delivery_overdue(
    local_effects: Effects, s3_client: Any, queue_url: str, clock_box: list[datetime]
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client, [_overdue_feed_config()])
    # On-time delivery last Friday (2026-07-17); nothing yet for Monday
    # (2026-07-20) -- both are weekdays, but only Monday is overdue.
    fx.ledger.append(
        [
            _record(
                "11111111-1111-4111-8111-111111111111",
                _OVERDUE_FEED_ID,
                datetime(2026, 7, 17, 8, 0, tzinfo=UTC),
            )
        ]
    )
    clock_box[0] = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)  # Monday, past the 09:00 UTC deadline

    result = detector.run(fx, registry_cache={})

    assert result["overdue_emitted"] == 1
    events = _drain_events(fx, queue_url)
    assert len(events) == 1
    assert events[0]["detail-type"] == "delivery-overdue"
    detail = events[0]["detail"]
    assert detail["feed_id"] == _OVERDUE_FEED_ID
    assert detail["expectation_date"] == "2026-07-20"
    assert detail["expected_by"] == "2026-07-20T09:00:00Z"


def test_g12b_second_sweep_after_marker_set_emits_nothing(
    local_effects: Effects, s3_client: Any, queue_url: str, clock_box: list[datetime]
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client, [_overdue_feed_config()])
    fx.ledger.append(
        [
            _record(
                "11111111-1111-4111-8111-111111111111",
                _OVERDUE_FEED_ID,
                datetime(2026, 7, 17, 8, 0, tzinfo=UTC),
            )
        ]
    )
    clock_box[0] = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)

    first = detector.run(fx, registry_cache={})
    assert first["overdue_emitted"] == 1
    _drain_events(fx, queue_url)  # isolate the second sweep's own assertion

    second = detector.run(fx, registry_cache={})

    assert second["overdue_emitted"] == 0  # marker from the first sweep holds
    assert _drain_events(fx, queue_url) == []


# --- stuck-claim sweep: both driver routes ------------------------------------


def test_stuck_claim_sweep_routes_s3_push_to_registrar_and_sftp_pull_to_driver(
    local_effects: Effects,
    s3_client: Any,
    queue_url: str,
    invoke_log: list[tuple[str, dict[str, Any]]],
    clock_box: list[datetime],
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client, [_s3_stuck_feed_config(), _sftp_stuck_feed_config()])
    t_dead = fx.now()

    # --- pre-seed a stale s3-push claim (WON, then never completed) --------
    s3_content = b"policy_id,premium\nP1,100.00\nTOTAL:1\n"
    s3_client.put_object(
        Bucket=fx.config.landing_bucket,
        Key=_S3_VESTIBULE_PREFIX + "statement.csv",
        Body=s3_content,
    )
    s3_sha256 = hashlib.sha256(s3_content).hexdigest()
    s3_delivery_id = "77777777-7777-4777-8777-777777777777"
    s3_staged = StagedObject(
        name="statement.csv",
        role="data",
        uri=s3_push._canonical_uri(
            fx.config.landing_bucket, _S3_FEED_ID, t_dead, s3_delivery_id, "statement.csv"
        ),
        bytes=len(s3_content),
        sha256=s3_sha256,
        src_key=_S3_VESTIBULE_PREFIX + "statement.csv",
    )
    s3_dead_req = RegistrationRequest(
        feed=_s3_stuck_feed_config(),
        delivery_id=s3_delivery_id,
        delivery_key="statement.csv",
        received_at=t_dead,
        driver="s3-push",
        driver_run_id="run-dead-s3",
        completeness=CompletenessResult(
            verdict="complete",
            reason=None,
            asserted_record_count=1,
            data_object_names=("statement.csv",),
        ),
        objects=[s3_staged],
    )
    s3_content_hash = canonical_content_hash([("statement.csv", s3_sha256)])
    s3_batch_id = mint_batch_id(_S3_FEED_ID, s3_content_hash)
    s3_trigger = registrar._trigger_for(s3_dead_req)
    assert s3_trigger == {"trigger_key": _S3_VESTIBULE_PREFIX + "statement.csv"}
    s3_claim = fx.cas.claim(s3_dead_req, s3_batch_id, "run-dead-s3", s3_trigger, t_dead)
    assert s3_claim.kind == "WON"

    # --- pre-seed a stale sftp-pull claim (WON, then never completed) ------
    sftp_delivery_id = "88888888-8888-4888-8888-888888888888"
    sftp_staged = StagedObject(
        name="part1.csv",
        role="data",
        uri=(
            f"s3://{fx.config.landing_bucket}/{_SFTP_FEED_ID}/"
            f"received_at=x/dl-{sftp_delivery_id}/part1.csv"
        ),
        bytes=10,
        sha256="c" * 64,
        src_key=None,
    )
    sftp_dead_req = RegistrationRequest(
        feed=_sftp_stuck_feed_config(),
        delivery_id=sftp_delivery_id,
        delivery_key="part1.csv",
        received_at=t_dead,
        driver="sftp-pull",
        driver_run_id="run-dead-sftp",
        completeness=CompletenessResult(
            verdict="complete",
            reason=None,
            asserted_record_count=None,
            data_object_names=("part1.csv",),
        ),
        objects=[sftp_staged],
    )
    sftp_content_hash = canonical_content_hash([("part1.csv", "c" * 64)])
    sftp_batch_id = mint_batch_id(_SFTP_FEED_ID, sftp_content_hash)
    sftp_trigger = registrar._trigger_for(sftp_dead_req)
    assert sftp_trigger == {"trigger_key": None}  # sftp-pull objects never carry a src_key
    sftp_claim = fx.cas.claim(sftp_dead_req, sftp_batch_id, "run-dead-sftp", sftp_trigger, t_dead)
    assert sftp_claim.kind == "WON"

    # Advance past the 1200 s staleness threshold, then sweep.
    clock_box[0] = t_dead + timedelta(minutes=21)

    recovered = detector.sweep_stuck_claims(fx)

    assert recovered == 2
    sftp_function = f"conveyer-test-driver-{detector._slug(_SFTP_FEED_ID)}"
    by_function = dict(invoke_log)
    assert set(by_function) == {"conveyer-test-registrar", sftp_function}
    assert by_function[sftp_function] == {"resume_batch_id": sftp_batch_id}
    registrar_payload = by_function["conveyer-test-registrar"]
    assert registrar_payload == {
        "detail": {
            "bucket": {"name": fx.config.landing_bucket},
            "object": {"key": _S3_VESTIBULE_PREFIX + "statement.csv"},
        }
    }

    # --- close the loop: replay the reconstructed event through the REAL
    # driver and confirm it resumes the dead run's identity, same shape
    # `test_s3_push_registration.py::test_g11_...` asserts for a manually
    # replayed event.
    outcomes = s3_push.acquire(registrar_payload, fx, "run-resumer", registry_cache={})

    assert len(outcomes) == 1
    assert outcomes[0].disposition == "registered"
    assert outcomes[0].delivery_id == s3_delivery_id

    rows = fx.ledger.scan_feed(_S3_FEED_ID, None)
    registered = folds.registered_deliveries(rows)
    assert len(registered) == 1
    assert registered[0].delivery_id == s3_delivery_id
    assert registered[0].driver_run_id == "run-dead-s3"  # resumes the DEAD run's identity

    events = _drain_events(fx, queue_url)
    assert len(events) == 1
    assert events[0]["detail-type"] == "delivery-registered"


# --- L-7 regression: stuck-claim recovery log never carries a full ClaimItem -


def test_stuck_claim_sweep_warning_log_omits_full_claim_item_repr(
    local_effects: Effects,
    s3_client: Any,
    queue_url: str,
    invoke_log: list[tuple[str, dict[str, Any]]],
    clock_box: list[datetime],
    caplog: pytest.LogCaptureFixture,
) -> None:
    del queue_url, invoke_log  # fixtures only for ordering/moto scaffolding here
    fx = local_effects
    _seed_registry(fx, s3_client, [_s3_stuck_feed_config()])
    t_dead = fx.now()

    s3_content = b"policy_id,premium\nP1,100.00\nTOTAL:1\n"
    s3_client.put_object(
        Bucket=fx.config.landing_bucket,
        Key=_S3_VESTIBULE_PREFIX + "statement.csv",
        Body=s3_content,
    )
    s3_sha256 = hashlib.sha256(s3_content).hexdigest()
    s3_delivery_id = "77777777-7777-4777-8777-777777777777"
    s3_staged = StagedObject(
        name="statement.csv",
        role="data",
        uri=s3_push._canonical_uri(
            fx.config.landing_bucket, _S3_FEED_ID, t_dead, s3_delivery_id, "statement.csv"
        ),
        bytes=len(s3_content),
        sha256=s3_sha256,
        src_key=_S3_VESTIBULE_PREFIX + "statement.csv",
    )
    s3_dead_req = RegistrationRequest(
        feed=_s3_stuck_feed_config(),
        delivery_id=s3_delivery_id,
        delivery_key="statement.csv",
        received_at=t_dead,
        driver="s3-push",
        driver_run_id="run-dead-s3",
        completeness=CompletenessResult(
            verdict="complete",
            reason=None,
            asserted_record_count=1,
            data_object_names=("statement.csv",),
        ),
        objects=[s3_staged],
    )
    s3_content_hash = canonical_content_hash([("statement.csv", s3_sha256)])
    s3_batch_id = mint_batch_id(_S3_FEED_ID, s3_content_hash)
    s3_trigger = registrar._trigger_for(s3_dead_req)
    s3_claim = fx.cas.claim(s3_dead_req, s3_batch_id, "run-dead-s3", s3_trigger, t_dead)
    assert s3_claim.kind == "WON"

    clock_box[0] = t_dead + timedelta(minutes=21)

    with caplog.at_level("WARNING", logger="ingestion.absence.detector"):
        recovered = detector.sweep_stuck_claims(fx)
    assert recovered == 1

    recovered_records = [r for r in caplog.records if "stuck claim recovered" in r.getMessage()]
    assert len(recovered_records) == 1
    message = recovered_records[0].getMessage()
    # feed_id/batch_id/status are the diagnostic minimum -- present.
    assert _S3_FEED_ID in message
    assert s3_batch_id in message
    assert "in_progress" in message
    # the OLD `%r` full-ClaimItem repr would have embedded these -- absent now.
    assert s3_sha256 not in message
    assert "statement.csv" not in message
    assert _S3_VESTIBULE_PREFIX not in message
    assert "ClaimItem(" not in message


# --- F-1 regression: trigger_key is role-based, not position-based -----------


def test_stuck_claim_sweep_trigger_key_is_role_based_not_position_based(
    local_effects: Effects,
    s3_client: Any,
    queue_url: str,
    invoke_log: list[tuple[str, dict[str, Any]]],
    clock_box: list[datetime],
) -> None:
    """F-1: `registrar._trigger_for` must select the manifest object's
    `src_key` by its `role` field, never by its position in
    `RegistrationRequest.objects` -- production always builds
    `[*data_staged, manifest_staged]` (manifest object LAST, `drivers/
    s3_push.py::_manifest_staged_objects`), so this test deliberately
    builds the claim's `objects_inventory` with the manifest object FIRST
    instead, proving the sweep still reconstructs the correct replay
    event (not the last data object's vestibule key) and the replay still
    resumes the dead run's identity end to end.
    """
    fx = local_effects
    _seed_registry(fx, s3_client, [_manifest_stuck_feed_config()])
    t_dead = fx.now()

    part1_content = b"policy_id,premium\nP1,100.00\n"
    part2_content = b"policy_id,premium\nP2,200.00\n"
    part1_sha256 = hashlib.sha256(part1_content).hexdigest()
    part2_sha256 = hashlib.sha256(part2_content).hexdigest()
    manifest_name = "manifest-2026-07-20.manifest.json"
    manifest_payload = {
        "manifest_version": 1,
        "manifest_id": "manifest-2026-07-20",
        "feed_id": _S3_MANIFEST_FEED_ID,
        "files": [
            {
                "name": "part1.csv",
                "bytes": len(part1_content),
                "sha256": part1_sha256,
                "record_count": 1,
            },
            {
                "name": "part2.csv",
                "bytes": len(part2_content),
                "sha256": part2_sha256,
                "record_count": 1,
            },
        ],
        "created_at": "2026-07-20T09:00:00Z",
    }
    manifest_bytes = json.dumps(manifest_payload).encode("utf-8")
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()

    s3_client.put_object(
        Bucket=fx.config.landing_bucket,
        Key=_S3_MANIFEST_VESTIBULE_PREFIX + "part1.csv",
        Body=part1_content,
    )
    s3_client.put_object(
        Bucket=fx.config.landing_bucket,
        Key=_S3_MANIFEST_VESTIBULE_PREFIX + "part2.csv",
        Body=part2_content,
    )
    s3_client.put_object(
        Bucket=fx.config.landing_bucket,
        Key=_S3_MANIFEST_VESTIBULE_PREFIX + manifest_name,
        Body=manifest_bytes,
    )

    delivery_id = "66666666-6666-4666-8666-666666666666"
    manifest_staged = StagedObject(
        name=manifest_name,
        role="manifest",
        uri=s3_push._canonical_uri(
            fx.config.landing_bucket, _S3_MANIFEST_FEED_ID, t_dead, delivery_id, manifest_name
        ),
        bytes=len(manifest_bytes),
        sha256=manifest_sha256,
        src_key=_S3_MANIFEST_VESTIBULE_PREFIX + manifest_name,
    )
    part1_staged = StagedObject(
        name="part1.csv",
        role="data",
        uri=s3_push._canonical_uri(
            fx.config.landing_bucket, _S3_MANIFEST_FEED_ID, t_dead, delivery_id, "part1.csv"
        ),
        bytes=len(part1_content),
        sha256=part1_sha256,
        src_key=_S3_MANIFEST_VESTIBULE_PREFIX + "part1.csv",
    )
    part2_staged = StagedObject(
        name="part2.csv",
        role="data",
        uri=s3_push._canonical_uri(
            fx.config.landing_bucket, _S3_MANIFEST_FEED_ID, t_dead, delivery_id, "part2.csv"
        ),
        bytes=len(part2_content),
        sha256=part2_sha256,
        src_key=_S3_MANIFEST_VESTIBULE_PREFIX + "part2.csv",
    )

    # REORDERED: the manifest object is FIRST -- opposite of
    # `_manifest_staged_objects`' `[*data_staged, manifest_staged]`
    # production order.
    dead_req = RegistrationRequest(
        feed=_manifest_stuck_feed_config(),
        delivery_id=delivery_id,
        delivery_key="manifest-2026-07-20",
        received_at=t_dead,
        driver="s3-push",
        driver_run_id="run-dead-manifest",
        completeness=CompletenessResult(
            verdict="complete",
            reason=None,
            asserted_record_count=2,
            data_object_names=("part1.csv", "part2.csv"),
        ),
        objects=[manifest_staged, part1_staged, part2_staged],
    )
    content_hash = canonical_content_hash(
        [("part1.csv", part1_sha256), ("part2.csv", part2_sha256)]
    )
    batch_id = mint_batch_id(_S3_MANIFEST_FEED_ID, content_hash)
    trigger = registrar._trigger_for(dead_req)
    assert trigger == {"trigger_key": _S3_MANIFEST_VESTIBULE_PREFIX + manifest_name}
    claim = fx.cas.claim(dead_req, batch_id, "run-dead-manifest", trigger, t_dead)
    assert claim.kind == "WON"

    clock_box[0] = t_dead + timedelta(minutes=21)

    recovered = detector.sweep_stuck_claims(fx)
    assert recovered == 1

    by_function = dict(invoke_log)
    registrar_payload = by_function["conveyer-test-registrar"]
    assert registrar_payload == {
        "detail": {
            "bucket": {"name": fx.config.landing_bucket},
            "object": {"key": _S3_MANIFEST_VESTIBULE_PREFIX + manifest_name},
        }
    }

    # --- close the loop: replay through the REAL driver and confirm it
    # resumes the dead run's identity -- would fail (misrouted to trailer
    # mode, or resolve to the wrong vestibule object) had the sweep picked
    # a data object's key instead of the manifest's.
    outcomes = s3_push.acquire(registrar_payload, fx, "run-resumer-manifest", registry_cache={})

    assert len(outcomes) == 1
    assert outcomes[0].disposition == "registered"
    assert outcomes[0].delivery_id == delivery_id  # resumes the DEAD run's identity

    rows = fx.ledger.scan_feed(_S3_MANIFEST_FEED_ID, None)
    registered = folds.registered_deliveries(rows)
    assert len(registered) == 1
    assert registered[0].delivery_id == delivery_id
    assert registered[0].driver_run_id == "run-dead-manifest"

    events = _drain_events(fx, queue_url)
    assert len(events) == 1
    assert events[0]["detail-type"] == "delivery-registered"
