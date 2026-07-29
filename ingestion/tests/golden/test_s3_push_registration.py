"""s3-push registration golden suite -- LLD §12.4 (G-01/03/04/05a/06/07/08),
§8.5 (G-11 crash recovery).

Drives the REAL handler (`ingestion.drivers.s3_push.acquire`, called with
synthetic S3->EventBridge "Object Created" events shaped per §10.7) against
`local_effects` (moto S3/DynamoDB/EventBridge + a `SqlCatalog` ledger --
`tests/conftest.py`). Fixture bytes live under
`sources/carrier-y/renewal-statements/tests/fixtures/` (§15.2: "happy
3-part delivery; duplicate-event replay; sha256-mismatch delivery" -- this
module additionally uses a "corrected" variant for G-06's supersession
scenario, not separately named in §15.2 but built from the same 3-part
shape with one file's content changed).

Per §8.3 ("G-03/04/06/11 first assert the plan (exact rows, copies, event),
then run the interpreter against the local stack to assert the world
agrees"): those four tests wrap `fx.cas` with an *observing* -- and, for
G-04, additionally *racing* -- `CasFx` built via `dataclasses.replace` (a
plain record of functions, per project convention; no mocking framework)
to capture the exact `RegistrationRequest`/`ClaimResult` the driver
computed, so the test can call `core.decisions.plan_registration` directly
and assert the resulting `RegistrationPlan` value BEFORE inspecting what
the real `execute()` (already run, inside `register_delivery`) left behind
in the world.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import pytest
from ingestion.core import decisions, folds
from ingestion.core.completeness import CompletenessResult, Defect
from ingestion.core.decisions import RegistrationRequest
from ingestion.core.hashing import canonical_content_hash
from ingestion.core.minting import mint_batch_id
from ingestion.core.model import (
    ClaimResult,
    Completeness,
    FeedConfig,
    ManifestV1,
    S3PushConnection,
    StagedObject,
)
from ingestion.core.naming import split_s3_uri
from ingestion.drivers import s3_push
from ingestion.effects.records import Effects
from ingestion.registration import registrar

_FEED_ID = "carrier-y/renewal-statements"
_VESTIBULE_PREFIX = f"{_FEED_ID}/incoming/"
_MANIFEST_NAME = "renewal-2026-07-24.manifest.json"
_FIXTURES_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "sources/carrier-y/renewal-statements/tests/fixtures"
)
_QUEUE = "conveyer-test-registration-capture"


def _feed_config() -> FeedConfig:
    return FeedConfig(
        feed_id=_FEED_ID,
        driver="s3-push",
        pipeline="pipelines/renewals",
        connection=S3PushConnection(
            partner_principal_arns=["arn:aws:iam::111111111111:role/carrier-y-uploader"]
        ),
        completeness=Completeness(mode="manifest"),
    )


_FEED = _feed_config()


# --- fixture loading / vestibule seeding ------------------------------------


def _fixture_bytes(name: str) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in (_FIXTURES_DIR / name).iterdir()}


def _upload_vestibule(
    s3_client: Any,
    landing_bucket: str,
    files: dict[str, bytes],
    *,
    skip: frozenset[str] = frozenset(),
) -> None:
    for name, content in files.items():
        if name in skip:
            continue
        s3_client.put_object(Bucket=landing_bucket, Key=_VESTIBULE_PREFIX + name, Body=content)


def _seed_registry(fx: Effects, s3_client: Any) -> None:
    bucket, key = split_s3_uri(fx.config.registry_uri)
    payload = {"registry_version": 1, "feeds": [json.loads(_FEED.model_dump_json())]}
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode("utf-8"))


def _s3_event(bucket: str, key: str) -> dict[str, Any]:
    """Synthetic S3 "Object Created" EventBridge notification (§10.7)."""
    return {
        "version": "0",
        "id": "evt-1",
        "detail-type": "Object Created",
        "source": "aws.s3",
        "account": "000000000000",
        "time": "2026-07-24T09:00:00Z",
        "region": "us-east-1",
        "resources": [f"arn:aws:s3:::{bucket}"],
        "detail": {
            "version": "0",
            "bucket": {"name": bucket},
            "object": {"key": key, "size": 1, "etag": "x", "sequencer": "y"},
            "request-id": "r1",
            "requester": "111111111111",
            "reason": "PutObject",
        },
    }


# --- event capture (SQS behind an EventBridge rule, per test_events_fx.py) -


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
        Name="capture-registered",
        EventBusName=local_effects.config.event_bus,
        EventPattern=json.dumps({"source": ["conveyer.ingestion"]}),
    )
    events_client.put_targets(
        Rule="capture-registered",
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


# --- s3 client + canonical-object assertions --------------------------------


@pytest.fixture
def s3_client(local_effects: Effects) -> Any:
    return boto3.client("s3", region_name=local_effects.config.aws_region)


def _assert_uris_exist(s3_client: Any, uris: list[str]) -> None:
    for uri in uris:
        bucket, key = split_s3_uri(uri)
        s3_client.head_object(Bucket=bucket, Key=key)  # raises if missing


# --- request construction mirroring the driver, for plan-level assertions --


def _build_request_from_fixture(
    landing_bucket: str,
    files: dict[str, bytes],
    manifest_name: str,
    *,
    received_at: datetime,
    delivery_id: str,
    driver_run_id: str,
) -> RegistrationRequest:
    """Builds the exact `RegistrationRequest` shape `s3_push._acquire_manifest`
    would build from `files` (a "vestibule contents" dict), without touching
    S3 -- used to construct a second, independently-simulated concurrent
    request (G-04) or a pre-seeded "dead run" request (G-11).
    """
    raw_manifest = files[manifest_name]
    manifest = ManifestV1.model_validate_json(raw_manifest)
    data_staged = [
        StagedObject(
            name=f.name,
            role="data",
            uri=s3_push._canonical_uri(landing_bucket, _FEED_ID, received_at, delivery_id, f.name),
            bytes=len(files[f.name]),
            sha256=hashlib.sha256(files[f.name]).hexdigest(),
            src_key=_VESTIBULE_PREFIX + f.name,
        )
        for f in manifest.files
    ]
    manifest_staged = StagedObject(
        name=manifest_name,
        role="manifest",
        uri=s3_push._canonical_uri(
            landing_bucket, _FEED_ID, received_at, delivery_id, manifest_name
        ),
        bytes=len(raw_manifest),
        sha256=hashlib.sha256(raw_manifest).hexdigest(),
        src_key=_VESTIBULE_PREFIX + manifest_name,
    )
    completeness = CompletenessResult(
        verdict="complete",
        reason=None,
        asserted_record_count=sum(f.record_count or 0 for f in manifest.files) or None,
        data_object_names=tuple(f.name for f in manifest.files),
    )
    return RegistrationRequest(
        feed=_FEED,
        delivery_id=delivery_id,
        delivery_key=manifest.manifest_id,
        received_at=received_at,
        driver="s3-push",
        driver_run_id=driver_run_id,
        completeness=completeness,
        objects=[*data_staged, manifest_staged],
    )


# --- observing / racing CasFx test doubles (§8.3's "first assert the plan") -


def _observing_cas(fx: Effects) -> tuple[Effects, list[tuple[Any, ...]]]:
    """Wrap `fx.cas.claim` to capture every call's full arguments + result,
    without changing behavior -- lets a test reconstruct+assert the exact
    `RegistrationPlan` a driver call produced, after the fact.
    """
    captured: list[tuple[Any, ...]] = []
    real_claim = fx.cas.claim

    def observing_claim(
        req: Any, batch_id: Any, run_id: Any, trigger: Any, now: Any
    ) -> ClaimResult:
        result = real_claim(req, batch_id, run_id, trigger, now)
        captured.append((req, batch_id, run_id, trigger, now, result))
        return result

    return dataclasses.replace(fx, cas=dataclasses.replace(fx.cas, claim=observing_claim)), captured


def _racing_cas(
    fx: Effects, req_b: RegistrationRequest, run_b: str, trigger_b: dict[str, Any]
) -> tuple[Effects, list[tuple[Any, ...]]]:
    """Like `_observing_cas`, but on the FIRST call (run A's real claim,
    genuinely WON) it ALSO immediately performs a second, independent claim
    for `req_b` against the SAME `batch_id` -- before run A's caller
    (`register_delivery`) has any chance to call `complete()` -- reproducing
    true interleaving (G-04: both claims observe the SAME prior state) with
    the REAL `CasFx`, not a fake state machine.
    """
    captured: list[tuple[Any, ...]] = []
    real_claim = fx.cas.claim

    def racing_claim(req: Any, batch_id: Any, run_id: Any, trigger: Any, now: Any) -> ClaimResult:
        result = real_claim(req, batch_id, run_id, trigger, now)
        if not captured:
            claim_b = real_claim(req_b, batch_id, run_b, trigger_b, now)
            captured.append((req_b, batch_id, run_b, trigger_b, now, claim_b))
        return result

    return dataclasses.replace(fx, cas=dataclasses.replace(fx.cas, claim=racing_claim)), captured


# --- G-01: happy manifest (3 parts + manifest), §15.2 -----------------------


def test_g01_happy_manifest_registers_and_copies_canonical_objects(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    files = _fixture_bytes("happy")
    _upload_vestibule(s3_client, fx.config.landing_bucket, files)
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    outcomes = s3_push.acquire(event, fx, "run-g01", registry_cache={})

    assert len(outcomes) == 1
    assert outcomes[0].disposition == "registered"

    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1
    row = rows[0]
    assert row.disposition == "registered"
    assert len(row.objects) == 4  # 3 data parts + the manifest itself
    assert len(row.object_uris) == 3
    _assert_uris_exist(s3_client, row.object_uris)
    _assert_uris_exist(s3_client, [o.uri for o in row.objects if o.uri is not None])

    events = _drain_events(fx, queue_url)
    assert len(events) == 1
    assert events[0]["detail-type"] == "delivery-registered"
    assert events[0]["detail"]["batch_id"] == row.batch_id


# --- M-2 (security-gate): a forged event with a foreign bucket registers ---
# nothing -- the registrar previously trusted the event-supplied bucket name
# for both the source read AND the destination bucket in every canonical URI.


_FOREIGN_BUCKET = "attacker-controlled-bucket"


def test_m2_foreign_bucket_event_registers_nothing(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    files = _fixture_bytes("happy")
    _upload_vestibule(s3_client, fx.config.landing_bucket, files)
    # The attacker's bucket is REAL and holds the SAME vestibule content --
    # not just a nonexistent-bucket typo -- so the old vulnerable code path
    # (bucket used for both the source read AND the canonical-URI/copy
    # destination) would have actually SUCCEEDED, registering a delivery
    # whose `object_uris` point into the attacker's bucket, not the fix
    # merely erroring out on a missing bucket.
    s3_client.create_bucket(Bucket=_FOREIGN_BUCKET)
    _upload_vestibule(s3_client, _FOREIGN_BUCKET, files)
    forged_event = _s3_event(_FOREIGN_BUCKET, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    outcomes = s3_push.acquire(forged_event, fx, "run-m2", registry_cache={})

    assert outcomes == []  # rejected before any registration attempt
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert rows == []
    assert _drain_events(fx, queue_url) == []
    # nothing was ever copied into the attacker's bucket either.
    listing = s3_client.list_objects_v2(Bucket=_FOREIGN_BUCKET, Prefix=f"{_FEED_ID}/received_at=")
    assert "Contents" not in listing


# --- H-1 (security-gate): TOCTOU -- a vestibule object mutated between hash --
# (T0) and copy (T1) must not silently land in the canonical copy. The landing
# bucket is versioned (LLD S10.1, `tests/conftest.py::local_effects`), so
# `stream_sha256`'s captured VersionId is what `copy_verbatim` pins to.


def _hash_then_mutate_store(
    fx: Effects, s3_client: Any, mutate_key: str, mutated_content: bytes
) -> Effects:
    """Wraps `fx.store.stream_sha256` so that immediately AFTER the REAL hash
    completes for `mutate_key` (T0), it also overwrites that SAME vestibule
    object with `mutated_content` -- reproducing the partner's retained
    `PutObject` on `incoming/*` landing between hash-time and the later
    `copy_verbatim` call (T1). The hash result itself (including the
    captured `version_id`/`etag`) is returned UNCHANGED -- exactly what the
    real driver code sees, race and all.
    """
    real_stream_sha256 = fx.store.stream_sha256

    def wrapped(bucket: str, key: str) -> tuple[str, int, str | None, str | None]:
        result = real_stream_sha256(bucket, key)
        if key == mutate_key:
            s3_client.put_object(Bucket=bucket, Key=key, Body=mutated_content)
        return result

    return dataclasses.replace(fx, store=dataclasses.replace(fx.store, stream_sha256=wrapped))


def test_h1_vestibule_object_mutated_between_hash_and_copy_registers_hashed_bytes(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    files = _fixture_bytes("happy")
    _upload_vestibule(s3_client, fx.config.landing_bucket, files)
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    original_part1 = files["part1.csv"]
    mutated_part1 = b"MUTATED-BYTES-SWAPPED-IN-BY-A-RETAINED-PUTOBJECT-AFTER-HASHING"
    assert mutated_part1 != original_part1
    fx_racy = _hash_then_mutate_store(fx, s3_client, _VESTIBULE_PREFIX + "part1.csv", mutated_part1)

    outcomes = s3_push.acquire(event, fx_racy, "run-h1", registry_cache={})

    assert len(outcomes) == 1
    assert outcomes[0].disposition == "registered"  # evaluate_manifest saw the HASHED bytes

    rows = fx.ledger.scan_feed(_FEED_ID, None)
    registered = [r for r in rows if r.disposition == "registered"]
    assert len(registered) == 1
    part1_object = next(o for o in registered[0].objects if o.name == "part1.csv")
    bucket, key = split_s3_uri(part1_object.uri)
    canonical_bytes = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()

    # The registered canonical object matches the HASHED bytes, not the
    # bytes that landed in the vestibule between hash-time and copy-time.
    assert canonical_bytes == original_part1
    assert canonical_bytes != mutated_part1


# --- H-1 RESIDUAL (security-gate): TOCTOU on the MANIFEST object's own copy -
# same window as above, but on the manifest itself: it is read via
# `get_bytes_pinned` (not `stream_sha256`), parsed, and its completeness
# assertions verified BEFORE the vestibule->canonical copy runs. A partner
# who swaps the manifest object between the read (T0) and the copy (T1) must
# not have the swapped bytes silently become the canonical, source-of-truth
# manifest.


def _read_then_mutate_manifest_store(
    fx: Effects, s3_client: Any, mutate_key: str, mutated_content: bytes
) -> Effects:
    """Wraps `fx.store.get_bytes_pinned` so that immediately AFTER the REAL
    read completes for `mutate_key` (T0), it also overwrites that SAME
    vestibule object with `mutated_content` -- reproducing a partner's
    retained `PutObject` on `incoming/*` landing between the manifest read
    and the later `copy_verbatim` call (T1). The read result itself
    (including the captured `version_id`/`etag`) is returned UNCHANGED --
    exactly what the real driver code sees, race and all.
    """
    real_get_bytes_pinned = fx.store.get_bytes_pinned

    def wrapped(
        bucket: str, key: str, max_bytes: int
    ) -> tuple[bytes, str | None, str | None] | Defect:
        result = real_get_bytes_pinned(bucket, key, max_bytes)
        if key == mutate_key:
            s3_client.put_object(Bucket=bucket, Key=key, Body=mutated_content)
        return result

    return dataclasses.replace(fx, store=dataclasses.replace(fx.store, get_bytes_pinned=wrapped))


def test_h1_residual_manifest_object_mutated_between_read_and_copy_registers_parsed_bytes(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    files = _fixture_bytes("happy")
    _upload_vestibule(s3_client, fx.config.landing_bucket, files)
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    original_manifest = files[_MANIFEST_NAME]
    mutated_manifest = (
        b'{"manifest_version": 1, "manifest_id": "SWAPPED-IN-MANIFEST-NEVER-VERIFIED", '
        b'"feed_id": "carrier-y/renewal-statements", "files": []}'
    )
    assert mutated_manifest != original_manifest
    fx_racy = _read_then_mutate_manifest_store(
        fx, s3_client, _VESTIBULE_PREFIX + _MANIFEST_NAME, mutated_manifest
    )

    outcomes = s3_push.acquire(event, fx_racy, "run-h1-manifest", registry_cache={})

    assert len(outcomes) == 1
    assert outcomes[0].disposition == "registered"  # evaluate_manifest saw the PARSED bytes

    rows = fx.ledger.scan_feed(_FEED_ID, None)
    registered = [r for r in rows if r.disposition == "registered"]
    assert len(registered) == 1
    manifest_object = next(o for o in registered[0].objects if o.role == "manifest")
    assert manifest_object.name == _MANIFEST_NAME
    bucket, key = split_s3_uri(manifest_object.uri)
    canonical_bytes = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()

    # The registered canonical MANIFEST matches the PARSED bytes (the ones
    # `evaluate_manifest` actually verified), not the bytes that landed in
    # the vestibule between read-time and copy-time.
    assert canonical_bytes == original_manifest
    assert canonical_bytes != mutated_manifest


# --- G-03: duplicate S3 event, same manifest, twice, sequential ------------


def test_g03_duplicate_sequential_event_yields_registered_and_duplicate(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    files = _fixture_bytes("happy")
    _upload_vestibule(s3_client, fx.config.landing_bucket, files)
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    fx_obs, captured = _observing_cas(fx)
    prior_before_second = fx.ledger.scan_feed(_FEED_ID, None)

    outcome1 = s3_push.acquire(event, fx_obs, "run-1", registry_cache={})[0]
    outcome2 = s3_push.acquire(event, fx_obs, "run-2", registry_cache={})[0]

    assert outcome1.disposition == "registered"
    assert outcome2.disposition == "duplicate"

    # §8.3: assert the SECOND call's plan first (its claim lost to a
    # COMPLETED claim, since this run is single-threaded and sequential).
    req2, batch_id2, run_id2, _trigger2, now2, claim2 = captured[-1]
    assert claim2.kind == "LOST_COMPLETED"
    plan2 = decisions.plan_registration(claim2, prior_before_second, req2, now2)
    assert [r.disposition for r in plan2.rows] == ["duplicate"]
    assert plan2.copies == ()
    assert plan2.event is None
    assert plan2.complete_claim is None

    # Then assert the world agrees.
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert sorted(r.disposition for r in rows) == ["duplicate", "registered"]
    events = _drain_events(fx, queue_url)
    assert len(events) == 1


# --- G-04: concurrent race via an injected/racing CasFx --------------------


def test_g04_concurrent_race_yields_one_won_one_lost_in_progress(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    files = _fixture_bytes("happy")
    _upload_vestibule(s3_client, fx.config.landing_bucket, files)
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    received_at_b = fx.now()
    req_b = _build_request_from_fixture(
        fx.config.landing_bucket,
        files,
        _MANIFEST_NAME,
        received_at=received_at_b,
        delivery_id="99999999-9999-4999-8999-999999999999",
        driver_run_id="run-b",
    )
    trigger_b = registrar._trigger_for(req_b)

    fx_racing, captured = _racing_cas(fx, req_b, "run-b", trigger_b)
    prior = fx.ledger.scan_feed(_FEED_ID, None)

    outcome_a = s3_push.acquire(event, fx_racing, "run-a", registry_cache={})[0]
    assert outcome_a.disposition == "registered"

    req_b_captured, batch_id_b, run_b, _trigger, now_b, claim_b = captured[0]
    assert claim_b.kind == "LOST_IN_PROGRESS"

    # §8.3: assert run B's plan before running its interpreter.
    plan_b = decisions.plan_registration(claim_b, prior, req_b_captured, now_b)
    assert [r.disposition for r in plan_b.rows] == ["duplicate"]
    assert plan_b.copies == ()
    assert plan_b.event is None
    assert plan_b.complete_claim is None

    outcome_b = registrar.execute(plan_b, fx, "run-b")
    assert outcome_b.disposition == "duplicate"

    # Both sides remembered; exactly 1 event (from the WON side only).
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert sorted(r.disposition for r in rows) == ["duplicate", "registered"]
    events = _drain_events(fx, queue_url)
    assert len(events) == 1


# --- G-05a: partner re-sends an identical file -- duplicate, no NEW event --


def test_g05a_identical_resend_produces_duplicate_row_and_no_new_event(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    files = _fixture_bytes("happy")
    _upload_vestibule(s3_client, fx.config.landing_bucket, files)
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    # Original delivery -- registers, emits its own event (drained below so
    # the assertion isolates what THIS test's resend call itself produces).
    first = s3_push.acquire(event, fx, "run-original", registry_cache={})[0]
    assert first.disposition == "registered"
    _drain_events(fx, queue_url)

    resend = s3_push.acquire(event, fx, "run-resend", registry_cache={})[0]

    assert resend.disposition == "duplicate"
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert sorted(r.disposition for r in rows) == ["duplicate", "registered"]
    assert _drain_events(fx, queue_url) == []


# --- G-06: corrected re-send -- supersession --------------------------------


def test_g06_corrected_resend_supersedes_prior_and_emits_new_event(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    happy_files = _fixture_bytes("happy")
    _upload_vestibule(s3_client, fx.config.landing_bucket, happy_files)
    first_event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    first = s3_push.acquire(first_event, fx, "run-original", registry_cache={})[0]
    assert first.disposition == "registered"
    _drain_events(fx, queue_url)

    # Corrected re-send: SAME manifest_id/delivery_key, different content.
    corrected_files = _fixture_bytes("corrected")
    _upload_vestibule(s3_client, fx.config.landing_bucket, corrected_files)
    corrected_event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    fx_obs, captured = _observing_cas(fx)
    prior_before_correction = fx.ledger.scan_feed(_FEED_ID, None)

    second = s3_push.acquire(corrected_event, fx_obs, "run-corrected", registry_cache={})[0]
    assert second.disposition == "registered"
    assert second.delivery_id != first.delivery_id

    req2, _batch_id2, _run_id2, _trigger2, now2, claim2 = captured[-1]
    assert claim2.kind == "WON"
    plan2 = decisions.plan_registration(claim2, prior_before_correction, req2, now2)
    assert [r.disposition for r in plan2.rows] == ["registered", "superseded"]
    assert plan2.rows[0].supersedes == first.delivery_id
    assert plan2.rows[1].delivery_id == first.delivery_id
    assert plan2.event is not None

    rows = fx.ledger.scan_feed(_FEED_ID, None)
    by_disposition = sorted(r.disposition for r in rows)
    assert by_disposition == ["registered", "registered", "superseded"]
    events = _drain_events(fx, queue_url)
    assert len(events) == 1
    assert events[0]["detail"]["delivery_id"] == second.delivery_id


# --- G-07: manifest lists a missing part ------------------------------------


def test_g07_missing_part_yields_incomplete_no_copy_no_event(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    files = _fixture_bytes("happy")
    _upload_vestibule(s3_client, fx.config.landing_bucket, files, skip=frozenset({"part3.csv"}))
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    outcome = s3_push.acquire(event, fx, "run-g07", registry_cache={})[0]

    assert outcome.disposition == "incomplete"
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1
    assert rows[0].disposition == "incomplete"
    assert rows[0].batch_id is None
    assert rows[0].object_uris == []

    # no canonical copy landed anywhere under this feed's prefix
    listing = s3_client.list_objects_v2(
        Bucket=fx.config.landing_bucket, Prefix=f"{_FEED_ID}/received_at="
    )
    assert "Contents" not in listing

    assert _drain_events(fx, queue_url) == []


# --- G-08: manifest sha256 mismatch -----------------------------------------


def test_g08_sha256_mismatch_yields_unreadable_no_event(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    files = _fixture_bytes("sha256_mismatch")
    _upload_vestibule(s3_client, fx.config.landing_bucket, files)
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    outcome = s3_push.acquire(event, fx, "run-g08", registry_cache={})[0]

    assert outcome.disposition == "unreadable"
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1
    assert rows[0].disposition == "unreadable"
    assert "sha256 mismatch" in (rows[0].notes or "")

    listing = s3_client.list_objects_v2(
        Bucket=fx.config.landing_bucket, Prefix=f"{_FEED_ID}/received_at="
    )
    assert "Contents" not in listing
    assert _drain_events(fx, queue_url) == []


# --- H-4 (security-gate): schema-invalid manifest never leaks partner content
# into the ledger row's `notes` -- `notes` is append-only and Athena-queryable
# forever (LLD §11.4), so a PII-shaped value inside an untrusted manifest that
# fails `ManifestV1` validation must never reach it.

_PII_MARKER = "ssn-078-05-1120-jane.doe@example.com"


def test_h4_schema_invalid_manifest_never_leaks_pii_into_ledger_notes(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    # `files.0.bytes` must be an int (ManifestV1) -- feeding it a PII-shaped
    # string is the exact leak vector: pydantic's default `str(exc)`
    # rendering embeds `input_value=<the offending string>` verbatim.
    bad_manifest = json.dumps(
        {
            "manifest_version": 1,
            "manifest_id": "bad-manifest",
            "feed_id": _FEED_ID,
            "files": [{"name": "part1.csv", "bytes": _PII_MARKER, "sha256": "a" * 64}],
        }
    ).encode("utf-8")
    s3_client.put_object(
        Bucket=fx.config.landing_bucket, Key=_VESTIBULE_PREFIX + _MANIFEST_NAME, Body=bad_manifest
    )
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    outcome = s3_push.acquire(event, fx, "run-h4", registry_cache={})[0]

    assert outcome.disposition == "unreadable"
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1
    assert rows[0].disposition == "unreadable"
    assert _PII_MARKER not in (rows[0].notes or "")
    assert "files.0.bytes" in (rows[0].notes or "")  # still diagnostic

    assert _drain_events(fx, queue_url) == []


# --- conveyer-nvh.48 (security-gate): a manifest declaring a path-y --------
# `files[0].name` must be rejected as `unreadable` -- `ManifestFile`'s own
# `is_clean_object_name` field validator (nvh.46's producer-side companion)
# fires inside `parse_manifest`, before `evaluate_manifest`/any vestibule
# listing or copy ever runs. Documented, accepted behavior change: this
# scenario previously flipped `incomplete` (a coincidental vestibule-
# basename mismatch, since no vestibule object is literally named
# "../../incoming/attacker.csv") -- it is now `unreadable` (the manifest
# itself is defective), judged more correct and now uniform with sftp-pull.


def test_manifest_declared_traversal_name_rejected_and_leaks_nothing(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    hostile_name = "../../incoming/attacker.csv"
    bad_manifest = json.dumps(
        {
            "manifest_version": 1,
            "manifest_id": "hostile-manifest",
            "feed_id": _FEED_ID,
            "files": [{"name": hostile_name, "bytes": 1, "sha256": "a" * 64}],
        }
    ).encode("utf-8")
    s3_client.put_object(
        Bucket=fx.config.landing_bucket, Key=_VESTIBULE_PREFIX + _MANIFEST_NAME, Body=bad_manifest
    )
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    outcome = s3_push.acquire(event, fx, "run-traversal", registry_cache={})[0]

    assert outcome.disposition == "unreadable"
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1
    assert rows[0].disposition == "unreadable"
    assert "files.0.name" in (rows[0].notes or "")
    assert "attacker" not in (rows[0].notes or "")
    assert hostile_name not in (rows[0].notes or "")

    # No canonical-target StagedObject was ever composed/copied: nothing
    # landed under this feed's canonical prefix at all.
    listing = s3_client.list_objects_v2(
        Bucket=fx.config.landing_bucket, Prefix=f"{_FEED_ID}/received_at="
    )
    assert "Contents" not in listing
    assert _drain_events(fx, queue_url) == []


# --- conveyer-nvh.48.11 (security-gate): event key basename fails ----------
# `is_clean_object_name` (e.g. `incoming/..`) -- misroute-guard style (log
# ERROR + return []), AFTER the feed lookup succeeds. Distinct from the
# `feed_id is None`/foreign-bucket misroute guards above: the key parses to
# a REAL, registered feed_id here, so only the basename itself is hostile.
# This is event-shaped noise, not a partner delivery -- no
# `record_nondelivery`, no ledger row.


def test_traversal_basename_event_key_returns_empty_and_no_ledger_append(
    local_effects: Effects, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + "..")

    outcomes = s3_push.acquire(event, fx, "run-traversal-basename", registry_cache={})

    assert outcomes == []
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert rows == []
    assert _drain_events(fx, queue_url) == []


# --- G-11: crash after claim -- TAKEN_OVER resumes the dead run's identity -


def test_g11_crash_after_claim_resumes_dead_runs_identity(
    local_effects: Effects, s3_client: Any, queue_url: str, clock_box: list[datetime]
) -> None:
    fx = local_effects
    _seed_registry(fx, s3_client)
    files = _fixture_bytes("happy")
    _upload_vestibule(s3_client, fx.config.landing_bucket, files)

    # Pre-seed a stale in_progress claim -- as if a driver invocation claimed
    # this exact content and then crashed before completing registration.
    t_dead = fx.now()
    dead_delivery_id = "77777777-7777-4777-8777-777777777777"
    dead_req = _build_request_from_fixture(
        fx.config.landing_bucket,
        files,
        _MANIFEST_NAME,
        received_at=t_dead,
        delivery_id=dead_delivery_id,
        driver_run_id="run-dead",
    )
    data_objects = [o for o in dead_req.objects if o.role == "data"]
    content_hash = canonical_content_hash([(o.name, o.sha256) for o in data_objects])
    dead_batch_id = mint_batch_id(_FEED_ID, content_hash)
    pre_seed_claim = fx.cas.claim(dead_req, dead_batch_id, "run-dead", {}, t_dead)
    assert pre_seed_claim.kind == "WON"

    # Advance past the 1200 s staleness threshold, then re-invoke the
    # handler -- a fresh driver invocation, own delivery_id, SAME vestibule
    # content -> same content_hash -> same batch_id -> hits the stale claim.
    clock_box[0] = t_dead + timedelta(minutes=21)

    fx_obs, captured = _observing_cas(fx)
    prior_before_resume = fx.ledger.scan_feed(_FEED_ID, None)
    event = _s3_event(fx.config.landing_bucket, _VESTIBULE_PREFIX + _MANIFEST_NAME)

    outcome = s3_push.acquire(event, fx_obs, "run-resumer", registry_cache={})[0]

    assert outcome.disposition == "registered"
    assert outcome.delivery_id == dead_delivery_id  # resumes the DEAD run's identity

    req_resume, _batch_id, _run_id, _trigger, now_resume, claim_resume = captured[-1]
    assert claim_resume.kind == "TAKEN_OVER"
    assert claim_resume.item is not None
    assert claim_resume.item.delivery_id == dead_delivery_id

    plan = decisions.plan_registration(claim_resume, prior_before_resume, req_resume, now_resume)
    assert [r.disposition for r in plan.rows] == ["registered"]
    assert plan.rows[0].delivery_id == dead_delivery_id
    assert len(plan.copies) == 4  # 3 data objects + the manifest
    assert plan.event is not None
    assert plan.complete_claim == (_FEED_ID, dead_batch_id)

    rows = fx.ledger.scan_feed(_FEED_ID, None)
    registered = folds.registered_deliveries(rows)
    assert len(registered) == 1
    assert registered[0].delivery_id == dead_delivery_id
    _assert_uris_exist(s3_client, registered[0].object_uris)

    events = _drain_events(fx, queue_url)
    assert len(events) == 1
