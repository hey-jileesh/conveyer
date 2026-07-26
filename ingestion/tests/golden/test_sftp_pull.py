"""sftp-pull golden suite -- LLD §12.4 (G-02/05b/05c/06-sftp/09/10/13), §9.2.

Drives the REAL driver (`ingestion.drivers.sftp_pull.acquire`) against
`local_effects` (moto S3/DynamoDB/EventBridge + a `SqlCatalog` ledger) and
the in-memory `sftp_store`/`sftp_fx_for` double from `tests/conftest.py`
(§12.1/§12.5) -- no mocking framework, ever (§12.2 IDIOM rule). Fixture
bytes for the manifest-mode scenarios live under
`sources/carrier-x/commission-statements/tests/fixtures/` (§15.1: "a 2-part
delivery + native manifest; a corrected re-send pair; an incomplete
manifest"). Trailer/timer-mode scenarios build their own single-file
content inline (no dedicated fixture directory needed for those simpler
shapes, matching the s3-push golden suite's own precedent of building
event/request payloads inline).

Two feed variants are used across the seven required golden IDs, since a
`FeedConfig` has exactly one `completeness.mode` and the golden IDs
individually require trailer, timer, and manifest behavior:

* G-02, G-05b, G-05c, G-09, G-10 -- trailer mode (single-file deliveries;
  the simplest shape to exercise selection/idempotency/window-filter
  behavior).
* G-06 (sftp variant) -- manifest mode, using the carrier-x fixtures above,
  matching the same shape M3's G-06 used for s3-push.
* G-13 -- timer mode (the only mode with a quiet-window filter).

A bonus (non-required) test exercises the carrier-x "incomplete manifest"
fixture, covering this driver's own new pre-stream missing/mismatch check
(§9.2: "parts missing/size-mismatched in listing" -- short-circuits before
any bytes move, unlike s3-push's post-stream `evaluate_manifest` check) --
not otherwise reachable by the six required golden IDs.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import pytest
from ingestion.core import folds
from ingestion.core.completeness import CompletenessResult
from ingestion.core.decisions import RegistrationRequest
from ingestion.core.hashing import canonical_content_hash
from ingestion.core.minting import mint_batch_id
from ingestion.core.model import (
    Completeness,
    FeedConfig,
    SftpConnection,
    StagedObject,
    TimerSpec,
    TrailerSpec,
    Trigger,
    Window,
)
from ingestion.drivers import sftp_pull
from ingestion.effects.records import Effects
from tests.conftest import SftpStore

_FEED_ID = "carrier-x/commission-statements"
_SECRET_REF = (
    "arn:aws:secretsmanager:us-east-1:000000000000:secret:"
    "conveyer-dev/sftp/carrier-x/commission-statements"
)
_REMOTE_PATH = "/outbound/commissions/"
_FIXTURES_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "sources/carrier-x/commission-statements/tests/fixtures"
)
_QUEUE = "conveyer-test-sftp-pull-capture"


# --- feed builders -----------------------------------------------------------


def _sftp_connection(file_pattern: str = "COMM_*") -> SftpConnection:
    return SftpConnection(
        secret_ref=_SECRET_REF, remote_path=_REMOTE_PATH, file_pattern=file_pattern
    )


def _trigger() -> Trigger:
    return Trigger(schedule="cron(0 13 ? * MON-FRI *)", timezone="America/New_York")


def _trailer_feed() -> FeedConfig:
    return FeedConfig(
        feed_id=_FEED_ID,
        driver="sftp-pull",
        pipeline="pipelines/commissions",
        connection=_sftp_connection(),
        trigger=_trigger(),
        completeness=Completeness(mode="trailer", trailer=TrailerSpec(pattern=r"TOTAL:\d+")),
    )


def _timer_feed(quiet_minutes: int = 30) -> FeedConfig:
    return FeedConfig(
        feed_id=_FEED_ID,
        driver="sftp-pull",
        pipeline="pipelines/commissions",
        connection=_sftp_connection(),
        trigger=_trigger(),
        completeness=Completeness(
            mode="timer",
            timer=TimerSpec(
                quiet_window_minutes=quiet_minutes,
                accepted_risk="Accepted: a file may be observed mid-write within the quiet window.",
            ),
        ),
    )


def _manifest_feed() -> FeedConfig:
    return FeedConfig(
        feed_id=_FEED_ID,
        driver="sftp-pull",
        pipeline="pipelines/commissions",
        connection=_sftp_connection(),
        trigger=_trigger(),
        completeness=Completeness(mode="manifest"),
    )


# --- sftp_store seeding ------------------------------------------------------


def _seed_remote(sftp_store: SftpStore, name: str, content: bytes, mtime: datetime) -> None:
    files = sftp_store.setdefault(_SECRET_REF, {})
    files[_REMOTE_PATH + name] = (content, mtime)


def _fixture_bytes(name: str) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in (_FIXTURES_DIR / name).iterdir()}


def _seed_fixture_set(sftp_store: SftpStore, fixture_name: str, mtime: datetime) -> None:
    for name, content in _fixture_bytes(fixture_name).items():
        _seed_remote(sftp_store, name, content, mtime)


# --- event capture (SQS behind an EventBridge rule, per test_events_fx.py) --


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
        Name="capture-sftp-registered",
        EventBusName=local_effects.config.event_bus,
        EventPattern=json.dumps({"source": ["conveyer.ingestion"]}),
    )
    events_client.put_targets(
        Rule="capture-sftp-registered",
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


def _assert_uris_exist(s3_client: Any, uris: list[str]) -> None:
    for uri in uris:
        bucket, key = sftp_pull._split_s3_uri(uri)
        s3_client.head_object(Bucket=bucket, Key=key)  # raises if missing


# --- G-02: happy path, sftp trailer file -------------------------------------


def test_g02_happy_trailer_file_registers(
    local_effects: Effects, sftp_store: SftpStore, s3_client: Any, queue_url: str
) -> None:
    fx = local_effects
    feed = _trailer_feed()
    content = b"policy_id,premium\nP1,100.00\nTOTAL:1\n"
    _seed_remote(sftp_store, "COMM_2026-07-24.csv", content, fx.now())

    outcomes = sftp_pull.acquire(feed, Window(None, None), fx, "run-g02")

    assert len(outcomes) == 1
    assert outcomes[0].disposition == "registered"

    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1
    row = rows[0]
    assert row.disposition == "registered"
    assert row.driver == "sftp-pull"
    assert len(row.objects) == 1
    _assert_uris_exist(s3_client, row.object_uris)

    events = _drain_events(fx, queue_url)
    assert len(events) == 1
    assert events[0]["detail-type"] == "delivery-registered"
    assert events[0]["detail"]["batch_id"] == row.batch_id


# --- G-05b: identical file re-listed, scheduled path -- zero new rows -------


def test_g05b_identical_file_relisted_on_scheduled_path_is_a_noop(
    local_effects: Effects, sftp_store: SftpStore
) -> None:
    fx = local_effects
    feed = _trailer_feed()
    content = b"policy_id,premium\nP1,100.00\nTOTAL:1\n"
    _seed_remote(sftp_store, "COMM_2026-07-24.csv", content, fx.now())

    first = sftp_pull.acquire(feed, Window(None, None), fx, "run-1")
    assert len(first) == 1
    assert first[0].disposition == "registered"

    # Scheduled re-run: same file, same bytes, no window, no force.
    second = sftp_pull.acquire(feed, Window(None, None), fx, "run-2")

    assert second == []  # selection itself is a no-op (D-16) -- zero rows, not even duplicate
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1


# --- G-05c: force re-pull over acquired window -- duplicate rows ------------


def test_g05c_force_repull_over_acquired_window_yields_duplicate_rows(
    local_effects: Effects, sftp_store: SftpStore, queue_url: str
) -> None:
    fx = local_effects
    feed = _trailer_feed()
    content = b"policy_id,premium\nP1,100.00\nTOTAL:1\n"
    _seed_remote(sftp_store, "COMM_2026-07-24.csv", content, fx.now())

    first = sftp_pull.acquire(feed, Window(None, None), fx, "run-1")
    assert first[0].disposition == "registered"
    _drain_events(fx, queue_url)  # isolate this test's own resend

    window = Window(start=fx.now() - timedelta(minutes=1), end=fx.now() + timedelta(minutes=1))
    second = sftp_pull.acquire(feed, window, fx, "run-2", force=True)

    assert len(second) == 1
    assert second[0].disposition == "duplicate"
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert sorted(r.disposition for r in rows) == ["duplicate", "registered"]
    assert _drain_events(fx, queue_url) == []  # no new event on a duplicate


# --- G-06 (sftp variant): corrected re-send -- supersession -----------------


def test_g06_sftp_corrected_resend_supersedes_prior_and_emits_new_event(
    local_effects: Effects, sftp_store: SftpStore, queue_url: str, clock_box: list[datetime]
) -> None:
    fx = local_effects
    feed = _manifest_feed()
    _seed_fixture_set(sftp_store, "happy", fx.now())

    first = sftp_pull.acquire(feed, Window(None, None), fx, "run-1")
    assert len(first) == 1
    assert first[0].disposition == "registered"
    _drain_events(fx, queue_url)

    # Corrected re-send: SAME manifest_id/delivery_key, different content,
    # under a NEW manifest filename (a genuinely new remote file -- the
    # partner does not overwrite an already-final manifest in place).
    clock_box[0] = clock_box[0] + timedelta(minutes=5)
    _seed_fixture_set(sftp_store, "corrected", fx.now())

    second = sftp_pull.acquire(feed, Window(None, None), fx, "run-2")

    assert len(second) == 1
    assert second[0].disposition == "registered"
    assert second[0].delivery_id != first[0].delivery_id

    rows = fx.ledger.scan_feed(_FEED_ID, None)
    by_key = [r for r in rows if r.delivery_key == "comm-2026-07-24"]
    assert sorted(r.disposition for r in by_key) == ["registered", "registered", "superseded"]
    superseded = next(r for r in by_key if r.disposition == "superseded")
    assert superseded.delivery_id == first[0].delivery_id
    new_delivery_id = second[0].delivery_id
    registered_new = next(
        r for r in by_key if r.disposition == "registered" and r.delivery_id == new_delivery_id
    )
    assert registered_new.supersedes == first[0].delivery_id

    events = _drain_events(fx, queue_url)
    assert len(events) == 1
    assert events[0]["detail"]["delivery_id"] == second[0].delivery_id


# --- G-09: trailer absent (truncated file) -----------------------------------


def test_g09_trailer_absent_yields_incomplete_no_event_and_not_restreamed(
    local_effects: Effects, sftp_store: SftpStore, queue_url: str
) -> None:
    """Truncated remote file (no `TOTAL:\\d+` tail line at all) -- unit-level
    behavior for `evaluate_trailer`'s empty-tail branch is already covered
    by `test_completeness.py::test_evaluate_trailer_incomplete_on_empty_tail`;
    this pins the end-to-end driver/ledger/event shape (§6.2 population
    table) plus the cross-run `observed_defective` fold behavior (§7.4), not
    otherwise reachable from the unit test.

    Unlike manifest mode's pre-stream check (G-07), trailer mode always
    streams the candidate to its canonical key BEFORE evaluating
    completeness (`_acquire_trailer_or_timer_candidate` reads the tail via
    `fx.store.get_tail` on the just-uploaded object) -- so this deliberately
    does NOT assert "no canonical copy" the way the manifest `incomplete`
    tests do; the ledger row simply never references it (`object_uris ==
    []`), an accepted orphan per the driver's own ordering note.
    """
    fx = local_effects
    feed = _trailer_feed()
    content = b"policy_id,premium\nP1,100.00\n"  # truncated -- no trailer line
    _seed_remote(sftp_store, "COMM_2026-07-24.csv", content, fx.now())

    outcomes = sftp_pull.acquire(feed, Window(None, None), fx, "run-g09")

    assert len(outcomes) == 1
    assert outcomes[0].disposition == "incomplete"

    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1
    row = rows[0]
    assert row.disposition == "incomplete"
    # §6.2 population table: `incomplete` -- never fully hashed, so the
    # identity columns are null.
    assert row.batch_id is None
    assert row.content_hash is None
    assert row.size_bytes is None
    assert row.notes == "trailer missing or malformed"
    assert row.object_uris == []

    assert _drain_events(fx, queue_url) == []

    # Next scheduled tick, identical (name, bytes): `folds.observed_defective`
    # keeps a defective trailer/timer candidate out of `select_candidates`
    # (§7.4) -- no re-stream, no new outcome, no new ledger row. (A fix
    # necessarily changes the byte count, so it WOULD be re-selected --
    # not exercised here.)
    second = sftp_pull.acquire(feed, Window(None, None), fx, "run-g09-second")

    assert second == []
    rows_after = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows_after) == 1


# --- G-10: operator window re-pull over acquired window ----------------------


def test_g10_operator_window_repull_over_acquired_window_yields_zero_new_registered_rows(
    local_effects: Effects, sftp_store: SftpStore
) -> None:
    fx = local_effects
    feed = _trailer_feed()
    content = b"policy_id,premium\nP1,100.00\nTOTAL:1\n"
    _seed_remote(sftp_store, "COMM_2026-07-24.csv", content, fx.now())

    first = sftp_pull.acquire(feed, Window(None, None), fx, "run-1")
    assert first[0].disposition == "registered"

    # Operator re-pull, explicit window covering the acquired file, no force.
    window = Window(start=fx.now() - timedelta(minutes=1), end=fx.now() + timedelta(minutes=1))
    second = sftp_pull.acquire(feed, window, fx, "run-2")

    assert second == []
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1
    assert all(r.disposition == "registered" for r in rows)


# --- G-13: sftp quiet-window ---------------------------------------------------


def test_g13_quiet_window_skips_too_fresh_file_then_next_run_picks_it_up(
    local_effects: Effects, sftp_store: SftpStore, clock_box: list[datetime]
) -> None:
    fx = local_effects
    feed = _timer_feed(quiet_minutes=30)
    _seed_remote(sftp_store, "COMM_fresh.csv", b"policy_id,commission\nP001,10.00\n", fx.now())

    first = sftp_pull.acquire(feed, Window(None, None), fx, "run-1")
    assert first == []
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert rows == []

    # Advance the fixture clock past the quiet window; next scheduled run.
    clock_box[0] = clock_box[0] + timedelta(minutes=31)
    second = sftp_pull.acquire(feed, Window(None, None), fx, "run-2")

    assert len(second) == 1
    assert second[0].disposition == "registered"
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1


# --- bonus: carrier-x "incomplete manifest" fixture (§15.1) ------------------


def test_sftp_manifest_missing_part_yields_incomplete_without_streaming(
    local_effects: Effects, sftp_store: SftpStore, s3_client: Any
) -> None:
    """Not one of the six required golden IDs -- covers this driver's own
    pre-stream missing/size-mismatch check (§9.2), which s3-push has no
    equivalent of (`evaluate_manifest` there always runs post-stream)."""
    fx = local_effects
    feed = _manifest_feed()
    _seed_fixture_set(sftp_store, "incomplete", fx.now())  # part2.csv deliberately absent

    outcomes = sftp_pull.acquire(feed, Window(None, None), fx, "run-incomplete")

    assert len(outcomes) == 1
    assert outcomes[0].disposition == "incomplete"
    rows = fx.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1
    assert rows[0].disposition == "incomplete"
    assert rows[0].batch_id is None  # never fully hashed -- no streaming occurred

    listing = s3_client.list_objects_v2(
        Bucket=fx.config.landing_bucket, Prefix=f"{_FEED_ID}/received_at="
    )
    assert "Contents" not in listing  # no canonical objects created


# --- bonus: §9.3 stuck-claim sweep resume (step 0) ---------------------------


def test_sftp_resume_batch_id_completes_a_crashed_delivery(
    local_effects: Effects, sftp_store: SftpStore, queue_url: str, clock_box: list[datetime]
) -> None:
    """Simulates a driver crash AFTER the turnstile claim (WON) but before
    E2-E5 finish -- the classic sweep-resume scenario (§9.2 step 0, §9.3).
    """
    fx = local_effects
    feed = _trailer_feed()
    content = b"policy_id,premium\nP1,100.00\nTOTAL:1\n"
    remote_path = sftp_pull._remote_file_path(_REMOTE_PATH, "COMM_dead.csv")
    sftp_store.setdefault(_SECRET_REF, {})[remote_path] = (content, fx.now())

    sftp = fx.sftp_fx_for(_SECRET_REF)
    received_at = fx.now()
    dead_delivery_id = "77777777-7777-4777-8777-777777777777"
    canonical = sftp_pull._canonical_uri(
        fx.config.landing_bucket, _FEED_ID, received_at, dead_delivery_id, "COMM_dead.csv"
    )
    bucket, key = sftp_pull._split_s3_uri(canonical)
    sha256_hex, total_bytes = fx.store.stream_upload(sftp.read_chunks(remote_path), bucket, key)

    staged = StagedObject(
        name="COMM_dead.csv",
        role="data",
        uri=canonical,
        bytes=total_bytes,
        sha256=sha256_hex,
        src_key=None,
    )
    completeness = CompletenessResult(
        verdict="complete",
        reason=None,
        asserted_record_count=None,
        data_object_names=("COMM_dead.csv",),
    )
    dead_req = RegistrationRequest(
        feed=feed,
        delivery_id=dead_delivery_id,
        delivery_key="COMM_dead.csv",
        received_at=received_at,
        driver="sftp-pull",
        driver_run_id="run-dead",
        completeness=completeness,
        objects=[staged],
    )
    content_hash = canonical_content_hash([("COMM_dead.csv", sha256_hex)])
    dead_batch_id = mint_batch_id(_FEED_ID, content_hash)
    claim = fx.cas.claim(dead_req, dead_batch_id, "run-dead", {}, fx.now())
    assert claim.kind == "WON"

    # Advance past the 1200 s staleness threshold, then resume.
    clock_box[0] = clock_box[0] + timedelta(minutes=21)
    outcomes = sftp_pull.acquire(
        feed, Window(None, None), fx, "run-resumer", resume_batch_id=dead_batch_id
    )

    assert len(outcomes) == 1
    assert outcomes[0].disposition == "registered"
    assert outcomes[0].delivery_id == dead_delivery_id

    rows = fx.ledger.scan_feed(_FEED_ID, None)
    registered = folds.registered_deliveries(rows)
    assert len(registered) == 1
    assert registered[0].delivery_id == dead_delivery_id
    assert registered[0].driver_run_id == "run-dead"  # resumes the DEAD run's identity

    events = _drain_events(fx, queue_url)
    assert len(events) == 1


# --- M-1 (security-gate): resume must use CasFx.get_claim (GetItem), NEVER --
# sweep_stale (Scan) -- the per-feed driver role does not, and must not, hold
# `dynamodb:Scan` on the CAS table.


def _scan_forbidding_cas(fx: Effects) -> Effects:
    """A `CasFx` whose `sweep_stale` raises immediately if called -- proves
    the resume path never reaches for it (a plain-function double per
    project convention, §12.2 IDIOM rule: no mocking framework).
    """

    def _forbidden_sweep_stale(now: datetime) -> list[Any]:
        raise AssertionError("sftp-pull resume must not call CasFx.sweep_stale (Scan) -- M-1")

    return dataclasses.replace(
        fx, cas=dataclasses.replace(fx.cas, sweep_stale=_forbidden_sweep_stale)
    )


def test_sftp_resume_batch_id_never_calls_sweep_stale(
    local_effects: Effects, sftp_store: SftpStore, queue_url: str, clock_box: list[datetime]
) -> None:
    """Same crash-recovery scenario as
    `test_sftp_resume_batch_id_completes_a_crashed_delivery`, but the resume
    call runs against a `CasFx` whose `sweep_stale` raises on any call --
    the OLD implementation (`fx.cas.sweep_stale(now)` + a linear scan for
    the matching item) would trip that raise; the fix (`fx.cas.get_claim`,
    a GetItem) never touches `sweep_stale` at all.
    """
    fx = local_effects
    feed = _trailer_feed()
    content = b"policy_id,premium\nP1,100.00\nTOTAL:1\n"
    remote_path = sftp_pull._remote_file_path(_REMOTE_PATH, "COMM_dead2.csv")
    sftp_store.setdefault(_SECRET_REF, {})[remote_path] = (content, fx.now())

    sftp = fx.sftp_fx_for(_SECRET_REF)
    received_at = fx.now()
    dead_delivery_id = "66666666-6666-4666-8666-666666666666"
    canonical = sftp_pull._canonical_uri(
        fx.config.landing_bucket, _FEED_ID, received_at, dead_delivery_id, "COMM_dead2.csv"
    )
    bucket, key = sftp_pull._split_s3_uri(canonical)
    sha256_hex, total_bytes = fx.store.stream_upload(sftp.read_chunks(remote_path), bucket, key)

    staged = StagedObject(
        name="COMM_dead2.csv",
        role="data",
        uri=canonical,
        bytes=total_bytes,
        sha256=sha256_hex,
        src_key=None,
    )
    completeness = CompletenessResult(
        verdict="complete",
        reason=None,
        asserted_record_count=None,
        data_object_names=("COMM_dead2.csv",),
    )
    dead_req = RegistrationRequest(
        feed=feed,
        delivery_id=dead_delivery_id,
        delivery_key="COMM_dead2.csv",
        received_at=received_at,
        driver="sftp-pull",
        driver_run_id="run-dead2",
        completeness=completeness,
        objects=[staged],
    )
    content_hash = canonical_content_hash([("COMM_dead2.csv", sha256_hex)])
    dead_batch_id = mint_batch_id(_FEED_ID, content_hash)
    claim = fx.cas.claim(dead_req, dead_batch_id, "run-dead2", {}, fx.now())
    assert claim.kind == "WON"

    clock_box[0] = clock_box[0] + timedelta(minutes=21)
    fx_no_scan = _scan_forbidding_cas(fx)

    outcomes = sftp_pull.acquire(
        feed, Window(None, None), fx_no_scan, "run-resumer2", resume_batch_id=dead_batch_id
    )

    assert len(outcomes) == 1
    assert outcomes[0].disposition == "registered"
    assert outcomes[0].delivery_id == dead_delivery_id

    rows = fx.ledger.scan_feed(_FEED_ID, None)
    registered = folds.registered_deliveries(rows)
    assert len(registered) == 1
    assert registered[0].delivery_id == dead_delivery_id

    events = _drain_events(fx, queue_url)
    assert len(events) == 1
