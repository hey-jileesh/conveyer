"""Unit tests for `ingestion.core.decisions` — LLD §8.3, §8, §9.4.

One plan test per `claim.kind` (WON/LOST_COMPLETED/LOST_IN_PROGRESS/
TAKEN_OVER), WON/TAKEN_OVER supersession detection (incl. TAKEN_OVER's
self-exclusion of the dead run's own already-appended row), append-on-change
suppression in `plan_nondelivery`, and `plan_reconciliation`.
"""

from datetime import UTC, datetime, timedelta

from ingestion.core import decisions
from ingestion.core.completeness import CompletenessResult, ObjectStat
from ingestion.core.model import (
    ClaimItem,
    ClaimResult,
    DeliveryRecord,
    FeedConfig,
    StagedObject,
)

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

SFTP_FEED = FeedConfig.model_validate(
    {
        "feed_id": "carrier-x/commission-statements",
        "driver": "sftp-pull",
        "pipeline": "pipelines/commissions",
        "connection": {
            "secret_ref": "arn:aws:secretsmanager:us-east-1:000000000000:secret:conveyer-dev/x",
            "remote_path": "/outbound/commissions/",
        },
        "trigger": {"schedule": "cron(0 13 ? * MON-FRI *)", "timezone": "America/New_York"},
        "completeness": {"mode": "manifest"},
    }
)
S3_FEED = FeedConfig.model_validate(
    {
        "feed_id": "carrier-y/renewal-statements",
        "driver": "s3-push",
        "pipeline": "pipelines/renewals",
        "connection": {
            "partner_principal_arns": ["arn:aws:iam::111111111111:role/carrier-y-uploader"]
        },
        "completeness": {"mode": "manifest"},
    }
)

_S3_PUSH_OBJECTS = [
    StagedObject(
        name="part1.csv",
        role="data",
        uri="s3://landing/carrier-y/renewal-statements/received_at=t/dl-abc/part1.csv",
        bytes=10,
        sha256="a" * 64,
        src_key="carrier-y/renewal-statements/incoming/part1.csv",
    ),
    StagedObject(
        name="m.json",
        role="manifest",
        uri="s3://landing/carrier-y/renewal-statements/received_at=t/dl-abc/m.json",
        bytes=5,
        sha256="b" * 64,
        src_key="carrier-y/renewal-statements/incoming/m.json",
    ),
]


def _completeness_result(record_count: int | None = 3) -> CompletenessResult:
    return CompletenessResult(
        verdict="complete",
        reason=None,
        asserted_record_count=record_count,
        data_object_names=("part1.csv",),
    )


def _s3_push_request(delivery_id: str = "d-new") -> decisions.RegistrationRequest:
    return decisions.RegistrationRequest(
        feed=S3_FEED,
        delivery_id=delivery_id,
        delivery_key="m1",
        received_at=NOW,
        driver="s3-push",
        driver_run_id="run1",
        completeness=_completeness_result(),
        objects=list(_S3_PUSH_OBJECTS),
    )


def _claim_item(
    *,
    delivery_id: str,
    batch_id: str,
    content_hash: str,
    driver: str = "s3-push",
    delivery_key: str = "m1",
    feed_id: str = S3_FEED.feed_id,
    objects_inventory: tuple[StagedObject, ...] = (),
    owner_run_id: str = "run-owner",
    completeness_mode: str = "manifest",
    size_bytes: int = 10,
    asserted_record_count: int | None = 3,
) -> ClaimItem:
    return ClaimItem(
        feed_id=feed_id,
        batch_id=batch_id,
        delivery_id=delivery_id,
        driver=driver,
        received_at=NOW,
        delivery_key=delivery_key,
        content_hash=content_hash,
        size_bytes=size_bytes,
        objects_inventory=objects_inventory,
        asserted_record_count=asserted_record_count,
        completeness_mode=completeness_mode,
        trigger={},
        owner_run_id=owner_run_id,
        status="in_progress",
        claimed_at=0,
        completed_at=None,
    )


def _row(
    delivery_id: str,
    delivery_key: str,
    disposition: str,
    received_at: datetime,
    content_hash: str,
    *,
    feed_id: str = S3_FEED.feed_id,
    batch_id: str = "b1",
) -> DeliveryRecord:
    return decisions._build_row(
        delivery_id=delivery_id,
        feed_id=feed_id,
        delivery_key=delivery_key,
        received_at=received_at,
        recorded_at=received_at,
        driver="s3-push",
        driver_run_id="run0",
        completeness_mode="manifest",
        asserted_record_count=None,
        disposition=disposition,  # type: ignore[arg-type]
        supersedes=None,
        content_hash=content_hash,
        batch_id=batch_id,
        size_bytes=1,
        objects=[],
        object_uris=[],
        manifest_ref=None,
    )


# --- one plan test per claim.kind -----------------------------------------------


def test_plan_registration_won() -> None:
    req = _s3_push_request()
    claim = ClaimResult(kind="WON", item=None)
    plan = decisions.plan_registration(claim, [], req, NOW)

    assert len(plan.rows) == 1
    row = plan.rows[0]
    assert row.disposition == "registered"
    assert row.delivery_id == "d-new"
    assert row.supersedes is None
    assert row.batch_id is not None
    assert row.content_hash is not None
    assert row.size_bytes == 10
    # copies: one per StagedObject with src_key set (both objects here).
    assert len(plan.copies) == 2
    assert {c.src_key for c in plan.copies} == {o.src_key for o in _S3_PUSH_OBJECTS}
    assert plan.event is not None
    assert plan.event.delivery_id == "d-new"
    assert plan.complete_claim == (S3_FEED.feed_id, row.batch_id)
    assert plan.outcome.disposition == "registered"
    assert plan.outcome.delivery_id == "d-new"
    assert plan.outcome.batch_id == row.batch_id


def test_plan_registration_lost_completed() -> None:
    req = _s3_push_request()
    winner = _claim_item(
        delivery_id="d-winner", batch_id="winner-batch", content_hash="sha256:" + "e" * 64
    )
    claim = ClaimResult(kind="LOST_COMPLETED", item=winner)
    plan = decisions.plan_registration(claim, [], req, NOW)

    assert len(plan.rows) == 1
    row = plan.rows[0]
    assert row.disposition == "duplicate"
    assert row.delivery_id == "d-new"  # this run's own delivery_id
    assert row.batch_id == "winner-batch"  # same batch_id as the item that beat it
    assert row.content_hash == winner.content_hash
    # s3-push duplicate: vestibule uri, not canonical (§6.2 population table).
    assert row.objects[0].uri == _S3_PUSH_OBJECTS[0].src_key
    assert plan.copies == ()
    assert plan.event is None
    assert plan.complete_claim is None
    assert plan.outcome.disposition == "duplicate"
    assert plan.outcome.batch_id == "winner-batch"


def test_plan_registration_lost_in_progress() -> None:
    """Same shape as LOST_COMPLETED — both sides of every race are remembered."""
    req = _s3_push_request()
    live_owner = _claim_item(
        delivery_id="d-owner", batch_id="owner-batch", content_hash="sha256:" + "f" * 64
    )
    claim = ClaimResult(kind="LOST_IN_PROGRESS", item=live_owner)
    plan = decisions.plan_registration(claim, [], req, NOW)

    assert len(plan.rows) == 1
    row = plan.rows[0]
    assert row.disposition == "duplicate"
    assert row.delivery_id == "d-new"
    assert row.batch_id == "owner-batch"
    assert plan.copies == ()
    assert plan.event is None
    assert plan.complete_claim is None


def test_plan_registration_taken_over() -> None:
    """As WON, but every row/copy/event is built from the dead run's identity
    in claim.item — byte-identical to what the dead run would have executed."""
    req = _s3_push_request()
    dead_item = _claim_item(
        delivery_id="d-dead",
        batch_id="dead-batch",
        content_hash="sha256:" + "c" * 64,
        objects_inventory=tuple(_S3_PUSH_OBJECTS),
        owner_run_id="run-resumer",
    )
    claim = ClaimResult(kind="TAKEN_OVER", item=dead_item)
    plan = decisions.plan_registration(claim, [], req, NOW)

    assert len(plan.rows) == 1
    row = plan.rows[0]
    assert row.disposition == "registered"
    # identity comes from claim.item, NOT req (req.delivery_id == "d-new").
    assert row.delivery_id == "d-dead"
    assert row.batch_id == "dead-batch"
    assert row.content_hash == dead_item.content_hash
    assert row.size_bytes == dead_item.size_bytes
    assert row.driver_run_id == "run-resumer"
    assert row.received_at == dead_item.received_at
    # copies rebuilt from the dead run's objects_inventory (both had src_key set).
    assert len(plan.copies) == 2
    assert plan.event is not None
    assert plan.event.delivery_id == "d-dead"
    assert plan.event.batch_id == "dead-batch"
    assert plan.complete_claim == (S3_FEED.feed_id, "dead-batch")
    assert plan.outcome.delivery_id == "d-dead"
    assert plan.outcome.batch_id == "dead-batch"


# --- supersession detection ------------------------------------------------------


def test_plan_registration_won_detects_correction_and_supersedes_prior() -> None:
    req = _s3_push_request()
    prior_registered = _row(
        "d-old", "m1", "registered", NOW - timedelta(days=1), "sha256:" + "9" * 64
    )
    claim = ClaimResult(kind="WON", item=None)
    plan = decisions.plan_registration(claim, [prior_registered], req, NOW)

    assert len(plan.rows) == 2
    new_row, accretion_row = plan.rows
    assert new_row.delivery_id == "d-new"
    assert new_row.disposition == "registered"
    assert new_row.supersedes == "d-old"
    assert accretion_row.delivery_id == "d-old"
    assert accretion_row.disposition == "superseded"
    assert accretion_row.recorded_at == NOW
    # identity columns copied verbatim from the original row.
    assert accretion_row.content_hash == prior_registered.content_hash
    assert accretion_row.received_at == prior_registered.received_at


def test_plan_registration_won_same_content_hash_is_not_a_correction() -> None:
    req = _s3_push_request()
    same_content_hash = decisions.canonical_content_hash(
        [(o.name, o.sha256) for o in _S3_PUSH_OBJECTS if o.role == "data"]
    )
    prior_registered = _row("d-old", "m1", "registered", NOW - timedelta(days=1), same_content_hash)
    claim = ClaimResult(kind="WON", item=None)
    plan = decisions.plan_registration(claim, [prior_registered], req, NOW)

    assert len(plan.rows) == 1
    assert plan.rows[0].supersedes is None


def test_plan_registration_won_ignores_prior_rows_with_different_delivery_key() -> None:
    req = _s3_push_request()
    unrelated = _row(
        "d-unrelated", "other-key", "registered", NOW - timedelta(days=1), "sha256:" + "9" * 64
    )
    claim = ClaimResult(kind="WON", item=None)
    plan = decisions.plan_registration(claim, [unrelated], req, NOW)
    assert len(plan.rows) == 1
    assert plan.rows[0].supersedes is None


def test_plan_registration_taken_over_detects_supersession() -> None:
    dead_item = _claim_item(
        delivery_id="d-dead",
        batch_id="dead-batch",
        content_hash="sha256:" + "c" * 64,
        objects_inventory=tuple(_S3_PUSH_OBJECTS),
    )
    claim = ClaimResult(kind="TAKEN_OVER", item=dead_item)
    prior_registered = _row(
        "d-really-old", "m1", "registered", NOW - timedelta(days=1), "sha256:" + "9" * 64
    )
    plan = decisions.plan_registration(claim, [prior_registered], _s3_push_request(), NOW)

    assert len(plan.rows) == 2
    new_row, accretion_row = plan.rows
    assert new_row.delivery_id == "d-dead"
    assert new_row.supersedes == "d-really-old"
    assert accretion_row.delivery_id == "d-really-old"
    assert accretion_row.disposition == "superseded"


def test_plan_registration_taken_over_excludes_its_own_prior_row() -> None:
    """If the dead run got as far as appending its OWN registered row before
    dying, TAKEN_OVER must not treat that row as something IT supersedes."""
    dead_item = _claim_item(
        delivery_id="d-dead",
        batch_id="dead-batch",
        content_hash="sha256:" + "c" * 64,
        objects_inventory=tuple(_S3_PUSH_OBJECTS),
    )
    claim = ClaimResult(kind="TAKEN_OVER", item=dead_item)
    own_prior_row = _row("d-dead", "m1", "registered", NOW, "sha256:" + "c" * 64)
    plan = decisions.plan_registration(claim, [own_prior_row], _s3_push_request(), NOW)
    assert len(plan.rows) == 1
    assert plan.rows[0].supersedes is None


# --- TAKEN_OVER narrows ClaimItem.completeness_mode for every mode -------------


def test_plan_registration_taken_over_narrows_timer_completeness_mode() -> None:
    """`ClaimItem.completeness_mode` is a plain `str`; TAKEN_OVER must narrow
    it to the `DeliveryRecord` literal for every valid value, not just the
    manifest-mode default used by the other TAKEN_OVER tests."""
    timer_object = StagedObject(
        name="t.csv",
        role="data",
        uri="s3://landing/carrier-x/commission-statements/received_at=t/dl-abc/t.csv",
        bytes=10,
        sha256="a" * 64,
        src_key=None,
    )
    dead_item = _claim_item(
        delivery_id="d-dead-timer",
        batch_id="dead-batch-timer",
        content_hash="sha256:" + "c" * 64,
        driver="sftp-pull",
        delivery_key="t.csv",
        feed_id=SFTP_FEED.feed_id,
        objects_inventory=(timer_object,),
        completeness_mode="timer",
        asserted_record_count=None,
    )
    claim = ClaimResult(kind="TAKEN_OVER", item=dead_item)
    req = decisions.RegistrationRequest(
        feed=SFTP_FEED,
        delivery_id="d-irrelevant",
        delivery_key="t.csv",
        received_at=NOW,
        driver="sftp-pull",
        driver_run_id="run1",
        completeness=_completeness_result(record_count=None),
        objects=[timer_object],
    )
    plan = decisions.plan_registration(claim, [], req, NOW)
    assert plan.rows[0].completeness_mode == "timer"


# --- sftp-pull duplicate rows use canonical uri, not vestibule ------------------


def test_plan_registration_lost_sftp_pull_uses_canonical_uri() -> None:
    """sftp-pull streams to canonical BEFORE claiming (§9.2's ordering note),
    so `src_key` is None and the duplicate row's objects carry the canonical
    uri — unlike the s3-push loser case."""
    sftp_object = StagedObject(
        name="part1.csv",
        role="data",
        uri="s3://landing/carrier-x/commission-statements/received_at=t/dl-abc/part1.csv",
        bytes=10,
        sha256="a" * 64,
        src_key=None,
    )
    req = decisions.RegistrationRequest(
        feed=SFTP_FEED,
        delivery_id="d-sftp",
        delivery_key="part1.csv",
        received_at=NOW,
        driver="sftp-pull",
        driver_run_id="run1",
        completeness=_completeness_result(record_count=None),
        objects=[sftp_object],
    )
    winner = _claim_item(
        delivery_id="d-winner-sftp",
        batch_id="winner-batch-sftp",
        content_hash="sha256:" + "e" * 64,
        driver="sftp-pull",
        delivery_key="part1.csv",
        feed_id=SFTP_FEED.feed_id,
        completeness_mode="trailer",
    )
    claim = ClaimResult(kind="LOST_COMPLETED", item=winner)
    plan = decisions.plan_registration(claim, [], req, NOW)
    assert plan.rows[0].objects[0].uri == sftp_object.uri


# --- plan_nondelivery: append-on-change ------------------------------------------


def test_plan_nondelivery_builds_a_row_when_no_prior_exists() -> None:
    observed = [ObjectStat(name="COMM_1.csv", bytes=100, sha256=None)]
    plan = decisions.plan_nondelivery(
        SFTP_FEED,
        "COMM_1.csv",
        "incomplete",
        observed,
        "trailer missing or malformed",
        [],
        NOW,
        "d-nd1",
        "run1",
    )
    assert len(plan.rows) == 1
    row = plan.rows[0]
    assert row.disposition == "incomplete"
    assert row.delivery_id == "d-nd1"
    assert row.batch_id is None
    assert row.content_hash is None
    assert row.notes == "trailer missing or malformed"
    assert plan.copies == () and plan.event is None and plan.complete_claim is None
    assert plan.outcome.delivery_id == "d-nd1"


def test_plan_nondelivery_suppresses_when_same_disposition_and_inventory() -> None:
    observed = [ObjectStat(name="COMM_1.csv", bytes=100, sha256=None)]
    first = decisions.plan_nondelivery(
        SFTP_FEED, "COMM_1.csv", "incomplete", observed, "trailer missing", [], NOW, "d-nd1", "run1"
    )
    again = decisions.plan_nondelivery(
        SFTP_FEED,
        "COMM_1.csv",
        "incomplete",
        observed,
        "trailer missing",
        [first.rows[0]],
        NOW + timedelta(hours=1),
        "d-nd2-wasted",
        "run2",
    )
    assert again.rows == ()
    assert again.copies == () and again.event is None and again.complete_claim is None
    # outcome reflects the EXISTING prior row, not the freshly-minted-but-unused id.
    assert again.outcome.delivery_id == "d-nd1"
    assert again.outcome.disposition == "incomplete"


def test_plan_nondelivery_does_not_suppress_on_byte_count_change() -> None:
    prior_observed = [ObjectStat(name="COMM_1.csv", bytes=100, sha256=None)]
    first = decisions.plan_nondelivery(
        SFTP_FEED,
        "COMM_1.csv",
        "incomplete",
        prior_observed,
        "trailer missing",
        [],
        NOW,
        "d-nd1",
        "run1",
    )
    changed_observed = [ObjectStat(name="COMM_1.csv", bytes=150, sha256=None)]
    second = decisions.plan_nondelivery(
        SFTP_FEED,
        "COMM_1.csv",
        "incomplete",
        changed_observed,
        "trailer missing",
        [first.rows[0]],
        NOW + timedelta(hours=1),
        "d-nd2",
        "run2",
    )
    assert len(second.rows) == 1
    assert second.rows[0].delivery_id == "d-nd2"


def test_plan_nondelivery_does_not_suppress_on_disposition_change() -> None:
    observed = [ObjectStat(name="COMM_1.csv", bytes=100, sha256=None)]
    first = decisions.plan_nondelivery(
        SFTP_FEED, "COMM_1.csv", "incomplete", observed, "trailer missing", [], NOW, "d-nd1", "run1"
    )
    second = decisions.plan_nondelivery(
        SFTP_FEED,
        "COMM_1.csv",
        "unreadable",
        observed,
        "sha256 mismatch",
        [first.rows[0]],
        NOW + timedelta(hours=1),
        "d-nd2",
        "run2",
    )
    assert len(second.rows) == 1
    assert second.rows[0].disposition == "unreadable"


# --- plan_nondelivery: content_hash population rule (§6.2) ----------------------


def test_plan_nondelivery_incomplete_never_populates_content_hash() -> None:
    fully_hashed = [ObjectStat(name="a.csv", bytes=10, sha256="a" * 64)]
    plan = decisions.plan_nondelivery(
        SFTP_FEED, "k1", "incomplete", fully_hashed, None, [], NOW, "d-nd1", "run1"
    )
    row = plan.rows[0]
    assert row.content_hash is None
    assert row.batch_id is None
    assert row.size_bytes is None


def test_plan_nondelivery_unreadable_populates_when_fully_hashed() -> None:
    fully_hashed = [ObjectStat(name="a.csv", bytes=10, sha256="a" * 64)]
    plan = decisions.plan_nondelivery(
        SFTP_FEED, "k1", "unreadable", fully_hashed, "sha256 mismatch", [], NOW, "d-nd1", "run1"
    )
    row = plan.rows[0]
    assert row.content_hash is not None
    assert row.batch_id is not None
    assert row.size_bytes == 10


def test_plan_nondelivery_unreadable_stays_null_when_not_fully_hashed() -> None:
    not_hashed = [ObjectStat(name="a.csv", bytes=10, sha256=None)]
    plan = decisions.plan_nondelivery(
        SFTP_FEED,
        "k1",
        "unreadable",
        not_hashed,
        "manifest parse failure",
        [],
        NOW,
        "d-nd1",
        "run1",
    )
    row = plan.rows[0]
    assert row.content_hash is None
    assert row.batch_id is None
    assert row.size_bytes is None


# --- plan_reconciliation (§9.4 step 3) -------------------------------------------


def test_plan_reconciliation_supersedes_all_but_newest() -> None:
    oldest = _row(
        "r-a", "k", "registered", NOW - timedelta(days=2), "sha256:" + "1" * 64, batch_id="b1"
    )
    middle = _row(
        "r-b", "k", "registered", NOW - timedelta(days=1), "sha256:" + "2" * 64, batch_id="b2"
    )
    newest = _row("r-c", "k", "registered", NOW, "sha256:" + "3" * 64, batch_id="b3")
    result = decisions.plan_reconciliation({"k": [oldest, middle, newest]}, NOW + timedelta(days=1))
    assert {r.delivery_id for r in result} == {"r-a", "r-b"}
    assert all(r.disposition == "superseded" for r in result)
    assert all(r.recorded_at == NOW + timedelta(days=1) for r in result)


def test_plan_reconciliation_no_rows_for_single_registered_delivery() -> None:
    only = _row("r-a", "k", "registered", NOW, "sha256:" + "1" * 64)
    result = decisions.plan_reconciliation({"k": [only]}, NOW + timedelta(days=1))
    assert result == ()


def test_plan_reconciliation_is_idempotent_given_same_input() -> None:
    oldest = _row(
        "r-a", "k", "registered", NOW - timedelta(days=1), "sha256:" + "1" * 64, batch_id="b1"
    )
    newest = _row("r-b", "k", "registered", NOW, "sha256:" + "2" * 64, batch_id="b2")
    live_duplicates = {"k": [oldest, newest]}
    first = decisions.plan_reconciliation(live_duplicates, NOW + timedelta(days=1))
    second = decisions.plan_reconciliation(live_duplicates, NOW + timedelta(days=1))
    assert first == second


def test_plan_reconciliation_handles_multiple_delivery_keys_independently() -> None:
    k1_old = _row("r-a", "k1", "registered", NOW - timedelta(days=1), "sha256:" + "1" * 64)
    k1_new = _row("r-b", "k1", "registered", NOW, "sha256:" + "2" * 64)
    k2_old = _row("r-c", "k2", "registered", NOW - timedelta(days=1), "sha256:" + "3" * 64)
    k2_new = _row("r-d", "k2", "registered", NOW, "sha256:" + "4" * 64)
    result = decisions.plan_reconciliation(
        {"k1": [k1_old, k1_new], "k2": [k2_old, k2_new]}, NOW + timedelta(days=1)
    )
    assert {r.delivery_id for r in result} == {"r-a", "r-c"}
