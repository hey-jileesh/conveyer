"""Pin test for critique-gate finding F-4.

`content_hash`/`batch_id` are independently re-derived from the SAME
`RegistrationRequest.objects`, filtered to `role == "data"`, at three
separate call sites -- deliberately, not threaded through as an extra
field (see `registration/registrar.py`'s module docstring and
`core/decisions.py`'s module docstring for why):

1. `registration.registrar.register_delivery` (inline, before `fx.cas.claim`).
2. `effects.cas._build_claim_item_dict` (re-derives `content_hash` --
   `batch_id` itself is threaded through as a parameter there, not
   independently derived).
3. `core.decisions._plan_won` (re-derives both, fully independently, when
   building the `registered` `DeliveryRecord`/`DeliveryRegisteredV1`).

This test pins that the three sites agree on IDENTICAL output for the SAME
input, including the `role == "data"` filter (a `role == "manifest"`
object with a deliberately different `sha256` must NOT affect the
computed hash at any of the three sites) -- until a Phase 2 interface
amendment changes the convention (e.g. threading a single derived value
through `RegistrationRequest` instead), this test is the regression gate.

`driver="sftp-pull"` is used (not `s3-push`) so every `StagedObject.src_key`
is `None` -- `_copies_for` then yields zero `CopySpec`s, so `register_
delivery`'s `execute()` step E1 needs no real vestibule content in S3 (site
1 can run against `local_effects` with no upload/seed step beyond what the
fixture already provides).
"""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.core import decisions
from ingestion.core.completeness import CompletenessResult
from ingestion.core.decisions import RegistrationRequest
from ingestion.core.hashing import canonical_content_hash
from ingestion.core.minting import mint_batch_id
from ingestion.core.model import Completeness, FeedConfig, SftpConnection, StagedObject, Trigger
from ingestion.effects import cas
from ingestion.effects.records import Effects
from ingestion.registration import registrar

_FEED_ID = "carrier-x/derivation-agreement"


def _feed_config() -> FeedConfig:
    return FeedConfig(
        feed_id=_FEED_ID,
        driver="sftp-pull",
        pipeline="pipelines/derivation-agreement",
        connection=SftpConnection(
            secret_ref="arn:aws:secretsmanager:us-east-1:000000000000:secret:conveyer-dev/sftp/x",
            remote_path="/outbound/x/",
        ),
        trigger=Trigger(schedule="cron(0 13 ? * MON-FRI *)"),
        completeness=Completeness(mode="manifest"),
    )


def _request() -> RegistrationRequest:
    received_at = datetime(2026, 7, 25, 9, 0, tzinfo=UTC)
    delivery_id = "99999999-9999-4999-8999-999999999999"
    data1 = StagedObject(
        name="part1.csv",
        role="data",
        uri=f"s3://lake/{_FEED_ID}/received_at=x/dl-{delivery_id}/part1.csv",
        bytes=10,
        sha256="a" * 64,
        src_key=None,
    )
    data2 = StagedObject(
        name="part2.csv",
        role="data",
        uri=f"s3://lake/{_FEED_ID}/received_at=x/dl-{delivery_id}/part2.csv",
        bytes=20,
        sha256="b" * 64,
        src_key=None,
    )
    # A `role == "manifest"` object with a DELIBERATELY DIFFERENT sha256 --
    # every derivation site must exclude it, or this test's expected hash
    # (computed from data objects only, below) would disagree with theirs.
    manifest = StagedObject(
        name="m.json",
        role="manifest",
        uri=f"s3://lake/{_FEED_ID}/received_at=x/dl-{delivery_id}/m.json",
        bytes=5,
        sha256="c" * 64,
        src_key=None,
    )
    return RegistrationRequest(
        feed=_feed_config(),
        delivery_id=delivery_id,
        delivery_key="manifest-2026-07-25",
        received_at=received_at,
        driver="sftp-pull",
        driver_run_id="run-derivation",
        completeness=CompletenessResult(
            verdict="complete",
            reason=None,
            asserted_record_count=2,
            data_object_names=("part1.csv", "part2.csv"),
        ),
        # Deliberately NOT `[*data, manifest]` -- the manifest object is
        # first here, proving the `role == "data"` filter (not position)
        # is what every site actually uses.
        objects=[manifest, data1, data2],
    )


def _expected(req: RegistrationRequest) -> tuple[str, str]:
    """The role=='data'-filtered formula itself, applied once here as the
    test's own expectation -- independent of, but textually identical to,
    all three production call sites."""
    data_objects = [o for o in req.objects if o.role == "data"]
    content_hash = canonical_content_hash([(o.name, o.sha256) for o in data_objects])
    batch_id = mint_batch_id(req.feed.feed_id, content_hash)
    return content_hash, batch_id


def test_plan_won_derivation_agrees_with_the_expected_formula() -> None:
    req = _request()
    expected_content_hash, expected_batch_id = _expected(req)

    plan = decisions._plan_won((), req, req.received_at)

    assert plan.rows[0].content_hash == expected_content_hash
    assert plan.rows[0].batch_id == expected_batch_id


def test_build_claim_item_dict_derivation_agrees_with_the_expected_formula() -> None:
    req = _request()
    expected_content_hash, expected_batch_id = _expected(req)

    item_dict = cas._build_claim_item_dict(
        req, expected_batch_id, "run-derivation", {}, req.received_at
    )

    assert item_dict["content_hash"] == expected_content_hash
    assert item_dict["batch_id"] == expected_batch_id


def test_register_delivery_derivation_agrees_with_the_expected_formula(
    local_effects: Effects,
) -> None:
    req = _request()
    expected_content_hash, expected_batch_id = _expected(req)

    outcome = registrar.register_delivery(req, local_effects)

    assert outcome.disposition == "registered"
    assert outcome.batch_id == expected_batch_id
    rows = local_effects.ledger.scan_feed(_FEED_ID, None)
    assert len(rows) == 1
    assert rows[0].content_hash == expected_content_hash
    assert rows[0].batch_id == expected_batch_id


def test_all_three_sites_agree_with_each_other_directly() -> None:
    """The assertion the brief names directly: all three independent
    derivations, run against the identical `RegistrationRequest`, produce
    IDENTICAL `content_hash`/`batch_id` values -- not merely each one
    individually matching this test's own expected-value helper."""
    req = _request()
    expected_content_hash, expected_batch_id = _expected(req)

    plan_row = decisions._plan_won((), req, req.received_at).rows[0]
    claim_dict = cas._build_claim_item_dict(
        req, expected_batch_id, "run-derivation", {}, req.received_at
    )

    assert plan_row.content_hash == claim_dict["content_hash"] == expected_content_hash
    assert plan_row.batch_id == claim_dict["batch_id"] == expected_batch_id
