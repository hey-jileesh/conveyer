"""Unit tests for `ingestion.core.model` — LLD §6.

Covers every cross-field validator named in LLD §6.1 as a distinct scenario
with an exact-message assertion, plus shape checks for the remaining
contracts (§6.2-§6.4, §6.7) and immutability of the frozen dataclasses
(§7.6, §8.1, §8.4). G-14's two named golden scenarios (timer+s3-push, extra
field) live in `tests/golden/test_g14_config_validation.py`; this file is
the broader unit-level companion.
"""

import dataclasses
from datetime import UTC, date, datetime
from typing import Any

import pytest
from ingestion.core.model import (
    ClaimItem,
    ClaimResult,
    DeliveryObject,
    DeliveryOutcome,
    DeliveryOverdueV1,
    DeliveryRecord,
    DeliveryRegisteredV1,
    Expectation,
    FeedConfig,
    ManifestFile,
    ManifestV1,
    S3PushConnection,
    SftpConnection,
    SftpSecret,
    StagedObject,
    TrailerSpec,
    Window,
)
from pydantic import ValidationError

NOW = datetime(2026, 7, 24, 12, 0, 0, tzinfo=UTC)

# Mirror the two exemplar feeds (§15.1/§15.2) as reusable valid bases.
BASE_SFTP_PULL: dict[str, Any] = {
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
BASE_S3_PUSH: dict[str, Any] = {
    "feed_id": "carrier-y/renewal-statements",
    "driver": "s3-push",
    "pipeline": "pipelines/renewals",
    "connection": {"partner_principal_arns": ["arn:aws:iam::111111111111:role/carrier-y-uploader"]},
    "completeness": {"mode": "manifest"},
}


def _error_messages(exc: ValidationError) -> list[str]:
    """Exact raised strings — see the identical helper in the G-14 golden test."""
    messages: list[str] = []
    for err in exc.errors():
        ctx_error = err.get("ctx", {}).get("error")
        messages.append(str(ctx_error) if ctx_error is not None else err["msg"])
    return messages


# --- §6.1 FeedConfig — happy paths ------------------------------------------


def test_sftp_pull_exemplar_validates() -> None:
    config = FeedConfig.model_validate(BASE_SFTP_PULL)
    assert config.driver == "sftp-pull"
    assert config.trigger is not None


def test_s3_push_exemplar_validates() -> None:
    config = FeedConfig.model_validate(BASE_S3_PUSH)
    assert config.driver == "s3-push"
    assert config.trigger is None


def test_timer_completeness_valid_for_sftp_pull() -> None:
    config = dict(BASE_SFTP_PULL)
    config["completeness"] = {
        "mode": "timer",
        "timer": {"quiet_window_minutes": 30, "accepted_risk": "y" * 25},
    }
    validated = FeedConfig.model_validate(config)
    assert validated.completeness.mode == "timer"


# --- §6.1 FeedConfig — cross-field validators, each a distinct message -----


def test_sftp_pull_requires_sftp_connection() -> None:
    config = dict(BASE_SFTP_PULL)
    config["connection"] = None
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == ["sftp-pull requires SftpConnection"]


def test_sftp_pull_requires_trigger() -> None:
    config = dict(BASE_SFTP_PULL)
    config["trigger"] = None
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == ["sftp-pull requires trigger"]


def test_s3_push_requires_s3_push_connection() -> None:
    config = dict(BASE_S3_PUSH)
    config["connection"] = {
        "secret_ref": "arn:aws:secretsmanager:us-east-1:000000000000:secret:x",
        "remote_path": "/a/",
    }
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == ["s3-push requires S3PushConnection"]


def test_s3_push_forbids_trigger() -> None:
    config = dict(BASE_S3_PUSH)
    config["trigger"] = {"schedule": "cron(0 13 ? * MON-FRI *)"}
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == ["s3-push forbids trigger"]


@pytest.mark.parametrize("driver", ["api-pull", "db-unload"])
def test_unimplemented_driver_rejected(driver: str) -> None:
    config = dict(BASE_S3_PUSH)
    config["driver"] = driver
    config["connection"] = None
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == ["driver not implemented in Phase 1"]


@pytest.mark.parametrize("bad_pattern", ["no-star.json", "*a*.json", "*", "*.js?n", "*[abc].json"])
def test_manifest_pattern_must_be_star_literal_suffix(bad_pattern: str) -> None:
    config = dict(BASE_S3_PUSH)
    config["completeness"] = {"mode": "manifest", "manifest_pattern": bad_pattern}
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == [
        "manifest_pattern must be '*<literal-suffix>' (leading '*', no other wildcards)"
    ]


def test_trailer_mode_requires_trailer_spec() -> None:
    config = dict(BASE_SFTP_PULL)
    config["completeness"] = {"mode": "trailer"}
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == [
        "completeness.trailer is required when mode == 'trailer'"
    ]


def test_timer_mode_requires_timer_spec() -> None:
    config = dict(BASE_SFTP_PULL)
    config["completeness"] = {"mode": "timer"}
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == [
        "completeness.timer is required when mode == 'timer'"
    ]


def test_timer_completeness_rejected_for_s3_push_d10() -> None:
    config = dict(BASE_S3_PUSH)
    config["completeness"] = {
        "mode": "timer",
        "timer": {"quiet_window_minutes": 15, "accepted_risk": "x" * 25},
    }
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == [
        "timer completeness is not supported for s3-push in Phase 1 (LLD D-10)"
    ]


def test_invalid_timezone_rejected() -> None:
    config = dict(BASE_SFTP_PULL)
    config["trigger"] = {"schedule": "cron(0 13 ? * MON-FRI *)", "timezone": "Not/AZone"}
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == [
        "timezone 'Not/AZone' is not a valid IANA timezone name"
    ]


def test_invalid_secret_ref_rejected() -> None:
    # Validated directly on SftpConnection, not through FeedConfig: the
    # `connection: SftpConnection | S3PushConnection` union has no
    # discriminator, so when neither member matches cleanly pydantic's smart
    # union reports errors from BOTH members (noise unrelated to this
    # field's own validator, which is what this test targets).
    with pytest.raises(ValidationError) as exc_info:
        SftpConnection.model_validate({"secret_ref": "not-an-arn", "remote_path": "/a/"})
    assert _error_messages(exc_info.value) == [
        "secret_ref must be a Secrets Manager ARN "
        "(arn:aws:secretsmanager:<region>:<account-id>:secret:<name>)"
    ]


# --- H-3 (security-gate): S3PushConnection.partner_principal_arns ----------
# The single field controlling EXTERNAL PutObject access to the vestibule
# (D-15) -- previously only `min_length=1`, so a source.yaml typo/malice
# (wildcard, malformed ARN) silently granted a foreign account write access.
#
# H-3 RESIDUAL: the original regex's `role/.+`/`user/.+` name segment matched
# an embedded wildcard (e.g. `role/*`) -- AWS rejects that shape at
# policy-apply time, so the practical impact was a failed `terraform apply`
# rather than an over-grant, but the config gate is where that error is
# cheap and legible. `role/nested/path-role` below is the "legitimate
# path-prefixed role" acceptance case the residual fix must not break.


@pytest.mark.parametrize(
    "arn",
    [
        "arn:aws:iam::123456789012:root",
        "arn:aws:iam::123456789012:role/carrier-y-uploader",
        "arn:aws:iam::123456789012:user/some-user",
        "arn:aws:iam::111111111111:role/nested/path-role",  # path-prefixed role: must still work
    ],
)
def test_partner_principal_arn_accepts_valid_iam_arn_forms(arn: str) -> None:
    connection = S3PushConnection(partner_principal_arns=[arn])
    assert connection.partner_principal_arns == [arn]


@pytest.mark.parametrize(
    "arn",
    [
        "arn:aws:iam::*:role/foo",  # wildcard account id
        "*",  # bare wildcard
        "arn:aws:iam::123:role/foo",  # account id not 12 digits
        "arn:aws:iam::1234567890123:role/foo",  # account id 13 digits
        "not-an-arn",  # malformed
        "arn:aws:iam::123456789012:group/foo",  # unsupported principal kind
        "arn:aws:s3:::some-bucket",  # wrong service entirely
        "arn:aws:iam::123456789012:role/*",  # H-3 RESIDUAL: embedded wildcard in role name
        "arn:aws:iam::123456789012:user/*",  # H-3 RESIDUAL: embedded wildcard in user name
        "arn:aws:iam::123456789012:role/foo*bar",  # wildcard mid-segment, not just the whole name
        "arn:aws:iam::123456789012:role/foo bar",  # H-3 RESIDUAL: whitespace in role name
    ],
)
def test_partner_principal_arn_rejects_invalid_forms(arn: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        S3PushConnection(partner_principal_arns=[arn])
    messages = _error_messages(exc_info.value)
    assert len(messages) == 1
    assert "partner_principal_arns must each be an IAM principal ARN" in messages[0]
    assert arn in messages[0]


def test_partner_principal_arn_reports_only_the_invalid_entries_in_a_mixed_list() -> None:
    good = "arn:aws:iam::123456789012:role/good-role"
    bad = "arn:aws:iam::*:role/bad"
    with pytest.raises(ValidationError) as exc_info:
        S3PushConnection(partner_principal_arns=[good, bad])
    (message,) = _error_messages(exc_info.value)
    assert bad in message
    assert good not in message


# --- §6.1/D-11 Expectation.expected — closed grammar (conveyer-4ot.25) -----


@pytest.mark.parametrize(
    "expected",
    [
        "daily",
        "weekdays",
        "weekly:mon",
        "weekly:tue",
        "weekly:wed",
        "weekly:thu",
        "weekly:fri",
        "weekly:sat",
        "weekly:sun",
        "monthly:1",
        "monthly:9",
        "monthly:10",
        "monthly:28",
    ],
)
def test_expectation_expected_accepts_every_d11_grammar_form(expected: str) -> None:
    exp = Expectation(expected=expected, by="06:00", timezone="UTC")
    assert exp.expected == expected


@pytest.mark.parametrize(
    "expected",
    ["Daily", "weekly:funday", "monthly:0", "monthly:29", "monthly:01", "hourly", ""],
)
def test_expectation_expected_rejects_grammar_outside_d11(expected: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        Expectation(expected=expected, by="06:00", timezone="UTC")
    assert _error_messages(exc_info.value) == [
        "expected must match the D-11 grammar: 'daily' | 'weekdays' | "
        "'weekly:<mon..sun>' | 'monthly:<1..28>'"
    ]


# --- §6.1 Expectation.by — strict 24h HH:MM (conveyer-4ot.25) --------------


@pytest.mark.parametrize("by", ["00:00", "06:00", "09:00", "23:59"])
def test_expectation_by_accepts_valid_24h_times(by: str) -> None:
    exp = Expectation(expected="daily", by=by, timezone="UTC")
    assert exp.by == by


@pytest.mark.parametrize("by", ["24:00", "23:60", "6:00", "06:0", "06:00:00", "abc", ""])
def test_expectation_by_rejects_malformed_times(by: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        Expectation(expected="daily", by=by, timezone="UTC")
    assert _error_messages(exc_info.value) == [
        "by must be a strict 24h 'HH:MM' time (e.g. '06:00')"
    ]


# --- §6.1 TrailerSpec.pattern/count_group (conveyer-4ot.25) ----------------


def test_trailer_spec_accepts_pattern_without_count_group() -> None:
    spec = TrailerSpec(pattern=r"^TOTAL \d+$")
    assert spec.count_group is None


def test_trailer_spec_accepts_count_group_naming_a_present_group() -> None:
    spec = TrailerSpec(pattern=r"^TOTAL (?P<count>\d+)$", count_group="count")
    assert spec.count_group == "count"


def test_trailer_spec_pattern_must_compile_as_regex() -> None:
    with pytest.raises(ValidationError) as exc_info:
        TrailerSpec(pattern="(unclosed")
    assert _error_messages(exc_info.value) == ["pattern must be a valid Python regex: '(unclosed'"]


def test_trailer_spec_count_group_must_be_present_in_pattern() -> None:
    with pytest.raises(ValidationError) as exc_info:
        TrailerSpec(pattern=r"^TOTAL (?P<count>\d+)$", count_group="other")
    assert _error_messages(exc_info.value) == [
        "count_group 'other' must name a group present in pattern"
    ]


def test_trailer_spec_count_group_rejected_when_pattern_has_no_named_groups() -> None:
    with pytest.raises(ValidationError) as exc_info:
        TrailerSpec(pattern=r"^TOTAL \d+$", count_group="count")
    assert _error_messages(exc_info.value) == [
        "count_group 'count' must name a group present in pattern"
    ]


def test_feed_config_extra_field_rejected() -> None:
    config = dict(BASE_S3_PUSH)
    config["unexpected"] = "nope"
    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)
    assert _error_messages(exc_info.value) == ["Extra inputs are not permitted"]


# --- §6.3 ManifestV1 / ManifestFile — shape only ----------------------------


def test_manifest_file_sha256_must_be_64_lowercase_hex() -> None:
    with pytest.raises(ValidationError) as exc_info:
        ManifestFile(name="a.csv", bytes=1, sha256="NOTHEX")
    assert _error_messages(exc_info.value) == ["sha256 must be 64 lowercase hex characters"]


def test_manifest_v1_allows_extra_fields() -> None:
    manifest = ManifestV1.model_validate(
        {
            "manifest_version": 1,
            "manifest_id": "m1",
            "feed_id": "carrier-x/commission-statements",
            "files": [{"name": "a.csv", "bytes": 1, "sha256": "a" * 64}],
            "vendor_specific": "ignored-by-us",
        }
    )
    assert manifest.model_dump()["vendor_specific"] == "ignored-by-us"


def test_manifest_v1_requires_at_least_one_file() -> None:
    with pytest.raises(ValidationError):
        ManifestV1(manifest_version=1, manifest_id="m1", feed_id="f", files=[])


def test_manifest_v1_caps_at_1000_files() -> None:
    files = [ManifestFile(name=f"f{i}.csv", bytes=1, sha256="a" * 64) for i in range(1001)]
    with pytest.raises(ValidationError):
        ManifestV1(manifest_version=1, manifest_id="m1", feed_id="f", files=files)


# --- §6.2 DeliveryRecord — population-by-disposition shape ------------------


def test_delivery_record_registered_row() -> None:
    row = DeliveryRecord(
        delivery_id="d1",
        feed_id="carrier-x/commission-statements",
        delivery_key="k1",
        batch_id="b1",
        content_hash="sha256:" + "a" * 64,
        size_bytes=10,
        object_uris=["s3://bucket/x"],
        objects=[
            DeliveryObject(
                name="a.csv", role="data", uri="s3://bucket/x", bytes=10, sha256="a" * 64
            )
        ],
        manifest_ref=None,
        asserted_record_count=None,
        completeness_mode="manifest",
        received_at=NOW,
        recorded_at=NOW,
        disposition="registered",
        supersedes=None,
        driver="sftp-pull",
        driver_run_id="run1",
        notes=None,
    )
    assert row.disposition == "registered"
    assert row.batch_id == "b1"


def test_delivery_record_incomplete_row_has_null_batch_fields() -> None:
    row = DeliveryRecord(
        delivery_id="d2",
        feed_id="f",
        delivery_key="k2",
        batch_id=None,
        content_hash=None,
        size_bytes=None,
        object_uris=[],
        objects=[DeliveryObject(name="a.csv", role="data", uri=None, bytes=10, sha256=None)],
        manifest_ref=None,
        asserted_record_count=None,
        completeness_mode="manifest",
        received_at=NOW,
        recorded_at=NOW,
        disposition="incomplete",
        supersedes=None,
        driver="sftp-pull",
        driver_run_id="run1",
        notes="manifest missing part",
    )
    assert row.batch_id is None
    assert row.content_hash is None
    assert row.size_bytes is None
    assert row.object_uris == []


def test_delivery_record_content_hash_must_match_canonical_format() -> None:
    with pytest.raises(ValidationError) as exc_info:
        DeliveryRecord(
            delivery_id="d3",
            feed_id="f",
            delivery_key="k3",
            batch_id="b1",
            content_hash="not-a-hash",
            size_bytes=1,
            object_uris=[],
            objects=[],
            manifest_ref=None,
            asserted_record_count=None,
            completeness_mode="manifest",
            received_at=NOW,
            recorded_at=NOW,
            disposition="registered",
            supersedes=None,
            driver="sftp-pull",
            driver_run_id="run1",
            notes=None,
        )
    assert _error_messages(exc_info.value) == [
        "content_hash must match 'sha256:<64 lowercase hex>'"
    ]


# --- §6.4 Events -------------------------------------------------------------


def test_delivery_registered_v1_defaults_schema_version() -> None:
    event = DeliveryRegisteredV1(
        feed_id="f",
        delivery_id="d1",
        batch_id="b1",
        delivery_key="k1",
        content_hash="sha256:" + "a" * 64,
        size_bytes=1,
        object_uris=["s3://x"],
        received_at=NOW,
        pipeline="pipelines/x",
    )
    assert event.schema_version == 1


def test_delivery_overdue_v1_defaults_schema_version() -> None:
    event = DeliveryOverdueV1(
        feed_id="f", expectation_date=date(2026, 1, 1), expected_by=NOW, checked_at=NOW
    )
    assert event.schema_version == 1


# --- §6.7 SftpSecret ---------------------------------------------------------


def test_sftp_secret_password_auth() -> None:
    secret = SftpSecret(host="h", username="u", auth={"kind": "password", "password": "p"})
    assert secret.port == 22
    assert secret.auth.kind == "password"


def test_sftp_secret_private_key_auth() -> None:
    secret = SftpSecret(
        host="h",
        username="u",
        auth={"kind": "private_key", "private_key_pem": "PEM", "passphrase": None},
        host_key_fingerprint="fp",
    )
    assert secret.auth.kind == "private_key"


def test_sftp_secret_rejects_unknown_auth_kind() -> None:
    with pytest.raises(ValidationError):
        SftpSecret(host="h", username="u", auth={"kind": "bogus"})


# --- §7.6/§8.1/§8.4 frozen dataclasses — immutable by construction ----------


def _make_staged_object() -> StagedObject:
    return StagedObject(
        name="a.csv", role="data", uri="s3://x", bytes=1, sha256="a" * 64, src_key=None
    )


def test_staged_object_is_frozen() -> None:
    staged = _make_staged_object()
    with pytest.raises(dataclasses.FrozenInstanceError):
        staged.name = "mutated"  # type: ignore[misc]


def test_window_is_frozen() -> None:
    window = Window(start=None, end=None)
    with pytest.raises(dataclasses.FrozenInstanceError):
        window.start = NOW  # type: ignore[misc]


def test_delivery_outcome_is_frozen() -> None:
    outcome = DeliveryOutcome(
        delivery_id="d1", batch_id=None, disposition="registered", feed_id="f", delivery_key="k1"
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        outcome.disposition = "duplicate"  # type: ignore[misc]


def test_claim_item_and_claim_result_are_frozen() -> None:
    staged = _make_staged_object()
    item = ClaimItem(
        feed_id="f",
        batch_id="b1",
        delivery_id="d1",
        driver="s3-push",
        received_at=NOW,
        delivery_key="k1",
        content_hash="sha256:" + "a" * 64,
        size_bytes=1,
        objects_inventory=(staged,),
        asserted_record_count=None,
        completeness_mode="manifest",
        trigger={},
        owner_run_id="run1",
        status="in_progress",
        claimed_at=1,
        completed_at=None,
    )
    result = ClaimResult(kind="WON", item=None)
    with pytest.raises(dataclasses.FrozenInstanceError):
        item.status = "completed"  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.kind = "TAKEN_OVER"  # type: ignore[misc]
