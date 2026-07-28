"""Pydantic contracts: FeedConfig, ManifestV1, DeliveryRecord, events, enums — LLD §6.

This module is the single source of truth for every contract in the ingestion
module (LLD §6 preamble). `tools/export_schemas.py` exports these as JSON
Schema into `contracts/` (`make schemas`); `tools/render_registry.py` and CI
both validate `source.yaml` files against `FeedConfig` (`make registry`).

Boundary contracts are pydantic v2 `BaseModel`s (parse, don't
validate-in-place); internal-only values that never cross a serialization
boundary are `@dataclass(frozen=True)` (LLD §7.0 rule 1). LLD §6.1 mandates
cross-field validation via `@model_validator`, each violation a distinct
`raise ValueError(...)` message — pydantic v2 has no non-raising failure
protocol for custom validators, so this is unavoidable. `tools/purity_linter.py`
(`conveyer-4ot.24`) resolves the §6.1/§12.2 tension with two mechanisms:
`raise` (only) is exempt inside any `@field_validator`/`@model_validator`
-decorated method body (any decorator spelling), and exactly two hardcoded
`(file, function)` pairs are exempt from BOTH `purity-try` and `purity-raise`
entirely — `completeness.py::parse_manifest` and this file's
`_check_iana_timezone` (a plain validator-support helper, not itself
decorated, that must catch-and-reraise `zoneinfo.ZoneInfoNotFoundError` as
`ValueError`). `try` stays banned everywhere else in this file, including
inside validator bodies — see `TrailerSpec`'s regex-compilability check,
which uses `contextlib.suppress(re.error)` (a `with` statement, not
`ast.Try`) instead of `try`/`except` to stay inside that budget.

This module deliberately does NOT import `ingestion.core.completeness` — the
import graph stays acyclic (completeness.py's value types are self-contained;
`RegistrationRequest`, which references `CompletenessResult`, lives in
`core/decisions.py`, built in M1).
"""

import contextlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

FEED_ID_PATTERN = r"^[a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*$"
_SECRET_ARN_RE = re.compile(r"^arn:aws:secretsmanager:[a-z0-9-]+:\d{12}:secret:.+$")
# H-3 (security-gate): `partner_principal_arns` is the single field controlling EXTERNAL
# PutObject access to the vestibule (D-15) -- must be a well-formed IAM principal ARN
# (root, or a role/user, never a wildcard) so a source.yaml typo/malice cannot grant a
# foreign account write access. Deploy-account allowlisting is a separate, deferred concern.
# H-3 RESIDUAL: the role/user name segment excludes '*' and whitespace -- AWS itself
# rejects wildcard principals of this shape at policy-apply time (so the un-narrowed
# `.+` was a failed-terraform-apply risk, not an over-grant), but the config gate is
# where that error is cheap and legible. Path-prefixed names (`role/path/name`) stay
# accepted -- `/` is not excluded.
_PARTNER_PRINCIPAL_ARN_RE = re.compile(r"^arn:aws:iam::[0-9]{12}:(root|role/[^\s*]+|user/[^\s*]+)$")
_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTENT_HASH_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
# D-11 closed grammar: "daily" | "weekdays" | "weekly:<mon..sun>" | "monthly:<1..28>".
_EXPECTATION_GRAMMAR_RE = re.compile(
    r"^(daily|weekdays|weekly:(mon|tue|wed|thu|fri|sat|sun)|monthly:([1-9]|1[0-9]|2[0-8]))$"
)
# Strict 24h "HH:MM" — used by `Expectation.by` and any future HH:MM field.
_HH_MM_RE = re.compile(r"^([01][0-9]|2[0-3]):[0-5][0-9]$")


def _check_iana_timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"timezone {value!r} is not a valid IANA timezone name") from exc
    return value


def _is_compilable_regex(pattern: str) -> bool:
    """Pure classifier: does `pattern` compile as a Python regex? Uses
    `contextlib.suppress` (a `with` statement, not `ast.Try`) to swallow
    `re.error` — `try`/`except` stays outside this file's two-entry
    try/raise allowlist (see the module docstring)."""
    compiled: re.Pattern[str] | None = None
    with contextlib.suppress(re.error):
        compiled = re.compile(pattern)
    return compiled is not None


# --- §6.1 `source.yaml` — `FeedConfig` -------------------------------------


class SftpConnection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    secret_ref: str  # full Secrets Manager ARN, validated by regex
    remote_path: str  # absolute remote dir, e.g. /outbound/commissions/
    file_pattern: str = "*"  # fnmatch glob applied to remote basenames

    @field_validator("secret_ref")
    @classmethod
    def _check_secret_ref(cls, value: str) -> str:
        if not _SECRET_ARN_RE.match(value):
            raise ValueError(
                "secret_ref must be a Secrets Manager ARN "
                "(arn:aws:secretsmanager:<region>:<account-id>:secret:<name>)"
            )
        return value


class S3PushConnection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    partner_principal_arns: list[str] = Field(min_length=1)  # external AWS principals granted
    # PutObject on this feed's incoming/ (D-15)

    @field_validator("partner_principal_arns")
    @classmethod
    def _check_partner_principal_arns(cls, value: list[str]) -> list[str]:
        invalid = [arn for arn in value if not _PARTNER_PRINCIPAL_ARN_RE.match(arn)]
        if invalid:
            raise ValueError(
                "partner_principal_arns must each be an IAM principal ARN "
                "(arn:aws:iam::<12-digit-account-id>:root|role/<name>|user/<name>), "
                f"no wildcards: invalid {invalid!r}"
            )
        return value


class Trigger(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schedule: str  # 6-field EventBridge cron, e.g. "cron(0 13 ? * MON-FRI *)"
    timezone: str = "UTC"  # IANA name; validated via zoneinfo

    @field_validator("timezone")
    @classmethod
    def _check_timezone(cls, value: str) -> str:
        return _check_iana_timezone(value)


class Expectation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected: str  # "daily" | "weekdays" | "weekly:mon".."weekly:sun"
    # | "monthly:1".."monthly:28"   (D-11)
    by: str  # "HH:MM" 24h
    timezone: str  # IANA name

    @field_validator("expected")
    @classmethod
    def _check_expected_grammar(cls, value: str) -> str:
        if not _EXPECTATION_GRAMMAR_RE.match(value):
            raise ValueError(
                "expected must match the D-11 grammar: 'daily' | 'weekdays' | "
                "'weekly:<mon..sun>' | 'monthly:<1..28>'"
            )
        return value

    @field_validator("by")
    @classmethod
    def _check_by_format(cls, value: str) -> str:
        if not _HH_MM_RE.match(value):
            raise ValueError("by must be a strict 24h 'HH:MM' time (e.g. '06:00')")
        return value

    @field_validator("timezone")
    @classmethod
    def _check_timezone(cls, value: str) -> str:
        return _check_iana_timezone(value)


class TrailerSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pattern: str  # Python regex the final non-empty line must fully match
    count_group: str | None = None  # named group holding the asserted record count

    @field_validator("pattern")
    @classmethod
    def _check_pattern_compiles(cls, value: str) -> str:
        if not _is_compilable_regex(value):
            raise ValueError(f"pattern must be a valid Python regex: {value!r}")
        return value

    @model_validator(mode="after")
    def _check_count_group_present(self) -> "TrailerSpec":
        if self.count_group is not None:
            # `self.pattern` is guaranteed compilable here: `mode="after"`
            # model validators only run once every field validator on this
            # model has already succeeded (verified in the kernel).
            group_names = re.compile(self.pattern).groupindex
            if self.count_group not in group_names:
                raise ValueError(
                    f"count_group {self.count_group!r} must name a group present in pattern"
                )
        return self


class TimerSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    quiet_window_minutes: int = Field(ge=1, le=1440)
    accepted_risk: str = Field(min_length=20)  # forced prose; reviewed in PR


class Completeness(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal["manifest", "trailer", "timer"]
    manifest_pattern: str = "*.manifest.json"  # used iff mode == manifest
    trailer: TrailerSpec | None = None  # required iff mode == trailer
    timer: TimerSpec | None = None  # required iff mode == timer

    @model_validator(mode="after")
    def _check_completeness(self) -> "Completeness":
        pattern = self.manifest_pattern
        is_star_suffix = (
            len(pattern) > 1
            and pattern.startswith("*")
            and "*" not in pattern[1:]
            and "?" not in pattern
            and "[" not in pattern
        )
        if not is_star_suffix:
            raise ValueError(
                "manifest_pattern must be '*<literal-suffix>' (leading '*', no other wildcards)"
            )
        if self.mode == "trailer" and self.trailer is None:
            raise ValueError("completeness.trailer is required when mode == 'trailer'")
        if self.mode == "timer" and self.timer is None:
            raise ValueError("completeness.timer is required when mode == 'timer'")
        return self


class FeedConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    feed_id: str = Field(pattern=FEED_ID_PATTERN)
    driver: Literal["s3-push", "sftp-pull", "api-pull", "db-unload"]
    pipeline: str  # stage-sequence package ref; opaque string here
    connection: SftpConnection | S3PushConnection | None = None
    # SftpConnection required iff sftp-pull;
    # S3PushConnection required iff s3-push (D-15)
    trigger: Trigger | None = None  # required iff pull driver
    expectation: Expectation | None = None
    completeness: Completeness
    format_hints: dict[str, JsonValue] = {}  # recorded into registry verbatim; the
    # ingestion runtime NEVER interprets these

    @model_validator(mode="after")
    def _check_driver_requirements(self) -> "FeedConfig":
        if self.driver in ("api-pull", "db-unload"):
            raise ValueError("driver not implemented in Phase 1")
        if self.driver == "sftp-pull":
            if not isinstance(self.connection, SftpConnection):
                raise ValueError("sftp-pull requires SftpConnection")
            if self.trigger is None:
                raise ValueError("sftp-pull requires trigger")
        elif self.driver == "s3-push":
            if not isinstance(self.connection, S3PushConnection):
                raise ValueError("s3-push requires S3PushConnection")
            if self.trigger is not None:
                raise ValueError("s3-push forbids trigger")
            if self.completeness.mode == "timer":
                raise ValueError(
                    "timer completeness is not supported for s3-push in Phase 1 (LLD D-10)"
                )
        return self


# --- §6.2 Delivery ledger — Iceberg table -----------------------------------


class DeliveryObject(BaseModel):
    """One entry of `DeliveryRecord.objects` — LLD §6.2's
    `list<struct<name:string, role:string, uri:string?, bytes:long, sha256:string?>>`.
    """

    model_config = ConfigDict(extra="forbid")
    name: str
    role: Literal["data", "manifest"]
    uri: str | None = None  # null when observed (listed) but never staged/streamed
    bytes: int
    sha256: str | None = None  # null when observed (listed) but never staged/streamed


class DeliveryRecord(BaseModel):
    """Mirrors the delivery ledger's column list exactly (LLD §6.2). Nullable
    columns per the population-by-disposition table there.
    """

    model_config = ConfigDict(extra="forbid")
    delivery_id: str  # UUIDv4 minted at acquisition/registration start
    feed_id: str  # <source>/<feed>
    delivery_key: str  # manifest_id, or the single data object's original basename
    batch_id: str | None  # §6.6; null when content was never fully hashed
    content_hash: str | None  # §6.5; null as above
    size_bytes: int | None  # sum of data-object bytes; null as above
    object_uris: list[str]  # data-object S3 URIs; may be empty
    objects: list[DeliveryObject]  # everything observed, including the manifest
    manifest_ref: str | None  # canonical URI of the manifest object (manifest mode)
    asserted_record_count: int | None  # trailer/manifest count assertion (D-5)
    completeness_mode: Literal["manifest", "trailer", "timer"]
    received_at: AwareDatetime  # acquisition start (== canonical-key timestamp, §5)
    recorded_at: AwareDatetime  # when THIS ledger row was appended
    disposition: Literal["registered", "duplicate", "superseded", "incomplete", "unreadable"]
    supersedes: str | None  # delivery_id this delivery corrects
    driver: Literal["s3-push", "sftp-pull"]
    driver_run_id: str  # Lambda request id
    notes: str | None  # human-readable reason for incomplete/unreadable rows

    @field_validator("content_hash")
    @classmethod
    def _check_content_hash(cls, value: str | None) -> str | None:
        if value is not None and not _CONTENT_HASH_RE.match(value):
            raise ValueError("content_hash must match 'sha256:<64 lowercase hex>'")
        return value


# --- §6.3 Conveyer manifest v1 — `ManifestV1` -------------------------------


class ManifestFile(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str  # basename, must be unique within the manifest (enforced in evaluate, M1)
    bytes: int = Field(ge=0)
    sha256: str  # 64 lowercase hex
    record_count: int | None = None

    @field_validator("sha256")
    @classmethod
    def _check_sha256(cls, value: str) -> str:
        if not _SHA256_HEX_RE.match(value):
            raise ValueError("sha256 must be 64 lowercase hex characters")
        return value


class ManifestV1(BaseModel):
    model_config = ConfigDict(extra="allow")  # sources may add fields; we ignore them
    manifest_version: Literal[1]
    manifest_id: str  # partner-unique; becomes delivery_key
    feed_id: str  # must equal the feed being registered, else unreadable
    files: list[ManifestFile] = Field(min_length=1, max_length=1000)  # cap keeps the CAS
    # claim item's objects_inventory under DynamoDB's 400 KB (§8.4)
    created_at: AwareDatetime | None = None


# --- §6.4 Events -------------------------------------------------------------


class DeliveryRegisteredV1(BaseModel):  # DetailType: "delivery-registered"
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    feed_id: str
    delivery_id: str
    batch_id: str
    delivery_key: str
    content_hash: str
    size_bytes: int
    object_uris: list[str]
    received_at: AwareDatetime
    pipeline: str  # copied from FeedConfig for the router


class DeliveryOverdueV1(BaseModel):  # DetailType: "delivery-overdue"
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    feed_id: str
    expectation_date: date  # the delivery-date that is missing
    expected_by: AwareDatetime  # deadline in UTC
    checked_at: AwareDatetime


# --- §6.7 SFTP secret schema -------------------------------------------------


class PasswordAuth(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["password"]
    password: str


class PrivateKeyAuth(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["private_key"]
    private_key_pem: str
    passphrase: str | None = None


class SftpSecret(BaseModel):
    """Secrets Manager `SecretString` shape (LLD §6.7). The *model* is pure;
    *fetching* the secret lives in `effects/secrets.py`.
    """

    model_config = ConfigDict(extra="forbid")
    host: str
    port: int = 22
    username: str
    auth: PasswordAuth | PrivateKeyAuth = Field(discriminator="kind")
    host_key_fingerprint: str | None = None  # SHA-256 base64, OpenSSH format; null → WARNING-logged
    # first-connection trust (effect-side concern)


# --- §8.1 `RegistrationRequest` support — `StagedObject` --------------------


@dataclass(frozen=True)
class StagedObject:
    name: str  # original basename
    role: Literal["data", "manifest"]
    uri: str  # CANONICAL target URI (computed from received_at + delivery_id;
    # the object may not exist there YET)
    bytes: int
    sha256: str
    src_key: str | None  # s3-push: vestibule key to copy from in step D2;
    # sftp-pull: None (already streamed to `uri`)
    src_version_id: str | None = None  # H-1 (security-gate, TOCTOU): the vestibule
    # object's S3 VersionId AT HASH TIME (from `StoreFx.stream_sha256`), threaded to
    # `CopySpec` so the D2 copy is pinned to the exact bytes that were hashed, not
    # whatever is current when the copy runs. Additive/None-tolerant: sftp-pull
    # objects (`src_key is None`) and the s3-push manifest object's own copy never
    # set it (unaffected, same as before this fix).
    src_etag: str | None = None  # fallback pin (`CopySourceIfMatch`) when no
    # VersionId is available (an unversioned bucket) -- also from hash time.


# --- §8.4 The turnstile — result types --------------------------------------


@dataclass(frozen=True)
class ClaimItem:  # mirror of the DynamoDB item, typed
    feed_id: str
    batch_id: str
    delivery_id: str
    driver: str
    received_at: datetime
    delivery_key: str
    content_hash: str
    size_bytes: int
    objects_inventory: tuple[StagedObject, ...]
    asserted_record_count: int | None
    completeness_mode: str
    trigger: dict[str, JsonValue]
    owner_run_id: str
    status: str
    claimed_at: int
    completed_at: int | None


@dataclass(frozen=True)
class ClaimResult:
    kind: Literal["WON", "LOST_COMPLETED", "LOST_IN_PROGRESS", "TAKEN_OVER"]
    item: ClaimItem | None  # None iff WON (this run's request IS the item);
    # on TAKEN_OVER, the DEAD run's item — the identity to adopt


# --- §7.6 The driver contract — `Window`, `DeliveryOutcome` -----------------


@dataclass(frozen=True)
class Window:  # half-open [start, end); None = derive from ledger fold
    start: datetime | None
    end: datetime | None


@dataclass(frozen=True)
class DeliveryOutcome:
    delivery_id: str
    batch_id: str | None
    disposition: str
    feed_id: str
    delivery_key: str
