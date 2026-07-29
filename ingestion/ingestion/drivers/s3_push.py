"""s3-push: event -> `RegistrationRequest` (vestibule move via the registrar)
-- LLD §8.2.

Exports one function, `acquire` -- the driver contract's one abstraction
(§7.6): `AcquireFn = Callable[[FeedConfig, Window, Effects], list[DeliveryOutcome]]`,
except s3-push's `acquire` is explicitly called out in the LLD as an
"event-shaped wrapper" (its input is a parsed S3-notification event, not a
`(FeedConfig, Window)` pair -- "s3-push is the degenerate driver: its
'acquire' is the registrar's vestibule move, driven by events rather than
windows", §7.6). All registration-worthy logic lives here, not in the
entrypoint (§7.1: "Handlers contain wiring only"); the entrypoint only
parses the Lambda trigger payload, builds `Effects`, and calls this
function with `context.aws_request_id` as `driver_run_id` -- kept OUT of
this module's signature so the driver never depends on a Lambda `context`
object (easier to test, and consistent with the driver contract taking
plain values).

No canonical write happens anywhere in this module (§8.2 step 3: "No
canonical write happens in this step") -- every `StagedObject.uri` is a
target that may not exist yet; the vestibule -> canonical copy is `execute`
step E1, run only *after* the turnstile claim, inside
`registration.registrar` (§8.5) -- so a duplicate-race loser never copies.
Vestibule objects are never deleted by this module (D-3; lifecycle expires
them after 14 d).

Per §7.6's abstraction rule ("if s3_push.py and sftp_pull.py need
driver-specific branches inside registration/ or effects/, the interface is
wrong"), this module calls only `registration.registrar.register_delivery`/
`record_nondelivery` and `Effects`' declared capabilities -- never
`ledger.append`/`emit` directly (enforced by the ownership grep golden
test).
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Any, Literal, cast

from ingestion.core.completeness import (
    Defect,
    ObjectStat,
    evaluate_manifest,
    evaluate_trailer,
    parse_manifest,
)
from ingestion.core.decisions import RegistrationRequest
from ingestion.core.model import DeliveryOutcome, FeedConfig, ManifestV1, StagedObject, TrailerSpec
from ingestion.core.naming import canonical_uri as _canonical_uri
from ingestion.core.naming import is_clean_object_name
from ingestion.effects.records import Effects, ObjectSummary
from ingestion.effects.registry import RegistryCache
from ingestion.effects.registry import load_feed_registry as _load_feed_configs
from ingestion.registration.registrar import record_nondelivery, register_delivery

_logger = logging.getLogger(__name__)

_VESTIBULE_SEGMENT = "incoming"
_MANIFEST_MAX_BYTES = 1024 * 1024  # 1 MiB cap, §8.2
_TRAILER_TAIL_BYTES = 4096  # §7.3: "last 4096 bytes of the single data object"

# §6.8: registry cached per Lambda container for <= 60 s, never re-read from
# the filesystem. This module-level dict IS the container-lifetime cache --
# every real invocation (via `entrypoints/registrar_s3.py`) shares it across
# a warm container without passing `registry_cache` explicitly. Tests pass
# their own fresh dict (`registry_cache={}`) for isolation between runs --
# see the golden suite -- rather than relying on this module's own state.
_DEFAULT_REGISTRY_CACHE: RegistryCache = {}


# --- small pure-ish helpers (no I/O; plain string/URI manipulation) --------
# `_canonical_uri` above is a re-export of `core.naming.canonical_uri`
# (critique-gate F-2: previously duplicated verbatim here, in
# `drivers/sftp_pull.py`, `absence/detector.py`, and
# `core/decisions.py::_parse_s3_uri`) -- kept under its original name so
# every existing internal call site (and every golden test that reaches
# into this module's "private" helper by the established convention, e.g.
# `s3_push._canonical_uri(...)`) keeps working unchanged.


def _basename(key: str) -> str:
    return key.rsplit("/", 1)[-1]


def _vestibule_prefix(feed_id: str) -> str:
    return f"{feed_id}/{_VESTIBULE_SEGMENT}/"


def _derive_feed_id(key: str) -> str | None:
    """`<source>/<feed>/incoming/<partner-chosen-name>` -> `<source>/<feed>`;
    `None` if `key` doesn't have that shape (misroute -- §8.2 step 1: "the
    rule pattern should make this impossible").
    """
    parts = key.split("/")
    if len(parts) < 4 or parts[2] != _VESTIBULE_SEGMENT:
        return None
    return f"{parts[0]}/{parts[1]}"


def _is_manifest_key(key: str, feed: FeedConfig) -> bool:
    """`manifest_pattern` is `*<literal-suffix>` (validated on `FeedConfig`,
    §6.1) -- strip the leading `*` and check the basename's suffix.
    """
    suffix = feed.completeness.manifest_pattern[1:]
    return _basename(key).endswith(suffix)


def _parse_event(event: dict[str, Any]) -> tuple[str, str]:
    """S3 "Object Created" EventBridge notification (§10.7's rule fires on
    this shape) -> (bucket, key).
    """
    detail = event["detail"]
    return detail["bucket"]["name"], detail["object"]["key"]


# --- registry loading (§6.8) -------------------------------------------------
# `_load_feed_configs` above is a re-export of `effects.registry.
# load_feed_registry` (critique-gate F-2: previously duplicated verbatim
# here, in `drivers/sftp_pull.py::load_feed_config`, and in
# `absence/detector.py::_load_feed_registry`).


# --- manifest mode (§8.2 step 2) --------------------------------------------


def _present_objects(
    fx: Effects, bucket: str, manifest: ManifestV1, vestibule_by_name: dict[str, ObjectSummary]
) -> tuple[list[ObjectStat], dict[str, tuple[str | None, str | None]]]:
    """`stream_sha256` every manifest-declared file that IS present in the
    vestibule listing (§8.2: "stream_sha256 declared-present files") --
    declared-but-absent files are simply omitted; `evaluate_manifest` itself
    detects their absence and reports `incomplete`.

    Also returns a `name -> (version_id, etag)` side mapping -- H-1
    (security-gate, TOCTOU): `ObjectStat` (`core/completeness.py`) stays a
    driver-agnostic "what was observed" value with no S3-specific fields (it
    is shared with sftp-pull's remote-file stats, which have no S3 version
    concept at all); this mapping is the s3-push-local vehicle that carries
    `stream_sha256`'s T0 version/etag capture through to
    `_manifest_staged_objects`'s `StagedObject` construction instead.
    """
    present: list[ObjectStat] = []
    versions_by_name: dict[str, tuple[str | None, str | None]] = {}
    for f in manifest.files:
        summary = vestibule_by_name.get(f.name)
        if summary is None:
            continue
        sha256_hex, actual_bytes, version_id, etag = fx.store.stream_sha256(bucket, summary.key)
        present.append(ObjectStat(name=f.name, bytes=actual_bytes, sha256=sha256_hex))
        versions_by_name[f.name] = (version_id, etag)
    return present, versions_by_name


def _manifest_staged_objects(
    landing_bucket: str,
    feed: FeedConfig,
    manifest: ManifestV1,
    present: list[ObjectStat],
    versions_by_name: dict[str, tuple[str | None, str | None]],
    vestibule_by_name: dict[str, ObjectSummary],
    manifest_key: str,
    raw_manifest: bytes,
    manifest_version_id: str | None,
    manifest_etag: str | None,
    received_at: datetime,
    delivery_id: str,
) -> list[StagedObject]:
    """Data objects (role="data") + the manifest object itself
    (role="manifest") -- `StagedObject.sha256` is non-nullable, so the
    manifest's own bytes (already read into `raw_manifest`) are hashed here.

    H-1 RESIDUAL (security-gate, TOCTOU): the manifest object's OWN copy is
    now pinned too -- `manifest_version_id`/`manifest_etag` come from
    `fx.store.get_bytes_pinned`'s capture of the SAME `GetObject` response
    `raw_manifest` was read from (`_acquire_manifest`), so `copy_verbatim`'s
    later D2 copy is fenced to the exact bytes `parse_manifest`/
    `evaluate_manifest` actually verified -- closing the same TOCTOU window
    `stream_sha256`'s callers already close for data objects.
    """
    stat_by_name = {s.name: s for s in present}
    # `present` is built by `_present_objects` exclusively from real
    # `stream_sha256` results (never `None`) -- a genuine type-level
    # narrowing, not a "trust me" ignore (matches `core/decisions.py`'s
    # established `cast` usage for the same shape of guarantee).
    data_staged = [
        StagedObject(
            name=f.name,
            role="data",
            uri=_canonical_uri(landing_bucket, feed.feed_id, received_at, delivery_id, f.name),
            bytes=stat_by_name[f.name].bytes,
            sha256=cast(str, stat_by_name[f.name].sha256),
            src_key=vestibule_by_name[f.name].key,
            src_version_id=versions_by_name[f.name][0],
            src_etag=versions_by_name[f.name][1],
        )
        for f in manifest.files
        if f.name in stat_by_name
    ]
    manifest_name = _basename(manifest_key)
    manifest_staged = StagedObject(
        name=manifest_name,
        role="manifest",
        uri=_canonical_uri(landing_bucket, feed.feed_id, received_at, delivery_id, manifest_name),
        bytes=len(raw_manifest),
        sha256=hashlib.sha256(raw_manifest).hexdigest(),
        src_key=manifest_key,
        src_version_id=manifest_version_id,
        src_etag=manifest_etag,
    )
    return [*data_staged, manifest_staged]


def _acquire_manifest(
    feed: FeedConfig, bucket: str, key: str, driver_run_id: str, fx: Effects
) -> DeliveryOutcome:
    """§8.2 step 2, manifest mode. Returns the single `DeliveryOutcome` from
    whichever of `record_nondelivery`/`register_delivery` fired.
    """
    # H-1 RESIDUAL (security-gate, TOCTOU): `get_bytes_pinned` (not plain `get_bytes`)
    # captures `VersionId`/`ETag` from the SAME `GetObject` response `raw` is read from,
    # so the manifest's own vestibule->canonical copy can be pinned identically to a
    # data object's (`_manifest_staged_objects`, below).
    read_result = fx.store.get_bytes_pinned(bucket, key, _MANIFEST_MAX_BYTES)
    if isinstance(read_result, Defect):
        # Manifest itself unreadable (too large): delivery_key falls back to
        # the manifest's own basename (§8.2), nothing was observed yet.
        return record_nondelivery(
            feed, _basename(key), "unreadable", [], read_result.reason, driver_run_id, fx
        )
    raw, manifest_version_id, manifest_etag = read_result

    parsed = parse_manifest(raw)
    if isinstance(parsed, Defect):
        return record_nondelivery(
            feed, _basename(key), "unreadable", [], parsed.reason, driver_run_id, fx
        )
    manifest = parsed

    vestibule_listing = fx.store.list_prefix(bucket, _vestibule_prefix(feed.feed_id))
    vestibule_by_name = {o.name: o for o in vestibule_listing}
    present, versions_by_name = _present_objects(fx, bucket, manifest, vestibule_by_name)
    completeness = evaluate_manifest(manifest, present, feed.feed_id)

    if completeness.verdict != "complete":
        # Incomplete -> stop, no retry (§8.2: the partner uploads the
        # manifest last, so a manifest-first race is a partner defect; the
        # append-on-change rule absorbs duplicate events meanwhile).
        disposition: Literal["incomplete", "unreadable"] = (
            "incomplete" if completeness.verdict == "incomplete" else "unreadable"
        )
        return record_nondelivery(
            feed, manifest.manifest_id, disposition, present, completeness.reason, driver_run_id, fx
        )

    received_at = fx.now()
    delivery_id = fx.new_delivery_id()
    objects = _manifest_staged_objects(
        # M-2 (security-gate): the CANONICAL destination bucket for every
        # `StagedObject.uri` comes from `fx.config` (trusted config), never the
        # event-supplied `bucket` -- `bucket` is only ever used above for the
        # vestibule/source reads (`list_prefix`/`_present_objects`).
        fx.config.landing_bucket,
        feed,
        manifest,
        present,
        versions_by_name,
        vestibule_by_name,
        key,
        raw,
        manifest_version_id,
        manifest_etag,
        received_at,
        delivery_id,
    )
    req = RegistrationRequest(
        feed=feed,
        delivery_id=delivery_id,
        delivery_key=manifest.manifest_id,
        received_at=received_at,
        driver="s3-push",
        driver_run_id=driver_run_id,
        completeness=completeness,
        objects=objects,
    )
    return register_delivery(req, fx)


# --- trailer mode (§8.2 step 2) ---------------------------------------------


def _acquire_trailer(
    feed: FeedConfig, bucket: str, key: str, driver_run_id: str, fx: Effects
) -> DeliveryOutcome:
    """§8.2 step 2, trailer mode: a single object IS the delivery.

    Only reached for a `mode == "trailer"` feed: the EventBridge rule for a
    `mode == "manifest"` feed carries a suffix condition that admits ONLY
    manifest-suffixed keys (§10.7), so a manifest-mode feed's individual
    data-part uploads never trigger this Lambda at all -- `feed.completeness
    .trailer` being non-`None` here is an infra-routing guarantee, the same
    "the rule pattern should make this impossible" trust boundary `§8.2`
    already leans on for the misroute case.
    """
    sha256_hex, size_bytes, version_id, etag = fx.store.stream_sha256(bucket, key)
    tail_bytes = fx.store.get_tail(bucket, key, _TRAILER_TAIL_BYTES)
    tail_text = tail_bytes.decode("utf-8", errors="replace")
    completeness = evaluate_trailer(tail_text, cast(TrailerSpec, feed.completeness.trailer))
    delivery_key = _basename(key)
    observed = [ObjectStat(name=delivery_key, bytes=size_bytes, sha256=sha256_hex)]

    if completeness.verdict != "complete":
        disposition: Literal["incomplete", "unreadable"] = (
            "incomplete" if completeness.verdict == "incomplete" else "unreadable"
        )
        return record_nondelivery(
            feed, delivery_key, disposition, observed, completeness.reason, driver_run_id, fx
        )

    received_at = fx.now()
    delivery_id = fx.new_delivery_id()
    staged = StagedObject(
        name=delivery_key,
        role="data",
        # M-2 (security-gate): canonical URI built from `fx.config.landing_bucket`
        # (trusted), never the event-supplied `bucket`.
        uri=_canonical_uri(
            fx.config.landing_bucket, feed.feed_id, received_at, delivery_id, delivery_key
        ),
        bytes=size_bytes,
        sha256=sha256_hex,
        src_key=key,
        # H-1 (security-gate, TOCTOU): pin the D2 copy to what was just hashed.
        src_version_id=version_id,
        src_etag=etag,
    )
    req = RegistrationRequest(
        feed=feed,
        delivery_id=delivery_id,
        delivery_key=delivery_key,
        received_at=received_at,
        driver="s3-push",
        driver_run_id=driver_run_id,
        completeness=completeness,
        objects=[staged],
    )
    return register_delivery(req, fx)


# --- entry point ("acquire", §7.6) ------------------------------------------


def acquire(
    event: dict[str, Any],
    fx: Effects,
    driver_run_id: str,
    *,
    registry_cache: RegistryCache | None = None,
) -> list[DeliveryOutcome]:
    """S3-push "acquire": event -> zero-or-one `DeliveryOutcome` (§8.2).

    `registry_cache` defaults to this module's own container-lifetime cache
    (`_DEFAULT_REGISTRY_CACHE`) -- production callers never pass it. Tests
    pass a fresh `{}` per test to get an isolated registry read regardless
    of `fx.now()` (golden tests reuse the same starting clock across
    independent test functions, so identity/time-based cache reuse would be
    fragile -- an explicit dict avoids that entirely).
    """
    cache = _DEFAULT_REGISTRY_CACHE if registry_cache is None else registry_cache
    bucket, key = _parse_event(event)

    if bucket != fx.config.landing_bucket:
        # M-2 (security-gate): the event-supplied bucket is UNTRUSTED -- without this
        # gate it flowed into both the source read AND (via `_manifest_staged_objects`/
        # `_acquire_trailer`'s canonical-URI construction) the destination bucket in
        # every `StagedObject.uri`, ledger `object_uris`, and the `delivery-registered`
        # event payload, letting a forged event register a delivery pointing at an
        # attacker-chosen bucket. Same shape as the misroute path below: log + return.
        _logger.error(
            "misrouted event: bucket %r does not match configured landing bucket %r",
            bucket,
            fx.config.landing_bucket,
        )
        return []

    feed_id = _derive_feed_id(key)
    if feed_id is None:
        _logger.error("misrouted event: key %r is not under any feed's incoming/ prefix", key)
        return []

    feed = _load_feed_configs(fx, cache).get(feed_id)
    if feed is None:
        _logger.error("misrouted event: no registered feed for feed_id %r (key %r)", feed_id, key)
        return []

    if not is_clean_object_name(_basename(key)):
        # conveyer-nvh.48.11: the event-supplied key's basename is not a
        # single clean object-name segment (e.g. a forged/misrouted event
        # whose key ends in `incoming/..`). This is event-shaped noise, not
        # a partner delivery -- same "log + return []" shape as the
        # misroute guards above, no `record_nondelivery` (no ledger row).
        _logger.error("misrouted event: key %r has an unsafe/non-canonical basename", key)
        return []

    _logger.info(
        "s3-push acquire start",
        extra={"feed_id": feed_id, "driver_run_id": driver_run_id},
    )
    if _is_manifest_key(key, feed):
        outcome = _acquire_manifest(feed, bucket, key, driver_run_id, fx)
    else:
        outcome = _acquire_trailer(feed, bucket, key, driver_run_id, fx)
    return [outcome]
