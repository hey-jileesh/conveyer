"""sftp-pull: acquire(feed, window, fx) -- LLD §9.2.

Exports `acquire`, the driver contract's one abstraction (§7.6). Like
`drivers/s3_push.py`'s "event-shaped wrapper" deviation, this module's
`acquire` extends the bare `AcquireFn = Callable[[FeedConfig, Window,
Effects], list[DeliveryOutcome]]` shape with three additional parameters the
declared type has no slot for: `driver_run_id` (mirrors s3_push's own
deviation -- `Effects` carries no ambient "current run" identity, §7.7),
`force` (bypasses the already-acquired/defective selection filters, §7.6's
operator-re-pull contract), and `resume_batch_id` (§9.3's stuck-claim sweep
resume, step 0 of §9.2's pseudocode). All three are keyword-only except
`driver_run_id`, matching s3_push's `registry_cache` precedent for the same
kind of extension.

Effects permitted inside `acquire`, per §7.6: writes to this feed's
canonical landing prefix (`fx.store.stream_upload`), and ledger appends
**via `registration.registrar.register_delivery`/`record_nondelivery`
only**. This module never calls `fx.ledger.append`/`fx.emit` directly (the
golden ownership test greps for this, same as `drivers/s3_push.py`).

**Ordering note (§9.2, deliberate, differs from s3-push):** every object is
streamed to its *canonical* key BEFORE the turnstile claim, because the
content hash -- hence `batch_id` -- is only known once the bytes have moved.
`register_delivery` itself still calls `fx.cas.claim` first thing (§8.5
step A); by the time it does, this module has already finished streaming.
A duplicate (`force` re-pull, or a defective-then-fixed file whose content
happens to match an already-registered delivery) therefore leaves an
orphaned canonical prefix that no `registered` row references -- harmless
by design (§9.2: "the ledger is the index... the duplicate row's objects
column documents the loser's URIs").

Manifest-mode candidates are pre-filtered against the CURRENT remote
listing (name/byte-size only, no streaming) before any bytes move --
missing or size-mismatched parts short-circuit straight to
`record_nondelivery(incomplete)` without ever touching the network for the
parts, the accepted cost/benefit trade for a real (metered, potentially
slow) SFTP transfer, unlike s3-push's cheap intra-region `stream_sha256`.
sha256 verification against the manifest's declared per-file hash happens
DURING streaming (hashed en route via `fx.store.stream_upload`); any
mismatch abandons the whole candidate via `record_nondelivery(unreadable)`
without registering, orphaning whatever was already streamed (accepted per
the ordering note above).

`acquire`'s manifest and trailer/timer candidate-processing loops share
ONE implementation, `_process_candidates` (critique-gate F-6): only
candidate SELECTION (`windows.select_manifests` vs `windows.
select_candidates`, genuinely different signatures) and the per-candidate
acquire function differ by mode; the §9.2 step 5 budget check, carry-over
logging, and per-delivery failure isolation live in exactly one place,
called once regardless of `feed.completeness.mode`.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from typing import Literal, cast

from ingestion import observability
from ingestion.core import folds, windows
from ingestion.core.completeness import (
    CompletenessResult,
    Defect,
    ObjectStat,
    evaluate_manifest,
    evaluate_trailer,
    parse_manifest,
)
from ingestion.core.decisions import RegistrationRequest
from ingestion.core.model import (
    ClaimItem,
    DeliveryOutcome,
    FeedConfig,
    SftpConnection,
    StagedObject,
    TrailerSpec,
    Window,
)
from ingestion.core.naming import canonical_prefix, is_clean_object_name
from ingestion.core.naming import canonical_uri as _canonical_uri
from ingestion.core.naming import split_s3_uri as _split_s3_uri
from ingestion.core.windows import RemoteFile
from ingestion.effects.records import Effects, SftpFx, TransientError
from ingestion.effects.registry import RegistryCache
from ingestion.effects.registry import load_feed_registry as _load_feed_registry
from ingestion.registration.registrar import record_nondelivery, register_delivery

_logger = logging.getLogger(__name__)

_MANIFEST_MAX_BYTES = 1024 * 1024  # 1 MiB cap, §9.2 ("remote manifest read ≤ 1 MiB")
_TRAILER_TAIL_BYTES = 4096  # §7.3: "last 4096 bytes of the single data object"

# §9.2 step 1's `lookback` and step 5's `budget` vars -- "read effect-side,
# simplest gap resolution": neither has a `RuntimeConfig` field (§7.2), so
# each is its own `CONVEYER_*` env var, read directly in this driver module
# (outside `core/`, so `os` is not purity-banned here, §12.2).
_LOOKBACK_DAYS_ENV = "CONVEYER_SFTP_LOOKBACK_DAYS"
_DEFAULT_LOOKBACK_DAYS = 45
_BUDGET_BYTES_ENV = "CONVEYER_DRIVER_BYTES_BUDGET"  # matches terraform's `driver_bytes_budget`
_DEFAULT_BUDGET_BYTES = 5 * 1024**3  # 5 GiB
_BUDGET_ELAPSED_SECONDS = 600  # 10 min; not env-configurable per the brief

# §6.8: registry cached per Lambda container for <= 60 s -- same rationale
# and shape as `drivers/s3_push.py::_DEFAULT_REGISTRY_CACHE` (each Lambda
# function is a separate process with its own module state, §7.7, so each
# driver module owning its own container-lifetime default `cache` is
# harmless; the loader body itself is now a single shared implementation,
# `effects/registry.py::load_feed_registry`, critique-gate F-2).
_DEFAULT_REGISTRY_CACHE: RegistryCache = {}


# --- small pure-ish helpers (no I/O; plain string/URI manipulation) --------
# `_canonical_uri`/`_split_s3_uri` above are re-exports of `core.naming`'s
# pure implementations (critique-gate F-2: previously duplicated verbatim
# here, in `drivers/s3_push.py`, `absence/detector.py`, and
# `core/decisions.py::_parse_s3_uri`) -- kept under their original names so
# every existing internal call site (and every golden test that reaches
# into this module's "private" helpers by the established convention, e.g.
# `sftp_pull._canonical_uri(...)`) keeps working unchanged.


def _remote_file_path(remote_path: str, name: str) -> str:
    """`remote_path` (absolute remote dir, e.g. `/outbound/commissions/`) +
    a basename from `listdir`/manifest declaration -> the full remote path
    `read_chunks` expects (matches `effects/sftp.py::_read_chunks`'s
    `sftp.open(path, "rb")`, which resolves relative to the SFTP server's
    own root, not a per-call cwd). Every `name` reaching this function now
    comes from a listing already filtered by `windows.select_candidates`/
    `select_manifests`'s step-0 `is_clean_object_name` guard (conveyer
    nvh.48.11), so remote-side path traversal via a hostile listing entry
    (e.g. `../../etc/passwd`) is closed as a side effect of that filter,
    without this function needing any check of its own."""
    base = remote_path if remote_path.endswith("/") else remote_path + "/"
    return base + name


def _in_window(mtime: datetime, window: Window) -> bool:
    """Half-open `[start, end)` per `Window`'s own docstring (§7.6)."""
    if window.start is not None and mtime < window.start:
        return False
    if window.end is not None and mtime >= window.end:
        return False
    return True


def _lookback_days() -> int:
    return int(os.environ.get(_LOOKBACK_DAYS_ENV, str(_DEFAULT_LOOKBACK_DAYS)))


def _budget_bytes() -> int:
    return int(os.environ.get(_BUDGET_BYTES_ENV, str(_DEFAULT_BUDGET_BYTES)))


def _budget_exceeded(now: datetime, start: datetime, bytes_so_far: int, budget_bytes: int) -> bool:
    """§9.2 step 5: "stop starting new deliveries once elapsed > 10 min or
    bytes > budget var"."""
    elapsed_s = (now - start).total_seconds()
    return elapsed_s > _BUDGET_ELAPSED_SECONDS or bytes_so_far > budget_bytes


def _log_carry_over(feed_id: str, remaining: int) -> None:
    _logger.warning(
        "sftp-pull budget exhausted; %d candidate(s) carried over to the next run",
        remaining,
        extra={"feed_id": feed_id},
    )
    observability.emit_metric("DriverCarryOver", remaining, feed_id)


def _log_unsafe_remote_names(feed_id: str, count: int) -> None:
    """conveyer-nvh.48.11: `count` of THIS listing's entries failed
    `is_clean_object_name`, counted pre-window (before the operator's
    mtime window is applied) so a hostile or misbehaving remote server is
    visible even on a windowed re-pull, rather than only reflecting
    `windows.select_candidates`/`select_manifests`'s step-0 filter on the
    narrowed listing -- logged/metered here (COUNT and `feed_id` ONLY,
    never the hostile names themselves, same as `record_nondelivery`'s
    (loc, type)-only notes for a manifest-declared traversal name) so an
    operator can see the defect without any attacker-controlled string
    ever reaching a log line or the ledger.
    """
    _logger.warning(
        "sftp-pull listing contained %d unsafe/non-canonical remote name(s); "
        "dropped before selection",
        count,
        extra={"feed_id": feed_id},
    )
    observability.emit_metric("UnsafeRemoteName", count, feed_id)


def _read_capped(chunks: Iterator[bytes], max_bytes: int) -> bytes | Defect:
    """Consume an `Iterator[bytes]` up to `max_bytes` -- a `Defect` (value,
    §7.0 rule 4), never an exception, if the stream exceeds the cap. Used
    for the manifest read (`SftpFx` has no `StoreFx.get_bytes`-style capped
    read; the cap must be enforced while consuming the iterator)."""
    buf = bytearray()
    for chunk in chunks:
        buf.extend(chunk)
        if len(buf) > max_bytes:
            return Defect(reason=f"manifest exceeds max_bytes cap {max_bytes}")
    return bytes(buf)


# --- registry loading (§6.8), single-feed variant for a per-feed Lambda ----


def load_feed_config(
    fx: Effects,
    feed_id: str,
    *,
    registry_cache: RegistryCache | None = None,
) -> FeedConfig | None:
    """Single-feed convenience wrapper around `effects.registry.
    load_feed_registry` (§6.8, critique-gate F-2 -- previously this
    function duplicated the whole read/parse/cache body itself). Exported
    (not `_`-prefixed) so `entrypoints/sftp_pull.py`'s wiring-only handler
    can call it without duplicating registry-parsing logic itself (§7.1:
    "any logic in an entrypoint is a review defect")."""
    cache = _DEFAULT_REGISTRY_CACHE if registry_cache is None else registry_cache
    return _load_feed_registry(fx, cache).get(feed_id)


# --- step 0: sweep-resume (§9.2, §9.3) --------------------------------------


def _verify_canonical_objects_present(feed: FeedConfig, item: ClaimItem, fx: Effects) -> None:
    """ "verify claim.item.objects_inventory all present at their canonical
    uris (list)" (§9.2 step 0) -- a single `list_prefix` of this delivery's
    fixed canonical prefix covers every object in one call. By the time a
    resumable `ClaimItem` exists at all, every object was already streamed
    to canonical BEFORE the claim (the ordering note above) -- so a mismatch
    here signals a genuinely inconsistent system state, not an expected
    race; `TransientError` (retry / DLQ / alarm, §7.3: "anything else is a
    bug and follows the same path") is the correct response, not a silent
    skip.
    """
    prefix = canonical_prefix(feed.feed_id, item.received_at, item.delivery_id)
    listing = fx.store.list_prefix(fx.config.landing_bucket, prefix)
    present = {(o.name, o.bytes) for o in listing}
    expected = {(o.name, o.bytes) for o in item.objects_inventory}
    missing = expected - present
    if missing:
        raise TransientError(
            f"resume verification failed for batch {item.batch_id!r}: canonical objects "
            f"missing at {prefix!r}: {sorted(missing)}"
        )


def _resume(
    feed: FeedConfig, resume_batch_id: str, driver_run_id: str, fx: Effects
) -> list[DeliveryOutcome]:
    """§9.2 step 0 / §9.3: "load claim item... rebuild RegistrationRequest
    from the item... register_delivery (enters at claim(); gets
    TAKEN_OVER; resumes D3-D5)".

    Looks the item up by its exact `(feed_id, resume_batch_id)` pk via
    `fx.cas.get_claim` -- a GetItem, not `fx.cas.sweep_stale`'s Scan (M-1,
    security-gate): the per-feed sftp-pull driver role does not, and must
    not, hold `dynamodb:Scan` on the CAS table -- `LeadingKeys` can
    constrain `GetItem`/`Query` to this feed's own items but cannot
    constrain a `Scan`, so granting it would let a compromised driver read
    every OTHER feed's in-flight claims, breaking the per-feed CAS
    blast-radius wall. `sweep_stale` stays exclusively the absence
    detector's (§9.3's own sweep is what discovers `resume_batch_id` in the
    first place and invokes this driver with it). By construction, this
    resume invocation is only ever triggered for a claim the sweep itself
    just found stale, so the item is expected to still be there. If it is
    not (already resolved by a racing recovery, or its TTL reaped it), that
    is a safe no-op -- log and return `[]`, not a raised error.
    """
    item = fx.cas.get_claim(feed.feed_id, resume_batch_id)
    if item is None:
        _logger.warning(
            "resume_batch_id %r not found among stale claims for feed %r -- already resolved",
            resume_batch_id,
            feed.feed_id,
            extra={"feed_id": feed.feed_id, "driver_run_id": driver_run_id},
        )
        return []

    _verify_canonical_objects_present(feed, item, fx)

    data_names = tuple(o.name for o in item.objects_inventory if o.role == "data")
    completeness = CompletenessResult(
        verdict="complete",
        reason=None,
        asserted_record_count=item.asserted_record_count,
        data_object_names=data_names,
    )
    req = RegistrationRequest(
        feed=feed,
        delivery_id=item.delivery_id,
        delivery_key=item.delivery_key,
        received_at=item.received_at,
        driver=item.driver,
        driver_run_id=driver_run_id,  # THIS invocation's identity, not item.owner_run_id
        completeness=completeness,
        objects=list(item.objects_inventory),
    )
    return [register_delivery(req, fx)]


# --- manifest mode (§9.2 step 4) --------------------------------------------


def _acquire_manifest_candidate(
    feed: FeedConfig,
    candidate: RemoteFile,
    listing_by_name: dict[str, RemoteFile],
    driver_run_id: str,
    fx: Effects,
    sftp: SftpFx,
) -> tuple[DeliveryOutcome, int]:
    """One `select_manifests` candidate -> `(DeliveryOutcome, bytes_streamed)`.
    `bytes_streamed` feeds the caller's budget accounting (§9.2 step 5) even
    on a non-registered outcome (streaming may have happened before a
    sha256-mismatch abandon).
    """
    connection = cast(SftpConnection, feed.connection)
    manifest_path = _remote_file_path(connection.remote_path, candidate.name)
    raw = _read_capped(sftp.read_chunks(manifest_path), _MANIFEST_MAX_BYTES)
    if isinstance(raw, Defect):
        outcome = record_nondelivery(
            feed, candidate.name, "unreadable", [], raw.reason, driver_run_id, fx
        )
        return outcome, 0

    parsed = parse_manifest(raw)
    if isinstance(parsed, Defect):
        outcome = record_nondelivery(
            feed, candidate.name, "unreadable", [], parsed.reason, driver_run_id, fx
        )
        return outcome, 0
    manifest = parsed

    # Pre-stream completeness check against the CURRENT listing (§9.2:
    # "parts missing/size-mismatched in listing") -- avoids pulling
    # anything over the wire for a delivery that cannot be complete.
    pre_observed: list[ObjectStat] = []
    incomplete_reasons: list[str] = []
    for declared in manifest.files:
        remote = listing_by_name.get(declared.name)
        if remote is None:
            incomplete_reasons.append(f"{declared.name} not present")
            continue
        pre_observed.append(ObjectStat(name=declared.name, bytes=remote.bytes, sha256=None))
        if remote.bytes != declared.bytes:
            incomplete_reasons.append(
                f"{declared.name} byte-size mismatch "
                f"(expected {declared.bytes}, observed {remote.bytes})"
            )
    if incomplete_reasons:
        outcome = record_nondelivery(
            feed,
            manifest.manifest_id,
            "incomplete",
            pre_observed,
            "; ".join(incomplete_reasons),
            driver_run_id,
            fx,
        )
        return outcome, 0

    # All parts present with matching size -- stream each part AND the
    # manifest to canonical, hashed en route (§9.2).
    received_at = fx.now()
    delivery_id = fx.new_delivery_id()
    landing_bucket = fx.config.landing_bucket
    present: list[ObjectStat] = []
    staged: list[StagedObject] = []
    bytes_streamed = 0
    for declared in manifest.files:
        remote_path = _remote_file_path(connection.remote_path, declared.name)
        canonical = _canonical_uri(
            landing_bucket, feed.feed_id, received_at, delivery_id, declared.name
        )
        bucket, key = _split_s3_uri(canonical)
        sha256_hex, total_bytes = fx.store.stream_upload(sftp.read_chunks(remote_path), bucket, key)
        bytes_streamed += total_bytes
        if sha256_hex != declared.sha256:
            outcome = record_nondelivery(
                feed,
                manifest.manifest_id,
                "unreadable",
                [ObjectStat(name=declared.name, bytes=total_bytes, sha256=sha256_hex)],
                f"{declared.name} sha256 mismatch "
                f"(expected {declared.sha256}, observed {sha256_hex})",
                driver_run_id,
                fx,
            )
            return outcome, bytes_streamed  # abandon -- orphaned canonical objects, §9.2 note
        present.append(ObjectStat(name=declared.name, bytes=total_bytes, sha256=sha256_hex))
        staged.append(
            StagedObject(
                name=declared.name,
                role="data",
                uri=canonical,
                bytes=total_bytes,
                sha256=sha256_hex,
                src_key=None,
            )
        )

    manifest_canonical = _canonical_uri(
        landing_bucket, feed.feed_id, received_at, delivery_id, candidate.name
    )
    m_bucket, m_key = _split_s3_uri(manifest_canonical)
    manifest_sha256, manifest_bytes = fx.store.stream_upload(iter([raw]), m_bucket, m_key)
    bytes_streamed += manifest_bytes
    staged.append(
        StagedObject(
            name=candidate.name,
            role="manifest",
            uri=manifest_canonical,
            bytes=manifest_bytes,
            sha256=manifest_sha256,
            src_key=None,
        )
    )

    completeness = evaluate_manifest(manifest, present, feed.feed_id)
    if completeness.verdict != "complete":
        # Only reachable via a defect our pre-checks don't cover (duplicate
        # manifest file names; feed_id mismatch) -- evaluate_manifest is the
        # single source of truth, reused rather than re-implemented.
        disposition: Literal["incomplete", "unreadable"] = (
            "incomplete" if completeness.verdict == "incomplete" else "unreadable"
        )
        outcome = record_nondelivery(
            feed, manifest.manifest_id, disposition, present, completeness.reason, driver_run_id, fx
        )
        return outcome, bytes_streamed

    req = RegistrationRequest(
        feed=feed,
        delivery_id=delivery_id,
        delivery_key=manifest.manifest_id,
        received_at=received_at,
        driver="sftp-pull",
        driver_run_id=driver_run_id,
        completeness=completeness,
        objects=staged,
    )
    outcome = register_delivery(req, fx)
    return outcome, bytes_streamed


# --- trailer / timer mode (§9.2 step 4) -------------------------------------


def _acquire_trailer_or_timer_candidate(
    feed: FeedConfig, candidate: RemoteFile, driver_run_id: str, fx: Effects, sftp: SftpFx
) -> tuple[DeliveryOutcome, int]:
    """One `select_candidates` candidate (trailer or timer mode) ->
    `(DeliveryOutcome, bytes_streamed)`."""
    connection = cast(SftpConnection, feed.connection)
    received_at = fx.now()
    delivery_id = fx.new_delivery_id()
    landing_bucket = fx.config.landing_bucket
    canonical = _canonical_uri(
        landing_bucket, feed.feed_id, received_at, delivery_id, candidate.name
    )
    bucket, key = _split_s3_uri(canonical)
    remote_path = _remote_file_path(connection.remote_path, candidate.name)
    sha256_hex, total_bytes = fx.store.stream_upload(sftp.read_chunks(remote_path), bucket, key)

    if feed.completeness.mode == "trailer":
        tail_bytes = fx.store.get_tail(bucket, key, _TRAILER_TAIL_BYTES)
        tail_text = tail_bytes.decode("utf-8", errors="replace")
        completeness = evaluate_trailer(tail_text, cast(TrailerSpec, feed.completeness.trailer))
        if completeness.verdict != "complete":
            disposition: Literal["incomplete", "unreadable"] = (
                "incomplete" if completeness.verdict == "incomplete" else "unreadable"
            )
            observed = [ObjectStat(name=candidate.name, bytes=total_bytes, sha256=sha256_hex)]
            outcome = record_nondelivery(
                feed, candidate.name, disposition, observed, completeness.reason, driver_run_id, fx
            )
            return outcome, total_bytes
    else:
        # timer mode: "complete by selection" (§9.2) -- `select_candidates`'s
        # quiet-window filter (step 3) already IS the completeness check.
        completeness = CompletenessResult(
            verdict="complete",
            reason=None,
            asserted_record_count=None,
            data_object_names=(candidate.name,),
        )

    staged = StagedObject(
        name=candidate.name,
        role="data",
        uri=canonical,
        bytes=total_bytes,
        sha256=sha256_hex,
        src_key=None,
    )
    req = RegistrationRequest(
        feed=feed,
        delivery_id=delivery_id,
        delivery_key=candidate.name,
        received_at=received_at,
        driver="sftp-pull",
        driver_run_id=driver_run_id,
        completeness=completeness,
        objects=[staged],
    )
    outcome = register_delivery(req, fx)
    return outcome, total_bytes


# --- entry point ("acquire", §7.6, §9.2) ------------------------------------


def _process_candidates(
    feed: FeedConfig,
    remaining: list[RemoteFile],
    acquire_one: Callable[[RemoteFile], tuple[DeliveryOutcome, int]],
    driver_run_id: str,
    now: datetime,
    fx: Effects,
) -> list[DeliveryOutcome]:
    """§9.2 step 5's candidate-processing loop, shared by BOTH completeness
    modes (critique-gate F-6: manifest and trailer/timer each previously
    carried their own copy of this exact scaffolding, differing only in
    candidate selection and the per-candidate acquire function): the
    budget check, carry-over logging, and per-delivery failure isolation
    all live here, in exactly one place. `acquire_one` closes over
    whatever mode-specific extras (`listing_by_name`, `sftp`, ...) its
    caller's `_acquire_manifest_candidate`/`_acquire_trailer_or_timer_
    candidate` needs -- this function only ever calls it with the
    candidate. Any `TransientError` is re-raised AFTER the loop so the run
    is marked failed and alarmed without blocking sibling deliveries in
    the same window.
    """
    outcomes: list[DeliveryOutcome] = []
    budget_bytes = _budget_bytes()
    bytes_so_far = 0
    pending_transient: TransientError | None = None
    while remaining:
        if _budget_exceeded(fx.now(), now, bytes_so_far, budget_bytes):
            _log_carry_over(feed.feed_id, len(remaining))
            break
        candidate = remaining.pop(0)
        try:
            outcome, streamed = acquire_one(candidate)
            outcomes.append(outcome)
            bytes_so_far += streamed
        except TransientError as exc:
            _logger.error(
                "transient failure acquiring %r: %s",
                candidate.name,
                exc,
                extra={"feed_id": feed.feed_id, "driver_run_id": driver_run_id},
            )
            pending_transient = pending_transient or exc
        except Exception:
            _logger.exception(
                "unexpected failure acquiring %r",
                candidate.name,
                extra={"feed_id": feed.feed_id, "driver_run_id": driver_run_id},
            )
    if pending_transient is not None:
        raise pending_transient
    return outcomes


def acquire(
    feed: FeedConfig,
    window: Window,
    fx: Effects,
    driver_run_id: str,
    *,
    force: bool = False,
    resume_batch_id: str | None = None,
) -> list[DeliveryOutcome]:
    """sftp-pull "acquire": §9.2's normative walk.

    `resume_batch_id` short-circuits to step 0 (§9.3 sweep resume) and
    returns -- nothing else in this function runs. Otherwise: fold the
    ledger for `acquired`/`defective`, list the remote directory (optionally
    windowed for an operator re-pull), select candidates per completeness
    mode, and process them in order via `_process_candidates` (§9.2 step
    5's budget/carry-over/failure-isolation scaffolding, shared by both
    modes, critique-gate F-6).
    """
    if resume_batch_id is not None:
        return _resume(feed, resume_batch_id, driver_run_id, fx)

    connection = cast(SftpConnection, feed.connection)
    now = fx.now()
    rows = fx.ledger.scan_feed(feed.feed_id, now - timedelta(days=_lookback_days()))
    acquired = folds.acquired_final(rows)
    defective = folds.observed_defective(rows)

    sftp = fx.sftp_fx_for(connection.secret_ref)
    listing = sftp.listdir(connection.remote_path)
    unsafe_count = sum(1 for f in listing if not is_clean_object_name(f.name))
    if unsafe_count > 0:
        _log_unsafe_remote_names(feed.feed_id, unsafe_count)
    if window.start is not None or window.end is not None:
        listing = [f for f in listing if _in_window(f.mtime, window)]

    remaining: list[RemoteFile]
    acquire_one: Callable[[RemoteFile], tuple[DeliveryOutcome, int]]
    if feed.completeness.mode == "manifest":
        listing_by_name = {f.name: f for f in listing}
        remaining = windows.select_manifests(
            listing, acquired, feed.completeness.manifest_pattern, force
        )

        def acquire_one(candidate: RemoteFile) -> tuple[DeliveryOutcome, int]:
            return _acquire_manifest_candidate(
                feed, candidate, listing_by_name, driver_run_id, fx, sftp
            )
    else:
        remaining = windows.select_candidates(
            listing, acquired, defective, connection.file_pattern, feed.completeness, now, force
        )

        def acquire_one(candidate: RemoteFile) -> tuple[DeliveryOutcome, int]:
            return _acquire_trailer_or_timer_candidate(feed, candidate, driver_run_id, fx, sftp)

    return _process_candidates(feed, remaining, acquire_one, driver_run_id, now, fx)
