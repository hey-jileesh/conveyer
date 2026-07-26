"""PURE planners: plan_registration / plan_nondelivery / plan_reconciliation — LLD §8.3.

"Decide, then do" (§7.0 rule 5): every planner here returns a `RegistrationPlan`
— the ENTIRE effect of a registration decision, as data — for a tiny
interpreter (`registration/registrar.py`, out of this module's scope) to walk
in a fixed order. No I/O, no `now()`; every "now" is a parameter.

Two deliberate deviations from the LLD §8.3 code block, both required to
build a schema-valid `DeliveryRecord` (non-nullable `delivery_id`,
`driver_run_id` columns) and not resolvable from the abbreviated signature
shown there:

* `plan_nondelivery` gains `delivery_id: str` and `driver_run_id: str`
  parameters. The LLD's own `record_nondelivery(feed, delivery_key,
  disposition, observed, notes, fx)` bullet (§8) has the identical gap — the
  effects-side caller is the only plausible source, mirroring `delivery_id`'s
  general rule ("UUIDv4, minted by the effect side... at the start of
  acquisition/registration", §5): mint a fresh one whether or not the
  append-on-change rule ends up suppressing the row.
* Vestibule-URI substitution for s3-push `duplicate` rows (§6.2 population
  table: "vestibule uris" not canonical) is represented as the bare
  `StagedObject.src_key` string, not a full `s3://bucket/key` URI — no
  bucket name is available anywhere in this module's pure inputs
  (`RegistrationRequest`/`ClaimItem` carry no bucket field). The vestibule
  copy is transient (14 d) and the winner's `registered` row holds the
  permanent URIs, so this is documentation-grade, not load-bearing.

`ObjectStat` (LLD §7.3) carries no `role` field, so every `DeliveryObject`
built by `plan_nondelivery` defaults to `role="data"`. This is functionally
inert for every fold in this codebase: `observed_defective` already excludes
`completeness_mode == "manifest"` rows entirely (§7.4), and `acquired_final`
does not discriminate by role. `asserted_record_count` likewise stays `None`
on every `plan_nondelivery` row — nullable, and no `CompletenessResult` is
available in that call to source it from.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast

from ingestion.core import folds
from ingestion.core.completeness import CompletenessResult, ObjectStat
from ingestion.core.hashing import canonical_content_hash
from ingestion.core.minting import mint_batch_id
from ingestion.core.model import (
    ClaimItem,
    ClaimResult,
    DeliveryObject,
    DeliveryOutcome,
    DeliveryRecord,
    DeliveryRegisteredV1,
    FeedConfig,
    StagedObject,
)
from ingestion.core.naming import split_s3_uri

# --- §8.1 `RegistrationRequest` ---------------------------------------------


@dataclass(frozen=True)
class RegistrationRequest:
    """LLD §8.1. Lives here (not `core/model.py`) because `completeness:
    CompletenessResult` — a `core/completeness.py` type — would otherwise
    force `model.py` to import `completeness.py`, breaking the acyclic
    import graph the two modules were deliberately kept apart to preserve.
    """

    feed: FeedConfig
    delivery_id: str  # minted by the caller (effects/ids.py)
    delivery_key: str  # manifest_id | single data object basename
    received_at: datetime  # acquisition start (§5); fixes the canonical prefix
    driver: str
    driver_run_id: str
    completeness: CompletenessResult  # already evaluated by the caller (complete=True)
    objects: list[StagedObject]  # data objects AND the manifest (role distinguishes)


# --- §8.3 Pure decision ------------------------------------------------------


@dataclass(frozen=True)
class CopySpec:
    src_bucket: str
    src_key: str
    dst_bucket: str
    dst_key: str
    src_version_id: str | None = None  # H-1 (security-gate, TOCTOU): threaded verbatim
    # from `StagedObject.src_version_id`/`src_etag` -- see that field's docstring.
    src_etag: str | None = None


@dataclass(frozen=True)
class RegistrationPlan:
    """The ENTIRE effect of a registration, as data (LLD §8.3)."""

    rows: tuple[DeliveryRecord, ...]
    copies: tuple[CopySpec, ...]
    event: DeliveryRegisteredV1 | None
    complete_claim: tuple[str, str] | None
    outcome: DeliveryOutcome


# --- shared helpers ----------------------------------------------------------


def _narrow_driver(value: str) -> Literal["s3-push", "sftp-pull"]:
    """`RegistrationRequest.driver`/`ClaimItem.driver`/`FeedConfig.driver` are
    plain `str`/wider `Literal`s; only `s3-push`/`sftp-pull` ever reach this
    module in practice (`FeedConfig` rejects `api-pull`/`db-unload` outright,
    §6.1), but `DeliveryRecord.driver`'s exact two-value `Literal` still
    needs an explicit narrowing for mypy.
    """
    return "s3-push" if value == "s3-push" else "sftp-pull"


def _narrow_completeness_mode(value: str) -> Literal["manifest", "trailer", "timer"]:
    """Same narrowing need as `_narrow_driver`, for `ClaimItem.completeness_mode`
    (plain `str`) on the TAKEN_OVER path."""
    if value == "manifest":
        return "manifest"
    if value == "timer":
        return "timer"
    return "trailer"


def _copy_spec(
    src_key: str, canonical_uri: str, src_version_id: str | None, src_etag: str | None
) -> CopySpec:
    bucket, dst_key = split_s3_uri(canonical_uri)
    return CopySpec(
        src_bucket=bucket,
        src_key=src_key,
        dst_bucket=bucket,
        dst_key=dst_key,
        src_version_id=src_version_id,
        src_etag=src_etag,
    )


def _copies_for(objects: Sequence[StagedObject]) -> tuple[CopySpec, ...]:
    """One `CopySpec` per object with `src_key` set — s3-push objects not
    yet copied from the vestibule (§8.1). sftp-pull objects (`src_key is
    None`, already streamed to `uri`) contribute nothing. Each `CopySpec`
    carries its object's `src_version_id`/`src_etag` verbatim (H-1,
    security-gate, TOCTOU) -- `None` for both is a valid, pre-fix-compatible
    "unpinned copy" (e.g. the s3-push manifest object, hashed via `get_bytes`
    rather than `stream_sha256`, never sets these).
    """
    return tuple(
        _copy_spec(o.src_key, o.uri, o.src_version_id, o.src_etag)
        for o in objects
        if o.src_key is not None
    )


def _staged_to_delivery_objects(
    objects: Sequence[StagedObject], *, use_src_key: bool
) -> tuple[list[DeliveryObject], list[str], str | None]:
    """Convert `StagedObject`s into the ledger row's `objects`/`object_uris`/
    `manifest_ref` per the §6.2 population table. `use_src_key=True` selects
    the vestibule-key representation for `duplicate` rows (a loser never
    copies, so the vestibule location is the only place the bytes actually
    exist); `use_src_key=False` always uses the canonical `uri` (registered/
    superseded/TAKEN_OVER rows).
    """
    delivery_objects: list[DeliveryObject] = []
    object_uris: list[str] = []
    manifest_ref: str | None = None
    for o in objects:
        uri = o.src_key if (use_src_key and o.src_key is not None) else o.uri
        delivery_objects.append(
            DeliveryObject(name=o.name, role=o.role, uri=uri, bytes=o.bytes, sha256=o.sha256)
        )
        if o.role == "data":
            object_uris.append(uri)
        else:
            manifest_ref = uri
    return delivery_objects, object_uris, manifest_ref


def _build_row(
    *,
    delivery_id: str,
    feed_id: str,
    delivery_key: str,
    received_at: datetime,
    recorded_at: datetime,
    driver: Literal["s3-push", "sftp-pull"],
    driver_run_id: str,
    completeness_mode: Literal["manifest", "trailer", "timer"],
    asserted_record_count: int | None,
    disposition: Literal["registered", "duplicate", "superseded", "incomplete", "unreadable"],
    supersedes: str | None,
    content_hash: str | None,
    batch_id: str | None,
    size_bytes: int | None,
    objects: list[DeliveryObject],
    object_uris: list[str],
    manifest_ref: str | None,
    notes: str | None = None,
) -> DeliveryRecord:
    return DeliveryRecord(
        delivery_id=delivery_id,
        feed_id=feed_id,
        delivery_key=delivery_key,
        batch_id=batch_id,
        content_hash=content_hash,
        size_bytes=size_bytes,
        object_uris=object_uris,
        objects=objects,
        manifest_ref=manifest_ref,
        asserted_record_count=asserted_record_count,
        completeness_mode=completeness_mode,
        received_at=received_at,
        recorded_at=recorded_at,
        disposition=disposition,
        supersedes=supersedes,
        driver=driver,
        driver_run_id=driver_run_id,
        notes=notes,
    )


def _build_event(
    *,
    feed_id: str,
    pipeline: str,
    delivery_id: str,
    batch_id: str,
    delivery_key: str,
    content_hash: str,
    size_bytes: int,
    object_uris: list[str],
    received_at: datetime,
) -> DeliveryRegisteredV1:
    return DeliveryRegisteredV1(
        feed_id=feed_id,
        delivery_id=delivery_id,
        batch_id=batch_id,
        delivery_key=delivery_key,
        content_hash=content_hash,
        size_bytes=size_bytes,
        object_uris=object_uris,
        received_at=received_at,
        pipeline=pipeline,
    )


def _detect_supersession(
    prior: Sequence[DeliveryRecord],
    delivery_key: str,
    self_delivery_id: str,
    content_hash: str,
    recorded_at: datetime,
) -> tuple[str | None, tuple[DeliveryRecord, ...]]:
    """Latest *registered* prior delivery (fold of `prior`, excluding this
    delivery's own id) with the same `delivery_key`: if its `content_hash`
    differs, it is superseded — return its `delivery_id` (for the new row's
    `supersedes`) and a `superseded` accretion row cloned from it verbatim
    except `disposition`/`recorded_at` (§6.2's "append-only... identity
    columns copied verbatim").
    """
    candidates = [
        r
        for r in folds.registered_deliveries(prior)
        if r.delivery_key == delivery_key and r.delivery_id != self_delivery_id
    ]
    if not candidates:
        return None, ()
    latest_prior = max(candidates, key=lambda r: (r.received_at, r.delivery_id))
    if latest_prior.content_hash == content_hash:
        return None, ()
    accretion = latest_prior.model_copy(
        update={"disposition": "superseded", "recorded_at": recorded_at}
    )
    return latest_prior.delivery_id, (accretion,)


# --- §8.3 plan_registration, per claim.kind ----------------------------------


def _plan_won(
    prior: Sequence[DeliveryRecord], req: RegistrationRequest, recorded_at: datetime
) -> RegistrationPlan:
    data_objects = [o for o in req.objects if o.role == "data"]
    content_hash = canonical_content_hash([(o.name, o.sha256) for o in data_objects])
    batch_id = mint_batch_id(req.feed.feed_id, content_hash)
    size_bytes = sum(o.bytes for o in data_objects)

    supersedes, accretion_rows = _detect_supersession(
        prior, req.delivery_key, req.delivery_id, content_hash, recorded_at
    )
    delivery_objects, object_uris, manifest_ref = _staged_to_delivery_objects(
        req.objects, use_src_key=False
    )

    registered_row = _build_row(
        delivery_id=req.delivery_id,
        feed_id=req.feed.feed_id,
        delivery_key=req.delivery_key,
        received_at=req.received_at,
        recorded_at=recorded_at,
        driver=_narrow_driver(req.driver),
        driver_run_id=req.driver_run_id,
        completeness_mode=req.feed.completeness.mode,
        asserted_record_count=req.completeness.asserted_record_count,
        disposition="registered",
        supersedes=supersedes,
        content_hash=content_hash,
        batch_id=batch_id,
        size_bytes=size_bytes,
        objects=delivery_objects,
        object_uris=object_uris,
        manifest_ref=manifest_ref,
    )
    event = _build_event(
        feed_id=req.feed.feed_id,
        pipeline=req.feed.pipeline,
        delivery_id=req.delivery_id,
        batch_id=batch_id,
        delivery_key=req.delivery_key,
        content_hash=content_hash,
        size_bytes=size_bytes,
        object_uris=object_uris,
        received_at=req.received_at,
    )
    outcome = DeliveryOutcome(
        delivery_id=req.delivery_id,
        batch_id=batch_id,
        disposition="registered",
        feed_id=req.feed.feed_id,
        delivery_key=req.delivery_key,
    )
    return RegistrationPlan(
        rows=(registered_row, *accretion_rows),
        copies=_copies_for(req.objects),
        event=event,
        complete_claim=(req.feed.feed_id, batch_id),
        outcome=outcome,
    )


def _plan_lost(
    claim: ClaimResult, req: RegistrationRequest, recorded_at: datetime
) -> RegistrationPlan:
    # LOST_COMPLETED/LOST_IN_PROGRESS always carry the colliding item (§8.4's
    # claim() GetItem branch) — never None (that is WON's signature only).
    item = cast(ClaimItem, claim.item)
    delivery_objects, object_uris, manifest_ref = _staged_to_delivery_objects(
        req.objects, use_src_key=True
    )
    row = _build_row(
        delivery_id=req.delivery_id,
        feed_id=req.feed.feed_id,
        delivery_key=req.delivery_key,
        received_at=req.received_at,
        recorded_at=recorded_at,
        driver=_narrow_driver(req.driver),
        driver_run_id=req.driver_run_id,
        completeness_mode=req.feed.completeness.mode,
        asserted_record_count=req.completeness.asserted_record_count,
        disposition="duplicate",
        supersedes=None,
        content_hash=item.content_hash,
        batch_id=item.batch_id,
        size_bytes=item.size_bytes,
        objects=delivery_objects,
        object_uris=object_uris,
        manifest_ref=manifest_ref,
    )
    outcome = DeliveryOutcome(
        delivery_id=req.delivery_id,
        batch_id=item.batch_id,
        disposition="duplicate",
        feed_id=req.feed.feed_id,
        delivery_key=req.delivery_key,
    )
    return RegistrationPlan(
        rows=(row,), copies=(), event=None, complete_claim=None, outcome=outcome
    )


def _plan_taken_over(
    claim: ClaimResult, prior: Sequence[DeliveryRecord], recorded_at: datetime, pipeline: str
) -> RegistrationPlan:
    # TAKEN_OVER always carries the dead run's item (§8.4) — never None.
    item = cast(ClaimItem, claim.item)
    delivery_objects, object_uris, manifest_ref = _staged_to_delivery_objects(
        list(item.objects_inventory), use_src_key=False
    )
    supersedes, accretion_rows = _detect_supersession(
        prior, item.delivery_key, item.delivery_id, item.content_hash, recorded_at
    )
    registered_row = _build_row(
        delivery_id=item.delivery_id,
        feed_id=item.feed_id,
        delivery_key=item.delivery_key,
        received_at=item.received_at,
        recorded_at=recorded_at,
        driver=_narrow_driver(item.driver),
        driver_run_id=item.owner_run_id,
        completeness_mode=_narrow_completeness_mode(item.completeness_mode),
        asserted_record_count=item.asserted_record_count,
        disposition="registered",
        supersedes=supersedes,
        content_hash=item.content_hash,
        batch_id=item.batch_id,
        size_bytes=item.size_bytes,
        objects=delivery_objects,
        object_uris=object_uris,
        manifest_ref=manifest_ref,
    )
    event = _build_event(
        feed_id=item.feed_id,
        pipeline=pipeline,
        delivery_id=item.delivery_id,
        batch_id=item.batch_id,
        delivery_key=item.delivery_key,
        content_hash=item.content_hash,
        size_bytes=item.size_bytes,
        object_uris=object_uris,
        received_at=item.received_at,
    )
    outcome = DeliveryOutcome(
        delivery_id=item.delivery_id,
        batch_id=item.batch_id,
        disposition="registered",
        feed_id=item.feed_id,
        delivery_key=item.delivery_key,
    )
    return RegistrationPlan(
        rows=(registered_row, *accretion_rows),
        copies=_copies_for(item.objects_inventory),
        event=event,
        complete_claim=(item.feed_id, item.batch_id),
        outcome=outcome,
    )


def plan_registration(
    claim: ClaimResult,
    prior: Sequence[DeliveryRecord],
    req: RegistrationRequest,
    recorded_at: datetime,
) -> RegistrationPlan:
    """Build the entire effect of a registration attempt as data (LLD §8.3),
    dispatching on `claim.kind`:

    * `WON` — registered row (+ superseded accretion row on correction);
      copies for every `StagedObject` with `src_key` set; event;
      `complete_claim` set.
    * `LOST_COMPLETED` / `LOST_IN_PROGRESS` — one `duplicate` row (this run's
      `delivery_id`, the colliding item's `batch_id`); no copies, no event,
      no claim completion. Both sides of every race are remembered.
    * `TAKEN_OVER` — as `WON`, but every row/copy/event is built from the
      dead run's identity in `claim.item` — NOTHING re-derived — so the plan
      is byte-identical to what the dead run would have executed.
      Supersession detection runs here too.
    """
    if claim.kind == "WON":
        return _plan_won(prior, req, recorded_at)
    if claim.kind in ("LOST_COMPLETED", "LOST_IN_PROGRESS"):
        return _plan_lost(claim, req, recorded_at)
    return _plan_taken_over(claim, prior, recorded_at, req.feed.pipeline)


# --- §8.3 plan_nondelivery ----------------------------------------------------


def _latest_row_for_key(
    prior: Sequence[DeliveryRecord], delivery_key: str
) -> DeliveryRecord | None:
    """Among CURRENT (latest-per-delivery_id) rows, the most recently
    recorded one sharing this `delivery_key` — distinct from
    `folds.latest_dispositions`, which groups by `delivery_id`: a
    repeatedly-incomplete manifest mints a fresh `delivery_id` per
    `record_nondelivery` call (see module docstring), so "the latest row for
    this delivery_key" must be found across delivery_ids.
    """
    candidates = [
        r for r in folds.latest_dispositions(prior).values() if r.delivery_key == delivery_key
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda r: (r.recorded_at, r.delivery_id))


def _same_inventory(prior_row: DeliveryRecord, observed: Sequence[ObjectStat]) -> bool:
    prior_set = frozenset((o.name, o.bytes) for o in prior_row.objects)
    observed_set = frozenset((o.name, o.bytes) for o in observed)
    return prior_set == observed_set


def _nondelivery_identity(
    feed_id: str, disposition: Literal["incomplete", "unreadable"], observed: Sequence[ObjectStat]
) -> tuple[str | None, str | None, int | None]:
    """§6.2 population table: `incomplete` is always null (never fully
    hashed); `unreadable` is populated iff hashing completed before the
    defect surfaced — i.e. every observed object carries a `sha256`.
    """
    if disposition == "unreadable" and observed and all(o.sha256 is not None for o in observed):
        pairs = [(o.name, cast(str, o.sha256)) for o in observed]
        content_hash = canonical_content_hash(pairs)
        batch_id = mint_batch_id(feed_id, content_hash)
        size_bytes = sum(o.bytes for o in observed)
        return content_hash, batch_id, size_bytes
    return None, None, None


def plan_nondelivery(
    feed: FeedConfig,
    delivery_key: str,
    disposition: Literal["incomplete", "unreadable"],
    observed: Sequence[ObjectStat],
    notes: str | None,
    prior: Sequence[DeliveryRecord],
    now: datetime,
    delivery_id: str,
    driver_run_id: str,
) -> RegistrationPlan:
    """`incomplete`/`unreadable` verdicts, never through the turnstile (LLD
    §8.3). Append-on-change rule: if the latest row for this `delivery_key`
    already has the same disposition AND the same observed `(name, bytes)`
    inventory, `rows=()` — never copies/event/claim either way. This is what
    keeps a stuck incomplete manifest, re-examined every scheduled run, from
    writing a row per run.

    See the module docstring for the `delivery_id`/`driver_run_id`
    parameters, which extend the LLD §8.3 abbreviated signature.
    """
    latest = _latest_row_for_key(prior, delivery_key)
    if (
        latest is not None
        and latest.disposition == disposition
        and _same_inventory(latest, observed)
    ):
        outcome = DeliveryOutcome(
            delivery_id=latest.delivery_id,
            batch_id=latest.batch_id,
            disposition=latest.disposition,
            feed_id=feed.feed_id,
            delivery_key=delivery_key,
        )
        return RegistrationPlan(
            rows=(), copies=(), event=None, complete_claim=None, outcome=outcome
        )

    content_hash, batch_id, size_bytes = _nondelivery_identity(feed.feed_id, disposition, observed)
    delivery_objects = [
        DeliveryObject(name=o.name, role="data", uri=None, bytes=o.bytes, sha256=o.sha256)
        for o in observed
    ]
    row = _build_row(
        delivery_id=delivery_id,
        feed_id=feed.feed_id,
        delivery_key=delivery_key,
        received_at=now,
        recorded_at=now,
        driver=_narrow_driver(feed.driver),
        driver_run_id=driver_run_id,
        completeness_mode=feed.completeness.mode,
        asserted_record_count=None,
        disposition=disposition,
        supersedes=None,
        content_hash=content_hash,
        batch_id=batch_id,
        size_bytes=size_bytes,
        objects=delivery_objects,
        object_uris=[],
        manifest_ref=None,
        notes=notes,
    )
    outcome = DeliveryOutcome(
        delivery_id=delivery_id,
        batch_id=batch_id,
        disposition=disposition,
        feed_id=feed.feed_id,
        delivery_key=delivery_key,
    )
    return RegistrationPlan(
        rows=(row,), copies=(), event=None, complete_claim=None, outcome=outcome
    )


# --- §9.4 step 3: plan_reconciliation -----------------------------------------


def plan_reconciliation(
    live_duplicates: Mapping[str, Sequence[DeliveryRecord]], now: datetime
) -> tuple[DeliveryRecord, ...]:
    """`live_duplicates`: `delivery_key -> the >1 live-registered
    DeliveryRecord rows sharing it` (already grouped by the caller's SQL
    query — the "current-dispositions" latest-disposition fold, §9.4/§11.4).
    For each group, append `superseded` accretion rows for all but the
    newest `received_at` (LLD §9.4 step 3, repairing the concurrent-
    correction race named in §8.3 — deliberately NOT fixed in the planner
    that creates it, per the brief: a `delivery_key` lock was explicitly
    rejected). Deterministic and idempotent: an unchanged input reproduces
    the same output rows; once reconciled, a fresh query no longer reports
    the delivery_key as having >1 live registered row.
    """
    rows: list[DeliveryRecord] = []
    for records in live_duplicates.values():
        if len(records) < 2:
            continue
        newest = max(records, key=lambda r: (r.received_at, r.delivery_id))
        for record in records:
            if record.delivery_id == newest.delivery_id:
                continue
            rows.append(record.model_copy(update={"disposition": "superseded", "recorded_at": now}))
    rows.sort(key=lambda r: (r.delivery_key, r.delivery_id))
    return tuple(rows)
