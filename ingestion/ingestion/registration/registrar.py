"""THIN interpreter: register_delivery / record_nondelivery / execute --
LLD §8, §8.5.

Exactly three functions, the only code anywhere (besides §9.4's
reconciliation, out of this bead's scope) that calls `fx.ledger.append` /
`fx.emit` -- the golden ownership test greps for this. All judgment lives
in the pure planners (`core.decisions.plan_registration`/`plan_nondelivery`,
§8.3); this module only walks the `RegistrationPlan` value they return, in
the fixed order §8.5 specifies.

Two deliberate deviations from the LLD's bare pseudocode, both required
because the planners' actual signatures (§8.3, built by M1) need identifiers
this module's callers must supply and the abbreviated LLD bullets omit --
see `core/decisions.py`'s module docstring for the `plan_nondelivery`
half of this and [[m1-folds-planners-design-notes]] in agent memory:

* `record_nondelivery` gains a `driver_run_id: str` parameter (inserted
  right before `fx`, mirroring where `plan_nondelivery` inserts its own
  extra parameters) -- `Effects` carries no ambient "current run" identity
  (deliberately: `now`/`new_delivery_id` are the only per-invocation values
  it exposes, per §7.7), so the caller (a driver) must pass its own
  `driver_run_id` through explicitly, exactly as it does for
  `register_delivery`'s `RegistrationRequest.driver_run_id`.
* `execute` gains a `run_id: str` parameter beyond the LLD's bare
  `execute(plan, fx)` pair -- needed for step E4's `fx.cas.complete(feed_id,
  batch_id, run_id, now)` fencing check (§8.4: `ConditionExpression
  owner_run_id = :run_id`). `RegistrationPlan.complete_claim` is
  deliberately a bare `(feed_id, batch_id)` 2-tuple (§8.3) with no run_id
  field, and `plan.rows[-1].driver_run_id` is NOT a substitute: on
  `TAKEN_OVER` that field holds the DEAD run's id (`_plan_taken_over` builds
  it from `claim.item.owner_run_id` verbatim, "so the plan is byte-identical
  to what the dead run would have written" -- see
  `core/decisions.py`/[[m2-ledger-cas-build-design-notes]]), whereas the
  CAS row's *stored* `owner_run_id` after a takeover is the RESUMING run's
  id. Completing with the dead run's id would fail `complete`'s fencing
  condition every time, silently leaving every resumed claim stuck
  `in_progress` forever. `run_id` is always the CURRENT invocation's
  identity -- `register_delivery` passes `req.driver_run_id` (the same
  value it used for the `claim()` call moments earlier); `record_nondelivery`
  passes its own `driver_run_id` (unused there, since `plan_nondelivery`
  never sets `complete_claim`, but required so `execute` has one shape).

`execute`'s body contains no judgment beyond the four `if plan.x` guards
(§8.5) -- one per optional `RegistrationPlan` field (`copies`, `rows`,
`event`, `complete_claim`); any per-row/per-disposition branching (e.g.
metric selection) lives in the private `_emit_metrics` helper, not in
`execute` itself.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Literal

from ingestion import observability
from ingestion.core.completeness import ObjectStat
from ingestion.core.decisions import (
    RegistrationPlan,
    RegistrationRequest,
    plan_nondelivery,
    plan_registration,
)
from ingestion.core.hashing import canonical_content_hash
from ingestion.core.minting import mint_batch_id
from ingestion.core.model import DeliveryOutcome, FeedConfig
from ingestion.effects.records import Effects

_logger = logging.getLogger(__name__)

# LLD §11.2's full metric list also covers metrics owned by OTHER modules
# (OverdueEmitted/StuckClaimsRecovered -- absence sweep, M5;
# SupersessionsReconciled -- maintenance, §9.4; DriverCarryOver -- driver
# scheduling, M4/M5); this is the subset `execute` observes directly from a
# `RegistrationPlan`'s rows.
_DISPOSITION_METRIC: dict[str, str] = {
    "registered": "DeliveriesRegistered",
    "duplicate": "Duplicates",
    "superseded": "Superseded",
    "incomplete": "Incomplete",
    "unreadable": "Unreadable",
}


def _trigger_for(req: RegistrationRequest) -> dict[str, str | None]:
    """§8.4: "trigger (JSON: s3-push -> the triggering vestibule key; pull ->
    the window)" -- recorded into the CAS claim item for operator audit
    only (`core.decisions` never reads `ClaimItem.trigger`, verified: no
    planner references it).

    Selects the key by the explicit `StagedObject.role` field, never by
    list position (critique-gate F-1: the crash-recovery replay path,
    `absence/detector.py::_synthetic_s3_event`, must not depend on an
    implicit "manifest object is always last" ordering invariant): the
    `role == "manifest"` object's `src_key` when one is present, else the
    single `role == "data"` object's `src_key` (trailer mode always has
    exactly one object, and it is the delivery). Driver-agnostic by
    construction: every s3-push `StagedObject` carries a vestibule
    `src_key`; every sftp-pull one carries `None` (already streamed to
    canonical, §9.2), so this naturally yields `{"trigger_key": None}` for
    sftp-pull without an `if driver == ...` branch (§7.6's abstraction rule).
    """
    manifest = next((o for o in req.objects if o.role == "manifest"), None)
    if manifest is not None:
        return {"trigger_key": manifest.src_key}
    data_objects = [o for o in req.objects if o.role == "data"]
    return {"trigger_key": data_objects[0].src_key}


def _emit_metrics(plan: RegistrationPlan) -> None:
    """E5's per-row/per-byte metric selection -- kept out of `execute`'s own
    body so `execute` itself stays limited to the four `if plan.x` guards.
    """
    feed_id = plan.outcome.feed_id
    for row in plan.rows:
        observability.emit_metric(_DISPOSITION_METRIC[row.disposition], 1, feed_id)
    registered_bytes = sum(
        row.size_bytes or 0 for row in plan.rows if row.disposition == "registered"
    )
    if registered_bytes:
        observability.emit_metric("BytesAcquired", registered_bytes, feed_id)


def execute(plan: RegistrationPlan, fx: Effects, run_id: str) -> DeliveryOutcome:
    """The interpreter (§8.5) -- fixed order, no judgment beyond the four
    `if plan.x` guards below:

    E1. copy every vestibule object the plan calls for (s3-push WON/
        TAKEN_OVER only; idempotent -- fixed canonical keys, a recovery
        re-copy writes identical bytes).
    E2. one ledger append (registered + any superseded accretion row land
        atomically).
    E3. one event, iff a registration actually happened.
    E4. complete the CAS claim, fenced on `run_id` (§8.4).
    E5. metrics; return the plan's outcome.
    """
    if plan.copies:
        for copy in plan.copies:
            fx.store.copy_verbatim(
                copy.src_bucket,
                copy.src_key,
                copy.dst_bucket,
                copy.dst_key,
                copy.src_version_id,
                copy.src_etag,
            )
    if plan.rows:
        fx.ledger.append(plan.rows)
    if plan.event:
        fx.emit("delivery-registered", plan.event)
    if plan.complete_claim:
        feed_id, batch_id = plan.complete_claim
        fx.cas.complete(feed_id, batch_id, run_id, fx.now())
    _emit_metrics(plan)
    return plan.outcome


def register_delivery(req: RegistrationRequest, fx: Effects) -> DeliveryOutcome:
    """The turnstile path (§8.5), for complete verified deliveries:
    claim -> plan_registration -> execute.

    `content_hash`/`batch_id` are (re-)derived here from `req.objects`'
    data objects via the same pure algorithm `core.decisions._plan_won`
    independently applies to the same objects when it later builds the
    ledger row (mirroring `effects/cas.py::_build_claim_item_dict`'s
    identical derivation) -- the two always agree because it is the same
    pure function over the same input, computed once more rather than
    threaded through as an extra field on `RegistrationRequest` (which the
    LLD's §8.1 shape does not carry).
    """
    data_objects = [o for o in req.objects if o.role == "data"]
    content_hash = canonical_content_hash([(o.name, o.sha256) for o in data_objects])
    batch_id = mint_batch_id(req.feed.feed_id, content_hash)
    run_id = req.driver_run_id
    trigger = _trigger_for(req)

    claim = fx.cas.claim(req, batch_id, run_id, trigger, fx.now())
    _logger.info(
        "cas claim result: %s",
        claim.kind,
        extra={
            "feed_id": req.feed.feed_id,
            "delivery_id": req.delivery_id,
            "batch_id": batch_id,
            "driver_run_id": run_id,
        },
    )
    prior = fx.ledger.scan_feed(req.feed.feed_id, None)
    plan = plan_registration(claim, prior, req, fx.now())
    outcome = execute(plan, fx, run_id)
    _logger.info(
        "register_delivery outcome: %s",
        outcome.disposition,
        extra={
            "feed_id": outcome.feed_id,
            "delivery_id": outcome.delivery_id,
            "batch_id": outcome.batch_id,
            "driver_run_id": run_id,
        },
    )
    return outcome


def record_nondelivery(
    feed: FeedConfig,
    delivery_key: str,
    disposition: Literal["incomplete", "unreadable"],
    observed: Sequence[ObjectStat],
    notes: str | None,
    driver_run_id: str,
    fx: Effects,
) -> DeliveryOutcome:
    """The non-turnstile path (§8.5) for `incomplete`/`unreadable` verdicts:
    plan_nondelivery -> execute. Both verdicts RETURN SUCCESS (recorded, not
    retried) -- callers never raise on either.

    A fresh `delivery_id` is minted on every call, whether or not the
    planner's append-on-change rule ends up suppressing the row (the mint
    is cheap; the row it would have produced is simply discarded on
    suppression) -- see `core/decisions.py`'s module docstring.
    """
    delivery_id = fx.new_delivery_id()
    now = fx.now()
    prior = fx.ledger.scan_feed(feed.feed_id, None)
    plan = plan_nondelivery(
        feed, delivery_key, disposition, observed, notes, prior, now, delivery_id, driver_run_id
    )
    outcome = execute(plan, fx, driver_run_id)
    _logger.info(
        "record_nondelivery outcome: %s",
        outcome.disposition,
        extra={
            "feed_id": feed.feed_id,
            "delivery_id": outcome.delivery_id,
            "driver_run_id": driver_run_id,
        },
    )
    return outcome
