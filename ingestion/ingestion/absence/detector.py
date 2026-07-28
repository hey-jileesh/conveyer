"""Overdue sweep + stuck-claim sweep -- LLD §9.3.

Hourly. Two independent sweeps against `Effects`, run in order by `run()`:

1. `sweep_overdue` -- for every registry feed with an `expectation`, compute
   `core.expectations.overdue_dates` against the fold of registered
   deliveries scanned from the ledger (`since=now-5d`, §9.3's literal
   window -- comfortably wider than `overdue_dates`' own feed-tz
   `lookback_days` default of 3, so it never truncates the calendar the
   pure function walks). For each still-unmarked overdue date,
   **emit-then-mark**, the order is normative (§9.3): a crash between emit
   and mark just re-emits next hour (at-least-once, D-14, consumers dedup
   on `(feed_id, expectation_date)`); mark-then-emit was rejected because a
   crash after marking would lose the overdue signal permanently.
2. `sweep_stuck_claims` -- for every stale CAS claim (`cas.sweep_stale`),
   route on `item.driver` and async-invoke the Lambda that can finish it:
   `s3-push` -> `${p}-registrar` with a synthetic "Object Created" event
   rebuilt from the claim's recorded `trigger`; `sftp-pull` ->
   `${p}-driver-<slug(feed_id)>` with `{"resume_batch_id": item.batch_id}`
   (§9.2 step 0 -- the resumer re-derives everything else via its own
   `sweep_stale` call, this module needs to know nothing more).

`registrar._trigger_for` (§8.4) records `{"trigger_key": ...}`, selected by
the `StagedObject.role` field (the `role == "manifest"` object's `src_key`
when present, else the single `role == "data"` object's `src_key`) -- NOT
by list position (critique-gate finding F-1: an earlier version relied on
`RegistrationRequest.objects` always having the manifest object last and
read `src_keys[-1]`, an implicit ordering invariant this module's own
`_synthetic_s3_event` now no longer needs to assume). `trigger_key` is
always the ONE vestibule key that re-drives the identical `acquire` routing
decision (`_is_manifest_key`) the dead run took, for both completeness
modes, without an `if mode == ...` branch -- the same
driver-agnostic-by-construction trick `_trigger_for` itself already uses.

`fx.emit` is called directly here for `delivery-overdue` -- NOT through
`registration/registrar.py`, whose module docstring/`tests/golden/
test_ownership.py` allowlist covers the REGISTRATION event path only.
§9.3's own pseudocode calls `fx.emit` from this module; `test_ownership.py`
`_ALLOWED_CALLERS` is extended by exactly this one entry (mirroring the
sibling m5-maintenance bead's `ledger.append` addition to the same
allowlist, for the same file, for the same reason -- see that test's
module docstring, which already anticipated exactly one more addition).

Feed registry loaded via `effects.registry.load_feed_registry` (§6.8, the
single implementation `drivers/s3_push.py`/`drivers/sftp_pull.py` also use
now, critique-gate finding F-2 -- this module previously carried its own
near-duplicate copy of the read/parse/cache body).

The `${p}` Lambda-function-name prefix (LLD §5: `${name_prefix}-${env}`)
has no slot on `RuntimeConfig` (`config.py` is not this bead's file to
extend) -- `name_prefix` is read from a new `CONVEYER_NAME_PREFIX` env var
(default `"conveyer"`, matching Terraform's own root variable default,
§10.1), the same "read effect-side, simplest gap resolution" pattern M4's
sftp-pull driver used for its own env-only config
(`CONVEYER_SFTP_LOOKBACK_DAYS` etc.) -- `absence/` is outside the purity
linter's PURITY scope (`core/**` + `sources/**` only), so `os` is legal here.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any

from ingestion import observability
from ingestion.core.expectations import expected_by, overdue_dates
from ingestion.core.folds import registered_deliveries
from ingestion.core.model import ClaimItem, DeliveryOverdueV1, FeedConfig
from ingestion.effects.records import Effects
from ingestion.effects.registry import RegistryCache
from ingestion.effects.registry import load_feed_registry as _load_feed_registry

_logger = logging.getLogger(__name__)

_OVERDUE_SCAN_LOOKBACK_DAYS = 5  # §9.3: "since=now-5d"
_OVERDUE_MARKER_TTL_DAYS = 35  # §8.4: "mark(pk, now, ttl_days=35)"

_NAME_PREFIX_ENV = "CONVEYER_NAME_PREFIX"
_DEFAULT_NAME_PREFIX = "conveyer"  # Terraform's own `name_prefix` default, §10.1

# §6.8: registry cached per Lambda container -- same shape/rationale as
# `drivers/s3_push.py::_DEFAULT_REGISTRY_CACHE` (production callers never
# pass `registry_cache`; tests pass a fresh `{}` per test for isolation).
# `_load_feed_registry` above is a re-export of `effects.registry.
# load_feed_registry` (critique-gate F-2).
_DEFAULT_REGISTRY_CACHE: RegistryCache = {}


# --- overdue sweep (§9.3, first half) ----------------------------------------


def _overdue_marker_pk(feed_id: str, expectation_date: date) -> str:
    return f"overdue#{feed_id}#{expectation_date.isoformat()}"


def sweep_overdue(feeds: Sequence[FeedConfig], fx: Effects) -> int:
    """Emit-then-mark for every still-unmarked overdue expectation date,
    across every feed that declares one. Returns the count of dates for
    which THIS sweep won the emit (0 on a rerun once every marker holds --
    G-12b).
    """
    now = fx.now()
    since = now - timedelta(days=_OVERDUE_SCAN_LOOKBACK_DAYS)
    emitted = 0
    for feed in feeds:
        if feed.expectation is None:
            continue
        rows = fx.ledger.scan_feed(feed.feed_id, since)
        received = [r.received_at for r in registered_deliveries(rows)]
        for expectation_date in overdue_dates(feed.expectation, received, now):
            pk = _overdue_marker_pk(feed.feed_id, expectation_date)
            if fx.cas.marker_exists(pk):
                continue
            event = DeliveryOverdueV1(
                feed_id=feed.feed_id,
                expectation_date=expectation_date,
                expected_by=expected_by(feed.expectation, expectation_date),
                checked_at=now,
            )
            fx.emit("delivery-overdue", event)
            observability.emit_metric("OverdueEmitted", 1, feed.feed_id)
            fx.cas.mark(pk, now, _OVERDUE_MARKER_TTL_DAYS)  # False = lost race, fine (§9.3)
            emitted += 1
    return emitted


# --- stuck-claim sweep (§9.3, second half) -----------------------------------


def _slug(feed_id: str) -> str:
    """`<source>/<feed>` -> `<source>--<feed>` (LLD §5/§10.7: "slashes -> --")."""
    return feed_id.replace("/", "--")


def _function_prefix(env: str) -> str:
    name_prefix = os.environ.get(_NAME_PREFIX_ENV, _DEFAULT_NAME_PREFIX)
    return f"{name_prefix}-{env}"


def _synthetic_s3_event(item: ClaimItem, landing_bucket: str) -> dict[str, Any] | None:
    """Rebuild the "Object Created" EventBridge notification shape
    `drivers/s3_push.py::_parse_event` reads (`detail.bucket.name`,
    `detail.object.key`) from the claim's recorded `trigger` -- `None` if
    the trigger carries no vestibule key to replay (defensive; s3-push
    claims always have at least one `src_key`, so this is not expected in
    practice).

    Reads the recorded key directly by its explicit field name
    (`trigger_key`, `registration/registrar.py::_trigger_for`, §8.4) --
    no positional assumption about `RegistrationRequest.objects`' order
    (critique-gate F-1).
    """
    key = item.trigger.get("trigger_key")
    if not isinstance(key, str):
        return None
    return {"detail": {"bucket": {"name": landing_bucket}, "object": {"key": key}}}


def sweep_stuck_claims(fx: Effects) -> int:
    """Async-invoke the right Lambda to finish every stale CAS claim.
    Returns the count of claims for which an invoke was actually issued.
    """
    now = fx.now()
    prefix = _function_prefix(fx.config.env)
    recovered = 0
    for item in fx.cas.sweep_stale(now):
        # L-7 (security-gate): never log the full `ClaimItem` repr -- it carries
        # `objects_inventory` (partner filenames/URIs, potentially PII-shaped),
        # `delivery_key`, and `trigger`, none of which belong in a plain-text log
        # line. feed_id/batch_id/status are enough to diagnose and audit a
        # recovery from CloudWatch.
        _logger.warning(
            "stuck claim recovered: feed_id=%s batch_id=%s status=%s",
            item.feed_id,
            item.batch_id,
            item.status,
            extra={"feed_id": item.feed_id, "batch_id": item.batch_id},
        )
        if item.driver == "s3-push":
            event = _synthetic_s3_event(item, fx.config.landing_bucket)
            if event is None:
                _logger.warning(
                    "stuck s3-push claim %s/%s has no vestibule src_key in trigger; cannot resume",
                    item.feed_id,
                    item.batch_id,
                )
                continue
            fx.invoke_async(f"{prefix}-registrar", event)
        elif item.driver == "sftp-pull":
            fx.invoke_async(
                f"{prefix}-driver-{_slug(item.feed_id)}", {"resume_batch_id": item.batch_id}
            )
        else:
            _logger.warning(
                "stuck claim %s/%s has unknown driver %r; cannot resume",
                item.feed_id,
                item.batch_id,
                item.driver,
            )
            continue
        observability.emit_metric("StuckClaimsRecovered", 1, item.feed_id)
        recovered += 1
    return recovered


# --- entry point --------------------------------------------------------------


def run(
    fx: Effects,
    *,
    registry_cache: RegistryCache | None = None,
) -> dict[str, int]:
    """Hourly entry point (§9.3): the overdue sweep, then the stuck-claim
    sweep. `registry_cache` defaults to this module's own container-lifetime
    cache; tests pass a fresh `{}` for isolation (same convention as
    `drivers/s3_push.py::acquire`).
    """
    cache = _DEFAULT_REGISTRY_CACHE if registry_cache is None else registry_cache
    feeds = list(_load_feed_registry(fx, cache).values())
    overdue_emitted = sweep_overdue(feeds, fx)
    claims_recovered = sweep_stuck_claims(fx)
    return {"overdue_emitted": overdue_emitted, "claims_recovered": claims_recovered}
