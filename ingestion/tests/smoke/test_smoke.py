"""Post-deploy smoke probe -- LLD §12.1/§12.5, `make smoke ENV=<env>`.

Runs against REAL dev AWS (never moto, §12.1's "Smoke (post-deploy) | real
dev AWS"): drops a carrier-y-shaped happy-path delivery (3 parts, then the
manifest LAST -- manifest arrival is what the deployed EventBridge rule
actually watches for a manifest-mode feed, §10.7) into
`carrier-y/renewal-statements`'s vestibule prefix as an external partner
would (plain `s3:PutObject`, no `Effects`/driver code -- this test IS the
partner), then polls the delivery ledger (pyiceberg `GlueCatalog` via the
production `effects.ledger.make_ledger_fx`, folded through
`core.folds.registered_deliveries`, §7.4) and the `/conveyer/<env>/ingestion
/events` CloudWatch log group (§10.5's observability rule target) for
exactly one matching `registered` row and one `delivery-registered` event.

The delivery's content is generated FRESH per run (a random `run_tag` folded
into the manifest_id and every data byte) rather than replaying the
committed carrier-y fixture verbatim: D-4's content-addressed `batch_id`
means replaying byte-identical content against an already-`registered` prior
smoke run would register as `duplicate` (G-05a) -- no new row, no new event
-- breaking this test's own idempotent-rerun property. A fresh run always
gets a fresh `batch_id`, so `make smoke ENV=dev` is itself safe to run
repeatedly against the same dev account.

Bounded polling only (LLD requirement: "bounded polling with timeout ->
clear failure message") -- `_poll_until` fails via `pytest.fail` with the
last observed state, never hangs.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3  # type: ignore[import-untyped]
import pytest
from ingestion.config import RuntimeConfig
from ingestion.core import folds
from ingestion.core.model import DeliveryRecord, ManifestFile, ManifestV1
from ingestion.effects.ledger import LedgerConfig, make_ledger_fx

_FEED_ID = "carrier-y/renewal-statements"
_VESTIBULE_PREFIX = f"{_FEED_ID}/incoming/"
_LOG_GROUP_TEMPLATE = "/conveyer/{env}/ingestion/events"

_POLL_TIMEOUT_S = 180.0
_POLL_INTERVAL_S = 5.0


# --- fixture delivery: fresh content every run (see module docstring) ------


def _smoke_delivery(run_tag: str, now: datetime) -> tuple[str, dict[str, bytes], tuple[str, ...]]:
    """Returns `(manifest_id, {name: bytes}, upload_order)` -- `upload_order`
    is parts first, manifest LAST (matches carrier-y's committed fixture
    shape, §15.2, and the s3-push manifest-mode arrival contract, §10.7).
    """
    manifest_id = f"smoke-{run_tag}"
    parts = {
        name: f"policy_id,premium\nSMOKE-{run_tag}-{i},100.00\n".encode()
        for i, name in enumerate(("part1.csv", "part2.csv", "part3.csv"), start=1)
    }
    manifest = ManifestV1(
        manifest_version=1,
        manifest_id=manifest_id,
        feed_id=_FEED_ID,
        files=[
            ManifestFile(name=name, bytes=len(content), sha256=hashlib.sha256(content).hexdigest())
            for name, content in parts.items()
        ],
        created_at=now,
    )
    manifest_name = f"{manifest_id}.manifest.json"
    files = {**parts, manifest_name: manifest.model_dump_json().encode("utf-8")}
    return manifest_id, files, (*parts.keys(), manifest_name)


def _upload_to_vestibule(
    s3_client: Any, landing_bucket: str, files: dict[str, bytes], upload_order: tuple[str, ...]
) -> None:
    for name in upload_order:
        s3_client.put_object(Bucket=landing_bucket, Key=_VESTIBULE_PREFIX + name, Body=files[name])


# --- bounded polling ---------------------------------------------------------


def _poll_until[T](
    check: Callable[[], T | None], timeout_s: float, interval_s: float, description: str
) -> T:
    deadline = time.monotonic() + timeout_s
    last_result: T | None = None
    while True:
        last_result = check()
        if last_result is not None:
            return last_result
        if time.monotonic() >= deadline:
            pytest.fail(
                f"timed out after {timeout_s}s waiting for {description} "
                f"(last observed: {last_result!r})"
            )
        time.sleep(interval_s)


def _poll_registered_row(
    ledger_fx: Any, feed_id: str, since: datetime, manifest_id: str
) -> DeliveryRecord:
    def _check() -> DeliveryRecord | None:
        rows = ledger_fx.scan_feed(feed_id, since)
        registered = [r for r in folds.registered_deliveries(rows) if r.delivery_key == manifest_id]
        if len(registered) > 1:
            pytest.fail(
                f"expected exactly one registered row for delivery_key={manifest_id!r}, "
                f"found {len(registered)}: {registered!r}"
            )
        return registered[0] if registered else None

    return _poll_until(
        _check, _POLL_TIMEOUT_S, _POLL_INTERVAL_S, f"a registered ledger row for {manifest_id!r}"
    )


def _matching_registered_events(raw_events: list[dict[str, Any]], manifest_id: str) -> list[Any]:
    matches = []
    for event in raw_events:
        try:
            payload = json.loads(event["message"])
        except json.JSONDecodeError:
            continue
        detail = payload.get("detail", {})
        if (
            payload.get("detail-type") == "delivery-registered"
            and detail.get("delivery_key") == manifest_id
        ):
            matches.append(payload)
    return matches


def _poll_registered_event(
    logs_client: Any, log_group: str, since_ms: int, manifest_id: str
) -> dict[str, Any]:
    def _check() -> dict[str, Any] | None:
        try:
            response = logs_client.filter_log_events(logGroupName=log_group, startTime=since_ms)
        except logs_client.exceptions.ResourceNotFoundException:
            return None
        matches = _matching_registered_events(response.get("events", []), manifest_id)
        if len(matches) > 1:
            pytest.fail(
                f"expected exactly one delivery-registered event for "
                f"delivery_key={manifest_id!r}, found {len(matches)}: {matches!r}"
            )
        return matches[0] if matches else None

    return _poll_until(
        _check,
        _POLL_TIMEOUT_S,
        _POLL_INTERVAL_S,
        f"a delivery-registered event for {manifest_id!r} in log group {log_group!r}",
    )


# --- the probe ----------------------------------------------------------------


def test_carrier_y_happy_path_registers_and_emits_event(
    smoke_config: RuntimeConfig, aws_credentials: None
) -> None:
    region = smoke_config.aws_region
    s3_client = boto3.client("s3", region_name=region)
    logs_client = boto3.client("logs", region_name=region)

    started_at = datetime.now(UTC)
    run_tag = uuid.uuid4().hex[:12]
    manifest_id, files, upload_order = _smoke_delivery(run_tag, started_at)

    _upload_to_vestibule(s3_client, smoke_config.landing_bucket, files, upload_order)

    ledger_fx = make_ledger_fx(
        LedgerConfig(
            catalog_kind="glue",
            glue_database=smoke_config.glue_database,
            table_name=smoke_config.ledger_table,
            warehouse_uri=f"s3://{smoke_config.lake_bucket}/ledger/",
        )
    )
    since = started_at - timedelta(minutes=1)  # small buffer against clock skew
    row = _poll_registered_row(ledger_fx, _FEED_ID, since, manifest_id)
    assert row.disposition == "registered"
    assert row.feed_id == _FEED_ID

    log_group = _LOG_GROUP_TEMPLATE.format(env=smoke_config.env)
    since_ms = int(started_at.timestamp() * 1000)
    event = _poll_registered_event(logs_client, log_group, since_ms, manifest_id)
    assert event["detail"]["batch_id"] == row.batch_id
    assert event["detail"]["feed_id"] == _FEED_ID
