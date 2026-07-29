"""Post-deploy smoke probe -- LLD 004.1 S10.6 step 6 / S12.1, `make -C spine
smoke ENV=<env>`.

Runs against REAL dev AWS (never moto): drops a single fixture object into
`conveyer-internal/identity-smoke`'s vestibule prefix as an external partner
would (plain `s3:PutObject`, no ingestion driver/effects code -- this test
IS the partner, mirroring `ingestion/tests/smoke/test_smoke.py`'s own
framing exactly), then polls THREE things per this bead's task framing:

1. The run ledger (pyiceberg `GlueCatalog` read, `<spine_db>.run_ledger`)
   for a `stage="publish"`, `outcome="ok"` row -- this is the batch's
   `batch-completed` signal (LLD I-18: "execution SUCCEEDED implies
   batch-completed emitted"), read via the run ledger rather than
   CloudWatch Logs because NO EventBridge rule sinks `conveyer.spine`
   events into a log group anywhere in this LLD's Terraform (verified:
   `modules/spine-platform/events.tf` only wires `delivery-registered` ->
   router; ingestion's own `observability` rule is scoped to
   `source = ["conveyer.ingestion"]` and therefore never sees spine's
   `batch-started`/`batch-completed` -- a real, named observability gap,
   not an oversight in this test). This is exactly the task's own "poll for
   batch-started/batch-completed on the bus OR via the run ledger" choice.
2. The identity exemplar's facts table for a row carrying that batch_id
   (pyiceberg read, `<lake_db>.<slug>__facts` -- see `conftest.py`'s
   `SmokeNames.identity_table_prefix` docstring for the slug-naming
   discrepancy this suite assumes is resolved by deploy time).
3. EMF metrics [T-14]: the Glue job's own continuous-logging log group for
   at least one printed EMF line (the `_aws`/CloudWatch-metrics-embedded-
   JSON marker) after this run started -- proves M6's "assert extraction
   end-to-end" obligation, at least at the "was anything printed" level (a
   real CloudWatch Metrics existence check is a further, optional
   strengthening left to the deploy-gate bead, 004.1 S13.1's checklist).

Content is generated FRESH per run (a random `run_tag` folded into the
delivery key and every row) for the same reason ingestion's own smoke test
does this: a byte-identical replay would content-address to the SAME
`batch_id` (D-4) and register as a no-op duplicate, breaking this test's own
idempotent-rerun property. A fresh run always mints a fresh `batch_id`, so
`make -C spine smoke ENV=dev` is safe to run repeatedly against the same
dev account.

Bounded polling only -- `_poll_until` fails via `pytest.fail` with the last
observed state, never hangs.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TypeVar

import boto3  # type: ignore[import-untyped]
import pytest
from _names import SmokeNames
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual
from pyiceberg.io import AWS_REGION

_FEED_ID = "conveyer-internal/identity-smoke"
_VESTIBULE_PREFIX = f"{_FEED_ID}/incoming/"
_PIPELINE = "pipelines/identity"

_POLL_TIMEOUT_S = 600.0  # generous: router -> SFN -> Glue cold start + 8 stages
_POLL_INTERVAL_S = 10.0


# --- fixture delivery: fresh content every run (see module docstring) ------


def _smoke_delivery(run_tag: str, now: datetime) -> tuple[str, bytes]:
    """Returns `(delivery_key, body)` -- a single trailer-mode object (LLD
    004.1 S12.6(4)/I-8's "single-object trailer... minimal mode": the
    identity-smoke feed's own `completeness.mode = "trailer"`,
    `pattern = "TOTAL:\\d+"`). The last non-empty line must fullmatch that
    pattern (`ingestion.core.completeness.evaluate_trailer`) for the
    delivery to register as complete; `count_group` is unset in this feed's
    `source.yaml`, so the numeric value itself is never asserted, only the
    line's presence/shape.
    """
    delivery_key = f"smoke-{run_tag}.csv"
    body = (
        "domain_id,event_time,source_ts,content_hash,payload\n"
        f"smoke-{run_tag},{now.isoformat()},{now.isoformat()},"
        f"h-{run_tag},smoke\n"
        "TOTAL:1\n"
    ).encode()
    return delivery_key, body


def _upload_to_vestibule(
    s3_client: Any, landing_bucket: str, delivery_key: str, body: bytes
) -> None:
    s3_client.put_object(Bucket=landing_bucket, Key=_VESTIBULE_PREFIX + delivery_key, Body=body)


# --- bounded polling ---------------------------------------------------------


_T = TypeVar("_T")


def _poll_until(
    check: Callable[[], _T | None], timeout_s: float, interval_s: float, description: str
) -> _T:
    deadline = time.monotonic() + timeout_s
    last_result: _T | None = None
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


# --- run ledger: batch-completed signal (see module docstring) -------------


def _poll_publish_ok_row(
    smoke_names: SmokeNames, since: datetime, delivery_key: str
) -> dict[str, Any]:
    catalog = GlueCatalog("conveyer-spine-smoke", **{AWS_REGION: smoke_names.region})

    def _check() -> dict[str, Any] | None:
        table = catalog.load_table(smoke_names.run_ledger_identifier)
        expr = And(
            EqualTo("pipeline", _PIPELINE),
            And(
                EqualTo("feed_id", _FEED_ID),
                And(
                    EqualTo("stage", "publish"),
                    And(EqualTo("outcome", "ok"), GreaterThanOrEqual("started_at", since)),
                ),
            ),
        )
        rows = table.scan(row_filter=expr).to_arrow().to_pylist()
        # `delivery_key` is not itself a run-ledger column (S6.5) -- this
        # smoke run's own `batch_id` is unknown until the ledger names it,
        # so the match is by feed_id + time window alone; a matching row
        # not belonging to THIS run would require a concurrent identity
        # smoke run in the same window, out of this suite's scope to guard.
        return rows[0] if rows else None

    return _poll_until(
        _check,
        _POLL_TIMEOUT_S,
        _POLL_INTERVAL_S,
        f"a publish/ok run-ledger row for feed_id={_FEED_ID!r} (delivery {delivery_key!r})",
    )


# --- facts table -------------------------------------------------------------


def _poll_facts_row(smoke_names: SmokeNames, batch_id: str) -> dict[str, Any]:
    catalog = GlueCatalog("conveyer-spine-smoke", **{AWS_REGION: smoke_names.region})

    def _check() -> dict[str, Any] | None:
        table = catalog.load_table(smoke_names.identity_facts_table)
        rows = table.scan(row_filter=EqualTo("batch_id", batch_id)).to_arrow().to_pylist()
        return rows[0] if rows else None

    return _poll_until(
        _check, _POLL_TIMEOUT_S, _POLL_INTERVAL_S, f"a facts-table row for batch_id={batch_id!r}"
    )


# --- EMF metrics [T-14] -----------------------------------------------------


def _poll_emf_line(logs_client: Any, log_group: str, since_ms: int) -> str:
    def _check() -> str | None:
        try:
            response = logs_client.filter_log_events(
                logGroupName=log_group, startTime=since_ms, filterPattern='"_aws"'
            )
        except logs_client.exceptions.ResourceNotFoundException:
            return None
        events = response.get("events", [])
        return events[0]["message"] if events else None

    return _poll_until(
        _check, _POLL_TIMEOUT_S, _POLL_INTERVAL_S, f"an EMF-marked log line in {log_group!r}"
    )


# --- the probe ----------------------------------------------------------------


def test_identity_smoke_happy_path_publishes_facts_and_emf(
    smoke_names: SmokeNames, aws_credentials: None
) -> None:
    s3_client = boto3.client("s3", region_name=smoke_names.region)
    logs_client = boto3.client("logs", region_name=smoke_names.region)

    started_at = datetime.now(UTC)
    since_ms = int(started_at.timestamp() * 1000)
    run_tag = uuid.uuid4().hex[:12]
    delivery_key, body = _smoke_delivery(run_tag, started_at)

    _upload_to_vestibule(s3_client, smoke_names.landing_bucket, delivery_key, body)

    ledger_row = _poll_publish_ok_row(smoke_names, started_at, delivery_key)
    batch_id = ledger_row["batch_id"]
    assert ledger_row["pipeline"] == _PIPELINE
    assert ledger_row["feed_id"] == _FEED_ID

    facts_row = _poll_facts_row(smoke_names, batch_id)
    assert facts_row["batch_id"] == batch_id

    emf_line = _poll_emf_line(logs_client, smoke_names.glue_job_log_group, since_ms)
    assert "BatchesCompleted" in emf_line or "_aws" in emf_line
