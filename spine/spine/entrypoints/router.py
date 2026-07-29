"""Router Lambda — routes `delivery-registered` to a single-flight `StartExecution`. §8.1.

**Zip Lambda: stdlib + boto3 ONLY** (I-8) -- no pydantic, no pyspark, no
`spine.core.model` import (that module is pydantic-shaped, §6). Packaged by
`make -C spine package-router` as `spine/entrypoints/router.py` +
`spine/core/naming.py` (plus the package `__init__.py`s needed to import
them) -- §7.1. `tests/unit/test_router_zip.py` builds that exact zip into a
private tmp dir and asserts its transitive imports never leave stdlib +
boto3, in a subprocess with `sys.path` restricted accordingly (§7.1's
constraint test).

**Wiring only -- any further logic here is a review defect** (004 §7.1, LLD
§8.1: "pipeline is producer-asserted and deliberately unverified against the
feed registry here" -- the enforcing checks are job-side). Config from env
(`CONVEYER_*`):

  `CONVEYER_SFN_ARN_PREFIX` -- e.g.
      "arn:aws:states:<region>:<acct>:stateMachine:<p>-spine-"
      (everything up to and including the trailing "-spine-"; the router
      appends `slug(pipeline)` to it, §5: "State machine | ${p}-spine-
      <slug>"). Terraform already composes `${p}` = `<name_prefix>-<env>`
      for every other spine name (§5) -- handing the router ONE fully-
      formed prefix (rather than separate `region`/`account_id`/
      `name_prefix`/`env` envs the router would have to re-assemble itself)
      means the router never re-derives `${p}` and can't drift from
      Terraform's own convention. Picked as "one clean shape" per this
      bead's own scope note.
  `CONVEYER_ARGV_BUDGET_BYTES` -- optional; conservative default below.
      Bounds the serialized, ALLOWLISTED detail (`fwd`, not the raw event --
      that's what actually rides in Glue's `--conveyer-delivery` argument,
      §8.2/config.py's `delivery_json`), so a producer padding the raw
      event with huge `extra` fields can't trip this budget for fields that
      never get forwarded anyway. §8.2 [T-5]: the real Glue `StartJobRun.
      Arguments` cap is verified at M6; this default is a conservative
      placeholder comfortably under the commonly-cited ~25 KB Glue argument
      ceiling, leaving headroom for the OTHER Glue arguments alongside
      `--conveyer-delivery` and for JSON-escaping overhead. Override via env
      once M6 pins the real number.

EMF metrics are a **duplicated** ~10-line stdlib dict-print helper, not an
import of `spine.observability` (which is itself stdlib-only in its OWN
imports -- verified -- but is NOT one of the two files `make -C spine
package-router`'s recipe copies into the zip, per §7.1's "containing only
router.py + core/naming.py"; importing it here would work in this repo's
tests but `ModuleNotFoundError` in the actually-deployed zip). Duplicating
these ~10 lines is the idiomatic call for this bead's scope note, not an
import-purity workaround.

Every non-collision error (missing/malformed field, oversized detail,
`StateMachineDoesNotExist` for an unprovisioned pipeline, or anything else)
propagates as a plain `ValueError`/whatever boto3 raises -- Lambda's default
retry-then-DLQ handles it (I-8's "raise -> Lambda retry -> DLQ -> alarm").
This module does NOT raise `spine.effects.records.TransientError`: that
class lives in a pydantic-adjacent module (`effects/records.py` imports
`pydantic.BaseModel`) this zip cannot import, and the router's failure path
already ends at the SAME place (Lambda retry -> DLQ) a `TransientError`
would reach inside the Glue job -- no shared exception type is needed for
that to hold across this deliberately separate deployment artifact.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import boto3  # type: ignore[import-untyped]

from spine.core import naming

logger = logging.getLogger(__name__)

# §6.1 v1 seed fields -- the ONLY fields ever forwarded (the allowlisted
# projection [S-2]: `extra` fields on the bus event must not ride into
# execution history / Glue argv).
V1_FIELDS: tuple[str, ...] = (
    "schema_version",
    "feed_id",
    "delivery_id",
    "batch_id",
    "delivery_key",
    "content_hash",
    "size_bytes",
    "object_uris",
    "received_at",
    "pipeline",
)

# Conservative default (see module docstring); override via
# CONVEYER_ARGV_BUDGET_BYTES once M6 pins the real Glue argv cap [T-5].
_DEFAULT_ARGV_BUDGET_BYTES = 8192

_METRIC_NAMESPACE = "Conveyer/Spine"  # §5, §11.1

# Distinct from `SingleFlightCollisions` (D-10/I-8) -- an oversized detail is
# a DIFFERENT failure mode (fail loud, not retry-loop [T-5, E-10]) and must
# be distinguishable in dashboards/alarms from a duplicate-event collision.
_METRIC_OVERSIZED_DETAIL = "RouterDetailOversized"
_METRIC_SINGLE_FLIGHT_COLLISION = "SingleFlightCollisions"


@dataclass(frozen=True)
class RouterConfig:
    sfn_arn_prefix: str
    argv_budget_bytes: int = _DEFAULT_ARGV_BUDGET_BYTES


def _config_from_env() -> RouterConfig:
    return RouterConfig(
        sfn_arn_prefix=os.environ["CONVEYER_SFN_ARN_PREFIX"],
        argv_budget_bytes=int(
            os.environ.get("CONVEYER_ARGV_BUDGET_BYTES", _DEFAULT_ARGV_BUDGET_BYTES)
        ),
    )


def _emit_metric(name: str, value: float, pipeline: str, feed_id: str) -> None:
    """Hand-rolled CloudWatch EMF, printed to stdout -- §11.1, duplicated
    from `spine/observability.py::emit_metric`'s shape (see module
    docstring for why this is a duplicate, not an import)."""
    payload: dict[str, object] = {
        "_aws": {
            "Timestamp": int(datetime.now(UTC).timestamp() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": _METRIC_NAMESPACE,
                    "Dimensions": [["pipeline", "feed_id"]],
                    "Metrics": [{"Name": name, "Unit": "Count"}],
                }
            ],
        },
        "pipeline": pipeline,
        "feed_id": feed_id,
        name: value,
    }
    print(json.dumps(payload))  # noqa: T201 -- the sanctioned §11.1 mechanism


def route(detail: dict[str, Any], sfn: Any, config: RouterConfig) -> dict[str, Any]:
    """The router's entire decision logic, factored out of `handler` so
    `sfn` (a boto3 `stepfunctions` client, or the record-of-functions test
    double, I-13) is an injected parameter, never a module-global -- clients
    are parameters (this bead's own testability requirement, mirrored on
    every other spine effect, e.g. `effects/build.py::make_runner_fx`).

    Returns `{"batch_id": ..., "started": bool}` -- `started=False` iff the
    execution already existed (single-flight collision, D-10: still success).
    """
    missing = [field for field in V1_FIELDS if field not in detail]
    if missing:
        raise ValueError(f"delivery-registered detail missing required v1 field(s): {missing!r}")

    # I-22: batch_id must be a UUIDv5. Reuses `naming.execution_name`, which
    # validates AND returns the (unchanged) batch_id -- the SFN execution
    # name IS the batch_id, exactly (§5).
    batch_id = naming.execution_name(detail["batch_id"])
    pipeline = detail["pipeline"]
    feed_id = detail["feed_id"]

    # §5 slug grammar, validated by `naming.slug` before any ARN composition.
    slug = naming.slug(pipeline)

    # [S-2] allowlist projection -- never the raw detail. `missing` above
    # already guarantees every V1_FIELDS key is present.
    fwd = {field: detail[field] for field in V1_FIELDS}
    serialized = json.dumps(fwd)

    # §8.2 [T-5]: bounds `fwd` (what actually rides in Glue's
    # `--conveyer-delivery` argument), not the raw `detail` -- an `extra`
    # field the allowlist already drops can't trip this budget.
    size_bytes = len(serialized.encode("utf-8"))
    if size_bytes > config.argv_budget_bytes:
        _emit_metric(_METRIC_OVERSIZED_DETAIL, 1.0, pipeline, feed_id)
        raise ValueError(
            f"delivery detail for batch_id={batch_id!r} is {size_bytes} bytes, "
            f"exceeding the Glue argv budget of {config.argv_budget_bytes} bytes "
            "(§8.2 [T-5]) -- fail loud, not retry-loop [E-10]"
        )

    arn = f"{config.sfn_arn_prefix}{slug}"

    try:
        sfn.start_execution(stateMachineArn=arn, name=batch_id, input=serialized)
    except sfn.exceptions.ExecutionAlreadyExists:
        # D-10: success. [T-20]: `StartExecution` with the same name AND
        # byte-identical input against a RUNNING execution returns 200 with
        # the existing ARN instead of raising -- the MOST COMMON duplicate
        # never reaches this branch, so `SingleFlightCollisions` undercounts
        # true joins. Detecting "joined existing" would need comparing the
        # success response's `startDate` against "now"; not implemented --
        # a documented undercount, not a silent one (§8.1's own note).
        logger.info(
            "execution already exists for batch_id=%s (single-flight collision, D-10: success)",
            batch_id,
        )
        _emit_metric(_METRIC_SINGLE_FLIGHT_COLLISION, 1.0, pipeline, feed_id)
        return {"batch_id": batch_id, "started": False}
    # `StateMachineDoesNotExist` (unprovisioned pipeline) and everything
    # else: propagate -- Lambda retry -> DLQ -> alarm (I-8). Not a silent
    # drop: an unprovisioned pipeline is an ops signal.

    return {"batch_id": batch_id, "started": True}


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    config = _config_from_env()
    sfn = boto3.client("stepfunctions")
    return route(event["detail"], sfn, config)
