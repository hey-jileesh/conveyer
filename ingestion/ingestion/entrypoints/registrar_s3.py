"""Registrar Lambda handler -- LLD §8.2, §7.1.

Wiring only (§7.1: "Handlers contain wiring only... Any logic in an
entrypoint is a review defect"): build `Effects` from the environment,
extract the Lambda request id, hand the raw event to
`drivers.s3_push.acquire`, return. Every decision (manifest/trailer mode,
completeness, staging, registration) lives in `drivers/s3_push.py` and
`registration/registrar.py`.
"""

from __future__ import annotations

from typing import Any

from ingestion import observability
from ingestion.config import from_env
from ingestion.drivers.s3_push import acquire
from ingestion.effects.build import build_effects


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    observability.install_json_handler()
    fx = build_effects(from_env())
    outcomes = acquire(event, fx, context.aws_request_id)
    return {"registered": len(outcomes)}
