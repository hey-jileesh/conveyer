"""G-14 — invalid `source.yaml` configs produce exact `FeedConfig` validation
error messages (LLD §12.4: "Unknown/invalid `source.yaml` (timer+s3-push;
extra field)" -> "`FeedConfig` validation errors, exact messages").

Pure: no local stack needed — `FeedConfig` validation is plain pydantic
parsing over dicts, exercised directly.
"""

from typing import Any

import pytest
from ingestion.core.model import FeedConfig
from pydantic import ValidationError

# Mirrors sources/carrier-y/renewal-statements/source.yaml (§15.2) — a valid
# s3-push feed, the base every G-14 scenario perturbs by exactly one field.
_BASE_S3_PUSH: dict[str, Any] = {
    "feed_id": "carrier-y/renewal-statements",
    "driver": "s3-push",
    "pipeline": "pipelines/renewals",
    "connection": {"partner_principal_arns": ["arn:aws:iam::111111111111:role/carrier-y-uploader"]},
    "expectation": {"expected": "weekly:mon", "by": "09:00", "timezone": "America/New_York"},
    "completeness": {"mode": "manifest", "manifest_pattern": "*.manifest.json"},
}


def _error_messages(exc: ValidationError) -> list[str]:
    """The exact string each validator raised, stripped of pydantic's
    "Value error, " decoration — `ctx.error` holds the raw exception for
    `@model_validator`/`@field_validator` failures; built-in pydantic
    violations (e.g. "Extra inputs are not permitted") carry no `ctx.error`
    and their `msg` is already the exact, undecorated string.
    """
    messages: list[str] = []
    for err in exc.errors():
        ctx_error = err.get("ctx", {}).get("error")
        messages.append(str(ctx_error) if ctx_error is not None else err["msg"])
    return messages


def test_g14_timer_completeness_rejected_for_s3_push() -> None:
    config = dict(_BASE_S3_PUSH)
    config["completeness"] = {
        "mode": "timer",
        "timer": {"quiet_window_minutes": 15, "accepted_risk": "x" * 25},
    }

    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)

    assert _error_messages(exc_info.value) == [
        "timer completeness is not supported for s3-push in Phase 1 (LLD D-10)"
    ]


def test_g14_extra_field_rejected() -> None:
    config = dict(_BASE_S3_PUSH)
    config["unexpected_field"] = "not allowed"

    with pytest.raises(ValidationError) as exc_info:
        FeedConfig.model_validate(config)

    assert _error_messages(exc_info.value) == ["Extra inputs are not permitted"]
