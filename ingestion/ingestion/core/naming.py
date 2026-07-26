"""Pure naming/URI helpers -- LLD §5, shared across drivers/absence/decisions.

Consolidates three previously-independent copies of the canonical-landing-key
convention (received-at formatting + canonical URI) and `s3://` URI parsing
that lived in `drivers/s3_push.py`, `drivers/sftp_pull.py`,
`absence/detector.py`, and `core/decisions.py::_parse_s3_uri` (critique-gate
finding F-2). Pure string/datetime manipulation only, no I/O -- lives in
`core/` precisely so `core/decisions.py` can use it without crossing the
core/effects boundary; the purity linter's PURITY scope (banned imports,
banned `raise`/`try`) applies here same as any other `ingestion/core/*.py`
module, and this module needs none of those constructs.
"""

from __future__ import annotations

from datetime import datetime


def format_received_at(ts: datetime) -> str:
    """§5: UTC, microseconds, basic ISO8601 (no dashes/colons)."""
    return ts.strftime("%Y%m%dT%H%M%S") + f"{ts.microsecond:06d}Z"


def canonical_prefix(feed_id: str, received_at: datetime, delivery_id: str) -> str:
    """§5 canonical landing prefix (no bucket, no object name):
    `<source>/<feed>/received_at=<ts>/dl-<delivery_id>/`.
    """
    ts = format_received_at(received_at)
    return f"{feed_id}/received_at={ts}/dl-{delivery_id}/"


def canonical_uri(
    landing_bucket: str, feed_id: str, received_at: datetime, delivery_id: str, name: str
) -> str:
    """§5 canonical landing key: `<source>/<feed>/received_at=<ts>/dl-<delivery_id>/<name>`."""
    return f"s3://{landing_bucket}/{canonical_prefix(feed_id, received_at, delivery_id)}{name}"


def split_s3_uri(uri: str) -> tuple[str, str]:
    """`s3://bucket/key...` -> (bucket, key)."""
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key
