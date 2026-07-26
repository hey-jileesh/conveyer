"""Per-feed sftp-pull driver Lambda handler -- LLD §9.2, §7.1.

Wiring only (§7.1: "Handlers contain wiring only... Any logic in an
entrypoint is a review defect"): build `Effects` from the environment,
resolve THIS function's single `FeedConfig` from the registry
(`CONVEYER_FEED_ID`, set per-function by `modules/feed`'s
`sftp_pull.tf`, §10.7), parse the trigger payload, hand off to
`drivers.sftp_pull.acquire`, return. Every decision (candidate selection,
completeness, staging, registration, resume) lives in
`drivers/sftp_pull.py` and `registration/registrar.py`.

Payload shapes (§7.6 / §9.2):
* `{}` -- scheduled run; window derived from the ledger fold.
* `{"window": {"start": "...", "end": "..."}, "force": bool}` -- operator
  re-pull.
* `{"resume_batch_id": "..."}` -- §9.3 stuck-claim sweep resume.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ingestion import observability
from ingestion.config import from_env
from ingestion.core.model import Window
from ingestion.drivers.sftp_pull import acquire, load_feed_config
from ingestion.effects.build import build_effects


def _parse_window(payload: dict[str, Any]) -> Window:
    raw = payload.get("window")
    if raw is None:
        return Window(start=None, end=None)
    start = datetime.fromisoformat(raw["start"]) if raw.get("start") else None
    end = datetime.fromisoformat(raw["end"]) if raw.get("end") else None
    return Window(start=start, end=end)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    observability.install_json_handler()
    fx = build_effects(from_env())
    feed_id = fx.config.feed_id
    if feed_id is None:
        raise RuntimeError("CONVEYER_FEED_ID is required for the sftp-pull driver function")
    feed = load_feed_config(fx, feed_id)
    if feed is None:
        raise RuntimeError(f"no registered feed for feed_id {feed_id!r}")

    outcomes = acquire(
        feed,
        _parse_window(event),
        fx,
        context.aws_request_id,
        force=bool(event.get("force", False)),
        resume_batch_id=event.get("resume_batch_id"),
    )
    return {"acquired": len(outcomes)}
