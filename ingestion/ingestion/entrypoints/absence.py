"""Absence detector Lambda handler -- LLD §9.3, §7.1.

Wiring only (§7.1: "Handlers contain wiring only... Any logic in an
entrypoint is a review defect"): build `Effects` from the environment, hand
off to `absence.detector.run`, return. Triggered hourly by EventBridge
Scheduler with an empty payload (§9.3) -- `event`/`context` carry nothing
this handler needs.
"""

from __future__ import annotations

from typing import Any

from ingestion import observability
from ingestion.absence.detector import run
from ingestion.config import from_env
from ingestion.effects.build import build_effects


def handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    del event, context
    observability.install_json_handler()
    fx = build_effects(from_env())
    return run(fx)
