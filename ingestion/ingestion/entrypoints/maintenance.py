"""Maintenance Lambda handler -- LLD §9.4, §7.1.

Wiring only (§7.1: "Handlers contain wiring only... Any logic in an
entrypoint is a review defect"): build `Effects` from the environment, build
the local `AthenaFx` from the same config, run the weekly job, return. Every
decision (OPTIMIZE/VACUUM SQL, polling, supersession reconciliation) lives in
`maintenance/optimize.py`. Triggered weekly by EventBridge Scheduler (§10.5);
`event`/`context` carry no payload this handler needs.
"""

from __future__ import annotations

from typing import Any

from ingestion import observability
from ingestion.config import from_env
from ingestion.effects.build import build_effects
from ingestion.maintenance.optimize import build_athena_fx, run_maintenance


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    observability.install_json_handler()
    fx = build_effects(from_env())
    athena = build_athena_fx(fx.config)
    superseded = run_maintenance(fx, athena)
    return {"supersessions_reconciled": len(superseded)}
