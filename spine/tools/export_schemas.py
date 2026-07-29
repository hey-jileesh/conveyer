"""Regenerate `spine/contracts/*.json` from `spine.core.model` — LLD §6 preamble, `make schemas`.

Mirrors `ingestion/tools/export_schemas.py`'s pattern exactly: `spine/core/
model.py` (plus `spine/config.py` for `RunConfig`, the one framework-owned
boundary model that lives outside `core/`) is the single source of truth for
every spine contract (LLD §6 preamble — "`make -C spine schemas` exports
JSON Schema; drift-checked in CI like 002.1 §6"); this script exports each
boundary contract as JSON Schema via `model_json_schema()`, serialized
deterministically (sorted keys, trailing newline) so `make schemas`'s
`git diff --exit-code -- contracts/` drift gate is stable across reruns on
an unchanged model.

M1 (bead `conveyer-nvh.13`) fills `_CONTRACTS` with the five boundary models
named in the LLD's §6 sections: the spine-side seed event (§6.1, its own
model per 004 D-13 -- no shared code with ingestion's own export), the
pipeline spec (§6.2), the framework-owned `RunConfig` (§6.4), and the two
lifecycle events (§6.6).
"""

import json
from pathlib import Path
from typing import Final

from pydantic import BaseModel
from spine import config
from spine.core import model

_PACKAGE_ROOT: Final[Path] = Path(__file__).resolve().parent.parent

_CONTRACTS: Final[tuple[tuple[type[BaseModel], Path], ...]] = (
    (model.DeliveryRegisteredV1, _PACKAGE_ROOT / "contracts/events/delivery-registered.v1.json"),
    (model.BatchStartedV1, _PACKAGE_ROOT / "contracts/events/batch-started.v1.json"),
    (model.BatchCompletedV1, _PACKAGE_ROOT / "contracts/events/batch-completed.v1.json"),
    (model.PipelineSpecModel, _PACKAGE_ROOT / "contracts/pipeline/pipeline-spec.v1.json"),
    (config.RunConfig, _PACKAGE_ROOT / "contracts/config/run-config.v1.json"),
)


def render_schema(model_cls: type[BaseModel]) -> str:
    """`model_json_schema()` serialized deterministically: sorted keys, trailing newline."""
    schema = model_cls.model_json_schema()
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def export_schemas() -> tuple[Path, ...]:
    written: list[Path] = []
    for model_cls, dest in _CONTRACTS:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(render_schema(model_cls))
        written.append(dest)
    return tuple(written)


def main() -> None:
    for dest in export_schemas():
        print(f"wrote {dest}")


if __name__ == "__main__":
    main()
