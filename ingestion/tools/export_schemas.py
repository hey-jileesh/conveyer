"""Regenerate `contracts/*.json` from `ingestion.core.model` — LLD §6, `make schemas`.

`ingestion/core/model.py` is the single source of truth for every contract
(LLD §6 preamble); this script exports the four boundary contracts named in
the repo tree (LLD §4) as JSON Schema via `model_json_schema()`, serialized
deterministically (sorted keys, trailing newline) so `make schemas`'s
`git diff --exit-code -- contracts/` drift gate is stable across reruns on
an unchanged model.
"""

import json
from pathlib import Path
from typing import Final

from ingestion.core import model
from pydantic import BaseModel

_PACKAGE_ROOT: Final[Path] = Path(__file__).resolve().parent.parent

_CONTRACTS: Final[tuple[tuple[type[BaseModel], Path], ...]] = (
    (model.DeliveryRegisteredV1, _PACKAGE_ROOT / "contracts/events/delivery-registered.v1.json"),
    (model.DeliveryOverdueV1, _PACKAGE_ROOT / "contracts/events/delivery-overdue.v1.json"),
    (model.ManifestV1, _PACKAGE_ROOT / "contracts/manifest/conveyer-manifest.v1.json"),
    (model.FeedConfig, _PACKAGE_ROOT / "contracts/source/source-config.v1.json"),
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
