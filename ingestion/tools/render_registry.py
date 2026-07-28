"""Validate + render `sources/**/source.yaml` locally — LLD §6.8, D-12, `make registry`.

Production registry rendering is owned by Terraform (D-12: `fileset` +
`yamldecode` over `sources/**/source.yaml`, merged to
`s3://${p}-artifacts/registry/feeds.json`). This script is the **local,
CI-run validation gate** for the same files: glob `sources/*/*/source.yaml`,
validate each as `FeedConfig`, and render the merged
`{"registry_version": 1, "feeds": [...]}` shape locally — no S3, no
Terraform. Feed entries are the **raw YAML dicts, verbatim**, matching
Terraform's `yamldecode` (no pydantic default-filling/normalization), so the
local render matches what infra will actually produce. On any invalid
`source.yaml`, print the validator messages and exit nonzero — this is the
authoritative validation gate; `terraform plan` only checks YAML
well-formedness (§6.1).
"""

import json
import sys
from pathlib import Path
from typing import Any, Final

import yaml  # type: ignore[import-untyped]  # no bundled/dev stub package yet
from ingestion.core.model import FeedConfig
from pydantic import ValidationError

_PACKAGE_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
_SOURCES_GLOB: Final[str] = "sources/*/*/source.yaml"


def discover_source_files(package_root: Path) -> tuple[Path, ...]:
    return tuple(sorted(package_root.glob(_SOURCES_GLOB)))


def render_registry(
    package_root: Path,
) -> tuple[dict[str, Any], tuple[tuple[Path, ValidationError], ...]]:
    """Validate every discovered `source.yaml` as `FeedConfig`.

    Returns the `{registry_version, feeds}` shape (feed entries verbatim from
    YAML) plus any `(path, ValidationError)` failures. `feeds` only contains
    entries for files that validated; callers must treat any errors as fatal
    (LLD §6.8/D-12 — an invalid `source.yaml` must never reach the registry).
    """
    feeds: list[dict[str, Any]] = []
    errors: list[tuple[Path, ValidationError]] = []
    for path in discover_source_files(package_root):
        raw = yaml.safe_load(path.read_text())
        try:
            FeedConfig.model_validate(raw)
        except ValidationError as exc:
            errors.append((path, exc))
            continue
        feeds.append(raw)
    registry = {"registry_version": 1, "feeds": feeds}
    return registry, tuple(errors)


def _print_errors(errors: tuple[tuple[Path, ValidationError], ...]) -> None:
    for path, exc in errors:
        print(f"INVALID {path}:", file=sys.stderr)
        for err in exc.errors():
            loc = ".".join(str(part) for part in err["loc"])
            location = f"{loc}: " if loc else ""
            print(f"  {location}{err['msg']}", file=sys.stderr)


def main() -> int:
    registry, errors = render_registry(_PACKAGE_ROOT)
    if errors:
        _print_errors(errors)
        return 1
    print(json.dumps(registry, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
