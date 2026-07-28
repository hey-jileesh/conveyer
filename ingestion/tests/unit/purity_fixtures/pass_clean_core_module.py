"""MUST-PASS: a plain pure core module — dataclasses, enums, functions over
values, no banned imports/calls, no `class`-shape violations, no
`raise`/`try` — the everyday case the linter should leave alone.

Simulated scope: ingestion/core/** (purity rules apply; nothing here trips
them).
"""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class CompletenessResult:
    verdict: Literal["complete", "incomplete", "defective"]
    reason: str | None


def is_complete(result: CompletenessResult) -> bool:
    return result.verdict == "complete"


def combine(results: tuple[CompletenessResult, ...]) -> bool:
    return all(is_complete(r) for r in results)
