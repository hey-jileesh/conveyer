"""MUST-PASS: `@dataclass(frozen=True)` via `from dataclasses import dataclass`
is the sanctioned value shape (LLD §7.0 rule 1).

Simulated scope: ingestion/core/** (purity rules apply too; nothing here
trips them).
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ObjectStat:
    name: str
    bytes: int
    sha256: str | None
