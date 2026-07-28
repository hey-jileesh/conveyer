"""MUST-PASS: the fully-qualified `@dataclasses.dataclass(frozen=True)`
spelling is equally sanctioned (both spellings must be recognized).

Simulated scope: ingestion/core/** (purity rules apply too; nothing here
trips them).
"""

import dataclasses


@dataclasses.dataclass(frozen=True)
class Defect:
    reason: str
