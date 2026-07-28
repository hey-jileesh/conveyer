"""MUST-PASS: the ONE hardcoded allowlist entry (LLD §7.3 / §12.2) — a `try`
inside a function literally named `parse_manifest`, when this file is
simulated at ingestion/core/completeness.py, is permitted: "the ONE place a
pydantic exception is caught-and-reified" despite core/ banning `try`
outright.

Simulated scope: ingestion/core/completeness.py (must match exactly for the
allowlist to apply — see the two `fail_try_*` fixtures for the negative
cases: wrong function name, and wrong file).
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Defect:
    reason: str


def parse_manifest(raw: bytes):
    try:
        manifest = _validate(raw)
    except ValueError as exc:
        return Defect(reason=str(exc))
    return manifest


def _validate(raw: bytes):
    return raw
