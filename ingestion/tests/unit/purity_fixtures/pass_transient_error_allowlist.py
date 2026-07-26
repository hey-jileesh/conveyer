"""MUST-PASS: the THIRD hardcoded (file, class name) allowlist entry (LLD
§7.3/§7.0 rule 4/§12.2 contradiction, bd conveyer-4ot.26) --
`ingestion/effects/records.py::TransientError`, "the only exception type in
the codebase" (§7.3), raised only by effect functions. `Exception` is not
one of IDIOM's three accepted class shapes (frozen dataclass / BaseModel /
Enum family), so this exact (file, class name) pair is exempted from
`idiom-class` -- narrower than a general Exception-subclass exemption (see
`fail_other_exception_subclass_in_effects.py` for the negative case).

Simulated scope: ingestion/effects/records.py (must match exactly for the
allowlist to apply -- see `fail_transient_error_wrong_file.py` for the
negative case: same class, wrong file).
"""


class TransientError(Exception):
    """Infra failure that should retry / alarm (§7.3)."""
