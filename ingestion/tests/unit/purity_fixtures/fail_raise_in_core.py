"""MUST-FAIL: `raise` is banned in core/** — defects are values (LLD §7.0
rule 4), never exceptions. This function has no `@field_validator`/
`@model_validator` decorator (it is a bare, undecorated function), so the
conveyer-4ot.24 validator-raise exemption does not apply -- it stays
flagged even after that exemption exists (see `pass_validator_raise.py` for
the positive case).

Simulated scope: ingestion/core/** (purity rules apply).
"""


def guard(x: int) -> int:
    if x < 0:
        raise ValueError("x must be non-negative")
    return x
