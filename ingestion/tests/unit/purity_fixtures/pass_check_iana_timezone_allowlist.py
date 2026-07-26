"""MUST-PASS: the SECOND hardcoded (file, function) allowlist entry (LLD
§6.1/§7.3/§12.2 director adjudication, conveyer-4ot.24) --
`ingestion/core/model.py::_check_iana_timezone`, the validator-support
helper `@field_validator("timezone")` methods delegate to. `zoneinfo.ZoneInfo`
signals an unknown IANA name by raising `ZoneInfoNotFoundError`, so this
plain (undecorated) helper must itself catch-and-reraise as `ValueError` --
both the `try` and the `raise` are exempt when this exact (file, function)
pair matches.

Simulated scope: ingestion/core/model.py (must match exactly for the
allowlist to apply -- see the two `fail_check_iana_timezone_*` fixtures for
the negative cases: wrong function name, and wrong file).
"""

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _check_iana_timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"timezone {value!r} is not a valid IANA timezone name") from exc
    return value
