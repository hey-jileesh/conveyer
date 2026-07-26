"""MUST-FAIL: the second try/raise allowlist entry is keyed to (file,
function) -- the file matches ingestion/core/model.py when simulated, but
the function is not `_check_iana_timezone`, so both the `try` and the
`raise` must still be flagged.

Simulated scope: ingestion/core/model.py (purity rules apply; this exact
rel_path is what the test simulates to exercise the allowlist).
"""

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _check_other_timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"timezone {value!r} is not a valid IANA timezone name") from exc
    return value
