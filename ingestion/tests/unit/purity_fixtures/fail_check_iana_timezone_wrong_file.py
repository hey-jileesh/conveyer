"""MUST-FAIL: the second try/raise allowlist entry is keyed to (file,
function) -- a function named `_check_iana_timezone` in any file other than
ingestion/core/model.py must still be flagged, on both the `try` and the
`raise`.

Simulated scope: ingestion/core/other_module.py (purity rules apply).
"""

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _check_iana_timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"timezone {value!r} is not a valid IANA timezone name") from exc
    return value
