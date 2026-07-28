"""MUST-FAIL: `try` is banned in core/** outside the one hardcoded allowlist
entry (ingestion/core/completeness.py::parse_manifest). This function is not
that one, even when simulated inside completeness.py itself.

Simulated scope: ingestion/core/** (purity rules apply).
"""


def not_parse_manifest(raw: bytes) -> bytes:
    try:
        result = raw.decode("utf-8").encode("utf-8")
    except UnicodeDecodeError:
        result = b""
    return result
