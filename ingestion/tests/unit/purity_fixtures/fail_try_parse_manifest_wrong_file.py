"""MUST-FAIL: the try/except allowlist is keyed to (file, function) — a
function named `parse_manifest` in any file other than
ingestion/core/completeness.py must still be flagged.

Simulated scope: ingestion/core/other_module.py (purity rules apply).
"""


def parse_manifest(raw: bytes) -> bytes | None:
    try:
        return raw.decode("utf-8").encode("utf-8")
    except UnicodeDecodeError:
        return None
