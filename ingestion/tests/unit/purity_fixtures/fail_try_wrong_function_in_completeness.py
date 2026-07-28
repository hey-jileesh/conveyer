"""MUST-FAIL: the try/except allowlist is keyed to (file, function) — the
file matches ingestion/core/completeness.py when simulated, but the
function is not `parse_manifest`, so the `try` must still be flagged.

Simulated scope: ingestion/core/completeness.py (purity rules apply; this
exact rel_path is what the test simulates to exercise the allowlist).
"""

from ingestion.core.model import ManifestV1


def parse_something_else(raw: bytes) -> ManifestV1 | None:
    try:
        return ManifestV1.model_validate_json(raw)
    except Exception:
        return None
