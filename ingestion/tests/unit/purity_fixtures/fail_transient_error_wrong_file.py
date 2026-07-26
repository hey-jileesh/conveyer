"""MUST-FAIL: the third allowlist entry is keyed to (file, class name) --
`class TransientError(Exception)` in any file other than
`ingestion/effects/records.py` must still be flagged as `idiom-class`, same
as the (file, function) try/raise allowlist entries are keyed exactly.

Simulated scope: ingestion/effects/other.py (idiom rule applies; this exact
rel_path is what the test simulates to exercise the allowlist boundary).
"""


class TransientError(Exception):
    """Same class name, wrong file -- must not be exempted."""
