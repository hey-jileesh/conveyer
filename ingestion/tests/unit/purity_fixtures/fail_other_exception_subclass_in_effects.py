"""MUST-FAIL: the third allowlist entry is a closed (file, class name) pair
naming exactly `TransientError` -- it is NOT a general "any Exception
subclass is allowed" exemption. A different Exception subclass, even sitting
right in the allowlisted file `ingestion/effects/records.py`, must still be
flagged as `idiom-class` (bd conveyer-4ot.26: "Do NOT add a general
Exception-subclass exemption").

Simulated scope: ingestion/effects/records.py (idiom rule applies; same file
as the one legitimate allowlist entry, different class name).
"""


class SomeOtherError(Exception):
    """Not TransientError -- must not be exempted, even in this file."""
