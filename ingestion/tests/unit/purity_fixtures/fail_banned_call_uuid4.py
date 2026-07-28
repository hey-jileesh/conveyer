"""MUST-FAIL: `uuid.uuid4()` is banned in core/** (nondeterministic); `uuid5`
is allowed since it is deterministic given its inputs.

Simulated scope: ingestion/core/** (purity rules apply).
"""

import uuid


def new_id() -> uuid.UUID:
    return uuid.uuid4()
