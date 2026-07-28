"""MUST-FAIL: `datetime.now()` is banned in core/** — "now" is always a
parameter (LLD §12.2).

Simulated scope: ingestion/core/** (purity rules apply).
"""

import datetime


def stamp() -> datetime.datetime:
    return datetime.datetime.now()
