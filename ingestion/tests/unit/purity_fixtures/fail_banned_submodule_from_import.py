"""MUST-FAIL: `from os.path import join` is a banned submodule from-import.

Simulated scope: ingestion/core/** (purity rules apply).
"""

from os.path import join


def joined(a: str, b: str) -> str:
    return join(a, b)
