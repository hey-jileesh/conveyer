"""MUST-FAIL: `import boto3 as b3` is a banned import under its alias.

Simulated scope: ingestion/core/** (purity rules apply).
"""

import boto3 as b3


def client_type() -> str:
    return type(b3).__name__
