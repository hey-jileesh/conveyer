"""MUST-PASS: effects modules are exempt from the PURITY rule group — boto3
imports and `datetime.now()` calls are fine outside core/** and sources/**.

Simulated scope: ingestion/effects/s3.py (purity rules do NOT apply; only
the idiom rules do, and this file trips none of them).
"""

from datetime import datetime

import boto3


def make_store_fx(client: "boto3.client") -> dict:
    def stamp() -> datetime:
        return datetime.now()

    return {"stamp": stamp}
