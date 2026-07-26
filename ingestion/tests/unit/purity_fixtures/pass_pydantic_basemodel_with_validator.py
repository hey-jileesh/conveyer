"""MUST-PASS: a pydantic `BaseModel` subclass with `@field_validator` /
`@model_validator` methods is the sanctioned boundary-parsing mechanism
(LLD §12.2 idiom rule) — those methods do not trip the class-shape ban.

Simulated scope: ingestion/core/** (purity rules apply too; nothing here
trips them).
"""

from pydantic import BaseModel, field_validator


class FeedConfig(BaseModel):
    feed_id: str

    @field_validator("feed_id")
    @classmethod
    def _check_feed_id(cls, value: str) -> str:
        return value
