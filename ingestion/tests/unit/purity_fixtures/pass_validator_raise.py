"""MUST-PASS: `raise ValueError(...)` inside a `@field_validator`/
`@model_validator` method is the sanctioned pydantic boundary-parsing
mechanism (LLD §6.1) -- exempt from `purity-raise` regardless of decorator
spelling (bare, attribute, or call form) or `@classmethod` pairing. `try` is
NOT exempted by this rule -- it stays banned outside the two hardcoded
(file, function) allowlist entries (see `fail_try_inside_validator.py`).
Director adjudication of the LLD §6.1/§12.2 contradiction, conveyer-4ot.24.

Simulated scope: ingestion/core/** (purity rules apply).
"""

import pydantic
from pydantic import BaseModel, field_validator, model_validator


class Widget(BaseModel):
    name: str
    tag: str
    quantity: int

    @field_validator("name")
    @classmethod
    def _check_name(cls, value: str) -> str:
        if not value:
            raise ValueError("name must not be empty")
        return value

    @pydantic.field_validator("tag")
    @classmethod
    def _check_tag(cls, value: str) -> str:
        if not value.isalpha():
            raise ValueError("tag must be alphabetic")
        return value

    @model_validator(mode="after")
    def _check_quantity(self) -> "Widget":
        if self.quantity < 0:
            raise ValueError("quantity must be non-negative")
        return self
