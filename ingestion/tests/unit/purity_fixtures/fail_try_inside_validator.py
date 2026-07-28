"""MUST-FAIL: `try` inside a `@field_validator`/`@model_validator` method is
still banned -- the validator-raise exemption (LLD §6.1/§12.2 director
adjudication, conveyer-4ot.24) is scoped ONLY to `raise`; `try` remains
banned outside the two hardcoded (file, function) allowlist entries. The
`raise` in the `except` clause below stays exempt (it is lexically inside a
`@field_validator`-decorated method) -- only the `try` itself is flagged.

Simulated scope: ingestion/core/** (purity rules apply).
"""

from pydantic import BaseModel, field_validator


class Foo(BaseModel):
    value: str

    @field_validator("value")
    @classmethod
    def _check_value(cls, value: str) -> str:
        try:
            int(value)
        except ValueError as exc:
            raise ValueError("value must be numeric") from exc
        return value
