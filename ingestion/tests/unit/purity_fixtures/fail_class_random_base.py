"""MUST-FAIL: a class subclassing something other than a frozen dataclass,
pydantic BaseModel, or enum.Enum is a banned class shape.

Simulated scope: any (idiom rule has no exemptions).
"""


class SomeBase:
    pass


class Widget(SomeBase):
    def __init__(self, name: str) -> None:
        self.name = name
