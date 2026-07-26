"""MUST-PASS: an `enum.Enum` subclass — here `enum.StrEnum`, one of the
allowed enum base spellings — is a sanctioned closed-vocabulary value shape.
"""

from enum import StrEnum


class Disposition(StrEnum):
    REGISTERED = "registered"
    DUPLICATE = "duplicate"
    SUPERSEDED = "superseded"
