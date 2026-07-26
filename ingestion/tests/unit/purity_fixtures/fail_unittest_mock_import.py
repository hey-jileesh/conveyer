"""MUST-FAIL: `unittest.mock` is banned everywhere — test doubles are records
of plain functions (LLD §7.7), never a mocking framework.

Simulated scope: any (idiom rule has no exemptions).
"""

from unittest import mock


def build_double():
    return mock.MagicMock()
