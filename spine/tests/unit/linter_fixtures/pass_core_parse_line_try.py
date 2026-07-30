"""MUST-PASS: `core/reading.py::parse_line`'s exact shape (§5.3, A-1) --
one `csv.reader` per line, wrapped in a `try`/`except csv.Error` that
converts a strict-parse failure into a `ParsedLine` value instead of letting
the exception escape (defects-as-values). This is the corpus's first
MUST-PASS exercise of the `purity-try` side of the `core` profile's
`ban_try_raise` (every other `_TRY_RAISE_ALLOWLIST` entry to date only
needed the `raise` half) -- `("spine/core/reading.py", "parse_line")` in
`tools/linter_configs/spine.py::_TRY_RAISE_ALLOWLIST` is what makes this
fixture clean; without it, the bare `try` block below would report
`purity-try` on its own (there is no `raise` here at all -- the `except`
clause returns a value instead).

Simulated scope: `spine/core/reading.py` (the `core` profile applies,
`ban_try_raise=True`; the fixture is registered against that EXACT
`rel_path` in `test_linter_spine_corpus.py`, since the allowlist keys off
`(rel_path, function_name)`).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass


@dataclass(frozen=True)
class ParsedLine:
    tokens: tuple[str, ...] | None
    error: str | None


def parse_line(line: str, delimiter: str, quotechar: str) -> ParsedLine:
    reader = csv.reader(
        [line],
        delimiter=delimiter,
        quotechar=quotechar,
        doublequote=True,
        escapechar=None,
        strict=True,
    )
    try:
        rows = list(reader)
    except csv.Error as exc:
        return ParsedLine(tokens=None, error=str(exc))
    if not rows:
        return ParsedLine(tokens=(), error=None)
    return ParsedLine(tokens=tuple(rows[0]), error=None)
