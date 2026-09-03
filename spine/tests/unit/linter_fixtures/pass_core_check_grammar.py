"""MUST-PASS: `core/check_grammar.py`'s own shape (006.1 §13.4 item 3) --
importing `sqlglot` (a NEW dependency, `spine/pyproject.toml`, `sqlglot==
<exact pin>`) from `spine/core/**` trips no rule: the `core` profile's
`banned_import_roots` (`tools/linter_configs/spine.py::_CORE_PROFILE`) is
`_INGESTION_CORE_BANNED_IMPORT_ROOTS + ("pyspark",)` -- `sqlglot` is pure
Python with no effects (parses text to an AST, no I/O), so it was never
added to that list. Modeled on the real `spine/core/check_grammar.py`:
module-level pure functions dispatching over the parsed `sqlglot.exp` node
kinds, no `try`/`except` here (that module's own ONE totality boundary is
already the dedicated `pass_core_parse_line_try.py` fixture's exact
precedent, keyed to its own `(rel_path, function_name)` allowlist entry --
this fixture stays deliberately narrower, exercising only the import
permission this LLD item is actually about).

Simulated scope: `spine/core/**` (the `core` profile applies).
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp


@dataclass(frozen=True)
class ParsedShape:
    node_kind: str


def parse_expression_kind(text: str) -> ParsedShape:
    tree = sqlglot.parse_one(text, dialect="spark")
    if isinstance(tree, exp.Column):
        return ParsedShape(node_kind="column")
    return ParsedShape(node_kind=type(tree).__name__)
