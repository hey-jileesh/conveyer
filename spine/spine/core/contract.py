"""`COLUMN_TYPE_RE` + `parse_column_type` — the single interpreter of the column-type
grammar. LLD 005.1 §3.2.

**File placement note (deviation from §3.2's code block, recorded per this
bead's brief):** §3.2's code fence shows `COLUMN_TYPE_RE` declared directly
above `ColumnSpec` in `core/model.py`. Implementing it literally there
creates a genuine circular import, empirically confirmed in the kernel
(bead conveyer-azr.11): `core/model.py`'s `ColumnSpec` validators need
`parse_column_type` (to inspect decimal precision/scale, temporal fmt,
min/max), so `model.py` must import this module; if this module in turn
imports `COLUMN_TYPE_RE` back from `model.py`, `model.py`'s own
module-level `import` line (executed before `COLUMN_TYPE_RE` is defined
further down the same file) fails with `ImportError`. `COLUMN_TYPE_RE`
lives here instead — the dependency-free, lower-level module — and
`core/model.py` imports it from here, mirroring how `model.py` already
imports `BATCH_ID_RE` etc. from `core/naming.py` (nvh.43's "factor to the
more foundational module" precedent) rather than `core/naming.py`'s OTHER
precedent (an "own copy" duplicate, forced there by a hard zip-purity
constraint `entrypoints/router.py` has and nothing here has). The grammar
text itself is unchanged from §3.2 — only which file declares the constant.

`core/model.py`'s `ColumnSpec.type` field uses `Field(pattern=COLUMN_TYPE_RE)`
(pydantic's Rust regex engine) for a SHAPE-only check at parse — match or no
match, no group extraction (pydantic's `pattern=` gives no hook for that).
This module owns the second half: turning an already-shape-valid type
string into a structured value (`ColumnType`: kind + params) that both
`core/model.py`'s own decimal-bounds/date-timestamp-fmt/min-max validators
(§3.2) and the frames compiler (`frames/checks.py::compile_contract`, §6.1,
N1) can inspect without re-deriving the grammar a second time — D-5's
"single interpreter" rule: raw DDL never sees types (raw columns are always
strings, D-5), and nothing outside `parse_column_type` parses a type
string's *structure*.

`parse_column_type` re-validates the full grammar itself (`.fullmatch()`
against Python's `re`, not the Rust engine pydantic's `Field(pattern=...)`
uses) rather than trusting a caller's prior pydantic pass: parse, don't
validate applies to this function's own contract too, so it is safe to call
directly (as tests do) without going through `ColumnSpec` first.

**§3.3 versions (A-11, bead conveyer-azr.13, n0-spec-migration)**:
`read_spec_version`/`check_version` live here rather than `core/model.py`
for the SAME circular-import reason as `COLUMN_TYPE_RE` above — they need
`core.canonical.canonical_json`, which has no dependency on `model.py`, but
their own type hints (`ReadSpecModel`/`RawContractModel`) are only used
under `TYPE_CHECKING` (`from __future__ import annotations` defers
evaluation): both functions call nothing but `.model_dump(mode="json")` on
their arguments, so no runtime import of `core/model.py` is needed, and one
never becomes necessary as long as that stays true. `read_spec_version` =
SHA-256 (lowercase hex, full) of the canonical JSON of the parsed
`ReadSpecModel`; `check_version` = SHA-256 of the canonical JSON of the
parsed pair `{"raw_contract": ..., "read_spec": ...}` (A-11's "the two
declared admission surfaces fuse into one reviewed unit" pair-hash). Both
computed once at binding (n3-context-wiring) and carried on the context,
never recomputed mid-run.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from spine.core.canonical import canonical_json

if TYPE_CHECKING:
    from spine.core.model import RawContractModel, ReadSpecModel

# §3.2's column-type grammar, verbatim. See this module's docstring for why
# it is declared here rather than in `core/model.py` (a circular-import
# fix, not a grammar change) -- `core/model.py` imports this constant for
# its own `ColumnSpec.type = Field(pattern=COLUMN_TYPE_RE)`.
COLUMN_TYPE_RE = (
    r"^(string|int|long|bool"
    r"|decimal\([1-9][0-9]?,(0|[1-9][0-9]?)\)"
    r"|date\([^()]+\)|timestamp\([^()]+\))$"
)
_COLUMN_TYPE_COMPILED = re.compile(COLUMN_TYPE_RE)

ColumnKind = Literal["string", "int", "long", "bool", "decimal", "date", "timestamp"]


@dataclass(frozen=True)
class ColumnType:
    kind: ColumnKind
    precision: int | None = None  # decimal only
    scale: int | None = None  # decimal only
    fmt: str | None = None  # date/timestamp only (Spark/Java pattern letters, unparsed here)


def parse_column_type(s: str) -> ColumnType:
    """Parse a `ColumnSpec.type` string into its structured `ColumnType`.
    Raises `ValueError` on a grammar violation -- allowlisted in
    `tools/linter_configs/spine.py` (`_TRY_RAISE_ALLOWLIST`), since this
    function is not itself a `@field_validator`/`@model_validator` (the
    `core` profile's `ban_try_raise` only exempts raises lexically inside a
    validator-decorated method)."""
    if not _COLUMN_TYPE_COMPILED.fullmatch(s):
        raise ValueError(f"not a valid column type grammar (005.1 §3.2): {s!r}")
    if s in ("string", "int", "long", "bool"):
        kind: ColumnKind = s  # type: ignore[assignment]  # narrowed by the `in` check above
        return ColumnType(kind=kind)
    if s.startswith("decimal("):
        inner = s[len("decimal(") : -1]
        precision_str, scale_str = inner.split(",")
        return ColumnType(kind="decimal", precision=int(precision_str), scale=int(scale_str))
    if s.startswith("date("):
        return ColumnType(kind="date", fmt=s[len("date(") : -1])
    if s.startswith("timestamp("):
        return ColumnType(kind="timestamp", fmt=s[len("timestamp(") : -1])
    raise AssertionError(f"unreachable: {s!r} fullmatched COLUMN_TYPE_RE but no branch handled it")


def read_spec_version(read: ReadSpecModel) -> str:
    """§3.3/A-11: SHA-256 (lowercase hex, full) of the canonical JSON of the
    PARSED `ReadSpecModel` -- `model_dump(mode="json")`, not the authored
    file text, so 009's re-homing of the authored surface cannot silently
    change version identity (key order, comments, defaults-vs-explicit all
    wash out through the parse)."""
    return hashlib.sha256(canonical_json(read.model_dump(mode="json")).encode("utf-8")).hexdigest()


def check_version(contract: RawContractModel, read: ReadSpecModel) -> str:
    """§3.3/A-11: SHA-256 of the canonical JSON of the parsed PAIR
    `{"raw_contract": ..., "read_spec": ...}` -- the two declared admission
    surfaces fuse into one reviewed unit (D-2), so their versions may
    honestly fuse into one hash answering "which declared admission surface
    rejected this row" without a per-reason sourcing rule."""
    pair = {
        "raw_contract": contract.model_dump(mode="json"),
        "read_spec": read.model_dump(mode="json"),
    }
    return hashlib.sha256(canonical_json(pair).encode("utf-8")).hexdigest()
