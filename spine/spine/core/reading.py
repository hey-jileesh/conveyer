"""The pure line/record shaper + header binder for `land`'s reader. LLD 005.1
§5.2/§5.3/§5.4/§5.5 (A-1, A-3, A-4).

**Scope, precisely** (this bead's brief, `conveyer-azr.12`, n0-reading): this
module owns the parse VERDICT and the header BINDING, both as plain,
total, exception-free-except-`parse_line` functions over Python values --
nothing here touches Spark, Hadoop, or any object store. The Hadoop
acquisition (`sc.newAPIHadoopFile`, one split per object), the BOM strip,
the header probe's I/O (reading an object's first `skip_leading_lines + 1`
lines), and raising the A-10 tier-1 defects that `bind_header`'s output
makes visible are `n2-reader`'s job (`effects/spark.py`), explicitly out of
scope here.

**`shape_row`/`multiline_records` (moved here from `effects/spark.py`,
critique F3, bead conveyer-azr.30)**: both are per-record SHAPING semantics
(§5.4's ragged/malformed verdict, §5.5's own second csv-reader semantics) --
plain, Spark-free Python over strings/tuples/dicts, exactly this module's
own "nothing here touches Spark" scope, even though the LLD introduces them
in §5.4/§5.5 rather than §5.2/§5.3. Originally sat in `effects/spark.py`
alongside the closures that actually acquire data (`mapPartitions`,
`wholetext=True` reads) -- that placement made A-1's own rationale ("the
parse verdict property-testable without Spark") only half true (`parse_line`
was testable without Spark; `shape_row`'s ragged/malformed verdict and
`multiline_records`'s "consume the remainder" verdict were not, since
importing `effects/spark.py` at all requires `pyspark` importable). Moving
them here restores that: both now sit under this module's own `core`
purity profile (`tools/linter_configs/spine.py`, no pyspark import, no
Spark-action attribute names) and this module's own `§12.4` hypothesis
property suite (`tests/unit/test_reading.py`). `effects/spark.py` keeps
acquisition, the `mapPartitions`/`wholetext` closures, and
`spark.createDataFrame` -- nothing semantic (LLD §5.1's own words).

`parse_line` (§5.3) -- **one `csv.reader` per line, deliberately**: feeding
a shared reader the whole line stream would let an unterminated quote
swallow subsequent lines, which is exactly the `multiline: false` contract's
forbidden behavior (A-1 owns the parse verdict precisely so it cannot drift
with the engine, and so it is property-testable without Spark). Verdicts:

* strict `csv.reader` raises `csv.Error` (unterminated quote: "unexpected
  end of data"; junk after a closing quote: `"',' expected after '\"'"`)
  -> malformed (`ParsedLine.tokens is None`, `.error` carries the raw csv
  message -- **never persisted**, §5.3's own words; it exists only for a
  caller that wants to log/debug, not for any durable column).
* token count vs. a binding's expected width (raggedness) is **not**
  `parse_line`'s concern -- "applied by the caller, which owns the width"
  (§5.3) -- `parse_line` has no width parameter at all.
* an empty line parses to `ParsedLine(tokens=(), error=None)` -- `csv.reader`
  itself already collapses a wholly-empty line to zero tokens (verified:
  `list(csv.reader(['']))  == [[]]`, one row, zero fields -- the ONLY input
  shape that yields an empty tuple, since even a lone space or a lone
  delimiter yields >=1 token). §5.1.6's "empty lines yield no record and no
  ordinal" is therefore representable without a third `ParsedLine` field --
  the caller recognizes "no record" as `tokens == ()`.
* NUL and every other control character are **values, not structural
  errors** (verified: `csv` under Python 3.11/3.12 accepts embedded NUL)--
  hygiene beyond structure is contract-grammar (`pattern`) territory,
  registered against `conveyer-nvh.48.14` (§15.2), not this reader's.

`bind_header` (§5.2/A-4) is pure and **total** -- it never raises. Exact
string equality decides a header token's target: a token equal to a
declared column name binds to that column; anything else binds to `extras`,
keyed by the token itself (the caller already has the original `tokens`
tuple in hand to read that verbatim string back out -- `HeaderBinding` does
not duplicate it as a second parallel array). Two failure conditions A-4
names as tier-1 defects are surfaced here as **data**, never raised --
`bind_header`'s own contract stays "the simplest thing" (§14): a pure
mapping function, not a validator. The effects-layer probe (`n2-reader`)
inspects `HeaderBinding.missing_required`/`.duplicate_name_groups` and
raises the A-10-shaped `ValueError`s itself:

* `missing_required`: declared `required` column names absent from every
  header token.
* `duplicate_name_groups`: 0-based position groups (each `len >= 2`) sharing
  one exact token text -- declared or undeclared alike (A-4: "any
  exact-duplicate header name (declared or not) is a tier-1 defect"). Which
  column, if any, a duplicated token names is derivable by the caller
  indexing back into its own `tokens` (and the declared column set) --
  not carried here, so the sharpest A-4 case (a mis-uploaded file whose
  first line is data) never risks a cell value threading through this
  module's return value.

Under `dialect.header is False` (headerless), `bind_header` **ignores
`tokens` entirely** and derives a purely positional binding straight from
`contract.columns`' own declared order -- position *i* -> declared column
*i*, `expected_width` = the declared width (A-4). `extras` is therefore
always `{}` for headerless feeds [DC-2]: synthesizing positional extras
keys would re-derive a header from data, which a headerless claim forbids.
`missing_required`/`duplicate_name_groups` are trivially empty on this path
(there is no header line to be missing from or duplicated within; A-4's
"a `header: false` contract declaring any `required` column is a spec-parse
defect" is `core/model.py`'s job at `PipelineSpecModel` parse, n0-spec-
migration, not re-checked here).

`DialectValue` is this module's own internal frozen value, derived from the
pydantic `DialectModel` (§3.1) by `dialect_value()` below -- `core/`'s
functions take plain values, never a pydantic `BaseModel` instance, as the
type of a hot-path parameter (`parse_line` runs once per data row); the one
narrow exception is `bind_header`'s `contract: RawContractModel` parameter,
which is only evaluated once per object (at header-probe time) and needs
the model's `columns`/`required` structure directly -- re-deriving that
shape as a third parallel value type here would be exactly the "single
interpreter" duplication D-5 exists to avoid (`core/model.py` already
structures it).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

    from spine.core.model import DialectModel, RawContractModel


@dataclass(frozen=True)
class DialectValue:
    """The internal frozen value `parse_line`/`bind_header` consume, derived
    from `DialectModel` (§3.1) by `dialect_value()` below. `delimiter`
    renames nothing; `quotechar` is `DialectModel.quote` under the `csv`
    module's own parameter name -- `header`/`multiline` carry through
    unchanged (`bind_header` reads `.header`; `.multiline` rides along for a
    caller that branches on it, e.g. n2's §5.5 path selection; neither
    function in this module reads it)."""

    delimiter: str
    quotechar: str
    header: bool
    multiline: bool


def dialect_value(dialect: DialectModel) -> DialectValue:
    """§3.1 -> this module's internal value. A total, non-raising projection
    -- `DialectModel` has already validated `delimiter`/`quote` (single
    ASCII printable, mutually distinct) at spec-parse time; nothing here
    re-checks that."""
    return DialectValue(
        delimiter=dialect.delimiter,
        quotechar=dialect.quote,
        header=dialect.header,
        multiline=dialect.multiline,
    )


@dataclass(frozen=True)
class ParsedLine:
    """§5.3. `tokens is None` iff malformed (`error` then carries the raw
    `csv.Error` message -- never persisted, per this module's docstring and
    §5.3's own words). `tokens == ()` iff the line was empty (§5.1.6: no
    record, no ordinal) -- the only input shape that produces a zero-length
    tuple; every other line, including a lone delimiter or a lone space,
    yields at least one token."""

    tokens: tuple[str, ...] | None
    error: str | None


def parse_line(line: str, dialect: DialectValue) -> ParsedLine:
    """§5.3: one `csv.reader` per line, deliberately -- see this module's
    docstring for why a shared reader across lines is forbidden under
    `multiline: false`. `doublequote=True` (RFC 4180 quote-doubling is the
    only supported escape mechanism, §3.1); `escapechar=None` (no backslash
    escaping); `strict=True` (turns "junk after a closing quote" into a
    raised `csv.Error` instead of silently absorbing it, §5.3's second
    verdict). This function needs `try` to convert `csv.Error` into a value
    -- allowlisted in `tools/linter_configs/spine.py`
    (`_TRY_RAISE_ALLOWLIST`), the one exemption this bead adds (§12.6 item 1)."""
    reader = csv.reader(
        [line],
        delimiter=dialect.delimiter,
        quotechar=dialect.quotechar,
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


@dataclass(frozen=True)
class HeaderBinding:
    """§5.2/A-4. `column_at[i]` is the declared column name bound at header
    position *i*, or `None` if position *i* binds to `extras` instead (its
    verbatim key is `tokens[i]` -- the caller already holds `tokens`, so it
    is not duplicated here). `expected_width` is the token count every data
    record in this object must match (the header's own width under
    `header: true`; the declared width under `header: false`, A-4) --
    raggedness against it is the caller's check, not this module's (§5.3).
    `missing_required`/`duplicate_name_groups` are the two A-4 tier-1
    conditions surfaced as data for the effects-layer probe to raise
    (§5.2/A-10); both are always empty under `header: false` (see this
    module's docstring)."""

    column_at: tuple[str | None, ...]
    expected_width: int
    missing_required: tuple[str, ...]
    duplicate_name_groups: tuple[tuple[int, ...], ...]


def bind_header(
    tokens: tuple[str, ...], contract: RawContractModel, dialect: DialectValue
) -> HeaderBinding:
    """§5.2/A-4: pure, total -- never raises. See this module's docstring
    for the header-true/header-false split and what each `HeaderBinding`
    field means."""
    if not dialect.header:
        column_at: tuple[str | None, ...] = tuple(column.name for column in contract.columns)
        return HeaderBinding(
            column_at=column_at,
            expected_width=len(column_at),
            missing_required=(),
            duplicate_name_groups=(),
        )

    declared = {column.name for column in contract.columns}
    column_at = tuple(token if token in declared else None for token in tokens)

    present = set(tokens)
    missing_required = tuple(
        column.name for column in contract.columns if column.required and column.name not in present
    )

    positions_by_token: dict[str, list[int]] = {}
    for position, token in enumerate(tokens):
        positions_by_token.setdefault(token, []).append(position)
    duplicate_name_groups = tuple(
        tuple(positions) for positions in positions_by_token.values() if len(positions) >= 2
    )

    return HeaderBinding(
        column_at=column_at,
        expected_width=len(tokens),
        missing_required=missing_required,
        duplicate_name_groups=duplicate_name_groups,
    )


# --- §5.4/§5.5's per-record shaping (moved from effects/spark.py, F3) ------


def shape_row(
    uri: str,
    object_seq: int,
    row_index: int,
    raw_text: str,
    tokens: tuple[str, ...] | None,
    binding: HeaderBinding,
    header_tokens: tuple[str, ...] | None,
    column_names: tuple[str, ...],
) -> tuple[Any, ...]:
    """One parsed (or malformed) record -> one raw row tuple, positional
    against `effects/spark.py::_admission_raw_row_schema`'s field order (the
    caller's own schema -- this function stays schema-agnostic, returning a
    plain positional tuple). Malformed (§5.3's verdict is `tokens is None`)
    or ragged (`len(tokens) != binding.expected_width`, "applied by the
    caller, which owns the width") both shape identically (§5.4): every
    declared column NULL, `extras = {}`, `malformed_text = raw_text` (the
    decoded line/record span, verbatim, unparsed). A well-formed record
    binds cell-by-cell from `binding.column_at`: a declared target keeps the
    token (empty string -> NULL, A-5); an `extras` target (`column_at[i] is
    None`) keys by the ORIGINAL header token at that position --
    `header_tokens[i]` -- never a token from THIS record (A-4: the extras
    key is the header's own claim, fixed once per object)."""
    malformed = tokens is None or len(tokens) != binding.expected_width
    if malformed:
        declared: tuple[str | None, ...] = tuple(None for _ in column_names)
        extras: dict[str, str] = {}
        malformed_text: str | None = raw_text
    else:
        assert tokens is not None, "malformed is False only when tokens is a real tuple"
        declared_map: dict[str, str | None] = {}
        extras = {}
        for position, token in enumerate(tokens):
            target = binding.column_at[position]
            if target is not None:
                declared_map[target] = token if token != "" else None
            else:
                key = header_tokens[position] if header_tokens is not None else str(position)
                extras[key] = token
        declared = tuple(declared_map.get(name) for name in column_names)
        malformed_text = None
    return (uri, object_seq, row_index, malformed_text, *declared, extras)


def multiline_records(
    body_lines: list[str], dialect: DialectValue
) -> Iterator[tuple[tuple[str, ...], str]]:
    """§5.5: ONE `csv.reader` over an object's entire (BOM-stripped,
    skip-dropped) physical-line span, yielding `(tokens, raw_text)` per
    record -- `raw_text` is the exact join of whichever physical lines
    (with their original line endings) this record consumed, computed via
    a `nonlocal` position counter rather than re-deriving it from token
    content, so a ragged multiline record's `malformed_text` (the caller's
    own job, via `shape_row` above) is genuinely verbatim source, the same
    contract as the per-line path. Deliberately a closure, not a class
    (this engine's idiom rule admits only frozen dataclass/`BaseModel`/
    `Enum`, and a mutable line-position tracker has no honest frozen
    shape). `strict=False` (unlike `parse_line`'s `strict=True`) is the
    deliberate, verified difference that lets an unterminated quote at true
    EOF "consume the file's remainder into one field" instead of raising.

    Needs `try` to turn `StopIteration` into a plain generator return --
    allowlisted in `tools/linter_configs/spine.py` (`_TRY_RAISE_ALLOWLIST`),
    the same mechanism `parse_line` above uses for its own `csv.Error`
    conversion."""
    consumed = 0

    def _remaining_lines() -> Iterator[str]:
        nonlocal consumed
        while consumed < len(body_lines):
            line = body_lines[consumed]
            consumed += 1
            yield line

    reader = csv.reader(
        _remaining_lines(),
        delimiter=dialect.delimiter,
        quotechar=dialect.quotechar,
        doublequote=True,
        escapechar=None,
        strict=False,
    )
    previous = 0
    while True:
        try:
            tokens = next(reader)
        except StopIteration:
            return
        raw_text = "".join(body_lines[previous:consumed])
        previous = consumed
        yield tuple(tokens), raw_text
