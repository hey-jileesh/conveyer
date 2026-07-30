"""Canonical JSON serialization + `row_hash`. LLD 005.1 §7.1/§7.2 (A-6).

The one implementation both admission quarantine writers use to shape
`row_snapshot` (§7.3's UDF seam wraps this module; `frames/quarantine.py`,
n1) — and the one 007's fact-hash canonicalization aligns to **by shared
property-test vectors** (`contracts/fixtures/canonical-json/`, §7.4), never
by importing this code (A-6, 004 D-13's fixture idiom). Nothing here talks
to Spark or pydantic: `canonical_json` is a total function of a plain Python
value to a `str`, injective over the value domain below, so `row_hash` (a
SHA-256 over its UTF-8 bytes) is a pure function of value-identity — two
snapshots collide iff they carry the same information (§7.2's "value-
identity, not occurrence-identity").

Encoding rules (normative, §7.1):

* **Objects** (any `Mapping`): keys sorted, separators `(",", ":")`, no
  insignificant whitespace. Key order is by **UTF-8 byte order** — but that
  is exactly Python's default `str` ordering (`<`) for every code point:
  UTF-8 is monotonic in code point value by construction, and CPython
  strings never carry surrogate pairs for astral-plane characters, so
  `sorted(keys)` needs no byte-level detour (probe-verified over the 1-/2-/
  3-/4-byte UTF-8 length boundaries, U+FFFD, and U+10FFFF). "Every schema
  key always present (explicit null)" is a property of how a *caller*
  builds a snapshot dict (§7.1's two pre_check/post_check structures) —
  this function serializes whatever mapping it is given; it does not know
  a schema to complete one against.
* **Strings**: verbatim — `ensure_ascii=False` (no `\\uXXXX` escaping of
  non-ASCII), no unicode normalization (NFC/NFD stay distinct), and
  **minimal** escaping per RFC 8259: only `"`, `\\`, and control chars
  U+0000-U+001F are escaped (the C0 set uses the short forms `\\b \\f \\n
  \\r \\t` where defined, else `\\u00XX`); everything else — U+FFFD, DEL,
  astral-plane code points — passes through raw.
* **Numbers**: `int` as bare JSON integers. `Decimal` as **strings** —
  `format(value, "f")`, never `str(value)`: `Decimal.__str__` switches to
  scientific notation once the adjusted exponent drops below -6 (reachable
  by a `decimal(p,s)` column with s close to p, e.g. `decimal(38,38)`),
  which would silently change a value's *textual* scale-preserving form;
  `format(value, "f")` is fixed-point unconditionally, so scale is exact
  and stable regardless of magnitude. **Sign-of-zero is preserved**:
  `Decimal("-0")` renders `"-0"`, distinct from `Decimal("0")`'s `"0"` —
  `format(value, "f")` does not normalize IEEE-754-style negative zero away,
  so the two are different `row_hash` inputs despite comparing numerically
  equal (`Decimal("-0") == Decimal("0")` is `True`). Unreachable through
  005.1's own admission paths today (`try_cast` normalizes a `"-0"` string
  to positive zero before any `Decimal` value exists, §6.2; pre_check
  snapshots carry raw strings, never typed `Decimal`s) but latent for any
  future caller that snapshots a typed `Decimal` directly (conveyer-azr.28)
  — documented rather than silently relied upon. Non-finite `Decimal` (`NaN`,
  `Infinity`) is rejected — canonical JSON has no such value. **`float` is
  rejected outright** (no admission value is a float; a future producer
  passing one is a defect, not a silent coercion) — checked before the
  `int` branch since `bool` (checked earlier still) is the only numeric
  subtype that must NOT fall through to the `int` branch as `0`/`1`.
* **Temporal**: `date` -> `"YYYY-MM-DD"` (`.isoformat()`). `datetime` ->
  RFC 3339 UTC, exactly six fractional digits, fixed width
  (`"2026-01-02T03:04:05.000000Z"`) — the aware value is converted to UTC
  first (`astimezone(UTC)`), so any input zone renders correctly;
  a **naive `datetime` is rejected** [DC-3]: the UDF boundary (§7.3) is
  exactly where naiveté would smuggle in the OS-local zone as an
  environment dependency, so this function refuses rather than guesses.
  An **aware `datetime` whose UTC conversion would fall outside
  `[datetime.min, datetime.max]`** (near MINYEAR/MAXYEAR with an offset that
  walks it past the boundary — there is no year 0) is rejected the same
  way, via a pure arithmetic pre-check rather than catching the
  `OverflowError` `.astimezone(UTC)` would otherwise raise (`core/**` bans
  `try`, §12.3; conveyer-azr.24).
  `CANONICAL_TIMESTAMP_FORMAT`/`CANONICAL_TIMESTAMP_SPARK_PATTERN` below
  are the one authored pair backing both this function's own rendering and
  §7.3's in-plan Spark `date_format` rendering (n1) — change one, change
  both call sites, never let them drift apart.
* **Bool/None**: `true`/`false`/`null`. **Sequences** (`list`/`tuple`):
  arrays, order preserved, never sorted.

`canonical_json`/`row_hash` are the one place in `spine/core/**` (which
otherwise bans `raise` outright, §12.3) that legitimately raises — surfaced
as one-line `raise`-only helper `_reject`, allowlisted by name in
`tools/linter_configs/spine.py::_TRY_RAISE_ALLOWLIST` (the `naming.py`/
`merge.py` raise-only-helper shape, not a validator).
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, NoReturn

# The one authored source for canonical timestamp rendering (§7.3): this
# module's own `_timestamp_str` uses the Python side; n1's in-plan Spark
# `date_format(col, CANONICAL_TIMESTAMP_SPARK_PATTERN)` (under the pinned
# UTC session zone, §6.2) uses the Spark side — property-tested against each
# other over generated instants (§12.4), never authored twice.
CANONICAL_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
CANONICAL_TIMESTAMP_SPARK_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"

# RFC 8259's minimal-escaping set: quote, backslash, and the control chars
# with a short form. Every other control char (U+0000-U+001F minus these six)
# falls through to the `\\u00XX` branch in `_encode_string`; every non-control
# char — including U+FFFD and astral-plane code points — passes through
# verbatim (`ensure_ascii=False`'s letter).
_ESCAPES: Mapping[str, str] = {
    '"': '\\"',
    "\\": "\\\\",
    "\b": "\\b",
    "\f": "\\f",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
}


def _reject(message: str) -> NoReturn:
    raise ValueError(f"canonical_json: {message}")


def _encode_string(value: str) -> str:
    out = ['"']
    for ch in value:
        esc = _ESCAPES.get(ch)
        if esc is not None:
            out.append(esc)
        elif ord(ch) < 0x20:
            out.append(f"\\u{ord(ch):04x}")
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def _decimal_str(value: Decimal) -> str:
    if not value.is_finite():
        # conveyer-azr.32/S-18: type/kind only -- this raises inside the
        # snapshot UDF over payload-classified data (§7.3); the value itself
        # must never ride a `ValueError` message into driver/executor logs.
        _reject("non-finite Decimal values are rejected")
    return format(value, "f")  # fixed-point, never scientific (see module docstring)


def _timestamp_str(value: datetime) -> str:
    tzinfo = value.tzinfo
    if tzinfo is None:
        _reject("naive datetime values are rejected (DC-3)")
    offset = tzinfo.utcoffset(value)
    if offset is None:
        _reject("naive datetime values are rejected (DC-3)")
    # An aware value at/near `datetime.min`/`datetime.max` whose UTC offset
    # would walk it past the representable range (MINYEAR=1 -- no year 0 --
    # or MAXYEAR=9999) makes `.astimezone(UTC)` raise `OverflowError` rather
    # than return; that value is outside canonical_json's domain, same
    # rejection class as naive-datetime/float above. `core/**` bans `try`
    # (§12.3), so this is a pure arithmetic pre-check, not exception-based:
    # comparing `offset` against the (always-representable, since both ends
    # are valid datetimes) span from `value` to each boundary.
    naive = value.replace(tzinfo=None)
    if offset > naive - datetime.min or -offset > datetime.max - naive:
        # conveyer-azr.32/S-18: value-free, same law as above.
        _reject("datetime out of representable range in UTC")
    return value.astimezone(UTC).strftime(CANONICAL_TIMESTAMP_FORMAT)


def _encode_object(value: Mapping[Any, Any]) -> str:
    for key in value:
        if not isinstance(key, str):
            # conveyer-azr.32/S-18: type name only, never the key's value.
            _reject(f"non-string object key rejected: type={type(key).__name__}")
    parts = [f"{_encode_string(k)}:{_encode(value[k])}" for k in sorted(value.keys())]
    return "{" + ",".join(parts) + "}"


def _encode_array(value: Sequence[Any]) -> str:
    return "[" + ",".join(_encode(item) for item in value) + "]"


def _encode(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):  # must precede `int` -- bool is an int subtype
        return "true" if value else "false"
    if isinstance(value, float):  # rejected outright -- no admission value is a float
        # conveyer-azr.32/S-18: value-free, same law as above.
        _reject("float values are rejected")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        return _encode_string(_decimal_str(value))
    if isinstance(value, datetime):  # must precede `date` -- datetime is a date subtype
        return _encode_string(_timestamp_str(value))
    if isinstance(value, date):
        return _encode_string(value.isoformat())
    if isinstance(value, str):
        return _encode_string(value)
    if isinstance(value, Mapping):
        return _encode_object(value)
    if isinstance(value, (list, tuple)):
        return _encode_array(value)
    _reject(f"unsupported type {type(value).__name__!r}")


def canonical_json(value: Any) -> str:
    """§7.1: the canonical serialization of any plain Python value drawn
    from the domain documented in this module's docstring."""
    return _encode(value)


def row_hash(snapshot: Any) -> str:
    """§7.2: `sha256(canonical_json(snapshot)).hexdigest()` -- 64 lowercase
    hex chars, value-identity over `snapshot` (not occurrence-identity)."""
    return hashlib.sha256(canonical_json(snapshot).encode("utf-8")).hexdigest()
