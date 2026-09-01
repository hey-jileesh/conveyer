"""Unit + property tests for `spine.core.canonical` — LLD 005.1 §7.1/§7.2/
§7.4/§12.4 (A-6).

**Fixture vector format** (`contracts/fixtures/canonical-json/*.json`, one
JSON array of `{"value": ..., "canonical": ..., "sha256": ...}` entries per
file, §7.4): `"value"` is plain JSON EXCEPT that `Decimal`/`date`/`datetime`
inputs -- which plain JSON has no native type for -- are written as a
single-key tagged object: `{"$decimal": "<exact string>"}`,
`{"$date": "<ISO date>"}`, `{"$timestamp": "<ISO 8601, offset required>"}`.
`_parse_fixture_value` below resolves the tags into native Python values
before calling `canonical_json`/`row_hash`; this parser is test-local (not
imported from `spine.core.canonical` itself) -- 007's own fact-hash suite
reads the same committed files and writes its OWN copy of this same small
parser, per A-6's "shared vectors, never shared code" (004 D-13's idiom).

**006.1 §16.3 item 2's post-structure addendum (bead conveyer-6pg.14, B4):**
`post-check-snapshot.json`'s one committed vector predates the
`_conveyer_fact_type` reserved key (P-7(b)) 006.1 §7.1 adds to the
post_check quarantine snapshot -- ruled **keep-as-is** (this suite's own
blanket reproduction test below still reproduces it byte-exact via a bare
`canonical_json`/`row_hash` call, which was always its whole claim; it was
never itself an example of "the shaper's output shape"). The new,
tag-bearing member of this family is `post-check-snapshot-tagged.json`,
whose vectors DO byte-reproduce through the real `frames/quarantine.py::
shape_post_quarantine` shaper directly -- see `tests/frames/test_
quarantine.py::test_shape_post_quarantine_reproduces_a_tagged_post_
structure_vector_byte_exact`.

**Injectivity property scope**: `canonical_json` is injective over the
value domain §7.1 documents, but that domain deliberately does NOT
type-tag `Decimal`/`date`/`datetime` on the wire -- all three render as a
bare JSON *string* (§7.1), so a plain string leaf whose text happens to
equal one of their renderings (e.g. the string `"5"` vs `Decimal("5")`, or
`"2026-01-02"` vs `date(2026, 1, 2)`) is indistinguishable from it by
construction. This is a property of the wire format the LLD chose (no type
tag), not a bug in this module -- real rows never hit it, because a given
snapshot key's type is fixed by its column's declared type for the life of
that column (D-5: raw is all-string; a type change is a new table, never an
in-place migration). `_is_ambiguous_string` excludes exactly those three
shapes from the property tests' string-leaf generator so the injectivity
check exercises real collisions only, never this accepted one.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st
from spine.core import canonical

_FIXTURES_DIR = Path(__file__).resolve().parents[3] / "contracts" / "fixtures" / "canonical-json"


def _parse_fixture_value(raw: Any) -> Any:
    if isinstance(raw, dict):
        if set(raw.keys()) == {"$decimal"}:
            return Decimal(raw["$decimal"])
        if set(raw.keys()) == {"$date"}:
            return date.fromisoformat(raw["$date"])
        if set(raw.keys()) == {"$timestamp"}:
            return datetime.fromisoformat(raw["$timestamp"])
        return {k: _parse_fixture_value(v) for k, v in raw.items()}
    if isinstance(raw, list):
        return [_parse_fixture_value(item) for item in raw]
    return raw


def _load_vectors() -> list[tuple[str, dict[str, Any]]]:
    vectors: list[tuple[str, dict[str, Any]]] = []
    for path in sorted(_FIXTURES_DIR.glob("*.json")):
        for entry in json.loads(path.read_text()):
            vectors.append((path.name, entry))
    return vectors


_VECTORS = _load_vectors()


def test_canonical_json_fixtures_exist() -> None:
    # Zero-cases guard, same convention as test_naming.py / test_parse_
    # fixtures.py -- fails loudly if the fixtures directory moves or empties
    # rather than letting the parametrized test below silently collect zero
    # cases.
    assert _FIXTURES_DIR.is_dir()
    assert _VECTORS


@pytest.mark.parametrize(
    "filename,entry",
    _VECTORS,
    ids=[f"{filename}#{i}" for i, (filename, _) in enumerate(_VECTORS)],
)
def test_canonical_json_reproduces_every_committed_vector(
    filename: str, entry: dict[str, Any]
) -> None:
    value = _parse_fixture_value(entry["value"])
    assert canonical.canonical_json(value) == entry["canonical"]
    assert canonical.row_hash(value) == entry["sha256"]


# --- key sorting ---------------------------------------------------------


def test_canonical_json_sorts_keys_by_code_point_order() -> None:
    assert canonical.canonical_json({"b": 1, "a": 2, "_c": 3}) == '{"_c":3,"a":2,"b":1}'


def test_canonical_json_sorts_ascii_uppercase_before_lowercase() -> None:
    # ASCII 'Z' (0x5A) < 'a' (0x61) -- also the UTF-8 byte order (§7.1).
    assert canonical.canonical_json({"a": 1, "Z": 2}) == '{"Z":2,"a":1}'


def test_canonical_json_output_independent_of_input_dict_order() -> None:
    d1 = {"b": 1, "a": 2, "c": 3}
    d2 = {"c": 3, "a": 2, "b": 1}
    assert canonical.canonical_json(d1) == canonical.canonical_json(d2)


# --- strings: verbatim, minimal escaping, no normalization ---------------


def test_canonical_json_string_verbatim_unicode() -> None:
    assert canonical.canonical_json("a�b") == '"a�b"'
    assert canonical.canonical_json("emoji:\U0001f600") == '"emoji:\U0001f600"'


def test_canonical_json_string_minimal_escaping() -> None:
    assert canonical.canonical_json('t\tn\nq"b\\') == '"t\\tn\\nq\\"b\\\\"'


def test_canonical_json_string_escapes_control_chars_not_in_short_set() -> None:
    assert canonical.canonical_json("\x01\x1f") == '"\\u0001\\u001f"'


def test_canonical_json_string_no_unicode_normalization() -> None:
    nfc = "é"  # é, single code point
    nfd = "é"  # e + combining acute accent
    assert canonical.canonical_json(nfc) != canonical.canonical_json(nfd)


# --- numbers: int, Decimal, float rejection -------------------------------


def test_canonical_json_int_is_bare() -> None:
    assert canonical.canonical_json(5) == "5"
    assert canonical.canonical_json(-5) == "-5"


def test_canonical_json_bool_not_coerced_to_int() -> None:
    assert canonical.canonical_json(True) == "true"
    assert canonical.canonical_json(False) == "false"


def test_canonical_json_decimal_preserves_scale() -> None:
    assert canonical.canonical_json(Decimal("1.20")) == '"1.20"'
    assert canonical.canonical_json(Decimal("1.2")) == '"1.2"'
    assert canonical.canonical_json(Decimal("0.00")) == '"0.00"'


def test_canonical_json_decimal_never_scientific_notation() -> None:
    # `Decimal.__str__` would render this as "1E-7" (adjusted exponent < -6);
    # canonical_json must stay fixed-point regardless of magnitude (§7.1).
    assert canonical.canonical_json(Decimal("0.0000001")) == '"0.0000001"'


def test_canonical_json_rejects_non_finite_decimal() -> None:
    # conveyer-azr.32/S-18: rejection is value-free -- this raises inside
    # the snapshot UDF over payload-classified data, so the offending
    # value must never ride the message into driver/executor logs.
    for bad in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
        with pytest.raises(ValueError, match="non-finite Decimal") as exc_info:
            canonical.canonical_json(bad)
        assert str(bad) not in str(exc_info.value)


def test_canonical_json_rejects_float() -> None:
    with pytest.raises(ValueError, match="float values are rejected") as exc_info:
        canonical.canonical_json(1.5)
    assert "1.5" not in str(exc_info.value)


def test_canonical_json_rejects_float_nested_in_a_map() -> None:
    with pytest.raises(ValueError, match="float values are rejected") as exc_info:
        canonical.canonical_json({"amount": 1.5})
    assert "1.5" not in str(exc_info.value)


# --- temporal: fixed-width timestamps, naive rejection [DC-3] ------------


def test_canonical_json_date_iso_format() -> None:
    assert canonical.canonical_json(date(2026, 1, 2)) == '"2026-01-02"'


def test_canonical_json_timestamp_fixed_six_fractional_digits() -> None:
    zero_micros = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert canonical.canonical_json(zero_micros) == '"2026-01-02T03:04:05.000000Z"'
    one_micro = datetime(2026, 1, 2, 3, 4, 5, 1, tzinfo=UTC)
    assert canonical.canonical_json(one_micro) == '"2026-01-02T03:04:05.000001Z"'


def test_canonical_json_timestamp_converts_offset_to_utc() -> None:
    minus_five = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=-5)))
    assert canonical.canonical_json(minus_five) == '"2026-01-02T08:04:05.000000Z"'


def test_canonical_json_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="naive datetime"):
        canonical.canonical_json(datetime(2026, 1, 2, 3, 4, 5))


def test_canonical_json_rejects_aware_datetime_overflowing_past_minyear() -> None:
    # conveyer-azr.24: `.astimezone(UTC)` on an aware value at `datetime.min`
    # with a positive UTC offset walks it below MINYEAR (there is no year 0)
    # and raises `OverflowError` rather than returning -- that value is
    # outside `canonical_json`'s domain and must reject, same class as
    # naive-datetime/float above (§7.1), not propagate a bare OverflowError.
    dt = datetime(1, 1, 1, 0, 0, tzinfo=timezone(timedelta(hours=5)))
    with pytest.raises(ValueError, match="out of representable range") as exc_info:
        canonical.canonical_json(dt)
    # conveyer-azr.32/S-18: value-free -- no repr of `dt` in the message.
    assert repr(dt) not in str(exc_info.value)


def test_canonical_json_rejects_aware_datetime_overflowing_past_maxyear() -> None:
    # Symmetric edge: an aware value at `datetime.max` with a negative UTC
    # offset walks it above MAXYEAR.
    dt = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone(timedelta(hours=-5)))
    with pytest.raises(ValueError, match="out of representable range") as exc_info:
        canonical.canonical_json(dt)
    assert repr(dt) not in str(exc_info.value)


def test_canonical_json_timestamp_uses_the_paired_python_format_constant() -> None:
    # A-7/§7.3: this constant is the ONE authored source n1's in-plan Spark
    # `date_format` rendering must reproduce -- pin that this module's own
    # rendering actually goes through it, not a second, undeclared format.
    dt = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone(timedelta(hours=9)))
    expected = dt.astimezone(UTC).strftime(canonical.CANONICAL_TIMESTAMP_FORMAT)
    assert canonical.canonical_json(dt) == f'"{expected}"'


def test_canonical_timestamp_spark_pattern_is_exported() -> None:
    assert canonical.CANONICAL_TIMESTAMP_SPARK_PATTERN == "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"


# --- bool/None, maps, sequences -------------------------------------------


def test_canonical_json_none_is_null() -> None:
    assert canonical.canonical_json(None) == "null"


def test_canonical_json_empty_map_and_array() -> None:
    assert canonical.canonical_json({}) == "{}"
    assert canonical.canonical_json([]) == "[]"


def test_canonical_json_nested_maps_sorted_at_every_level() -> None:
    value = {"outer_b": {"z": 1, "a": 2}, "outer_a": 3}
    assert canonical.canonical_json(value) == '{"outer_a":3,"outer_b":{"a":2,"z":1}}'


def test_canonical_json_sequence_order_preserved_not_sorted() -> None:
    assert canonical.canonical_json([3, 1, 2]) == "[3,1,2]"
    assert canonical.canonical_json((3, 1, 2)) == "[3,1,2]"


# --- structural rejects ----------------------------------------------------


def test_canonical_json_rejects_non_string_map_key() -> None:
    # conveyer-azr.32/S-18: message names the key's TYPE only, never its
    # value -- this raises inside the snapshot UDF over payload-classified
    # data, and the value must never ride the message into driver/executor
    # logs.
    with pytest.raises(ValueError, match=r"non-string object key rejected: type=int"):
        canonical.canonical_json({1: "x"})


def test_canonical_json_rejects_unsupported_type() -> None:
    class NotSupported:
        pass

    with pytest.raises(ValueError, match="unsupported type"):
        canonical.canonical_json(NotSupported())


# --- row_hash --------------------------------------------------------------


def test_row_hash_is_64_lowercase_hex() -> None:
    h = canonical.row_hash({"a": 1})
    assert re.fullmatch(r"[0-9a-f]{64}", h)


def test_row_hash_order_independent_same_value_identity() -> None:
    assert canonical.row_hash({"a": 1, "b": None}) == canonical.row_hash({"b": None, "a": 1})


def test_row_hash_differs_for_different_values() -> None:
    assert canonical.row_hash({"a": 1}) != canonical.row_hash({"a": 2})


# --- property tests (§12.4) -------------------------------------------------

_DECIMAL_SHAPE = re.compile(r"-?\d+(\.\d+)?")
_DATE_SHAPE = re.compile(r"\d{4}-\d{2}-\d{2}")
_TIMESTAMP_SHAPE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")


def _is_ambiguous_string(value: str) -> bool:
    # See the module docstring's "Injectivity property scope" note.
    return bool(
        _DECIMAL_SHAPE.fullmatch(value)
        or _DATE_SHAPE.fullmatch(value)
        or _TIMESTAMP_SHAPE.fullmatch(value)
    )


_STRING_LEAF = st.text(max_size=12).filter(lambda s: not _is_ambiguous_string(s))
_KEY = st.text(max_size=8)


@st.composite
def _decimal_leaf(draw: st.DrawFn) -> Decimal:
    places = draw(st.integers(min_value=0, max_value=6))
    return draw(
        st.decimals(
            min_value=Decimal("-1000000"),
            max_value=Decimal("1000000"),
            allow_nan=False,
            allow_infinity=False,
            places=places,
        )
    )


@st.composite
def _timestamp_leaf(draw: st.DrawFn) -> datetime:
    # Bounded a day off each end so the largest sampled offset below (8h)
    # can never walk the UTC-converted value past MINYEAR/MAXYEAR
    # (conveyer-azr.24) -- this generator's domain is meant to be VALID
    # inputs only, the same idiom that already keeps floats and naive
    # datetimes out of `_LEAF`; the out-of-range reject path itself is
    # covered directly, not through this generic fuzz strategy.
    naive = draw(
        st.datetimes(
            min_value=datetime.min + timedelta(days=1),
            max_value=datetime.max - timedelta(days=1),
        )
    )
    tz = draw(st.sampled_from([UTC, timezone(timedelta(hours=5)), timezone(timedelta(hours=-8))]))
    return naive.replace(tzinfo=tz)


_LEAF = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    _decimal_leaf(),
    st.dates(),
    _timestamp_leaf(),
    _STRING_LEAF,
)

_VALUE = st.recursive(
    _LEAF,
    lambda children: st.one_of(
        st.dictionaries(_KEY, children, max_size=4),
        st.lists(children, max_size=4),
    ),
    max_leaves=15,
)


def _canonical_json_or_none(value: Any) -> str | None:
    # Injectivity is a claim over `canonical_json`'s value domain (§7.1);
    # a `_reject`-ing input (ValueError) is outside that domain by
    # definition, so it has no canonical form to compare -- `None` here
    # means "not comparable", not "collides with everything".
    try:
        return canonical.canonical_json(value)
    except ValueError:
        return None


@given(a=_VALUE, b=_VALUE)
@settings(max_examples=300)
@example(
    # conveyer-azr.24 regression: aware datetime at `datetime.min` with a
    # positive UTC offset -- must reject (ValueError), not raise a bare
    # `OverflowError` out of `.astimezone(UTC)`.
    a=datetime(1, 1, 1, 0, 0, tzinfo=timezone(timedelta(hours=5))),
    b=datetime(1, 1, 1, 0, 0, tzinfo=timezone(timedelta(hours=5))),
)
@example(
    # Symmetric edge: `datetime.max` with a negative UTC offset.
    a=datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone(timedelta(hours=-5))),
    b=datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone(timedelta(hours=-5))),
)
def test_canonical_json_injective_over_generated_values(a: Any, b: Any) -> None:
    ca, cb = _canonical_json_or_none(a), _canonical_json_or_none(b)
    if ca is not None and cb is not None and ca == cb:
        assert a == b


@given(d=st.dictionaries(_KEY, _LEAF, max_size=8))
@settings(max_examples=200)
def test_canonical_json_key_sort_stable_under_reordering(d: dict[str, Any]) -> None:
    reordered = dict(reversed(list(d.items())))
    assert canonical.canonical_json(d) == canonical.canonical_json(reordered)


@given(dt=st.datetimes())
@settings(max_examples=100)
def test_canonical_json_naive_datetime_always_rejected(dt: datetime) -> None:
    with pytest.raises(ValueError, match="naive datetime"):
        canonical.canonical_json(dt)


@given(f=st.floats())
@settings(max_examples=100)
def test_canonical_json_float_always_rejected(f: float) -> None:
    with pytest.raises(ValueError, match="float values are rejected"):
        canonical.canonical_json(f)
