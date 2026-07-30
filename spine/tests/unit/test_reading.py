"""Unit + property tests for `spine.core.reading` — LLD 005.1 §5.2/§5.3, §12.4
(A-1, A-3, A-4).

`parse_line`'s strict-error corpus pins the two `csv.Error` messages §5.3
cites verbatim (engine-verified, not paraphrased); the round-trip property
confirms `parse_line` inverts `csv.writer` for any token tuple free of
embedded `\\n`/`\\r`, and a companion property pins the "newlines-in-quotes
rejected under `multiline: false`" verdict by simulating exactly what the
real Hadoop-fed reader does to a `csv.writer`-rendered record containing an
embedded newline: split it into physical lines *before* handing any one
fragment to `parse_line` (§5.1's single-split acquisition never hands this
function an unsplit multi-line string). `bind_header`'s corpus exercises
both the header-true (declared/extras/missing-required/duplicate) and
header-false (positional, `tokens`-ignoring, DC-2's always-empty-extras)
paths, plus a totality property confirming it never raises over arbitrary
generated tokens/columns.

`shape_row`/`multiline_records` (moved here from `effects/spark.py`,
critique F3, bead conveyer-azr.30) get their own corpus + property below:
`shape_row`'s malformed/ragged/well-formed shaping (§5.4) and a totality +
invariant property built the same way `bind_header`'s own property is (a
real `bind_header`-derived `HeaderBinding`, arbitrary row tokens);
`multiline_records`'s multi-record/unterminated-quote/embedded-newline
corpus (§5.5) and a round-trip property generalizing `parse_line`'s own
(above) to a multi-line, multi-record body written via `csv.writer` with a
real `\\r\\n` line terminator.
"""

from __future__ import annotations

import csv
import io

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from spine.core import model, reading

# --- shared fixtures ---------------------------------------------------------

_DEFAULT_DIALECT = reading.dialect_value(model.DialectModel(format="csv"))
_HEADERLESS_DIALECT = reading.dialect_value(model.DialectModel(format="csv", header=False))
_CUSTOM_DIALECT = reading.dialect_value(model.DialectModel(format="csv", delimiter=";", quote="'"))


def _contract(**column_kwargs_by_name: dict[str, object]) -> model.RawContractModel:
    """Builds a `RawContractModel` from `{name: {field: value, ...}, ...}` --
    a terser corpus-authoring shape than spelling every `ColumnSpec(...)`
    call out per test."""
    columns = [
        model.ColumnSpec(name=name, **kwargs) for name, kwargs in column_kwargs_by_name.items()
    ]
    return model.RawContractModel(columns=columns)


def _write_line(tokens: tuple[str, ...], delimiter: str = ",", quotechar: str = '"') -> str:
    """Test-local `csv.writer` helper for the round-trip properties --
    `lineterminator=""` so the written text is exactly one physical line
    (no trailing `\\r\\n` the real Hadoop line-reader would otherwise have
    already stripped)."""
    buf = io.StringIO()
    writer = csv.writer(
        buf,
        delimiter=delimiter,
        quotechar=quotechar,
        doublequote=True,
        escapechar=None,
        lineterminator="",
    )
    writer.writerow(tokens)
    return buf.getvalue()


# --- DialectValue / dialect_value --------------------------------------------


def test_dialect_value_projects_delimiter_quote_header_multiline() -> None:
    dialect = model.DialectModel(
        format="csv", delimiter=";", quote="'", header=False, multiline=True
    )
    value = reading.dialect_value(dialect)
    assert value == reading.DialectValue(delimiter=";", quotechar="'", header=False, multiline=True)


def test_dialect_value_is_frozen() -> None:
    value = reading.dialect_value(model.DialectModel(format="csv"))
    with pytest.raises(AttributeError):
        value.delimiter = ","  # type: ignore[misc]


# --- parse_line: happy path + shape ------------------------------------------


def test_parse_line_simple_row() -> None:
    parsed = reading.parse_line("a,b,c", _DEFAULT_DIALECT)
    assert parsed == reading.ParsedLine(tokens=("a", "b", "c"), error=None)


def test_parse_line_quoted_embedded_delimiter() -> None:
    parsed = reading.parse_line('a,"b,c",d', _DEFAULT_DIALECT)
    assert parsed.tokens == ("a", "b,c", "d")
    assert parsed.error is None


def test_parse_line_doubled_quote_escaping() -> None:
    parsed = reading.parse_line('a,"he said ""hi""",b', _DEFAULT_DIALECT)
    assert parsed.tokens == ("a", 'he said "hi"', "b")


def test_parse_line_custom_delimiter_and_quotechar() -> None:
    parsed = reading.parse_line("a;'b;c';d", _CUSTOM_DIALECT)
    assert parsed.tokens == ("a", "b;c", "d")


def test_parse_line_lone_delimiter_yields_two_empty_tokens() -> None:
    parsed = reading.parse_line(",", _DEFAULT_DIALECT)
    assert parsed == reading.ParsedLine(tokens=("", ""), error=None)


def test_parse_line_lone_space_is_one_token_not_empty() -> None:
    parsed = reading.parse_line(" ", _DEFAULT_DIALECT)
    assert parsed == reading.ParsedLine(tokens=(" ",), error=None)


# --- parse_line: empty line -> no record (§5.1.6) ----------------------------


def test_parse_line_empty_line_yields_empty_tuple_not_none() -> None:
    """§5.1.6: "empty lines yield no record and no ordinal" -- the caller
    recognizes this as `tokens == ()`, distinct from malformed
    (`tokens is None`)."""
    parsed = reading.parse_line("", _DEFAULT_DIALECT)
    assert parsed.tokens == ()
    assert parsed.tokens is not None
    assert parsed.error is None


# --- parse_line: NUL and control chars are values, not structural errors ----


def test_parse_line_nul_byte_is_a_value() -> None:
    parsed = reading.parse_line("a,b\x00c,d", _DEFAULT_DIALECT)
    assert parsed.tokens == ("a", "b\x00c", "d")
    assert parsed.error is None


@pytest.mark.parametrize("control_char", ["\x01", "\x02", "\x1f", "\x7f"])
def test_parse_line_other_control_chars_are_values(control_char: str) -> None:
    parsed = reading.parse_line(f"a,b{control_char}c,d", _DEFAULT_DIALECT)
    assert parsed.tokens == ("a", f"b{control_char}c", "d")
    assert parsed.error is None


# --- parse_line: strict-error corpus (§5.3, csv.Error -> malformed) ---------


def test_parse_line_unterminated_quote_is_malformed() -> None:
    parsed = reading.parse_line('a,"unterminated,b', _DEFAULT_DIALECT)
    assert parsed.tokens is None
    assert parsed.error == "unexpected end of data"


def test_parse_line_junk_after_closing_quote_is_malformed() -> None:
    parsed = reading.parse_line('a,"closed"junk,b', _DEFAULT_DIALECT)
    assert parsed.tokens is None
    assert parsed.error == "',' expected after '\"'"


def test_parse_line_error_is_never_a_persisted_shape() -> None:
    """§5.3: "never persisted" -- pinned here as a shape assertion (`error`
    is a plain `str`, not itself wrapped in anything durable-looking) rather
    than a text-content assertion, since the persistence discipline is a
    caller-side (n2/pre_check) property this module cannot itself enforce."""
    parsed = reading.parse_line('a,"unterminated', _DEFAULT_DIALECT)
    assert isinstance(parsed.error, str)
    assert parsed.tokens is None


# --- parse_line: round-trip + newline-rejection properties (§12.4) ----------

_TOKEN_TEXT = st.text(
    alphabet=st.characters(
        blacklist_characters="\n\r", blacklist_categories=("Cs",), max_codepoint=0x2FFFF
    ),
    max_size=12,
)


@given(tokens=st.lists(_TOKEN_TEXT, max_size=6).map(tuple))
@settings(max_examples=200)
def test_parse_line_round_trips_csv_writer_output(tokens: tuple[str, ...]) -> None:
    """Any token tuple free of embedded `\\n`/`\\r` (incl. embedded
    delimiters and quotes) survives `csv.writer` -> `parse_line` exactly."""
    written = _write_line(tokens)
    parsed = reading.parse_line(written, _DEFAULT_DIALECT)
    assert parsed.error is None
    assert parsed.tokens == tokens


@given(
    tokens=st.lists(_TOKEN_TEXT, min_size=1, max_size=6).map(tuple),
    newline_token_index=st.integers(min_value=0),
)
@settings(max_examples=100)
def test_parse_line_rejects_newline_split_across_physical_lines(
    tokens: tuple[str, ...], newline_token_index: int
) -> None:
    """`multiline: false`'s forbidden case: a token containing an embedded
    newline gets quoted-with-a-real-`\\n` by `csv.writer` (RFC 4180); under
    `multiline: false` the upstream Hadoop reader has already split the
    *encoded* record into separate physical lines at that `\\n` before
    `parse_line` ever sees any of it (§5.1 step 1) -- feeding the FIRST
    physical fragment to `parse_line` in isolation must be rejected
    (unterminated quote), never silently accepted as a truncated record."""
    idx = newline_token_index % len(tokens)
    tokens_with_newline = tuple(
        token + "\n" + token if i == idx else token for i, token in enumerate(tokens)
    )
    written = _write_line(tokens_with_newline)
    assert "\n" in written  # the writer only embeds a raw newline inside a quoted field

    first_physical_line = written.split("\n", 1)[0]
    parsed = reading.parse_line(first_physical_line, _DEFAULT_DIALECT)
    assert parsed.tokens is None
    assert parsed.error is not None


# --- bind_header: header:true accept/reject corpus (§5.2/A-4) ---------------

_THREE_COLUMN_CONTRACT = _contract(
    domain_id={"required": True, "nullable": False},
    amount={},
    status={"required": True},
)


def test_bind_header_exact_match_declared_order() -> None:
    binding = reading.bind_header(
        ("domain_id", "amount", "status"), _THREE_COLUMN_CONTRACT, _DEFAULT_DIALECT
    )
    assert binding == reading.HeaderBinding(
        column_at=("domain_id", "amount", "status"),
        expected_width=3,
        missing_required=(),
        duplicate_name_groups=(),
    )


def test_bind_header_reordered_declared_columns_still_bind() -> None:
    """Per-object binding is by exact name, not position -- multi-object
    deliveries with differing column order land correctly (A-4)."""
    binding = reading.bind_header(
        ("status", "domain_id", "amount"), _THREE_COLUMN_CONTRACT, _DEFAULT_DIALECT
    )
    assert binding.column_at == ("status", "domain_id", "amount")
    assert binding.missing_required == ()


def test_bind_header_undeclared_column_binds_to_extras() -> None:
    binding = reading.bind_header(
        ("status", "vendor", "domain_id", "amount"), _THREE_COLUMN_CONTRACT, _DEFAULT_DIALECT
    )
    assert binding.column_at == ("status", None, "domain_id", "amount")
    assert binding.expected_width == 4
    assert binding.missing_required == ()
    assert binding.duplicate_name_groups == ()


def test_bind_header_missing_required_column_is_surfaced_as_data() -> None:
    binding = reading.bind_header(("domain_id", "amount"), _THREE_COLUMN_CONTRACT, _DEFAULT_DIALECT)
    assert binding.missing_required == ("status",)
    assert binding.duplicate_name_groups == ()  # not itself a raise -- pure data


def test_bind_header_missing_non_required_column_is_not_surfaced() -> None:
    binding = reading.bind_header(("domain_id", "status"), _THREE_COLUMN_CONTRACT, _DEFAULT_DIALECT)
    assert binding.missing_required == ()  # "amount" absent, but not required


def test_bind_header_duplicate_declared_name_positions() -> None:
    binding = reading.bind_header(
        ("domain_id", "domain_id", "amount", "status"), _THREE_COLUMN_CONTRACT, _DEFAULT_DIALECT
    )
    assert binding.duplicate_name_groups == ((0, 1),)
    assert binding.missing_required == ()


def test_bind_header_duplicate_undeclared_name_positions() -> None:
    binding = reading.bind_header(
        ("domain_id", "amount", "status", "x", "x"), _THREE_COLUMN_CONTRACT, _DEFAULT_DIALECT
    )
    assert binding.duplicate_name_groups == ((3, 4),)


def test_bind_header_missing_required_and_duplicate_together() -> None:
    binding = reading.bind_header(
        ("domain_id", "domain_id"), _THREE_COLUMN_CONTRACT, _DEFAULT_DIALECT
    )
    assert binding.missing_required == ("status",)
    assert binding.duplicate_name_groups == ((0, 1),)


def test_bind_header_empty_string_header_token_binds_to_extras() -> None:
    binding = reading.bind_header(
        ("domain_id", "amount", "status", ""), _THREE_COLUMN_CONTRACT, _DEFAULT_DIALECT
    )
    assert binding.column_at[-1] is None
    assert binding.expected_width == 4


# --- bind_header: header:false positional corpus (A-4, DC-2) ----------------


def test_bind_header_headerless_ignores_tokens_entirely() -> None:
    """A-4: `header: false` binds positionally at the declared width --
    whatever `tokens` a caller happens to pass is irrelevant."""
    ignored = reading.bind_header(
        ("nonsense", "does", "not", "matter"), _THREE_COLUMN_CONTRACT, _HEADERLESS_DIALECT
    )
    empty = reading.bind_header((), _THREE_COLUMN_CONTRACT, _HEADERLESS_DIALECT)
    assert ignored == empty
    assert ignored == reading.HeaderBinding(
        column_at=("domain_id", "amount", "status"),
        expected_width=3,
        missing_required=(),
        duplicate_name_groups=(),
    )


def test_bind_header_headerless_extras_always_empty() -> None:
    """DC-2: `extras` is always `{}` under `header: false` -- no `column_at`
    position is ever `None`."""
    binding = reading.bind_header((), _THREE_COLUMN_CONTRACT, _HEADERLESS_DIALECT)
    assert None not in binding.column_at


def test_bind_header_headerless_expected_width_is_declared_width() -> None:
    binding = reading.bind_header((), _THREE_COLUMN_CONTRACT, _HEADERLESS_DIALECT)
    assert binding.expected_width == len(_THREE_COLUMN_CONTRACT.columns)


# --- bind_header: totality + invariant property (§12.4) ---------------------

_COLUMN_NAME = st.from_regex(r"[A-Za-z_][A-Za-z0-9_]{0,10}", fullmatch=True)
_HEADER_TOKEN = st.text(max_size=10)


@given(
    column_names=st.lists(_COLUMN_NAME, min_size=1, max_size=5, unique=True),
    tokens=st.lists(_HEADER_TOKEN, max_size=8),
    header=st.booleans(),
)
@settings(max_examples=200)
def test_bind_header_is_total_and_internally_consistent(
    column_names: list[str], tokens: list[str], header: bool
) -> None:
    """`bind_header` never raises (pure total function, §5.2); every
    `HeaderBinding` it produces satisfies its own documented invariants
    regardless of how adversarial `tokens`/`column_names` are."""
    columns = [model.ColumnSpec(name=name) for name in column_names]
    contract = model.RawContractModel(columns=columns)
    dialect = reading.dialect_value(model.DialectModel(format="csv", header=header))

    binding = reading.bind_header(tuple(tokens), contract, dialect)

    assert len(binding.column_at) == binding.expected_width
    for bound_name in binding.column_at:
        assert bound_name is None or bound_name in column_names

    if not header:
        assert binding.expected_width == len(column_names)
        assert binding.missing_required == ()
        assert binding.duplicate_name_groups == ()
        assert None not in binding.column_at  # DC-2
    else:
        assert binding.expected_width == len(tokens)
        for group in binding.duplicate_name_groups:
            assert len(group) >= 2
            assert len({tokens[i] for i in group}) == 1  # one shared token text per group
        for missing_name in binding.missing_required:
            assert missing_name not in tokens


# --- shape_row: well-formed/malformed/ragged shaping (§5.4, moved here from -
# --- effects/spark.py, critique F3, bead conveyer-azr.30) -------------------

_SHAPE_ROW_CONTRACT = _contract(domain_id={}, payload={})
_SHAPE_ROW_DIALECT = _DEFAULT_DIALECT


def _shape_row_binding(header_tokens: tuple[str, ...]) -> reading.HeaderBinding:
    return reading.bind_header(header_tokens, _SHAPE_ROW_CONTRACT, _SHAPE_ROW_DIALECT)


def test_shape_row_well_formed_binds_declared_and_extras() -> None:
    binding = _shape_row_binding(("domain_id", "vendor", "payload"))
    row = reading.shape_row(
        "s3://x/a.csv",
        1,
        1,
        "id-1,acme,widget",
        ("id-1", "acme", "widget"),
        binding,
        ("domain_id", "vendor", "payload"),
        ("domain_id", "payload"),
    )
    assert row == ("s3://x/a.csv", 1, 1, None, "id-1", "widget", {"vendor": "acme"})


def test_shape_row_empty_token_becomes_none_a5() -> None:
    binding = _shape_row_binding(("domain_id", "vendor", "payload"))
    row = reading.shape_row(
        "s3://x/a.csv",
        1,
        2,
        "id-2,,widget",
        ("id-2", "", "widget"),
        binding,
        ("domain_id", "vendor", "payload"),
        ("domain_id", "payload"),
    )
    assert row == ("s3://x/a.csv", 1, 2, None, "id-2", "widget", {"vendor": ""})


def test_shape_row_malformed_tokens_none_shapes_every_declared_column_null() -> None:
    binding = _shape_row_binding(("domain_id", "vendor", "payload"))
    row = reading.shape_row(
        "s3://x/a.csv",
        1,
        3,
        'id-3,"unterminated',
        None,
        binding,
        ("domain_id", "vendor", "payload"),
        ("domain_id", "payload"),
    )
    assert row == ("s3://x/a.csv", 1, 3, 'id-3,"unterminated', None, None, {})


def test_shape_row_ragged_too_few_tokens_shapes_identically_to_malformed() -> None:
    """§5.4: ragged (`len(tokens) != binding.expected_width`) shapes exactly
    like `tokens is None` -- same malformed branch, no distinction carried
    into the returned row."""
    binding = _shape_row_binding(("domain_id", "vendor", "payload"))
    row = reading.shape_row(
        "s3://x/a.csv",
        1,
        4,
        "id-4,acme",
        ("id-4", "acme"),
        binding,
        ("domain_id", "vendor", "payload"),
        ("domain_id", "payload"),
    )
    assert row == ("s3://x/a.csv", 1, 4, "id-4,acme", None, None, {})


def test_shape_row_extras_keyed_by_header_token_not_this_records_value() -> None:
    """A-4: the extras key is the HEADER's own claim at that position, fixed
    once per object -- never re-derived from the current record's tokens."""
    binding = _shape_row_binding(("domain_id", "vendor", "payload"))
    row = reading.shape_row(
        "s3://x/a.csv",
        1,
        1,
        "id-1,ignored-value,widget",
        ("id-1", "ignored-value", "widget"),
        binding,
        ("domain_id", "vendor", "payload"),
        ("domain_id", "payload"),
    )
    assert row[-1] == {"vendor": "ignored-value"}  # keyed by the header token "vendor"


def test_shape_row_headerless_extras_keyed_by_position() -> None:
    """When `header_tokens is None` (headerless feeds), an extras position
    keys by its own `str(position)` -- there is no header token to key by."""
    binding = reading.HeaderBinding(
        column_at=("domain_id", None, "payload"),
        expected_width=3,
        missing_required=(),
        duplicate_name_groups=(),
    )
    row = reading.shape_row(
        "s3://x/a.csv",
        1,
        1,
        "id-1,extra,widget",
        ("id-1", "extra", "widget"),
        binding,
        None,
        ("domain_id", "payload"),
    )
    assert row == ("s3://x/a.csv", 1, 1, None, "id-1", "widget", {"1": "extra"})


# --- shape_row: totality + shape-invariant property (§12.4) -----------------


@given(
    column_names=st.lists(_COLUMN_NAME, min_size=1, max_size=5, unique=True),
    header_tokens_list=st.lists(_HEADER_TOKEN, max_size=8),
    row_tokens_list=st.lists(st.text(max_size=8), max_size=8),
)
@settings(max_examples=200)
def test_shape_row_is_total_and_shapes_malformed_uniformly(
    column_names: list[str],
    header_tokens_list: list[str],
    row_tokens_list: list[str],
) -> None:
    """`shape_row` never raises (pure total function, §5.4); every returned
    row satisfies its own documented invariants regardless of how
    adversarial `row_tokens`/`column_names` are -- built the same way
    `bind_header`'s own totality property is (above): a REAL `bind_header`-
    derived `HeaderBinding`, arbitrary row tokens on top of it."""
    columns = [model.ColumnSpec(name=name) for name in column_names]
    contract = model.RawContractModel(columns=columns)
    dialect = reading.dialect_value(model.DialectModel(format="csv"))
    header_tokens = tuple(header_tokens_list)
    binding = reading.bind_header(header_tokens, contract, dialect)

    row_tokens: tuple[str, ...] = tuple(row_tokens_list)
    row = reading.shape_row(
        "s3://x", 1, 1, "raw-text", row_tokens, binding, header_tokens, tuple(column_names)
    )

    assert len(row) == 4 + len(column_names) + 1
    uri, object_seq, row_index, malformed_text, *declared, extras = row
    assert (uri, object_seq, row_index) == ("s3://x", 1, 1)

    malformed = len(row_tokens) != binding.expected_width
    if malformed:
        assert malformed_text == "raw-text"
        assert all(v is None for v in declared)
        assert extras == {}
    else:
        assert malformed_text is None
        assert set(extras) <= set(header_tokens)  # every extras key is a real header token

    # `tokens=None` always shapes malformed, uniformly, regardless of width.
    row_none = reading.shape_row(
        "s3://x", 1, 1, "raw-text", None, binding, header_tokens, tuple(column_names)
    )
    assert row_none == ("s3://x", 1, 1, "raw-text", *([None] * len(column_names)), {})


# --- multiline_records: multi-record/unterminated-quote corpus (§5.5, moved -
# --- here from effects/spark.py, critique F3, bead conveyer-azr.30) ---------


def test_multiline_records_multiple_records_one_reader() -> None:
    body = ["domain_id,payload\n", 'id-1,"hello\n', 'world"\n', "id-2,plain\n"]
    records = list(reading.multiline_records(body, _DEFAULT_DIALECT))
    assert records == [
        (("domain_id", "payload"), "domain_id,payload\n"),
        (("id-1", "hello\nworld"), 'id-1,"hello\nworld"\n'),
        (("id-2", "plain"), "id-2,plain\n"),
    ]


def test_multiline_records_unterminated_quote_consumes_remainder_strict_false() -> None:
    """§5.5's declared trade-off: an unterminated quote at true EOF consumes
    the file's remainder into ONE field (`strict=False`), rather than
    raising the way `parse_line`'s own `strict=True` would."""
    body = ["domain_id,payload\n", 'id-1,"hello\n', "id-2,plain\n"]
    records = list(reading.multiline_records(body, _DEFAULT_DIALECT))
    assert records == [
        (("domain_id", "payload"), "domain_id,payload\n"),
        (("id-1", "hello\nid-2,plain\n"), 'id-1,"hello\nid-2,plain\n'),
    ]


def test_multiline_records_empty_body_yields_no_records() -> None:
    assert list(reading.multiline_records([], _DEFAULT_DIALECT)) == []


def test_multiline_records_raw_text_spans_are_contiguous_and_exact() -> None:
    """`raw_text` per record is the verbatim join of whichever physical
    lines (with their original line endings) the reader consumed -- proven
    here by reconstructing the ENTIRE source from every yielded span."""
    body = ["a,b\n", "c,d\n", 'e,"f\n', 'g"\n']
    records = list(reading.multiline_records(body, _DEFAULT_DIALECT))
    assert "".join(raw_text for _, raw_text in records) == "".join(body)


# --- multiline_records: round-trip property (§12.4) --------------------------

# Every char `str.splitlines()` treats as a line boundary OTHER than plain
# "\n" (which is deliberately INCLUDED -- the interesting multiline case) is
# excluded: `\r` because `\r\n` is the writer's own record terminator (an
# embedded bare `\r` would collide with splitlines()'s `\r`/`\r\n` handling,
# a pre-existing property of using `str.splitlines()` to split the body at
# all -- `effects/spark.py::_shape_multiline_object` does the identical
# split on real driver-fetched text); the rest (`\x0b\x0c\x1c\x1d\x1e\x85`)
# are boundaries `str.splitlines()` recognizes that plain `\r\n`-terminated
# CSV never intends as one.
_MULTILINE_TOKEN_TEXT = st.text(
    alphabet=st.characters(
        blacklist_characters="\r\x0b\x0c\x1c\x1d\x1e\x85",
        blacklist_categories=("Cs",),
        max_codepoint=0x2FFFF,
    ),
    max_size=8,
)


def _write_records(
    records: list[tuple[str, ...]], delimiter: str = ",", quotechar: str = '"'
) -> str:
    """Like `_write_line` above, but a REAL `\\r\\n` line terminator and one
    or more records -- `multiline_records`'s own contract needs an actual
    physical-line-splittable body, unlike `parse_line`'s single-line-at-a-
    time round trip."""
    buf = io.StringIO()
    writer = csv.writer(
        buf,
        delimiter=delimiter,
        quotechar=quotechar,
        doublequote=True,
        escapechar=None,
        lineterminator="\r\n",
    )
    for record in records:
        writer.writerow(record)
    return buf.getvalue()


@given(
    records=st.lists(
        st.lists(_MULTILINE_TOKEN_TEXT, min_size=1, max_size=4).map(tuple), min_size=1, max_size=5
    ),
)
@settings(max_examples=200)
def test_multiline_records_round_trips_csv_writer_output(records: list[tuple[str, ...]]) -> None:
    """Any record list free of the excluded boundary chars survives
    `csv.writer` -> `str.splitlines(keepends=True)` -> `multiline_records`
    exactly, token-for-token AND span-for-span (the concatenation of every
    yielded `raw_text` reconstructs the written source exactly)."""
    written = _write_records(records)
    body_lines = written.splitlines(keepends=True)

    recovered = list(reading.multiline_records(body_lines, _DEFAULT_DIALECT))

    assert [tokens for tokens, _ in recovered] == records
    assert "".join(raw_text for _, raw_text in recovered) == written
