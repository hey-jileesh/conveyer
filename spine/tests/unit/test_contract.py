"""Unit + property tests for `spine.core.contract` — LLD 005.1 §3.2, §12.4.

`parse_column_type` is the single interpreter of the column-type grammar
(D-5): this suite pins its accept/reject corpus (mirroring
`COLUMN_TYPE_RE`'s shape exactly) and the structured `ColumnType` values it
extracts for each kind (decimal precision/scale as `int`, date/timestamp
`fmt` as the raw, unparsed pattern-letter string). `test_model.py`/
`test_model_patterns.py` cover the SAME grammar as exercised THROUGH
`ColumnSpec` (the pydantic boundary); this file exercises the function
directly, per its own module docstring ("safe to call directly ... without
going through `ColumnSpec` first").
"""

import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from spine.core import contract
from spine.core.model import ColumnSpec, DialectModel, RawContractModel, ReadSpecModel

# --- accept corpus: every kind, structured value pinned --------------------


def test_string_kind() -> None:
    parsed = contract.parse_column_type("string")
    assert parsed == contract.ColumnType(kind="string")


def test_int_kind() -> None:
    assert contract.parse_column_type("int") == contract.ColumnType(kind="int")


def test_long_kind() -> None:
    assert contract.parse_column_type("long") == contract.ColumnType(kind="long")


def test_bool_kind() -> None:
    assert contract.parse_column_type("bool") == contract.ColumnType(kind="bool")


@pytest.mark.parametrize(
    ("type_str", "precision", "scale"),
    [
        ("decimal(1,0)", 1, 0),
        ("decimal(5,2)", 5, 2),
        ("decimal(38,0)", 38, 0),
        ("decimal(38,38)", 38, 38),
        (
            "decimal(99,99)",
            99,
            99,
        ),  # shape-legal; DC-10's <=38 bound is model.py's job, not this one
    ],
)
def test_decimal_kind_extracts_precision_scale(type_str: str, precision: int, scale: int) -> None:
    parsed = contract.parse_column_type(type_str)
    assert parsed == contract.ColumnType(kind="decimal", precision=precision, scale=scale)


@pytest.mark.parametrize(
    ("type_str", "fmt"),
    [
        ("date(yyyy-MM-dd)", "yyyy-MM-dd"),
        ("date(MM/dd/yyyy)", "MM/dd/yyyy"),
        ("timestamp(yyyy-MM-dd'T'HH:mm:ss)", "yyyy-MM-dd'T'HH:mm:ss"),
        (
            "timestamp(yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z')",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'",
        ),  # the exact §7.3 row_hash rendering fmt
    ],
)
def test_temporal_kind_extracts_fmt_unparsed(type_str: str, fmt: str) -> None:
    kind = "date" if type_str.startswith("date(") else "timestamp"
    parsed = contract.parse_column_type(type_str)
    assert parsed == contract.ColumnType(kind=kind, fmt=fmt)  # type: ignore[arg-type]


# --- reject corpus: grammar violations --------------------------------------


@pytest.mark.parametrize(
    "type_str",
    [
        "",
        "float",  # not in the grammar's fixed-kind set
        "STRING",  # case-sensitive
        "decimal(5)",  # missing scale
        "decimal()",  # empty parens
        "decimal(5,)",  # missing scale digits
        "decimal(,5)",  # missing precision digits
        "decimal(05,2)",  # leading zero not in [1-9][0-9]?
        "decimal(100,2)",  # 3-digit precision exceeds the grammar's 2-digit shape
        "date()",  # empty fmt
        "date(yyyy-MM-dd",  # unbalanced paren
        "date(yyyy(MM)dd)",  # nested paren, excluded by [^()]+
        "timestamp()",
        "decimal(5,2)\n",  # trailing newline (nvh.34 regression class)
        "\ndecimal(5,2)",  # leading newline
        "dec imal(5,2)",  # embedded space
    ],
)
def test_rejects_grammar_violations(type_str: str) -> None:
    with pytest.raises(ValueError, match="not a valid column type grammar"):
        contract.parse_column_type(type_str)


# --- COLUMN_TYPE_RE itself: shape-only, mirrors ColumnSpec.type's Field(pattern=...) ---


def test_column_type_re_is_the_grammar_parse_column_type_enforces() -> None:
    compiled = re.compile(contract.COLUMN_TYPE_RE)
    assert compiled.fullmatch("decimal(5,2)")
    assert compiled.fullmatch("decimal(100,2)") is None


# --- property tests (§12.4) -------------------------------------------------


@given(
    precision=st.integers(min_value=1, max_value=99), scale=st.integers(min_value=0, max_value=99)
)
@settings(max_examples=200)
def test_decimal_round_trips_precision_scale(precision: int, scale: int) -> None:
    """Every grammar-shaped `decimal(p,s)` (p, s each 1-2 digits, p's leading
    digit nonzero) round-trips through `parse_column_type` to the exact
    integers declared -- independent of DC-10's semantic bound (>=38 is
    `core/model.py`'s job, not this function's)."""
    type_str = f"decimal({precision},{scale})"
    parsed = contract.parse_column_type(type_str)
    assert parsed.kind == "decimal"
    assert parsed.precision == precision
    assert parsed.scale == scale


@given(
    fmt=st.text(
        alphabet=st.characters(
            blacklist_characters="()", blacklist_categories=("Cs",), max_codepoint=0x2FFFF
        ),
        min_size=1,
        max_size=40,
    ).filter(lambda s: "(" not in s and ")" not in s)
)
@settings(max_examples=100)
def test_temporal_fmt_survives_unparsed(fmt: str) -> None:
    """Any non-empty, paren-free `fmt` (the grammar's `[^()]+`) round-trips
    verbatim through `date(...)`/`timestamp(...)` -- `parse_column_type`
    does not interpret or normalize the Java/Spark pattern letters, only
    extracts them (§3.2: the JVM is the temporal-format authority)."""
    parsed = contract.parse_column_type(f"date({fmt})")
    assert parsed == contract.ColumnType(kind="date", fmt=fmt)
    parsed_ts = contract.parse_column_type(f"timestamp({fmt})")
    assert parsed_ts == contract.ColumnType(kind="timestamp", fmt=fmt)


@given(
    junk=st.text(min_size=0, max_size=20).filter(
        lambda s: (
            s not in ("string", "int", "long", "bool")
            and not s.startswith("decimal(")
            and not s.startswith("date(")
            and not s.startswith("timestamp(")
        )
    )
)
@settings(max_examples=100)
def test_non_grammar_strings_always_raise(junk: str) -> None:
    with pytest.raises(ValueError, match="not a valid column type grammar"):
        contract.parse_column_type(junk)


# --- §3.3 versions (A-11, bead conveyer-azr.13, n0-spec-migration) ----------

_READ = ReadSpecModel(dialect=DialectModel(format="csv"))
_RAW_CONTRACT = RawContractModel(
    columns=[ColumnSpec(name="domain_id", required=True, nullable=False)]
)
_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


def test_read_spec_version_is_64_char_lowercase_hex() -> None:
    assert _SHA256_HEX_RE.fullmatch(contract.read_spec_version(_READ))


def test_check_version_is_64_char_lowercase_hex() -> None:
    assert _SHA256_HEX_RE.fullmatch(contract.check_version(_RAW_CONTRACT, _READ))


def test_read_spec_version_changes_when_read_changes() -> None:
    other_read = ReadSpecModel(dialect=DialectModel(format="csv", header=False))
    assert contract.read_spec_version(other_read) != contract.read_spec_version(_READ)


def test_check_version_is_a_pair_hash_sensitive_to_either_surface() -> None:
    """A-11: `check_version` fuses BOTH declared admission surfaces into one
    hash -- changing either the contract or the read spec must change it,
    even though `read_spec_version` only tracks `read`."""
    base = contract.check_version(_RAW_CONTRACT, _READ)

    other_contract = RawContractModel(columns=[ColumnSpec(name="other")])
    assert contract.check_version(other_contract, _READ) != base

    other_read = ReadSpecModel(dialect=DialectModel(format="csv", header=False))
    assert contract.check_version(_RAW_CONTRACT, other_read) != base
    # read_spec_version is unaffected by a raw_contract-only change (it never
    # sees the contract at all).
    assert contract.read_spec_version(_READ) == contract.read_spec_version(_READ)


# --- property test: version hashes insensitive to spec-file key order ------
#
# `read_spec_version`/`check_version` hash the PARSED form
# (`model_dump(mode="json")`), never the authored file's own key order --
# 005.1 §12.4's "version hashes insensitive to spec-file key order
# (parsed-form property, A-11)". Generates permutations of a fixed
# key/value set (top-level AND the nested `dialect`/per-column dicts) and
# asserts every permutation parses to the identical version hash.

_READ_DICT_STRATEGY = st.permutations(
    [
        ("compression", "gzip"),
        ("charset", "utf-8"),
        ("skip_leading_lines", 2),
        (
            "dialect",
            [
                ("format", "csv"),
                ("delimiter", ";"),
                ("quote", "'"),
                ("header", False),
                ("multiline", True),
            ],
        ),
    ]
).flatmap(
    lambda top: st.permutations(dict(top)["dialect"]).map(
        lambda dialect_items: {**dict(top), "dialect": dict(dialect_items)}
    )
)


@given(shuffled=_READ_DICT_STRATEGY)
@settings(max_examples=50)
def test_read_spec_version_insensitive_to_key_order(shuffled: dict[str, object]) -> None:
    canonical_order = ReadSpecModel(
        compression="gzip",
        charset="utf-8",
        skip_leading_lines=2,
        dialect=DialectModel(format="csv", delimiter=";", quote="'", header=False, multiline=True),
    )
    shuffled_parsed = ReadSpecModel(**shuffled)
    shuffled_version = contract.read_spec_version(shuffled_parsed)
    assert shuffled_version == contract.read_spec_version(canonical_order)


_CONTRACT_COLUMN_STRATEGY = st.permutations(
    [("name", "a"), ("type", "int"), ("min", "1"), ("max", "10")]
).map(dict)


@given(shuffled_column=_CONTRACT_COLUMN_STRATEGY)
@settings(max_examples=25)
def test_check_version_insensitive_to_column_key_order(shuffled_column: dict[str, object]) -> None:
    canonical = RawContractModel(columns=[ColumnSpec(name="a", type="int", min="1", max="10")])
    shuffled = RawContractModel(columns=[ColumnSpec(**shuffled_column)])
    assert contract.check_version(shuffled, _READ) == contract.check_version(canonical, _READ)
