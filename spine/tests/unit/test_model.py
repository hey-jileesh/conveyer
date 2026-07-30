"""Unit tests for `spine.core.model` — LLD §6.1, §6.2, §6.6, §7.5.

Covers: `DeliveryRegisteredV1`'s tolerant-reader `extra="allow"` and
`object_uris` bounds (I-22); both M0 delivery-registered fixtures parsing
(§12.6); `PipelineSpecModel`/`CoEffectDecl`'s `extra="forbid"` and table-
identifier grammar (§6.2, §6.7); `BatchStartedV1`/`BatchCompletedV1` shape
(§6.6); `LineageStamp` immutability (§7.5 [C-5]).
"""

import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError
from spine.core import model

_FIXTURES_DIR = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "fixtures"
    / "events"
    / "delivery-registered"
)

_VALID_SEED: dict[str, Any] = {
    "schema_version": 1,
    "feed_id": "carrier-x/commission-statements",
    "delivery_id": "a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
    "batch_id": "b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
    "delivery_key": "statement-2026-07.csv",
    "content_hash": "sha256:1b57d99bac736c030171a604e3f1e6d96e6ed860f1b1aca5e58506dbfa4ee7d6",
    "size_bytes": 4096,
    "object_uris": ["s3://bucket/statement.csv"],
    "received_at": datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
    "pipeline": "pipelines/commissions",
}


# --- §6.1 DeliveryRegisteredV1: tolerant reader ------------------------------


def test_tolerant_reader_ignores_unknown_fields() -> None:
    data = dict(_VALID_SEED)
    data["some_future_field"] = "unrecognized"
    parsed = model.DeliveryRegisteredV1.model_validate(data)
    assert parsed.model_extra == {"some_future_field": "unrecognized"}


# --- both M0 fixtures parse (§12.6) ------------------------------------------


def test_fixtures_dir_has_both_m0_fixtures() -> None:
    names = sorted(p.name for p in _FIXTURES_DIR.glob("*.json"))
    assert names == ["v1-minimal.json", "v1-multi-object.json"]


@pytest.mark.parametrize("fixture_path", sorted(_FIXTURES_DIR.glob("*.json")), ids=lambda p: p.name)
def test_seed_model_parses_m0_fixtures(fixture_path: Path) -> None:
    data = json.loads(fixture_path.read_text())
    parsed = model.DeliveryRegisteredV1.model_validate(data)
    assert parsed.schema_version == 1
    assert parsed.batch_id == data["batch_id"]
    assert parsed.pipeline == data["pipeline"]


# --- object_uris bounds (I-22) -----------------------------------------------


def test_object_uris_rejects_empty_list() -> None:
    with pytest.raises(ValidationError):
        model.DeliveryRegisteredV1(**{**_VALID_SEED, "object_uris": []})


def test_object_uris_accepts_256_entries() -> None:
    uris = [f"s3://bucket/{i}" for i in range(256)]
    parsed = model.DeliveryRegisteredV1(**{**_VALID_SEED, "object_uris": uris})
    assert len(parsed.object_uris) == 256


def test_object_uris_rejects_257_entries() -> None:
    uris = [f"s3://bucket/{i}" for i in range(257)]
    with pytest.raises(ValidationError):
        model.DeliveryRegisteredV1(**{**_VALID_SEED, "object_uris": uris})


def test_object_uris_accepts_1024_char_entry() -> None:
    uri = "s3://" + "a" * 1019  # exactly 1024 chars
    parsed = model.DeliveryRegisteredV1(**{**_VALID_SEED, "object_uris": [uri]})
    assert len(parsed.object_uris[0]) == 1024


def test_object_uris_rejects_1025_char_entry() -> None:
    uri = "s3://" + "a" * 1020  # exactly 1025 chars
    with pytest.raises(ValidationError):
        model.DeliveryRegisteredV1(**{**_VALID_SEED, "object_uris": [uri]})


# --- §6.2 PipelineSpecModel / CoEffectDecl: extra="forbid" ------------------

_VALID_SPEC: dict[str, Any] = {
    "pipeline": "pipelines/commissions",
    "transforms_module": "pipelines.commissions.transforms",
    "raw_table": "lake.commissions__raw",
    "quarantine_table": "lake.commissions__quarantine",
    "fact_table": "lake.commissions__facts",
    "state_table": "lake.commissions__state",
    "read": {"dialect": {"format": "csv"}},
    "raw_contract": {"columns": [{"name": "id"}]},
}


def test_pipeline_spec_model_accepts_valid_shape() -> None:
    spec = model.PipelineSpecModel(**_VALID_SPEC)
    assert spec.fold == "default-lww"
    assert spec.serialize is False
    assert spec.sla_minutes == 480
    assert spec.read.dialect.format == "csv"
    assert spec.raw_contract.columns[0].name == "id"


# --- 005.1 §3.4/A-12: `read`/`raw_contract` required, `required_columns`/`read`-dict deleted ---


def test_pipeline_spec_model_requires_read() -> None:
    data = {k: v for k, v in _VALID_SPEC.items() if k != "read"}
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(**data)


def test_pipeline_spec_model_requires_raw_contract() -> None:
    data = {k: v for k, v in _VALID_SPEC.items() if k != "raw_contract"}
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(**data)


def test_pipeline_spec_model_no_longer_accepts_required_columns() -> None:
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(**_VALID_SPEC, required_columns=["domain_id"])


# --- 005.1 §3.2's two cross-model validators (A-4, [R2-2]) ------------------


def test_pipeline_spec_model_rejects_header_false_with_required_column() -> None:
    with pytest.raises(ValidationError, match="A-4"):
        model.PipelineSpecModel(
            **{
                **_VALID_SPEC,
                "read": {"dialect": {"format": "csv", "header": False}},
                "raw_contract": {"columns": [{"name": "id", "required": True}]},
            }
        )


def test_pipeline_spec_model_rejects_nullable_false_without_required_under_header_true() -> None:
    with pytest.raises(ValidationError, match=r"\[R2-2\]"):
        model.PipelineSpecModel(
            **{
                **_VALID_SPEC,
                "read": {"dialect": {"format": "csv", "header": True}},
                "raw_contract": {"columns": [{"name": "id", "nullable": False}]},
            }
        )


def test_pipeline_spec_model_allows_nullable_false_vacuously_under_header_false() -> None:
    """[R2-2] is vacuous under `header: false` -- positional binding always
    provides the column, so `nullable: false` without `required: true`
    is not a trap there (unlike under `header: true`)."""
    spec = model.PipelineSpecModel(
        **{
            **_VALID_SPEC,
            "read": {"dialect": {"format": "csv", "header": False}},
            "raw_contract": {"columns": [{"name": "id", "nullable": False, "required": False}]},
        }
    )
    assert spec.raw_contract.columns[0].nullable is False


def test_pipeline_spec_model_allows_required_and_nullable_false_together() -> None:
    spec = model.PipelineSpecModel(
        **{
            **_VALID_SPEC,
            "read": {"dialect": {"format": "csv", "header": True}},
            "raw_contract": {"columns": [{"name": "id", "nullable": False, "required": True}]},
        }
    )
    assert spec.raw_contract.columns[0].required is True
    assert spec.raw_contract.columns[0].nullable is False


def test_pipeline_spec_model_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(**{**_VALID_SPEC, "unexpected_field": True})


def test_co_effect_decl_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        model.CoEffectDecl(table="lake.rate_cards", unexpected_field=True)


@pytest.mark.parametrize(
    "table",
    [
        "badtable",  # no dot at all
        "lake.bad-table",  # dash in the table component
        "lake.",  # empty table component
        ".table",  # empty db component
        "lake.bad table",  # space
    ],
)
def test_pipeline_spec_model_rejects_bad_table_identifiers(table: str) -> None:
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(**{**_VALID_SPEC, "raw_table": table})


def test_co_effect_decl_rejects_bad_table_identifier() -> None:
    with pytest.raises(ValidationError):
        model.CoEffectDecl(table="bad-table")


def test_pipeline_spec_model_co_effects_round_trip() -> None:
    spec = model.PipelineSpecModel(
        **_VALID_SPEC,
        co_effects={"rate_cards": model.CoEffectDecl(table="lake.rate_cards", own_state=True)},
    )
    assert spec.co_effects["rate_cards"].own_state is True


# --- §6.6 lifecycle events ---------------------------------------------------


def test_batch_started_v1_shape() -> None:
    started = model.BatchStartedV1(
        pipeline="pipelines/commissions",
        feed_id="carrier-x/commission-statements",
        batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
        delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
        raw_count=10,
        land_snapshot_id=123,
        started_at=datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
    )
    assert started.schema_version == 1


def test_batch_started_v1_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        model.BatchStartedV1(
            pipeline="pipelines/commissions",
            feed_id="carrier-x/commission-statements",
            batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
            delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
            raw_count=10,
            land_snapshot_id=123,
            started_at=datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
            unexpected_field=True,
        )


def test_batch_completed_v1_naming_split_h1() -> None:
    """`fact_count` (batch truth) is a distinct field from the ledger's
    `facts_appended` (attempt truth) -- this model carries only the former,
    named `fact_count`, per [H-1]."""
    completed = model.BatchCompletedV1(
        pipeline="pipelines/commissions",
        feed_id="carrier-x/commission-statements",
        batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
        delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
        raw_count=10,
        pre_quarantined=0,
        post_quarantined=0,
        fact_count=10,
        fact_snapshot_id=456,
        state_snapshot_id=789,
        completed_at=datetime(2026, 7, 25, 9, 5, 0, tzinfo=UTC),
    )
    assert completed.fact_count == 10
    assert not hasattr(completed, "facts_appended")


# --- §7.5 [C-5] LineageStamp: frozen dataclass value ------------------------


def test_lineage_stamp_is_frozen() -> None:
    stamp = model.LineageStamp(
        batch_id="b2c3d4e5-f6a7-5b2c-9d3e-4f5a6b7c8d9e",
        delivery_id="a1b2c3d4-e5f6-4a1b-8c2d-3e4f5a6b7c8d",
        feed_id="carrier-x/commission-statements",
        received_at=datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
    )
    assert stamp.source_uri is None
    with pytest.raises(dataclasses.FrozenInstanceError):
        stamp.batch_id = "different"  # type: ignore[misc]


# --- 005.1 §3.1 DialectModel / ReadSpecModel ---------------------------------


def test_dialect_model_defaults() -> None:
    dialect = model.DialectModel(format="csv")
    assert dialect.delimiter == ","
    assert dialect.quote == '"'
    assert dialect.header is True
    assert dialect.multiline is False


def test_dialect_model_accepts_distinct_single_char_delims() -> None:
    dialect = model.DialectModel(format="csv", delimiter=";", quote="'")
    assert dialect.delimiter == ";"
    assert dialect.quote == "'"


def test_dialect_model_rejects_format_other_than_csv() -> None:
    with pytest.raises(ValidationError):
        model.DialectModel(format="jsonl")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("delimiter", "quote"),
    [
        (",", ","),  # delimiter == quote
        (";", ";"),
    ],
)
def test_dialect_model_rejects_delimiter_equal_to_quote(delimiter: str, quote: str) -> None:
    with pytest.raises(ValidationError, match="delimiter and quote must differ"):
        model.DialectModel(format="csv", delimiter=delimiter, quote=quote)


@pytest.mark.parametrize(
    "bad_char",
    [
        "",  # empty
        "::",  # 2 chars
        "\t",  # tab: ascii, not printable
        "\n",  # newline: ascii, not printable
        "\x7f",  # DEL: ascii, not printable
        "é",  # printable, not ascii
        "😀",  # printable, not ascii
    ],
)
def test_dialect_model_rejects_non_single_ascii_printable_delimiter(bad_char: str) -> None:
    with pytest.raises(ValidationError):
        model.DialectModel(format="csv", delimiter=bad_char)


@pytest.mark.parametrize(
    "bad_char",
    ["", "::", "\t", "\x7f", "é"],
)
def test_dialect_model_rejects_non_single_ascii_printable_quote(bad_char: str) -> None:
    with pytest.raises(ValidationError):
        model.DialectModel(format="csv", quote=bad_char)


@pytest.mark.parametrize("printable", [" ", ",", "|", ";", "'", "\\"])
def test_dialect_model_accepts_every_ascii_printable_delimiter(printable: str) -> None:
    dialect = model.DialectModel(format="csv", delimiter=printable, quote='"')
    assert dialect.delimiter == printable


def test_read_spec_model_defaults() -> None:
    spec = model.ReadSpecModel(dialect=model.DialectModel(format="csv"))
    assert spec.compression == "none"
    assert spec.charset == "utf-8"
    assert spec.skip_leading_lines == 0


@pytest.mark.parametrize("compression", ["none", "gzip"])
def test_read_spec_model_accepts_implemented_compression(compression: str) -> None:
    spec = model.ReadSpecModel(dialect=model.DialectModel(format="csv"), compression=compression)
    assert spec.compression == compression


def test_read_spec_model_rejects_zstd_with_a10_message_grammar() -> None:
    """zstd is a reserved ladder value (A-2): the Literal type admits it so
    this validator can raise A-10's `admission-defect/<code>: <machine
    detail>` message grammar, naming the gap, rather than a bare pydantic
    literal-violation message standing in for it."""
    with pytest.raises(ValidationError, match="admission-defect/reserved-ladder-value"):
        model.ReadSpecModel(dialect=model.DialectModel(format="csv"), compression="zstd")


def test_read_spec_model_rejects_non_utf8_charset() -> None:
    with pytest.raises(ValidationError):
        model.ReadSpecModel(dialect=model.DialectModel(format="csv"), charset="latin-1")  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [0, 4096])
def test_read_spec_model_accepts_skip_leading_lines_bounds(value: int) -> None:
    spec = model.ReadSpecModel(dialect=model.DialectModel(format="csv"), skip_leading_lines=value)
    assert spec.skip_leading_lines == value


@pytest.mark.parametrize("value", [-1, 4097])
def test_read_spec_model_rejects_skip_leading_lines_out_of_bounds(value: int) -> None:
    with pytest.raises(ValidationError):
        model.ReadSpecModel(dialect=model.DialectModel(format="csv"), skip_leading_lines=value)


def test_read_spec_model_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        model.ReadSpecModel(dialect=model.DialectModel(format="csv"), bogus=True)  # type: ignore[call-arg]


# --- 005.1 §3.2 ColumnSpec: type-aware validators ----------------------------


def test_column_spec_defaults() -> None:
    col = model.ColumnSpec(name="amount")
    assert col.type == "string"
    assert col.required is False
    assert col.nullable is True
    assert col.allowed_values is None
    assert col.pattern is None
    assert col.min is None
    assert col.max is None


@pytest.mark.parametrize(
    "type_str",
    ["string", "int", "long", "bool", "decimal(5,2)", "decimal(38,0)", "date(yyyy-MM-dd)"],
)
def test_column_spec_accepts_every_kind(type_str: str) -> None:
    assert model.ColumnSpec(name="c", type=type_str).type == type_str


# --- decimal precision/scale [DC-10] -----------------------------------------


@pytest.mark.parametrize("type_str", ["decimal(1,0)", "decimal(38,0)", "decimal(38,38)"])
def test_column_spec_accepts_decimal_within_dc10_bounds(type_str: str) -> None:
    assert model.ColumnSpec(name="c", type=type_str).type == type_str


@pytest.mark.parametrize(
    "type_str",
    [
        "decimal(39,0)",  # precision > 38
        "decimal(99,2)",  # precision > 38 (shape-legal, semantically rejected)
        "decimal(2,3)",  # scale > precision
    ],
)
def test_column_spec_rejects_decimal_violating_dc10_bounds(type_str: str) -> None:
    with pytest.raises(ValidationError):
        model.ColumnSpec(name="c", type=type_str)


# --- date/timestamp fmt: non-empty, Spark datetime pattern alphabet ---------


@pytest.mark.parametrize(
    "type_str",
    [
        "date(yyyy-MM-dd)",
        "date(MM/dd/yyyy)",
        "timestamp(yyyy-MM-dd HH:mm:ss)",
        "timestamp(yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z')",  # the exact §7.3 row_hash rendering fmt
    ],
)
def test_column_spec_accepts_conforming_datetime_fmt(type_str: str) -> None:
    assert model.ColumnSpec(name="c", type=type_str).type == type_str


@pytest.mark.parametrize(
    "type_str",
    [
        "date(   )",  # whitespace-only -> empty after strip
        "date(JUNK)",  # letters outside the Spark alphabet (J, U, N, K)
        "date(%Y-%b-%d)",  # python-strftime style: 'b' (month-abbrev) isn't a Spark pattern letter
        "timestamp(yyyy-MM-dd'unterminated)",  # unbalanced quote
    ],
)
def test_column_spec_rejects_nonconforming_datetime_fmt(type_str: str) -> None:
    with pytest.raises(ValidationError):
        model.ColumnSpec(name="c", type=type_str)


# --- pattern: best-effort re.compile typo check only ([DC-4]) --------------


@pytest.mark.parametrize("pattern", [r"[a-z]+", r"\d{3}-\d{4}", r"^ABC\d+$", ""])
def test_column_spec_accepts_compilable_pattern(pattern: str) -> None:
    assert model.ColumnSpec(name="c", pattern=pattern).pattern == pattern


def test_column_spec_accepts_none_pattern() -> None:
    assert model.ColumnSpec(name="c").pattern is None


@pytest.mark.parametrize("pattern", [r"[a-z", r"(unclosed", r"*bad"])
def test_column_spec_rejects_uncompilable_pattern(pattern: str) -> None:
    with pytest.raises(ValidationError):
        model.ColumnSpec(name="c", pattern=pattern)


# --- allowed_values: non-empty when present, each <= 1024 chars ------------


def test_column_spec_accepts_allowed_values_list() -> None:
    col = model.ColumnSpec(name="c", allowed_values=["a", "b", "c"])
    assert col.allowed_values == ["a", "b", "c"]


def test_column_spec_rejects_empty_allowed_values_list() -> None:
    with pytest.raises(ValidationError):
        model.ColumnSpec(name="c", allowed_values=[])


def test_column_spec_accepts_allowed_values_entry_exactly_1024_chars() -> None:
    col = model.ColumnSpec(name="c", allowed_values=["x" * 1024])
    assert len(col.allowed_values[0]) == 1024  # type: ignore[index]


def test_column_spec_rejects_allowed_values_entry_over_1024_chars() -> None:
    with pytest.raises(ValidationError):
        model.ColumnSpec(name="c", allowed_values=["x" * 1025])


# --- min/max: type gate, plain-literal parse, ordering ([R2-5b]) -----------


def test_column_spec_rejects_min_max_on_string_column() -> None:
    with pytest.raises(ValidationError):
        model.ColumnSpec(name="c", type="string", min="1")


def test_column_spec_rejects_min_max_on_bool_column() -> None:
    with pytest.raises(ValidationError):
        model.ColumnSpec(name="c", type="bool", max="true")


@pytest.mark.parametrize("type_str", ["int", "long", "decimal(5,2)", "date(yyyy-MM-dd)"])
def test_column_spec_accepts_min_only(type_str: str) -> None:
    value = (
        "1"
        if type_str in ("int", "long")
        else ("1.20" if type_str.startswith("decimal") else "2024-01-01")
    )
    col = model.ColumnSpec(name="c", type=type_str, min=value)
    assert col.min == value
    assert col.max is None


def test_column_spec_accepts_int_min_max_ordered() -> None:
    col = model.ColumnSpec(name="c", type="int", min="1", max="10")
    assert (col.min, col.max) == ("1", "10")


def test_column_spec_rejects_int_min_greater_than_max() -> None:
    with pytest.raises(ValidationError, match="min must be <= max"):
        model.ColumnSpec(name="c", type="int", min="10", max="1")


def test_column_spec_accepts_int_min_equal_max() -> None:
    col = model.ColumnSpec(name="c", type="int", min="5", max="5")
    assert (col.min, col.max) == ("5", "5")


@pytest.mark.parametrize("bad_literal", ["1.5", "abc", "1_000", ""])
def test_column_spec_rejects_non_plain_int_literal_min(bad_literal: str) -> None:
    with pytest.raises(ValidationError):
        model.ColumnSpec(name="c", type="int", min=bad_literal)


def test_column_spec_accepts_decimal_min_max_ordered() -> None:
    col = model.ColumnSpec(name="c", type="decimal(5,2)", min="1.20", max="3.50")
    assert (col.min, col.max) == ("1.20", "3.50")


def test_column_spec_rejects_decimal_min_greater_than_max() -> None:
    with pytest.raises(ValidationError, match="min must be <= max"):
        model.ColumnSpec(name="c", type="decimal(5,2)", min="3.50", max="1.20")


@pytest.mark.parametrize("bad_literal", ["abc", "1_000.5", "NaN", "Infinity", "-Infinity", ""])
def test_column_spec_rejects_non_plain_decimal_literal_min(bad_literal: str) -> None:
    """`NaN`/`Infinity` are Python `Decimal`-parseable but not plain numeric
    literals -- and a NaN min/max pair would silently pass a naive `min <=
    max` comparison (NaN compares unequal to everything)."""
    with pytest.raises(ValidationError):
        model.ColumnSpec(name="c", type="decimal(5,2)", min=bad_literal)


def test_column_spec_accepts_temporal_min_max_structural_only() -> None:
    """Temporal min/max gets only a non-empty-string structural check here
    -- no ordering comparison is attempted (deferred to compile, [R2-5b]:
    see `core/model.py`'s module docstring for why)."""
    col = model.ColumnSpec(name="c", type="date(yyyy-MM-dd)", min="9999-12-31", max="0001-01-01")
    assert (col.min, col.max) == ("9999-12-31", "0001-01-01")  # NOT rejected despite min > max


def test_column_spec_rejects_empty_temporal_min() -> None:
    with pytest.raises(ValidationError):
        model.ColumnSpec(name="c", type="date(yyyy-MM-dd)", min="")


# --- 005.1 §3.2 RawContractModel: column-set rules --------------------------


def test_raw_contract_model_accepts_minimal_valid_contract() -> None:
    raw_contract = model.RawContractModel(columns=[model.ColumnSpec(name="amount")])
    assert len(raw_contract.columns) == 1
    assert raw_contract.forbid_replacement_chars is True


def test_raw_contract_model_rejects_empty_columns() -> None:
    with pytest.raises(ValidationError):
        model.RawContractModel(columns=[])


def test_raw_contract_model_accepts_1024_columns() -> None:
    columns = [model.ColumnSpec(name=f"c{i}") for i in range(1024)]
    raw_contract = model.RawContractModel(columns=columns)
    assert len(raw_contract.columns) == 1024


def test_raw_contract_model_rejects_1025_columns() -> None:
    columns = [model.ColumnSpec(name=f"c{i}") for i in range(1025)]
    with pytest.raises(ValidationError):
        model.RawContractModel(columns=columns)


def test_raw_contract_model_rejects_duplicate_column_names() -> None:
    with pytest.raises(ValidationError, match="column names must be unique"):
        model.RawContractModel(
            columns=[model.ColumnSpec(name="amount"), model.ColumnSpec(name="amount")]
        )


@pytest.mark.parametrize("framework_name", sorted(model.FRAMEWORK_RAW_COLUMNS))
def test_raw_contract_model_rejects_each_framework_column_name(framework_name: str) -> None:
    with pytest.raises(ValidationError, match="framework raw columns"):
        model.RawContractModel(columns=[model.ColumnSpec(name=framework_name)])


@pytest.mark.parametrize("reserved_name", ["_conveyer_x", "_conveyer_row_hash", "_conveyer_"])
def test_raw_contract_model_rejects_conveyer_prefixed_names(reserved_name: str) -> None:
    with pytest.raises(ValidationError, match="reserved prefix"):
        model.RawContractModel(columns=[model.ColumnSpec(name=reserved_name)])


def test_raw_contract_model_accepts_forbid_replacement_chars_false() -> None:
    raw_contract = model.RawContractModel(
        columns=[model.ColumnSpec(name="amount")], forbid_replacement_chars=False
    )
    assert raw_contract.forbid_replacement_chars is False


def test_raw_contract_model_rejects_extra_field() -> None:
    with pytest.raises(ValidationError):
        model.RawContractModel(columns=[model.ColumnSpec(name="amount")], bogus=True)  # type: ignore[call-arg]


# --- §12.4 property test: [DC-6] framework/`_conveyer_` name rejection -----


@given(name=st.sampled_from(sorted(model.FRAMEWORK_RAW_COLUMNS)))
@settings(max_examples=len(model.FRAMEWORK_RAW_COLUMNS))
def test_property_every_framework_column_name_rejected(name: str) -> None:
    with pytest.raises(ValidationError):
        model.RawContractModel(columns=[model.ColumnSpec(name=name)])


@given(
    suffix=st.text(
        alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_",
        max_size=20,
    )
)
@settings(max_examples=100)
def test_property_every_conveyer_prefixed_name_rejected(suffix: str) -> None:
    # `_conveyer_` + any alnum/underscore suffix (incl. empty) is always
    # COLUMN_NAME_RE-conforming (starts with `_`, rest is `[A-Za-z0-9_]*`),
    # so this generates only names that would otherwise be legal.
    name = f"_conveyer_{suffix}"
    with pytest.raises(ValidationError):
        model.RawContractModel(columns=[model.ColumnSpec(name=name)])
