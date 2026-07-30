"""Unit tests for `spine.core.merge` — LLD §6.7, [S-10].

`merge_spec` shapes `MergeSpec` from a `PipelineSpecModel` plus the fold
source's field names and ordering columns; EVERY identifier (target table's
dot-components, `key_cols`, `ordering_cols`, `update_cols`) is validated
before any SQL is assembled -- including `update_cols`, which come from the
*pipeline transform's output schema*, not from reviewed YAML: a hostile/
malformed df-derived column name is a defect, not something to escape
around.
"""

import dataclasses
import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from spine.core import merge
from spine.core.model import PipelineSpecModel

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _make_spec(domain_id_col: str = "domain_id") -> PipelineSpecModel:
    return PipelineSpecModel(
        pipeline="pipelines/commissions",
        transforms_module="pipelines.commissions.transforms",
        raw_table="lake.commissions__raw",
        quarantine_table="lake.commissions__quarantine",
        fact_table="lake.commissions__facts",
        state_table="lake.commissions__state",
        domain_id_col=domain_id_col,
        read={"dialect": {"format": "csv"}},
        raw_contract={"columns": [{"name": "id"}]},
    )


# --- merge_spec shaping --------------------------------------------------------


def test_merge_spec_shapes_key_ordering_update_cols() -> None:
    spec = _make_spec()
    result = merge.merge_spec(spec, ["domain_id", "amount", "event_time"], ["event_time"])
    assert result.target_table == "lake.commissions__state"
    assert result.key_cols == ("domain_id",)
    assert result.ordering_cols == ("event_time",)
    assert result.update_cols == ("amount", "event_time")


def test_merge_spec_update_cols_is_all_non_key_source_columns() -> None:
    spec = _make_spec()
    result = merge.merge_spec(spec, ["domain_id", "a", "b", "c"], [])
    assert result.update_cols == ("a", "b", "c")


def test_merge_spec_uses_custom_domain_id_col() -> None:
    spec = _make_spec(domain_id_col="policy_id")
    result = merge.merge_spec(spec, ["policy_id", "amount"], [])
    assert result.key_cols == ("policy_id",)
    assert result.update_cols == ("amount",)


def test_merge_spec_is_frozen() -> None:
    spec = _make_spec()
    result = merge.merge_spec(spec, ["domain_id", "amount"], [])
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.target_table = "x"  # type: ignore[misc]


# --- identifier validation: hostile df-derived column names [S-10] ----------


@pytest.mark.parametrize(
    "bad_col",
    [
        "amount total",  # space
        "amount.total",  # dot
        "amount`total",  # backtick
        "amount;DROP TABLE x",  # semicolon / injection attempt
        "am-ount",  # hyphen (not in the identifier grammar)
        "1amount",  # leading digit
        "",  # empty
        "amount\n",  # embedded newline
        "amount'total",  # quote
        "améount",  # unicode
    ],
)
def test_merge_spec_rejects_hostile_update_col_names(bad_col: str) -> None:
    spec = _make_spec()
    with pytest.raises(ValueError, match="identifier"):
        merge.merge_spec(spec, ["domain_id", bad_col], [])


@pytest.mark.parametrize("bad_col", ["ordering time", "order.time"])
def test_merge_spec_rejects_hostile_ordering_col_names(bad_col: str) -> None:
    spec = _make_spec()
    with pytest.raises(ValueError, match="identifier"):
        merge.merge_spec(spec, ["domain_id", "amount"], [bad_col])


def test_merge_spec_rejects_non_conforming_domain_id_col() -> None:
    spec = _make_spec(domain_id_col="domain id")
    with pytest.raises(ValueError, match="identifier"):
        merge.merge_spec(spec, ["domain id", "amount"], [])


@pytest.mark.parametrize("good_col", ["amount", "event_time", "_private", "Amount2", "a1_b2_C3"])
def test_merge_spec_accepts_conforming_column_names(good_col: str) -> None:
    spec = _make_spec()
    result = merge.merge_spec(spec, ["domain_id", good_col], [])
    assert result.update_cols == (good_col,)


# --- quote_identifier -----------------------------------------------------------


def test_quote_identifier_wraps_in_backticks() -> None:
    assert merge.quote_identifier("amount") == "`amount`"


def test_quote_identifier_doubles_embedded_backtick_defense_in_depth() -> None:
    # identifiers that pass `_check_identifier` never contain a backtick, so
    # this exercises the doubling logic directly as defense-in-depth, not a
    # value that could reach here through `merge_spec`.
    assert merge.quote_identifier("a`b") == "`a``b`"


# --- property test: any hostile-shaped column string is rejected before ----
# --- any SQL is assembled (no exception escapes past ValueError) -----------


@given(
    bad_col=st.text(
        alphabet=st.characters(blacklist_categories=("Cs",), whitelist_characters=" .`;'\n"),
        min_size=1,
        max_size=20,
    ).filter(lambda s: not _IDENTIFIER_RE.match(s))
)
@settings(max_examples=200)
def test_merge_spec_never_accepts_a_non_conforming_column_name(bad_col: str) -> None:
    spec = _make_spec()
    with pytest.raises(ValueError):
        merge.merge_spec(spec, ["domain_id", bad_col], [])
