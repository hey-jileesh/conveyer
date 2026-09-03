"""Unit tests for `spine.core.merge` — LLD 004.1 §6.7 [S-10]; LLD 007.1
§4.1 (`MergeSpec` v2 derivation) and §8.2 (`ordering_predicate`'s rendering
decision).

**v1 -> v2 (bead conveyer-6pg.17, B6).** `merge_spec` no longer takes a
`PipelineSpecModel` + caller-supplied source-schema/ordering-cols (v1's
shape, retired when 006.1 P-1's hard cut deleted the singular
`fact_table`/`state_table`/`domain_id_col`-as-source-of-truth fields it
read) -- it derives `MergeSpec` **purely from one `FactTypeModel`** (§4.1).
`MergeSpec`'s own four-field dataclass shape is unchanged."""

from __future__ import annotations

import dataclasses
import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from spine.core import merge
from spine.core.model import FactTypeModel

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _make_fact_type(
    *,
    domain_id_col: str = "domain_id",
    ordering: list[str] | None = None,
    extra_columns: list[dict[str, str]] | None = None,
) -> FactTypeModel:
    columns = [{"name": domain_id_col, "type": "string"}]
    columns.extend(extra_columns or [{"name": "amount", "type": "decimal(10,2)"}])
    return FactTypeModel(
        fact_table="lake.commissions__facts",
        state_table="lake.commissions__state",
        schema={
            "columns": columns,
            "domain_id_col": domain_id_col,
            "record_key": [domain_id_col],
            "ordering": ordering or [],
        },
    )


# --- merge_spec v2 derivation ------------------------------------------------


def test_merge_spec_shapes_key_ordering_update_cols() -> None:
    fact_type = _make_fact_type(
        ordering=["event_time"],
        extra_columns=[
            {"name": "amount", "type": "decimal(10,2)"},
            {"name": "event_time", "type": "timestamp"},
        ],
    )
    result = merge.merge_spec(fact_type)
    assert result.target_table == "lake.commissions__state"
    assert result.key_cols == ("domain_id",)
    assert result.ordering_cols == ("event_time", "source_ts", "content_hash")
    # update_cols = every §6.2 state column except key_cols: the seven
    # framework stamps (constant order) then the declared non-key columns
    # (contract order) -- "the winning fact carried whole".
    assert result.update_cols == (
        "batch_id",
        "delivery_id",
        "feed_id",
        "received_at",
        "source_ts",
        "content_hash",
        "record_key",
        "amount",
        "event_time",
    )


def test_merge_spec_ordering_cols_defaults_to_just_the_framework_suffix() -> None:
    # no `ordering:` declared -- §4.1: "may be empty: order = (source_ts,
    # content_hash)"
    fact_type = _make_fact_type()
    result = merge.merge_spec(fact_type)
    assert result.ordering_cols == ("source_ts", "content_hash")


def test_merge_spec_uses_custom_domain_id_col() -> None:
    fact_type = _make_fact_type(domain_id_col="policy_id")
    result = merge.merge_spec(fact_type)
    assert result.key_cols == ("policy_id",)
    assert "policy_id" not in result.update_cols
    assert "amount" in result.update_cols


def test_merge_spec_update_cols_excludes_only_the_key_never_other_declared_cols() -> None:
    fact_type = _make_fact_type(
        extra_columns=[
            {"name": "a", "type": "string"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "string"},
        ]
    )
    result = merge.merge_spec(fact_type)
    assert "a" in result.update_cols
    assert "b" in result.update_cols
    assert "c" in result.update_cols
    assert "domain_id" not in result.update_cols


def test_merge_spec_is_frozen() -> None:
    fact_type = _make_fact_type()
    result = merge.merge_spec(fact_type)
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.target_table = "x"  # type: ignore[misc]


def test_merge_spec_stamps_precede_declared_columns_in_update_cols() -> None:
    # §6.1: "Column order — initial creation only: stamps, then declared" --
    # `merge_spec` mirrors that order mechanically.
    fact_type = _make_fact_type()
    result = merge.merge_spec(fact_type)
    assert result.update_cols.index("content_hash") < result.update_cols.index("amount")


# --- MergeSpec.__post_init__: the ordering-suffix construction assertion ----


def test_merge_spec_construction_asserts_the_ordering_suffix() -> None:
    # §4.1: "Construction asserts the suffix; a MergeSpec without it is a
    # framework bug, not a reachable spec state" -- fires for ANY
    # MergeSpec construction, not just this module's own v2 derivation.
    with pytest.raises(AssertionError, match="source_ts.*content_hash"):
        merge.MergeSpec(
            target_table="lake.x",
            key_cols=("domain_id",),
            ordering_cols=("event_time",),
            update_cols=("amount",),
        )


def test_merge_spec_construction_accepts_the_minimal_two_element_suffix() -> None:
    spec = merge.MergeSpec(
        target_table="lake.x",
        key_cols=("domain_id",),
        ordering_cols=("source_ts", "content_hash"),
        update_cols=("amount",),
    )
    assert spec.ordering_cols == ("source_ts", "content_hash")


def test_merge_spec_construction_rejects_wrong_order_suffix() -> None:
    with pytest.raises(AssertionError):
        merge.MergeSpec(
            target_table="lake.x",
            key_cols=("domain_id",),
            ordering_cols=("content_hash", "source_ts"),  # swapped
            update_cols=(),
        )


# --- identifier validation: v2 still defends every identifier it selects ---
#
# Every name `merge_spec` selects (`domain_id_col`, declared `ordering:`
# columns, declared column names) already passed `FactColumnSpec.name`'s
# OWN `Field(pattern=COLUMN_NAME_RE)` check -- the IDENTICAL grammar as
# `merge.py`'s own `_IDENTIFIER_RE` -- before a `FactTypeModel` can even be
# constructed (a hostile column name is a `pydantic.ValidationError` at
# THAT boundary, never reaching `merge_spec`). `merge_spec`'s own
# `_check_identifier` calls are therefore belt-and-braces on the v2 path
# (never trusted-because-checked-elsewhere, [S-10] -- the same posture
# `quote_identifier`'s own docstring already names for its backtick
# doubling), not a path any `FactTypeModel`-shaped input can trip -- tested
# directly below, against the helper itself.


def test_check_identifier_rejects_a_hostile_name_directly() -> None:
    with pytest.raises(ValueError, match="identifier"):
        merge._check_identifier("amount total", "test")


# --- ordering_predicate: §8.2's field-wise rendering, over hand-built cases --


def test_ordering_predicate_two_element_minimum_case() -> None:
    fact_type = _make_fact_type()
    spec = merge.merge_spec(fact_type)  # ordering_cols == (source_ts, content_hash)
    predicate = merge.ordering_predicate(spec)
    expected = (
        "(((t.`source_ts` IS NULL AND s.`source_ts` IS NOT NULL) "
        "OR (s.`source_ts` IS NOT NULL AND t.`source_ts` IS NOT NULL AND "
        "s.`source_ts` > t.`source_ts`)) "
        "OR (s.`source_ts` <=> t.`source_ts` AND "
        "((t.`content_hash` IS NULL AND s.`content_hash` IS NOT NULL) "
        "OR (s.`content_hash` IS NOT NULL AND t.`content_hash` IS NOT NULL AND "
        "s.`content_hash` > t.`content_hash`))))"
    )
    assert predicate == expected


def test_ordering_predicate_three_element_case_references_every_column_both_sides() -> None:
    fact_type = _make_fact_type(
        ordering=["event_time"],
        extra_columns=[
            {"name": "amount", "type": "decimal(10,2)"},
            {"name": "event_time", "type": "timestamp"},
        ],
    )
    spec = merge.merge_spec(fact_type)
    predicate = merge.ordering_predicate(spec)
    for col in ("event_time", "source_ts", "content_hash"):
        assert f"s.`{col}`" in predicate
        assert f"t.`{col}`" in predicate
    # lexicographic nesting: the FIRST declared element's gt/tie terms
    # appear OUTERMOST (its `gt_1 OR (tie_1 AND ...)` shape).
    assert predicate.startswith("(((t.`event_time`")


def test_ordering_predicate_single_declared_column_leading_element_is_the_declared_one() -> None:
    fact_type = _make_fact_type(
        ordering=["policy_effective_date"],
        extra_columns=[
            {"name": "amount", "type": "decimal(10,2)"},
            {"name": "policy_effective_date", "type": "date"},
        ],
    )
    spec = merge.merge_spec(fact_type)
    predicate = merge.ordering_predicate(spec)
    assert predicate.startswith("(((t.`policy_effective_date`")


def test_ordering_predicate_every_term_is_three_valued_total() -> None:
    # §8.2: "every term rendered three-valued-total (never evaluating to
    # SQL NULL)" -- the generated text never contains a bare `s.col >
    # t.col` unguarded by an IS NOT NULL pair, and every tie uses SQL's
    # null-safe `<=>`, never bare `=`.
    fact_type = _make_fact_type()
    spec = merge.merge_spec(fact_type)
    predicate = merge.ordering_predicate(spec)
    assert " = " not in predicate  # only `<=>` for equality, never bare `=`
    assert "<=>" in predicate


# --- quote_identifier ---------------------------------------------------------


def test_quote_identifier_wraps_in_backticks() -> None:
    assert merge.quote_identifier("amount") == "`amount`"


def test_quote_identifier_doubles_embedded_backtick_defense_in_depth() -> None:
    assert merge.quote_identifier("a`b") == "`a``b`"


# --- property test: hostile-shaped identifiers never reach SQL assembly ----


@given(
    bad_col=st.text(
        alphabet=st.characters(blacklist_categories=("Cs",), whitelist_characters=" .`;'\n"),
        min_size=1,
        max_size=20,
    ).filter(lambda s: not _IDENTIFIER_RE.match(s))
)
@settings(max_examples=200)
def test_check_identifier_never_accepts_a_non_conforming_column_name(bad_col: str) -> None:
    with pytest.raises(ValueError):
        merge._check_identifier(bad_col, "test")
