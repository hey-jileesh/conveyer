"""Unit + property tests for `spine.core.check_grammar` — LLD 006.1 §6
(P-2's gate 1; P-9 rule 1); §13.1 **G-07** (the gatekeeper accept/reject
corpus); §13.2 (the gatekeeper/family property suite, this module's slice).

**G-07's obligation, verbatim (§13.1):** every allowlisted construct
accepted with its expected node set asserted per construct (the parse-shape
pinning that DEFINES the allowed set [EM-2]); `ValidatedExpr.authored_text
== input` asserted on every accept row [EM-6][AE-2]; both typed-literal
spellings accepted (`DATE '…'`, `date('…')`) [EM-1]; named non-members
rejected (`rand`, `current_timestamp`, window, subquery, lambda/HOF,
`CAST`/`::`/`int(x)`/`TryCast` in every shape [EM-1], suffix + keyword
typed literals `1.5D`/`1.5F`/`1.5BD`/`123L`/`DOUBLE '…'` [AE-8], `INTERVAL`,
`*`, double + scientific literals, `NULL` inside an `IN` list [EM-12],
`LIKE … ESCAPE` [AE-9], cross-family operand mixes [AE-1] — per function
and per argument position, the [DC-3] signature corpus obligation (§6.5
rule 1), incl. the named divergence points: `concat` argument families,
`replace`/`substring` result families in enclosing comparisons,
`greatest`/`least` homogeneity, `nullif` polymorphism — aggregate outside
aggregate position, unknown column).

Every accept/reject claim in this file was discovered/pinned empirically in
the REPL kernel BEFORE `check_grammar.py`'s handlers were written (the
`repl-driven-python` loop) — this file is the durable record of that
session, not a re-derivation from prose. A schema fixed here (`_SCHEMA`) is
shared across every parametrized case."""

from __future__ import annotations

import pytest
import sqlglot
from hypothesis import given, settings
from hypothesis import strategies as st
from spine.core import check_grammar as cg
from sqlglot import exp

_SCHEMA: dict[str, cg.Family | None] = {
    "amount": "numeric",
    "qty": "numeric",
    "name": "string",
    "flag": "boolean",
    "seen_at": "temporal",
}


def _node_kinds(text: str) -> frozenset[type]:
    """[EM-2]/A006-3: the exact set of node TYPES anywhere in the parse tree
    (deduplicated, not a multiset) -- pinning the SHAPE an admitted
    construct parses into, not merely its accept verdict, so a sqlglot bump
    that re-shapes an admitted construct (even between two already-admitted
    node kinds) fails this corpus at the shape."""
    tree = sqlglot.parse_one(text, dialect="spark")
    return frozenset(type(node) for node in tree.walk())


def _accept(
    text: str, position: cg.Position = "scalar", nodes: frozenset[type] | None = None
) -> cg.ValidatedExpr:
    result = cg.validate_expression(text, position, _SCHEMA)
    assert isinstance(result, cg.ValidatedExpr), f"expected accept, got {result!r}"
    assert result.authored_text == text  # [EM-6][AE-2]: executed-text identity
    if nodes is not None:
        actual = _node_kinds(text)
        assert actual == nodes, (
            f"{text!r}: node set drifted -- expected "
            f"{sorted(c.__name__ for c in nodes)}, got {sorted(c.__name__ for c in actual)}"
        )
    return result


def _reject(text: str, position: cg.Position = "scalar") -> cg.GrammarDefect:
    result = cg.validate_expression(text, position, _SCHEMA)
    assert isinstance(result, cg.GrammarDefect), f"expected reject, got {result!r}"
    return result


# --- [EM-2] parse-shape pinning: the allowed set is defined against the
# PINNED PARSER'S actual node kinds, asserted directly (not inferred) -------


def test_em2_date_literal_and_function_call_both_parse_as_cast() -> None:
    for text in ("DATE '2026-01-01'", "date('2026-01-01')"):
        assert type(sqlglot.parse_one(text, dialect="spark")) is exp.Cast


def test_em2_timestamp_literal_and_function_call_both_parse_as_cast() -> None:
    for text in ("TIMESTAMP '2026-01-01 00:00:00'", "timestamp('2026-01-01 00:00:00')"):
        assert type(sqlglot.parse_one(text, dialect="spark")) is exp.Cast


def test_em2_cast_syntax_and_double_colon_parse_as_trycast_a_cast_subclass() -> None:
    for text in ("CAST(x AS int)", "x::int"):
        node = sqlglot.parse_one(text, dialect="spark")
        assert type(node) is exp.TryCast
        assert isinstance(
            node, exp.Cast
        )  # TryCast IS a Cast subclass -- exact-type dispatch required


def test_em2_constructor_cast_parses_as_cast_same_kind_as_typed_literal() -> None:
    assert type(sqlglot.parse_one("int(x)", dialect="spark")) is exp.Cast


def test_em2_nvl_normalizes_to_coalesce() -> None:
    assert type(sqlglot.parse_one("nvl(a, b)", dialect="spark")) is exp.Coalesce


def test_em2_ltrim_rtrim_normalize_to_trim() -> None:
    for text in ("ltrim(x)", "rtrim(x)", "trim(x)"):
        assert type(sqlglot.parse_one(text, dialect="spark")) is exp.Trim


def test_em2_year_injects_tsordstodate_wrapper() -> None:
    node = sqlglot.parse_one("year(ts)", dialect="spark")
    assert type(node) is exp.Year
    assert type(node.this) is exp.TsOrDsToDate


def test_em2_date_sub_synthesizes_mul_neg() -> None:
    node = sqlglot.parse_one("date_sub(a, 5)", dialect="spark")
    assert type(node) is exp.TsOrDsAdd
    assert type(node.expression) is exp.Mul
    assert type(node.expression.expression) is exp.Neg


def test_em1_scientific_notation_literal_shares_node_kind_with_plain_decimal() -> None:
    for text in ("1.245E2", "1.245"):
        assert type(sqlglot.parse_one(text, dialect="spark")) is exp.Literal


# --- accept corpus: every §6.1/§6.2 member, node set + authored_text -------


def test_accept_column_and_comparisons() -> None:
    for text, op in [
        ("amount > 5", exp.GT),
        ("amount >= 5", exp.GTE),
        ("amount < 5", exp.LT),
        ("amount <= 5", exp.LTE),
        ("amount != 5", exp.NEQ),
        ("amount = 5", exp.EQ),
    ]:
        result = _accept(text, nodes=frozenset({exp.Column, exp.Identifier, exp.Literal, op}))
        assert result.family == "boolean"
        assert result.referenced_columns == frozenset({"amount"})


def test_accept_null_safe_eq() -> None:
    nodes = frozenset({exp.Column, exp.Identifier, exp.NullSafeEQ})
    assert _accept("amount <=> qty", nodes=nodes).family == "boolean"


def test_accept_boolean_connectives() -> None:
    assert (
        _accept(
            "flag AND amount > 0",
            nodes=frozenset({exp.And, exp.Column, exp.GT, exp.Identifier, exp.Literal}),
        ).family
        == "boolean"
    )
    assert (
        _accept(
            "flag OR amount > 0",
            nodes=frozenset({exp.Column, exp.GT, exp.Identifier, exp.Literal, exp.Or}),
        ).family
        == "boolean"
    )
    assert (
        _accept("NOT flag", nodes=frozenset({exp.Column, exp.Identifier, exp.Not})).family
        == "boolean"
    )


def test_accept_null_tests() -> None:
    assert (
        _accept(
            "amount IS NULL", nodes=frozenset({exp.Column, exp.Identifier, exp.Is, exp.Null})
        ).family
        == "boolean"
    )
    assert (
        _accept(
            "amount IS NOT NULL",
            nodes=frozenset({exp.Column, exp.Identifier, exp.Is, exp.Not, exp.Null}),
        ).family
        == "boolean"
    )


def test_accept_in_and_between() -> None:
    assert (
        _accept(
            "amount IN (1, 2, 3)",
            nodes=frozenset({exp.Column, exp.Identifier, exp.In, exp.Literal}),
        ).family
        == "boolean"
    )
    assert (
        _accept(
            "amount BETWEEN 1 AND 10",
            nodes=frozenset({exp.Between, exp.Column, exp.Identifier, exp.Literal}),
        ).family
        == "boolean"
    )
    assert (
        _accept(
            "amount NOT BETWEEN 1 AND 10",
            nodes=frozenset({exp.Between, exp.Column, exp.Identifier, exp.Literal, exp.Not}),
        ).family
        == "boolean"
    )
    assert (
        _accept(
            "amount NOT IN (1, 2, 3)",
            nodes=frozenset({exp.Column, exp.Identifier, exp.In, exp.Literal, exp.Not}),
        ).family
        == "boolean"
    )


def test_accept_like() -> None:
    nodes = frozenset({exp.Column, exp.Identifier, exp.Like, exp.Literal})
    assert _accept("name LIKE 'x%'", nodes=nodes).family == "boolean"


def test_accept_typed_literals_both_spellings() -> None:
    # [EM-1]: both typed-literal spellings accepted -- same node set for
    # every spelling of both DATE and TIMESTAMP.
    expected = frozenset({exp.Cast, exp.Column, exp.DataType, exp.EQ, exp.Identifier, exp.Literal})
    for text in (
        "seen_at = DATE '2026-01-01'",
        "seen_at = date('2026-01-01')",
        "seen_at = TIMESTAMP '2026-01-01 00:00:00'",
        "seen_at = timestamp('2026-01-01 00:00:00')",
    ):
        assert _accept(text, nodes=expected).family == "boolean"


def test_accept_arithmetic() -> None:
    for text, op, cmp_kind in [
        ("amount + qty > 0", exp.Add, exp.GT),
        ("amount - qty > 0", exp.Sub, exp.GT),
        ("amount * qty > 0", exp.Mul, exp.GT),
        ("amount / qty > 0", exp.Div, exp.GT),
        ("amount % qty = 0", exp.Mod, exp.EQ),
    ]:
        nodes = frozenset({exp.Column, exp.Identifier, exp.Literal, op, cmp_kind})
        assert _accept(text, nodes=nodes).family == "boolean"


def test_accept_case_coalesce_nullif() -> None:
    assert (
        _accept(
            "CASE WHEN amount > 0 THEN 1 ELSE 0 END = 1",
            nodes=frozenset(
                {exp.Case, exp.Column, exp.EQ, exp.GT, exp.Identifier, exp.If, exp.Literal}
            ),
        ).family
        == "boolean"
    )
    coalesce_nodes = frozenset({exp.Coalesce, exp.Column, exp.EQ, exp.Identifier, exp.Literal})
    assert _accept("coalesce(name, 'x') = 'x'", nodes=coalesce_nodes).family == "boolean"
    assert _accept("nvl(name, 'x') = 'x'", nodes=coalesce_nodes).family == "boolean"
    assert _accept("ifnull(name, 'x') = 'x'", nodes=coalesce_nodes).family == "boolean"
    assert (
        _accept(
            "nullif(amount, 0) IS NULL",
            nodes=frozenset(
                {exp.Column, exp.Identifier, exp.Is, exp.Literal, exp.Null, exp.Nullif}
            ),
        ).family
        == "boolean"
    )


def test_accept_numeric_functions() -> None:
    for text, kind in [
        ("abs(amount) > 0", exp.Abs),
        ("round(amount) > 0", exp.Round),
        ("round(amount, 2) > 0", exp.Round),
        ("floor(amount) > 0", exp.Floor),
        ("ceil(amount) > 0", exp.Ceil),
        ("greatest(amount, qty) > 0", exp.Greatest),
        ("least(amount, qty) > 0", exp.Least),
    ]:
        nodes = frozenset({exp.Column, exp.GT, exp.Identifier, exp.Literal, kind})
        assert _accept(text, nodes=nodes).family == "boolean"


def test_accept_string_functions() -> None:
    for text, nodes in [
        (
            "length(name) > 0",
            frozenset({exp.Column, exp.GT, exp.Identifier, exp.Length, exp.Literal}),
        ),
        ("trim(name) = name", frozenset({exp.Column, exp.EQ, exp.Identifier, exp.Trim})),
        ("ltrim(name) = name", frozenset({exp.Column, exp.EQ, exp.Identifier, exp.Trim})),
        ("rtrim(name) = name", frozenset({exp.Column, exp.EQ, exp.Identifier, exp.Trim})),
        ("upper(name) = name", frozenset({exp.Column, exp.EQ, exp.Identifier, exp.Upper})),
        ("lower(name) = name", frozenset({exp.Column, exp.EQ, exp.Identifier, exp.Lower})),
        (
            "substring(name, 1, 2) = name",
            frozenset({exp.Column, exp.EQ, exp.Identifier, exp.Literal, exp.Substring}),
        ),
        (
            "substring(name, 1) = name",
            frozenset({exp.Column, exp.EQ, exp.Identifier, exp.Literal, exp.Substring}),
        ),
        ("concat(name, name) = name", frozenset({exp.Column, exp.Concat, exp.EQ, exp.Identifier})),
        (
            "replace(name, 'a', 'b') = name",
            frozenset({exp.Column, exp.EQ, exp.Identifier, exp.Literal, exp.Replace}),
        ),
    ]:
        assert _accept(text, nodes=nodes).family == "boolean"


def test_accept_temporal_functions() -> None:
    for text, nodes in [
        (
            "year(seen_at) = 2026",
            frozenset(
                {exp.Column, exp.EQ, exp.Identifier, exp.Literal, exp.TsOrDsToDate, exp.Year}
            ),
        ),
        (
            "month(seen_at) = 1",
            frozenset(
                {exp.Column, exp.EQ, exp.Identifier, exp.Literal, exp.Month, exp.TsOrDsToDate}
            ),
        ),
        (
            "day(seen_at) = 1",
            frozenset({exp.Column, exp.Day, exp.EQ, exp.Identifier, exp.Literal, exp.TsOrDsToDate}),
        ),
        (
            "datediff(seen_at, seen_at) = 0",
            frozenset(
                {exp.Column, exp.DateDiff, exp.EQ, exp.Identifier, exp.Literal, exp.TsOrDsToDate}
            ),
        ),
        (
            "date_add(seen_at, 5) = seen_at",
            frozenset({exp.Column, exp.EQ, exp.Identifier, exp.Literal, exp.TsOrDsAdd, exp.Var}),
        ),
        (
            "date_sub(seen_at, 5) = seen_at",
            frozenset(
                {
                    exp.Column,
                    exp.EQ,
                    exp.Identifier,
                    exp.Literal,
                    exp.Mul,
                    exp.Neg,
                    exp.TsOrDsAdd,
                    exp.Var,
                }
            ),
        ),
    ]:
        assert _accept(text, nodes=nodes).family == "boolean"


def test_accept_referenced_columns_union_across_a_multi_column_expression() -> None:
    result = _accept(
        "amount > 0 AND name = 'x' AND flag",
        nodes=frozenset({exp.And, exp.Column, exp.EQ, exp.GT, exp.Identifier, exp.Literal}),
    )
    assert result.referenced_columns == frozenset({"amount", "name", "flag"})


def test_accept_parentheses() -> None:
    nodes = frozenset({exp.And, exp.Column, exp.GT, exp.Identifier, exp.Literal, exp.Paren})
    assert _accept("(amount > 0) AND flag", nodes=nodes).family == "boolean"


def test_accept_aggregate_position_members() -> None:
    for text, nodes in [
        ("sum(amount) > 0", frozenset({exp.Column, exp.GT, exp.Identifier, exp.Literal, exp.Sum})),
        ("count(1) > 0", frozenset({exp.Count, exp.GT, exp.Literal})),
        (
            "count(amount) > 0",
            frozenset({exp.Column, exp.Count, exp.GT, exp.Identifier, exp.Literal}),
        ),
        ("avg(amount) > 0", frozenset({exp.Avg, exp.Column, exp.GT, exp.Identifier, exp.Literal})),
        (
            "min(seen_at) IS NOT NULL",
            frozenset({exp.Column, exp.Identifier, exp.Is, exp.Min, exp.Not, exp.Null}),
        ),
        (
            "max(seen_at) IS NOT NULL",
            frozenset({exp.Column, exp.Identifier, exp.Is, exp.Max, exp.Not, exp.Null}),
        ),
    ]:
        assert _accept(text, "aggregate", nodes=nodes).family == "boolean"


def test_accept_aggregate_combined_with_scalar_arithmetic() -> None:
    nodes = frozenset({exp.Add, exp.Column, exp.GT, exp.Identifier, exp.Literal, exp.Sum})
    assert _accept("sum(amount) + sum(qty) > 0", "aggregate", nodes=nodes).family == "boolean"
    assert _accept("sum(amount) + 5 > 0", "aggregate", nodes=nodes).family == "boolean"


# --- reject corpus: every named non-member, per §13.1's own enumeration ----


def test_reject_rand_and_current_timestamp() -> None:
    assert _reject("rand() > 0").code == "check-expression-rejected"
    assert _reject("current_timestamp() > seen_at").code == "check-expression-rejected"


def test_reject_window() -> None:
    assert _reject("sum(amount) OVER (PARTITION BY name) > 0").code == "check-expression-rejected"


def test_reject_subquery() -> None:
    assert _reject("amount IN (SELECT 1)").code == "check-expression-rejected"


def test_reject_lambda_hof() -> None:
    assert _reject("transform(name, x -> x) IS NOT NULL").code == "check-expression-rejected"


def test_reject_cast_and_double_colon_and_trycast_every_shape() -> None:
    for text in ["CAST(amount AS int)", "amount::int", "TRY_CAST(amount AS int)"]:
        assert _reject(text).code == "check-expression-rejected"


def test_reject_constructor_casts() -> None:
    for text in ["int(amount)", "double(amount)", "boolean(flag)"]:
        assert _reject(text).code == "check-expression-rejected"


def test_reject_typed_literal_suffix_forms() -> None:
    # [AE-8]: each pinned as its own reject row -- the corpus, not an
    # assumption about the parser's AST for each spelling.
    for text in ["1.5D > 0", "1.5F > 0", "1.5BD > 0", "123L > 0"]:
        assert _reject(text).code == "check-expression-rejected"


def test_reject_keyword_typed_literal_other_than_date_timestamp() -> None:
    assert _reject("amount = DOUBLE '1.5'").code == "check-expression-rejected"


def test_reject_interval() -> None:
    assert _reject("seen_at > INTERVAL 1 DAY").code == "check-expression-rejected"


def test_reject_star_via_count() -> None:
    # [EM-9]: `*` is not in the allowed set -- count(*) unauthorable.
    assert _reject("count(*) > 0", "aggregate").code == "check-expression-rejected"


def test_reject_double_and_scientific_literals() -> None:
    assert _reject("amount = 1.245E2").code == "check-expression-rejected"


def test_reject_null_inside_in_list() -> None:
    # [EM-12]: the NOT IN trap returning in authored form.
    assert _reject("amount IN (1, NULL, 3)").code == "check-expression-rejected"


def test_reject_like_escape_clause() -> None:
    # [AE-9]
    assert _reject(r"name LIKE 'x%' ESCAPE '\\'").code == "check-expression-rejected"


def test_reject_aggregate_outside_aggregate_position() -> None:
    assert _reject("sum(amount) > 0", "scalar").code == "check-expression-rejected"


def test_reject_unknown_column_is_not_itself_a_grammar_defect() -> None:
    # K3 (column existence against a bound type's declared columns) is the
    # CALLER's separate concern (an undeclared column is polymorphic here,
    # §6.5's own rule) -- this module accepts it, carrying it forward as a
    # referenced column for the caller to reject.
    result = _accept("unknown_col > 0")
    assert result.referenced_columns == frozenset({"unknown_col"})


def test_reject_qualified_column_reference() -> None:
    assert _reject("t.amount > 0").code == "check-expression-rejected"


def test_reject_not_like_shorthand() -> None:
    assert _reject("name NOT LIKE 'x%'").code == "check-expression-rejected"


def test_reject_is_true_false_distinct_from() -> None:
    assert _reject("flag IS TRUE").code == "check-expression-rejected"


def test_reject_hidden_extra_arguments() -> None:
    # Empirically-discovered overloads sqlglot's grammar accepts for OTHER
    # spellings of the same function name -- none are in this grammar.
    for text in [
        "trim(name, name) = name",
        "length(name, 5) > 0",
        "round(amount, 2, true) > 0",
        "floor(amount, 2) > 0",
        "floor(amount TO DAY) > 0",
        "ceil(amount, 2) > 0",
        "amount IN UNNEST(name)",
        "datediff(day, seen_at, seen_at) = 0",
    ]:
        assert _reject(text).code == "check-expression-rejected", text


def test_reject_string_concat_operator_and_int_div() -> None:
    assert _reject("name || name = name").code == "check-expression-rejected"
    assert _reject("amount DIV qty > 0").code == "check-expression-rejected"


def test_reject_bracket_and_placeholder() -> None:
    assert _reject("amount[0] > 0").code == "check-expression-rejected"
    assert _reject(":param > 0").code == "check-expression-rejected"


def test_reject_unparseable_text() -> None:
    for text in ["", "a AND", ")((", "a = 'unterminated"]:
        assert _reject(text).code == "check-expression-rejected"


def test_reject_no_nested_aggregate() -> None:
    assert _reject("sum(sum(amount))", "aggregate").code == "check-expression-rejected"


# --- [DC-3] per-function family-signature corpus obligation: >=1
# signature-conforming accept row and >=1 cross-family reject row per
# argument position, including the named divergence points ------------------


def test_dc3_concat_argument_families() -> None:
    assert _accept("concat(name, name) = name").family == "boolean"
    assert _reject("concat(name, amount) = name").code == "check-expression-mixed-types"


def test_dc3_substring_result_family_feeds_enclosing_comparison() -> None:
    assert _accept("substring(name, 1, 2) = name").family == "boolean"
    assert _reject("substring(name, 1, 2) > amount").code == "check-expression-mixed-types"


def test_dc3_replace_result_family_feeds_enclosing_comparison() -> None:
    assert _accept("replace(name, 'a', 'b') = name").family == "boolean"
    assert _reject("replace(name, 'a', 'b') > amount").code == "check-expression-mixed-types"


def test_dc3_greatest_least_argument_homogeneity() -> None:
    assert _accept("greatest(amount, qty) > 0").family == "boolean"
    assert _accept("greatest(seen_at, seen_at) > seen_at").family == "boolean"
    assert _reject("greatest(amount, name)").code == "check-expression-mixed-types"
    assert _reject("least(amount, name)").code == "check-expression-mixed-types"


def test_dc3_nullif_polymorphism() -> None:
    assert _accept("nullif(name, name) = name").family == "boolean"
    assert _accept("nullif(amount, qty) = amount").family == "boolean"
    assert _reject("nullif(amount, name)").code == "check-expression-mixed-types"


def test_dc3_coalesce_homogeneity_null_polymorphic() -> None:
    assert _accept("coalesce(name, name) = name").family == "boolean"
    assert _accept("coalesce(NULL, amount) > 0").family == "boolean"  # NULL is polymorphic
    assert _reject("coalesce(amount, 'x')").code == "check-expression-mixed-types"


def test_dc3_comparison_string_vs_numeric_the_2_53_construction() -> None:
    # [AE-1]: the exact hazard P-9 rule 1 exists to close -- a string never
    # meets a numeric/temporal in a comparison.
    assert _reject("amount != '9007199254740993'").code == "check-expression-mixed-types"


def test_dc3_between_cross_family() -> None:
    assert _reject("amount BETWEEN 1 AND 'x'").code == "check-expression-mixed-types"


def test_dc3_in_list_cross_family() -> None:
    assert _reject("amount IN (1, 'x')").code == "check-expression-mixed-types"


def test_dc3_arithmetic_requires_numeric() -> None:
    assert _reject("amount + name").code == "check-expression-mixed-types"


def test_dc3_string_functions_require_string() -> None:
    assert _reject("length(amount)").code == "check-expression-mixed-types"
    assert _reject("upper(amount)").code == "check-expression-mixed-types"
    assert _reject("trim(amount)").code == "check-expression-mixed-types"


def test_dc3_case_branches_and_condition_family() -> None:
    assert (
        _reject("CASE WHEN flag THEN amount ELSE name END").code == "check-expression-mixed-types"
    )
    assert _reject("CASE WHEN amount THEN 1 ELSE 0 END").code == "check-expression-mixed-types"


def test_dc3_like_requires_string() -> None:
    assert _reject("amount LIKE 'x%'").code == "check-expression-mixed-types"


def test_dc3_aggregate_argument_family() -> None:
    assert _reject("sum(name) > 0", "aggregate").code == "check-expression-mixed-types"
    assert _reject("avg(flag) > 0", "aggregate").code == "check-expression-mixed-types"


def test_dc3_not_requires_boolean_operand() -> None:
    # A006-2's own named gap: NOT's boolean operand had no cross-family
    # reject row (the accept side is `test_accept_boolean_connectives`).
    assert _reject("NOT amount").code == "check-expression-mixed-types"


# --- [DC-3] A006-2: the per-function, PER-ARGUMENT-POSITION signature
# corpus -- the one normative home of the family signatures encoded in
# check_grammar.py's handlers (P-9 Y-note [DC-3]; §6.4; §6.5 rule 1; §13.1
# G-07: "no prose signature table exists, deliberately"). One row per
# (function, argument position): `accept_text` is a signature-conforming
# call, `reject_text` swaps EXACTLY that position's argument to a
# cross-family column (`_SCHEMA`'s own `name`/`amount`/`qty`/`flag`/
# `seen_at`) and must trip `check-expression-mixed-types`. `reject_text is
# None` for the three aggregate members (`count`/`min`/`max`) whose
# argument is STRUCTURALLY family-unrestricted by design (§7.5's
# reconciliation ground) -- no cross-family violation exists to author,
# documented here rather than faked. The function set below is tied to
# `check_grammar`'s own data module (the completeness assertion just below)
# so this corpus cannot silently drift from `SCALAR_FUNCTION_NAMES`/
# `AGGREGATE_FUNCTION_NAMES` as either side grows.
_DC3_SIGNATURE_CORPUS: tuple[tuple[str, int, str, str | None, cg.Position], ...] = (
    ("abs", 1, "abs(amount) > 0", "abs(name) > 0", "scalar"),
    ("round", 1, "round(amount, 2) > 0", "round(name, 2) > 0", "scalar"),
    ("round", 2, "round(amount, 2) > 0", "round(amount, name) > 0", "scalar"),
    ("floor", 1, "floor(amount) > 0", "floor(name) > 0", "scalar"),
    ("ceil", 1, "ceil(amount) > 0", "ceil(name) > 0", "scalar"),
    ("greatest", 1, "greatest(amount, qty) > 0", "greatest(amount, name)", "scalar"),
    ("least", 1, "least(amount, qty) > 0", "least(amount, name)", "scalar"),
    ("length", 1, "length(name) > 0", "length(amount) > 0", "scalar"),
    ("trim", 1, "trim(name) = name", "trim(amount) = name", "scalar"),
    ("ltrim", 1, "ltrim(name) = name", "ltrim(amount) = name", "scalar"),
    ("rtrim", 1, "rtrim(name) = name", "rtrim(amount) = name", "scalar"),
    ("upper", 1, "upper(name) = name", "upper(amount) = name", "scalar"),
    ("lower", 1, "lower(name) = name", "lower(amount) = name", "scalar"),
    ("substring", 1, "substring(name, 1, 2) = name", "substring(amount, 1, 2) = name", "scalar"),
    ("substring", 2, "substring(name, 1) = name", "substring(name, name) = name", "scalar"),
    ("substring", 3, "substring(name, 1, 2) = name", "substring(name, 1, name) = name", "scalar"),
    ("concat", 1, "concat(name, name) = name", "concat(amount, name) = name", "scalar"),
    ("replace", 1, "replace(name, 'a', 'b') = name", "replace(amount, 'a', 'b') = name", "scalar"),
    ("replace", 2, "replace(name, 'a', 'b') = name", "replace(name, amount, 'b') = name", "scalar"),
    ("replace", 3, "replace(name, 'a', 'b') = name", "replace(name, 'a', amount) = name", "scalar"),
    ("coalesce", 1, "coalesce(name, name) = name", "coalesce(amount, 'x')", "scalar"),
    ("nvl", 1, "nvl(name, 'x') = 'x'", "nvl(amount, 'x')", "scalar"),
    ("ifnull", 1, "ifnull(name, 'x') = 'x'", "ifnull(amount, 'x')", "scalar"),
    ("nullif", 1, "nullif(amount, qty) = amount", "nullif(amount, name)", "scalar"),
    ("year", 1, "year(seen_at) = 2026", "year(amount) = 2026", "scalar"),
    ("month", 1, "month(seen_at) = 1", "month(amount) = 1", "scalar"),
    ("day", 1, "day(seen_at) = 1", "day(amount) = 1", "scalar"),
    ("datediff", 1, "datediff(seen_at, seen_at) = 0", "datediff(amount, seen_at) = 0", "scalar"),
    ("datediff", 2, "datediff(seen_at, seen_at) = 0", "datediff(seen_at, amount) = 0", "scalar"),
    ("date_add", 1, "date_add(seen_at, 5) = seen_at", "date_add(amount, 5) = seen_at", "scalar"),
    (
        "date_add",
        2,
        "date_add(seen_at, 5) = seen_at",
        "date_add(seen_at, name) = seen_at",
        "scalar",
    ),
    ("date_sub", 1, "date_sub(seen_at, 5) = seen_at", "date_sub(amount, 5) = seen_at", "scalar"),
    (
        "date_sub",
        2,
        "date_sub(seen_at, 5) = seen_at",
        "date_sub(seen_at, name) = seen_at",
        "scalar",
    ),
    ("sum", 1, "sum(amount) > 0", "sum(name) > 0", "aggregate"),
    ("avg", 1, "avg(amount) > 0", "avg(flag) > 0", "aggregate"),
    ("count", 1, "count(amount) > 0", None, "aggregate"),
    ("min", 1, "min(seen_at) IS NOT NULL", None, "aggregate"),
    ("max", 1, "max(seen_at) IS NOT NULL", None, "aggregate"),
)


@pytest.mark.parametrize(
    "function,position_index,accept_text,reject_text,position",
    _DC3_SIGNATURE_CORPUS,
    ids=[f"{row[0]}-arg{row[1]}" for row in _DC3_SIGNATURE_CORPUS],
)
def test_dc3_signature_corpus_per_function_per_argument_position(
    function: str,
    position_index: int,
    accept_text: str,
    reject_text: str | None,
    position: cg.Position,
) -> None:
    result = cg.validate_expression(accept_text, position, _SCHEMA)
    assert isinstance(result, cg.ValidatedExpr), f"{function} arg{position_index}: {result!r}"
    if reject_text is not None:
        rejected = cg.validate_expression(reject_text, position, _SCHEMA)
        assert isinstance(rejected, cg.GrammarDefect), (
            f"{function} arg{position_index}: {rejected!r}"
        )
        assert rejected.code == "check-expression-mixed-types"


def test_dc3_signature_corpus_covers_every_function() -> None:
    # The completeness assertion: the corpus's own function set is EXACTLY
    # the data module's (`SCALAR_FUNCTION_NAMES`/`AGGREGATE_FUNCTION_NAMES`)
    # -- neither side can silently drift from the other.
    corpus_functions = {row[0] for row in _DC3_SIGNATURE_CORPUS}
    assert corpus_functions == cg.SCALAR_FUNCTION_NAMES | cg.AGGREGATE_FUNCTION_NAMES


def test_dc3_corpus_reject_rows_required_for_every_concrete_arg_family() -> None:
    # [M5 critique fix]: the corpus's per-position reject/no-reject choice
    # is checked against `FunctionEntry.arg_families` so it cannot silently
    # drift from that data module -- every position declared as a CONCRETE
    # family (not the `None` "polymorphic" marker) must have a corpus row
    # proving a cross-family swap at that position is refused. Only the
    # forward direction is asserted: a `None` arg_family covers two
    # semantically different cases `FunctionEntry` does not itself
    # distinguish -- structurally-unrestricted (`count`/`min`/`max`'s
    # argument, no cross-family violation exists to author) and
    # homogeneous-but-must-unify-with-siblings (`greatest`/`least`/
    # `coalesce`/`nvl`/`ifnull`/`nullif`, which DO have a real reject row) --
    # so the absence of a declared family does not, by itself, predict
    # whether a reject row should exist.
    entry_by_name = {e.name: e for e in cg.SCALAR_FUNCTIONS} | {
        e.name: e for e in cg.AGGREGATE_FUNCTIONS
    }
    for function, position_index, _accept_text, reject_text, _position in _DC3_SIGNATURE_CORPUS:
        entry = entry_by_name[function]
        declared_family = entry.arg_families[position_index - 1]
        if declared_family is not None:
            assert reject_text is not None, (
                f"{function} arg{position_index} is declared {declared_family!r} in "
                "FunctionEntry.arg_families but the corpus has no cross-family reject row"
            )


# --- [M5 critique fix] one arity-violation row per function, tied to
# `FunctionEntry.arity` -- every allowlisted function's declared (min, max)
# argument-count bound is proven refused OUTSIDE that bound by an executed
# row, not merely asserted from the data module's own (until now
# unverified) `arity` field. Several rows double as regression pins for
# hidden-extra-argument-field holes this derivation itself found and fixed
# in `check_grammar.py`: `count`/`min`/`max` never checked their shared
# `expressions` field (silently admitting a second, even cross-family,
# argument -- `min(seen_at, amount)` used to validate clean) and
# `substring` never checked its `zero_start` 4th field (silently admitting
# a fully UNVALIDATED 4th argument, including a bare column reference).
# `reject_text` is `check-expression-rejected` in every row -- both a
# native sqlglot `ParseError` (converted by `_parse_expression`) and a
# handler-level hidden-field refusal use that same code, so the corpus
# needs no separate "how it failed" column.
_DC3_ARITY_CORPUS: tuple[tuple[str, str, cg.Position], ...] = (
    ("abs", "abs(amount, qty) > 0", "scalar"),
    ("round", "round(amount, 2, 3) > 0", "scalar"),
    ("floor", "floor(amount, 2) > 0", "scalar"),
    ("ceil", "ceil(amount, 2) > 0", "scalar"),
    ("greatest", "greatest() > 0", "scalar"),
    ("least", "least() > 0", "scalar"),
    ("length", "length(name, 2) > 0", "scalar"),
    ("trim", "trim(name, name) = name", "scalar"),
    ("ltrim", "ltrim(name, name) = name", "scalar"),
    ("rtrim", "rtrim(name, name) = name", "scalar"),
    ("upper", "upper(name, name) = name", "scalar"),
    ("lower", "lower(name, name) = name", "scalar"),
    ("substring", "substring(name, 1, 2, 3) = name", "scalar"),
    ("concat", "concat() = name", "scalar"),
    ("replace", "replace(name, 'a', 'b', 'c') = name", "scalar"),
    ("coalesce", "coalesce() = name", "scalar"),
    ("nvl", "nvl() = name", "scalar"),
    ("ifnull", "ifnull() = name", "scalar"),
    ("nullif", "nullif(amount, qty, qty) = amount", "scalar"),
    ("year", "year(seen_at, seen_at) = 2026", "scalar"),
    ("month", "month(seen_at, seen_at) = 1", "scalar"),
    ("day", "day(seen_at, seen_at) = 1", "scalar"),
    ("datediff", "datediff(seen_at, seen_at, seen_at) = 0", "scalar"),
    ("date_add", "date_add(seen_at, 5, 5) = seen_at", "scalar"),
    ("date_sub", "date_sub(seen_at) = seen_at", "scalar"),
    ("sum", "sum(amount, qty) > 0", "aggregate"),
    ("count", "count(amount, qty) > 0", "aggregate"),
    ("avg", "avg(amount, qty) > 0", "aggregate"),
    ("min", "min(seen_at, seen_at) IS NOT NULL", "aggregate"),
    ("max", "max(seen_at, seen_at) IS NOT NULL", "aggregate"),
)


@pytest.mark.parametrize(
    "function,reject_text,position",
    _DC3_ARITY_CORPUS,
    ids=[row[0] for row in _DC3_ARITY_CORPUS],
)
def test_dc3_arity_corpus_per_function(
    function: str, reject_text: str, position: cg.Position
) -> None:
    result = cg.validate_expression(reject_text, position, _SCHEMA)
    assert isinstance(result, cg.GrammarDefect), f"{function}: {result!r}"
    assert result.code == "check-expression-rejected"


def test_dc3_arity_corpus_covers_every_function() -> None:
    # Same completeness shape as the signature corpus: one arity row per
    # function, tied to the same data-module function sets.
    corpus_functions = {row[0] for row in _DC3_ARITY_CORPUS}
    assert corpus_functions == cg.SCALAR_FUNCTION_NAMES | cg.AGGREGATE_FUNCTION_NAMES


# --- A006-5: the Track-A-facing data surface (arity/portability notes) -----


def test_function_entries_match_handler_tables() -> None:
    # Forward: every entry's node_kind is a real key in its handler table.
    for entry in cg.SCALAR_FUNCTIONS:
        assert entry.node_kind in cg._HANDLERS_SCALAR, entry
        assert entry.portability_note
        assert entry.arity[0] <= entry.arity[1]
    for entry in cg.AGGREGATE_FUNCTIONS:
        assert entry.node_kind in cg._HANDLERS_AGGREGATE_EXTRA, entry
        assert entry.portability_note
        assert entry.arity[0] <= entry.arity[1]
    # Backward ("vice versa"): every FUNCTION-CALL node kind in the scalar
    # handler table (i.e. excluding operators/leaves/the year-month-day-
    # datediff-injected TsOrDsToDate wrapper, never authored directly) has
    # at least one FunctionEntry naming it.
    non_function_scalar_kinds = {
        exp.Column,
        exp.Literal,
        exp.Boolean,
        exp.Null,
        exp.Paren,
        exp.Cast,
        exp.EQ,
        exp.NEQ,
        exp.LT,
        exp.LTE,
        exp.GT,
        exp.GTE,
        exp.NullSafeEQ,
        exp.And,
        exp.Or,
        exp.Not,
        exp.Is,
        exp.In,
        exp.Between,
        exp.Like,
        exp.Add,
        exp.Sub,
        exp.Mul,
        exp.Div,
        exp.Mod,
        exp.Neg,
        exp.Case,
        exp.TsOrDsToDate,
    }
    handler_function_kinds = set(cg._HANDLERS_SCALAR) - non_function_scalar_kinds
    assert {e.node_kind for e in cg.SCALAR_FUNCTIONS} == handler_function_kinds
    assert {e.node_kind for e in cg.AGGREGATE_FUNCTIONS} == set(cg._HANDLERS_AGGREGATE_EXTRA)
    assert {e.name for e in cg.SCALAR_FUNCTIONS} == cg.SCALAR_FUNCTION_NAMES
    assert {e.name for e in cg.AGGREGATE_FUNCTIONS} == cg.AGGREGATE_FUNCTION_NAMES


# --- A006-12: named reject/refusal rows absent from G-07 --------------------


def test_reject_count_distinct_and_rlike() -> None:
    # §6.1: DISTINCT is a named non-member (previously reject-by-omission
    # with no corpus row); §6.2: RLIKE is a named non-member.
    assert _reject("count(DISTINCT amount) > 0", "aggregate").code == "check-expression-rejected"
    assert _reject("name RLIKE 'x'").code == "check-expression-rejected"


# --- D006-3 (coordinator ruling): IN-list members must be literals ---------


def test_d006_3_in_list_column_member_rejected() -> None:
    # Before the fix, a column (or arbitrary expression) IN-list member
    # walked the full scalar handler table and accepted -- structurally
    # equivalent to `amount = qty OR amount = 5`, an authorable form outside
    # §6.2's own prose ("IN over literal lists only").
    assert _reject("amount IN (qty, 5)").code == "check-expression-rejected"


def test_d006_3_in_list_typed_literal_members_still_accepted() -> None:
    # The [EM-1] typed-literal Cast shape is still an admitted IN-list
    # member (it is a literal, structurally) -- the tightening excludes
    # columns/expressions only, not the other admitted literal shapes.
    result = _accept("seen_at IN (DATE '2026-01-01', DATE '2026-01-02')")
    assert result.family == "boolean"


# --- F1 (critique fix): IN-list negative numeric literals -------------------


def test_f1_in_list_negative_numeric_literal_accepted() -> None:
    # sqlglot parses `-1` as `exp.Neg(exp.Literal)`, not `exp.Literal`
    # itself -- a literal list under §6.2 that the D006-3 tightening
    # regressed (previously accepted, now refused with a misleading
    # "column or expression" detail). `exp.Neg` wrapping a non-string
    # `exp.Literal` is admitted; the normal walk still family-checks it.
    result = _accept(
        "amount IN (-1, 5)",
        nodes=frozenset({exp.Column, exp.Identifier, exp.In, exp.Literal, exp.Neg}),
    )
    assert result.family == "boolean"


def test_f1_in_list_negated_column_member_still_rejected() -> None:
    # `-qty` is also `exp.Neg`, but wrapping a `Column`, not a `Literal` --
    # the F1 fix's admission test is narrow enough to keep this refused,
    # same as the pre-existing bare-column D006-3 case.
    assert _reject("amount IN (-qty, 5)").code == "check-expression-rejected"


# --- N2 (critique fix, swb.28): min/max aggregate RESULT family in an ------
# --- enclosing comparison ---------------------------------------------------


def test_n2_min_max_result_family_tightening_still_rejected() -> None:
    # [M5 critique fix]'s `_aggregate_unary(None, None)` pass-through
    # (swb.24) was itself unpinned in the TIGHTENING direction:
    # `test_dc3_aggregate_argument_family` only pins the argument-family
    # check INSIDE the aggregate (`sum(name) > 0`); min/max's own RESOLVED
    # result family, compared against a cross-family literal OUTSIDE the
    # aggregate, must independently still reject.
    assert _reject("min(seen_at) = 5", "aggregate").code == "check-expression-mixed-types"
    assert _reject("max(name) = 5", "aggregate").code == "check-expression-mixed-types"


def test_n2_min_max_result_family_same_family_widening_accepted() -> None:
    # The complementary WIDENING direction (§7.5's reconciliation ground):
    # a same-family comparison over min/max's own polymorphic result family
    # was previously false-refused (numeric result hardcoded pre-swb.24)
    # and now validates clean -- the only DC-3 corpus rows for min/max
    # (`min(seen_at) IS NOT NULL`) never exercised this, since `IS NOT
    # NULL` erases the operand's family before any comparison.
    assert _accept("min(name) = 'x'", "aggregate").family == "boolean"


# --- property suite (§13.2's slice for this module) -------------------------


@given(
    left=st.sampled_from(["amount", "qty"]),
    right=st.sampled_from(["name", "flag"]),
    op=st.sampled_from(["=", "!=", "<", "<=", ">", ">="]),
)
@settings(max_examples=50)
def test_property_family_fail_closed_over_generated_cross_family_comparisons(
    left: str, right: str, op: str
) -> None:
    """[AE-1]/P-9 rule 1: generated cross-family operand mixes always
    reject, over every comparison operator."""
    result = cg.validate_expression(f"{left} {op} {right}", "scalar", _SCHEMA)
    assert isinstance(result, cg.GrammarDefect)
    assert result.code == "check-expression-mixed-types"


@given(
    left=st.sampled_from(["amount", "qty"]),
    right=st.sampled_from(["amount", "qty"]),
    op=st.sampled_from(["=", "!=", "<", "<=", ">", ">=", "+", "-", "*", "/"]),
)
@settings(max_examples=50)
def test_property_same_family_never_rejected_on_family_grounds(
    left: str, right: str, op: str
) -> None:
    """The complement of the fail-closed property: two SAME-family numeric
    operands never trip `check-expression-mixed-types` under any admitted
    operator this test generates."""
    result = cg.validate_expression(f"{left} {op} {right}", "scalar", _SCHEMA)
    assert isinstance(result, cg.ValidatedExpr)


@given(
    text=st.sampled_from(
        [
            "amount > 0",
            "name = 'x'",
            "flag AND amount > 0",
            "amount IS NULL",
            "amount + qty > 0",
            "length(name) > 0",
            "coalesce(name, 'x') = 'x'",
        ]
    )
)
@settings(max_examples=20)
def test_property_authored_text_identity_over_every_accept_shape(text: str) -> None:
    """[EM-6][AE-2]: the string handed forward equals the authored input
    byte-exact, over a representative sample of every accepted shape."""
    result = cg.validate_expression(text, "scalar", _SCHEMA)
    assert isinstance(result, cg.ValidatedExpr)
    assert result.authored_text == text


@given(node_kind=st.sampled_from(["rand()", "current_timestamp()", ":param", "amount[0]"]))
@settings(max_examples=10)
def test_property_gatekeeper_fail_closed_regardless_of_nesting_depth(node_kind: str) -> None:
    """Gatekeeper fail-closed: a non-member node anywhere in the tree
    rejects the WHOLE expression, regardless of how deeply it is nested
    inside otherwise-admitted structure."""
    nested = f"amount > 0 AND (flag OR ({node_kind} IS NOT NULL))"
    result = cg.validate_expression(nested, "scalar", _SCHEMA)
    assert isinstance(result, cg.GrammarDefect)
    assert result.code == "check-expression-rejected"


# --- family_of_kind: the fact-column-kind -> Family reduction --------------


def test_family_of_kind_covers_every_fact_column_kind() -> None:
    assert cg.family_of_kind("string") == "string"
    assert cg.family_of_kind("int") == "numeric"
    assert cg.family_of_kind("long") == "numeric"
    assert cg.family_of_kind("decimal") == "numeric"
    assert cg.family_of_kind("date") == "temporal"
    assert cg.family_of_kind("timestamp") == "temporal"
    assert cg.family_of_kind("bool") == "boolean"


def test_family_of_kind_unrecognized_kind_returns_none() -> None:
    assert cg.family_of_kind("garbage") is None


# --- §7.5/[EM-4] `compile_aggregate` -- dormant behind P-6/K7 (conveyer-swb.15,
# D006-1's "build now, dormant" ruling). Scope: the five §6.3 aggregates,
# §6.2 arithmetic, columns, and literals -- §13.2's own DC-2 property scope,
# this module's own new-section docstring.


def _compile_agg(text: str) -> cg.CompiledAggregate | cg.GrammarDefect:
    validated = cg.validate_expression(text, "aggregate", _SCHEMA)
    assert isinstance(validated, cg.ValidatedExpr), f"expected accept, got {validated!r}"
    return cg.compile_aggregate(validated, _SCHEMA)


def test_compile_aggregate_accepts_a_bare_sum_and_coalesces_it() -> None:
    result = _compile_agg("sum(amount)")
    assert isinstance(result, cg.CompiledAggregate)
    assert result.tree == cg.AggCall(
        kind="sum", argument=cg.AggColumnRef(name="amount"), coalesce_zero=True
    )
    assert result.referenced_columns == frozenset({"amount"})


def test_compile_aggregate_accepts_combined_arithmetic_over_two_sums() -> None:
    # [EM-4]'s own worked example: `sum(net) + sum(fees)` -- each `sum`
    # call is its own coalesce-to-zero node, never a top-level wrap.
    result = _compile_agg("sum(amount) + sum(qty)")
    assert isinstance(result, cg.CompiledAggregate)
    assert result.tree == cg.AggBinOp(
        op="+",
        left=cg.AggCall(kind="sum", argument=cg.AggColumnRef(name="amount"), coalesce_zero=True),
        right=cg.AggCall(kind="sum", argument=cg.AggColumnRef(name="qty"), coalesce_zero=True),
    )
    assert result.referenced_columns == frozenset({"amount", "qty"})


def test_compile_aggregate_accepts_the_count_1_row_count_idiom() -> None:
    result = _compile_agg("count(1)")
    assert isinstance(result, cg.CompiledAggregate)
    assert result.tree == cg.AggCall(
        kind="count", argument=cg.AggLiteral(text="1"), coalesce_zero=False
    )
    assert result.referenced_columns == frozenset()


def test_compile_aggregate_min_max_avg_never_coalesce_to_zero() -> None:
    # §7.5 verbatim: "min/max/avg take no zero" -- `coalesce_zero=False`
    # for all three, unlike `sum`'s own `True`.
    for fn in ("min", "max", "avg"):
        result = _compile_agg(f"{fn}(amount)")
        assert isinstance(result, cg.CompiledAggregate)
        assert isinstance(result.tree, cg.AggCall)
        assert result.tree.coalesce_zero is False, fn


def test_compile_aggregate_arithmetic_and_negation_compose() -> None:
    result = _compile_agg("-sum(amount) % 2")
    assert isinstance(result, cg.CompiledAggregate)
    assert result.tree == cg.AggBinOp(
        op="%",
        left=cg.AggNeg(
            operand=cg.AggCall(
                kind="sum", argument=cg.AggColumnRef(name="amount"), coalesce_zero=True
            )
        ),
        right=cg.AggLiteral(text="2"),
    )


def test_compile_aggregate_rejects_a_construct_outside_its_scoped_surface() -> None:
    # `sum(CASE WHEN ... END)` is grammar-ACCEPTED by gate 1 (§6.1/§6.2) but
    # outside `compile_aggregate`'s own scope (the module's own new-section
    # docstring: aggregates + §6.2 arithmetic + literals only, §13.2's DC-2
    # property scope) -- a named, deletable coverage gap, not a silent
    # mistranslation.
    result = _compile_agg("sum(CASE WHEN amount > 0 THEN amount ELSE 0 END)")
    assert isinstance(result, cg.GrammarDefect)
    assert result.code == "check-expression-rejected"


def test_compile_aggregate_authored_text_reparse_is_deterministic() -> None:
    # compile_aggregate re-parses `validated.authored_text` (never the raw
    # caller-supplied text directly) -- byte-identical re-parse, same tree,
    # same result, every time (pure, total).
    validated = cg.validate_expression("sum(amount) + sum(qty)", "aggregate", _SCHEMA)
    assert isinstance(validated, cg.ValidatedExpr)
    first = cg.compile_aggregate(validated, _SCHEMA)
    second = cg.compile_aggregate(validated, _SCHEMA)
    assert first == second
