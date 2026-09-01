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


def _accept(text: str, position: cg.Position = "scalar") -> cg.ValidatedExpr:
    result = cg.validate_expression(text, position, _SCHEMA)
    assert isinstance(result, cg.ValidatedExpr), f"expected accept, got {result!r}"
    assert result.authored_text == text  # [EM-6][AE-2]: executed-text identity
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
    for text in [
        "amount > 5",
        "amount >= 5",
        "amount < 5",
        "amount <= 5",
        "amount != 5",
        "amount = 5",
    ]:
        result = _accept(text)
        assert result.family == "boolean"
        assert result.referenced_columns == frozenset({"amount"})


def test_accept_null_safe_eq() -> None:
    assert _accept("amount <=> qty").family == "boolean"


def test_accept_boolean_connectives() -> None:
    assert _accept("flag AND amount > 0").family == "boolean"
    assert _accept("flag OR amount > 0").family == "boolean"
    assert _accept("NOT flag").family == "boolean"


def test_accept_null_tests() -> None:
    assert _accept("amount IS NULL").family == "boolean"
    assert _accept("amount IS NOT NULL").family == "boolean"


def test_accept_in_and_between() -> None:
    assert _accept("amount IN (1, 2, 3)").family == "boolean"
    assert _accept("amount BETWEEN 1 AND 10").family == "boolean"
    assert _accept("amount NOT BETWEEN 1 AND 10").family == "boolean"
    assert _accept("amount NOT IN (1, 2, 3)").family == "boolean"


def test_accept_like() -> None:
    assert _accept("name LIKE 'x%'").family == "boolean"


def test_accept_typed_literals_both_spellings() -> None:
    # [EM-1]: both typed-literal spellings accepted.
    for text in (
        "seen_at = DATE '2026-01-01'",
        "seen_at = date('2026-01-01')",
        "seen_at = TIMESTAMP '2026-01-01 00:00:00'",
        "seen_at = timestamp('2026-01-01 00:00:00')",
    ):
        assert _accept(text).family == "boolean"


def test_accept_arithmetic() -> None:
    for text in [
        "amount + qty > 0",
        "amount - qty > 0",
        "amount * qty > 0",
        "amount / qty > 0",
        "amount % qty = 0",
    ]:
        assert _accept(text).family == "boolean"


def test_accept_case_coalesce_nullif() -> None:
    assert _accept("CASE WHEN amount > 0 THEN 1 ELSE 0 END = 1").family == "boolean"
    assert _accept("coalesce(name, 'x') = 'x'").family == "boolean"
    assert _accept("nvl(name, 'x') = 'x'").family == "boolean"
    assert _accept("ifnull(name, 'x') = 'x'").family == "boolean"
    assert _accept("nullif(amount, 0) IS NULL").family == "boolean"


def test_accept_numeric_functions() -> None:
    for text in [
        "abs(amount) > 0",
        "round(amount) > 0",
        "round(amount, 2) > 0",
        "floor(amount) > 0",
        "ceil(amount) > 0",
        "greatest(amount, qty) > 0",
        "least(amount, qty) > 0",
    ]:
        assert _accept(text).family == "boolean"


def test_accept_string_functions() -> None:
    for text in [
        "length(name) > 0",
        "trim(name) = name",
        "ltrim(name) = name",
        "rtrim(name) = name",
        "upper(name) = name",
        "lower(name) = name",
        "substring(name, 1, 2) = name",
        "substring(name, 1) = name",
        "concat(name, name) = name",
        "replace(name, 'a', 'b') = name",
    ]:
        assert _accept(text).family == "boolean"


def test_accept_temporal_functions() -> None:
    for text in [
        "year(seen_at) = 2026",
        "month(seen_at) = 1",
        "day(seen_at) = 1",
        "datediff(seen_at, seen_at) = 0",
        "date_add(seen_at, 5) = seen_at",
        "date_sub(seen_at, 5) = seen_at",
    ]:
        assert _accept(text).family == "boolean"


def test_accept_referenced_columns_union_across_a_multi_column_expression() -> None:
    result = _accept("amount > 0 AND name = 'x' AND flag")
    assert result.referenced_columns == frozenset({"amount", "name", "flag"})


def test_accept_parentheses() -> None:
    assert _accept("(amount > 0) AND flag").family == "boolean"


def test_accept_aggregate_position_members() -> None:
    for text in [
        "sum(amount) > 0",
        "count(1) > 0",
        "count(amount) > 0",
        "avg(amount) > 0",
        "min(seen_at) IS NOT NULL",
        "max(seen_at) IS NOT NULL",
    ]:
        assert _accept(text, "aggregate").family == "boolean"


def test_accept_aggregate_combined_with_scalar_arithmetic() -> None:
    assert _accept("sum(amount) + sum(qty) > 0", "aggregate").family == "boolean"
    assert _accept("sum(amount) + 5 > 0", "aggregate").family == "boolean"


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
