"""`frames.business_checks` — the post_check interpreter's compile/evaluate/
projection half. LLD 006.1 §7 (compilation, evaluation, reason shaping),
§8.1 (fresh-path count identity), §13.1 **G-08** (the executable allowlist
semantics table, A-15's idiom).

**G-08's scope in THIS milestone (B1).** §13.1 G-08 covers "every §6.2/§6.3
member" — §6.2 is the SCALAR position (row `expr`, `control.expr`, and every
function-argument position), fully buildable now; §6.3 is the AGGREGATE
position (`batch_check.aggregate` only), whose sole consumer is `batch_check`
— structurally dormant until the 005 v1.x member grammar lands (P-6; K7
refuses any `batch_check` at bind, so no aggregate-position compiler exists
yet, matching the milestone table's own G-05(b–h) "blocked on 005 v1.x
members — named wait"). The scalar-position rows below are asserted on the
REAL Spark engine under the exact session pins (`frames.checks.SESSION_PINS`)
this suite already shares; the aggregate-position rows are explicitly
`pytest.mark.skip`-marked with that same citation, not silently omitted.

Every row/expectation below was discovered empirically in the kernel
(`repl-driven-python`'s own rule) before being written here — this file is
the durable record of that session, not a re-derivation from LLD prose.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from spine.core import check_grammar as cg
from spine.core.model import ChecksModel, FactSchemaModel, MembershipCheckModel, RowCheckModel
from spine.frames import business_checks as bc
from spine.frames.checks import SESSION_PINS
from spine.probes import g08_parity

# --- shared fixtures ---------------------------------------------------------

_ORDERS_SCHEMA = FactSchemaModel(
    columns=[
        {"name": "domain_id", "type": "string"},
        {"name": "amount", "type": "decimal(10,2)"},
        {"name": "status", "type": "string"},
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
    ordering=[],
)

_ORDERS_CAND_COLUMNS = ["domain_id", "amount", "status"]
_ORDERS_CAND_SCHEMA = StructType(
    [
        StructField("domain_id", StringType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("status", StringType(), True),
    ]
)


def _orders_checks(*extra: RowCheckModel | MembershipCheckModel) -> ChecksModel:
    return ChecksModel(
        checks=[
            RowCheckModel(
                kind="row",
                id="positive-amount",
                fact_type="orders",
                expr="amount > 0",
                reason="business/negative-amount",
            ),
            *extra,
        ]
    )


def _orders_df(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    return spark.createDataFrame(rows, schema=_ORDERS_CAND_SCHEMA)


# --- compile_business_checks: entry order (§7.1, P-5) ------------------------


def test_compile_puts_the_implicit_check_first_then_authored_order(spark: SparkSession) -> None:
    # `spark` is unused directly -- `compile_business_checks` builds `F.col`
    # `Column` values (never executed, D-4's own "plain-value pure" claim),
    # which still needs a live `SparkContext` to CONSTRUCT (not run); this
    # forces the shared session-scoped fixture to exist before the call,
    # the same requirement `frames/checks.py`'s own `compile_contract` tests
    # satisfy implicitly via file-order (an earlier spark-requesting test in
    # the same file already built it) -- explicit here rather than relying
    # on cross-test ordering.
    checks = _orders_checks(
        RowCheckModel(
            kind="row",
            id="known-status",
            fact_type="orders",
            expr="status IN ('open', 'closed')",
            reason="business/bad-status",
        )
    )
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)

    assert [e.id for e in compiled.entries] == [
        "missing-domain-id",
        "positive-amount",
        "known-status",
    ]
    assert compiled.entries[0].reason == "business/missing-domain-id"
    assert compiled.entries[0].version == "fw-1"


def test_compile_filters_checks_to_the_bound_fact_type_only(spark: SparkSession) -> None:
    checks = ChecksModel(
        checks=[
            RowCheckModel(
                kind="row",
                id="other-type-check",
                fact_type="shipments",
                expr="amount > 0",
                reason="business/x",
            ),
        ]
    )
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)

    assert [e.id for e in compiled.entries] == ["missing-domain-id"]


def test_compile_per_check_version_is_stable_and_content_sensitive(spark: SparkSession) -> None:
    checks_a = _orders_checks()
    checks_b = _orders_checks()  # identical content, different object
    v_a = bc.compile_business_checks(checks_a, "orders", _ORDERS_SCHEMA).entries[1].version
    v_b = bc.compile_business_checks(checks_b, "orders", _ORDERS_SCHEMA).entries[1].version
    assert v_a == v_b  # content-hash, not object identity

    checks_c = ChecksModel(
        checks=[
            RowCheckModel(
                kind="row",
                id="positive-amount",
                fact_type="orders",
                expr="amount >= 0",
                reason="business/negative-amount",  # body changed
            )
        ]
    )
    v_c = bc.compile_business_checks(checks_c, "orders", _ORDERS_SCHEMA).entries[1].version
    assert v_c != v_a  # a content edit changes the per-check version


# --- evaluate: three-valued law, ordering, count identity (§7.2, §8.1) ------


def test_evaluate_three_valued_law_null_expr_passes(spark: SparkSession) -> None:
    checks = _orders_checks()
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)
    df = _orders_df(spark, [("d1", None, "open")])  # amount NULL -> `amount > 0` is NULL

    evaluated = bc.evaluate(df, compiled, {})
    admitted = bc.admitted_candidates(evaluated)

    assert admitted.count() == 1
    assert bc.business_violations(evaluated).count() == 0


def test_evaluate_implicit_check_fires_first_on_null_domain_id(spark: SparkSession) -> None:
    checks = _orders_checks()
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)
    df = _orders_df(
        spark, [(None, Decimal("-5.00"), "open")]
    )  # BOTH implicit and positive-amount fail

    evaluated = bc.evaluate(df, compiled, {})
    viol = bc.business_violations(evaluated)
    row = viol.collect()[0]

    assert row["reason_code"] == "business/missing-domain-id"  # entry-0-first, D-6
    detail = json.loads(row["reason_detail"])
    assert [entry["id"] for entry in detail] == ["missing-domain-id", "positive-amount"]


def test_evaluate_fresh_path_count_identity(spark: SparkSession) -> None:
    checks = _orders_checks()
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)
    df = _orders_df(
        spark,
        [
            ("d1", Decimal("10.00"), "open"),
            ("d2", Decimal("-1.00"), "open"),
            (None, Decimal("1.00"), "x"),
        ],
    )

    evaluated = bc.evaluate(df, compiled, {})
    admitted = bc.admitted_candidates(evaluated)
    viol = bc.business_violations(evaluated)

    assert df.count() == admitted.count() + viol.count()


def test_evaluate_reason_detail_is_id_and_version_only_never_reason(spark: SparkSession) -> None:
    checks = _orders_checks()
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)
    df = _orders_df(spark, [("d1", Decimal("-5.00"), "open")])

    viol = bc.business_violations(bc.evaluate(df, compiled, {}))
    detail = json.loads(viol.collect()[0]["reason_detail"])

    assert detail == [{"id": "positive-amount", "version": compiled.entries[1].version}]
    assert all(set(entry.keys()) == {"id", "version"} for entry in detail)


# --- membership: NULL tolerance, DISTINCT dedup, tuple equality (P-8) -------


def test_evaluate_membership_never_fires_on_null_key_material(spark: SparkSession) -> None:
    checks = _orders_checks(
        MembershipCheckModel(
            kind="membership",
            id="status-known",
            fact_type="orders",
            columns=["status"],
            co_effect="status_ref",
            ref_columns=["valid_status"],
            reason="business/unknown-status",
        )
    )
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)
    df = _orders_df(spark, [("d1", Decimal("1.00"), None)])  # status NULL
    ref = spark.createDataFrame([("open",)], schema=["valid_status"])

    viol = bc.business_violations(bc.evaluate(df, compiled, {"status_ref": ref}))
    assert viol.count() == 0


def test_evaluate_membership_joins_against_distinct_projection_no_fanout(
    spark: SparkSession,
) -> None:
    checks = _orders_checks(
        MembershipCheckModel(
            kind="membership",
            id="status-known",
            fact_type="orders",
            columns=["status"],
            co_effect="status_ref",
            ref_columns=["valid_status"],
            reason="business/unknown-status",
        )
    )
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)
    df = _orders_df(spark, [("d1", Decimal("1.00"), "open")])
    # duplicate "open" rows in the co-effect -- must not fan the candidate out
    ref = spark.createDataFrame([("open",), ("open",), ("closed",)], schema=["valid_status"])

    evaluated = bc.evaluate(df, compiled, {"status_ref": ref})
    assert evaluated.count() == 1


def test_evaluate_membership_marker_column_never_leaks(spark: SparkSession) -> None:
    checks = _orders_checks(
        MembershipCheckModel(
            kind="membership",
            id="status-known",
            fact_type="orders",
            columns=["status"],
            co_effect="status_ref",
            ref_columns=["valid_status"],
            reason="business/unknown-status",
        )
    )
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)
    df = _orders_df(spark, [("d1", Decimal("1.00"), "open")])
    ref = spark.createDataFrame([("open",)], schema=["valid_status"])

    evaluated = bc.evaluate(df, compiled, {"status_ref": ref})
    admitted = bc.admitted_candidates(evaluated)

    assert "_conveyer_m_status-known" not in evaluated.schema.fieldNames()
    assert set(admitted.columns) == set(_ORDERS_CAND_COLUMNS)


def test_evaluate_membership_multi_column_is_tuple_equality(spark: SparkSession) -> None:
    pair_schema = FactSchemaModel(
        columns=[
            {"name": "domain_id", "type": "string"},
            {"name": "a", "type": "string"},
            {"name": "b", "type": "string"},
        ],
        domain_id_col="domain_id",
        record_key=["domain_id"],
        ordering=[],
    )
    checks = ChecksModel(
        checks=[
            MembershipCheckModel(
                kind="membership",
                id="pair-known",
                fact_type="pairs",
                columns=["a", "b"],
                co_effect="pair_ref",
                ref_columns=["ra", "rb"],
                reason="business/unknown-pair",
            )
        ]
    )
    compiled = bc.compile_business_checks(checks, "pairs", pair_schema)
    df = spark.createDataFrame([("d1", "x", "1"), ("d2", "x", "2")], schema=["domain_id", "a", "b"])
    ref = spark.createDataFrame([("x", "1")], schema=["ra", "rb"])

    viol = bc.business_violations(bc.evaluate(df, compiled, {"pair_ref": ref}))
    assert sorted(r["domain_id"] for r in viol.collect()) == ["d2"]  # only the non-matching pair


def test_evaluate_raises_on_undeclared_co_effect(spark: SparkSession) -> None:
    checks = _orders_checks(
        MembershipCheckModel(
            kind="membership",
            id="status-known",
            fact_type="orders",
            columns=["status"],
            co_effect="status_ref",
            ref_columns=["valid_status"],
            reason="business/unknown-status",
        )
    )
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)
    df = _orders_df(spark, [("d1", Decimal("1.00"), "open")])

    with pytest.raises(ValueError, match="undeclared co_effect"):
        bc.evaluate(df, compiled, {})


# --- reason_detail truncation at 32 (005.1 §6.4's rule, reused, §7.3) -------


def test_business_violations_reason_detail_truncates_at_32_with_marker(
    spark: SparkSession,
) -> None:
    n = 33
    columns = [{"name": "domain_id", "type": "string"}] + [
        {"name": f"c{i}", "type": "int"} for i in range(n)
    ]
    schema = FactSchemaModel(
        columns=columns, domain_id_col="domain_id", record_key=["domain_id"], ordering=[]
    )
    checks = ChecksModel(
        checks=[
            RowCheckModel(
                kind="row",
                id=f"c{i}-positive",
                fact_type="wide",
                expr=f"c{i} > 0",
                reason="business/non-positive",
            )
            for i in range(n)
        ]
    )
    compiled = bc.compile_business_checks(checks, "wide", schema)
    row = ("d1", *([0] * n))  # every c{i} fails its own check simultaneously
    df = spark.createDataFrame([row], schema=["domain_id", *[f"c{i}" for i in range(n)]])

    viol = bc.business_violations(bc.evaluate(df, compiled, {}))
    detail = json.loads(viol.collect()[0]["reason_detail"])

    assert len(detail) == 33  # 32 real entries + 1 truncation marker
    assert detail[-1] == {"truncated": 33}
    assert all("id" in entry for entry in detail[:-1])


def test_business_violations_reason_detail_has_no_marker_under_the_limit(
    spark: SparkSession,
) -> None:
    checks = _orders_checks()
    compiled = bc.compile_business_checks(checks, "orders", _ORDERS_SCHEMA)
    df = _orders_df(spark, [("d1", Decimal("-5.00"), "open")])

    viol = bc.business_violations(bc.evaluate(df, compiled, {}))
    detail = json.loads(viol.collect()[0]["reason_detail"])

    assert all("truncated" not in entry for entry in detail)


# --- _compiled_expr: the one F.expr call site (§6.4's executed-text rule) --


def test_compiled_expr_raises_on_a_grammar_rejected_construct() -> None:
    with pytest.raises(ValueError, match="check-expression-rejected"):
        bc._compiled_expr("rand()", {"amount": "numeric"})


def test_compiled_expr_executes_the_authored_text_byte_exact(spark: SparkSession) -> None:
    col = bc._compiled_expr("amount > 0", {"amount": "numeric"})
    df = spark.createDataFrame([(Decimal("5.00"),)], schema=["amount"])
    assert df.select(col.alias("v")).collect()[0]["v"] is True


# ==============================================================================
# G-08 -- the executable allowlist semantics table (A-15 idiom), scalar position
# ==============================================================================

# Critique gate wf_24a3125f-ecc F3 (bead conveyer-6pg.32): single-homed in
# `spine.probes.g08_parity` -- `G08_VECTORS` there IS G-08's normative
# allowlist-semantics enumeration (006.1 §6.4's CI pin), the wheel-shipped
# canonical copy; this file no longer keeps its own byte-duplicate
# `_G08_CASES`/`_G08_SCHEMA`/`_G08_FAMILY`/`_G08_ROW` (the P-1
# two-sources-for-one-enumeration class G-08 itself exists to reject).
# `tests/unit/test_g08_parity_probe.py` already established the precedent
# of reaching into this module's private internals directly, as its own
# "local rehearsal" of the SAME probe -- this suite does the same, so both
# the DATA (`G08_VECTORS`) and the EVALUATION semantics (`_evaluate`,
# kind/raw-aware: value comparison, dtype comparison, or `raw=True`
# grammar-bypass) come from one place.


@pytest.fixture
def _g08_df(spark: SparkSession) -> DataFrame:
    # The shared session's `_BASE_CONF` already carries `**SESSION_PINS`
    # (`tests/conftest.py`) -- asserted here, not re-set, so a future
    # conftest change that ever un-wires the pins fails THIS suite loudly
    # rather than silently evaluating G-08 under the wrong semantics.
    assert (
        spark.conf.get("spark.sql.session.timeZone") == SESSION_PINS["spark.sql.session.timeZone"]
    )
    assert spark.conf.get("spark.sql.ansi.enabled") == SESSION_PINS["spark.sql.ansi.enabled"]
    return g08_parity._build_probe_df(spark)


@pytest.mark.parametrize(
    "vector", g08_parity.G08_VECTORS, ids=[v.case_id for v in g08_parity.G08_VECTORS]
)
def test_g08_executable_semantics_table(
    _g08_df: DataFrame, vector: g08_parity.ParityVector
) -> None:
    # Subsumes the four former standalone assertions this file used to
    # carry separately (`null-safe-eq-both-null`, `row-position-int-div-
    # int-is-double`, `decimal-division-stays-decimal`, and the four
    # round/bround discriminator rows `round-half-up-no-scale-arg`/
    # `bround-bankers-half`/`bround-bankers-scale2`/`bround-bankers-no-
    # scale-arg` -- all now `_SUPPLEMENTARY_VECTORS` rows in the imported
    # table, parametrized here exactly like every other row) -- [EM-8]'s
    # round-vs-bankers CONTRAST (round(2.5)==3 but bround(2.5)==2) is still
    # fully exercised, just as two separate parametrized rows rather than
    # one hand-written compound assertion.
    result = g08_parity._evaluate(_g08_df, vector)
    assert result.passed, (
        f"{vector.case_id}: expr={vector.expr!r} expected={result.expected!r} "
        f"actual={result.actual!r} error={result.error}"
    )


def test_g08_bround_is_not_grammar_admitted() -> None:
    # `bround` is deliberately NOT in sec6.2's numeric-fns allowlist (only
    # `round` is) -- confirms the negative control above is actually
    # exercising an EXCLUDED construct, not a gap in this test's own schema.
    result = cg.validate_expression("bround(2.5)", "scalar", g08_parity._G08_FAMILY)
    assert isinstance(result, cg.GrammarDefect)


@pytest.mark.skip(
    reason="sec6.3 aggregate position: batch_check is structurally dormant "
    "until the 005 v1.x member grammar lands (P-6; K7 refuses every "
    "batch_check at bind) -- no compile_aggregate exists yet to exercise. "
    "Named wait, matching the milestone table's own G-05(b-h) annotation; "
    "B4's scope."
)
def test_g08_count_1_vs_count_nullable_col_null_skip() -> (
    None
): ...  # [EM-9]: count(1) counts rows; count(nullable_col) skips NULLs


@pytest.mark.skip(
    reason="sec6.3 aggregate position: batch_check is structurally dormant "
    "until the 005 v1.x member grammar lands (P-6; K7 refuses every "
    "batch_check at bind) -- no compile_aggregate exists yet to exercise. "
    "Named wait, matching the milestone table's own G-05(b-h) annotation; "
    "B4's scope."
)
def test_g08_empty_set_per_aggregate_node_coalesce() -> (
    None
): ...  # [EM-4]: sum(net)+sum(fees) -> 0 over an empty set; sum(net)+5 -> 5


@pytest.mark.skip(
    reason="sec6.3 aggregate position: batch_check is structurally dormant "
    "until the 005 v1.x member grammar lands (P-6; K7 refuses every "
    "batch_check at bind) -- no compile_aggregate exists yet to exercise. "
    "Named wait, matching the milestone table's own G-05(b-h) annotation; "
    "B4's scope."
)
def test_g08_aggregate_unavailable_all_null_and_overflow_sum() -> (
    None
): ...  # [EM-5]: all-NULL-column sum and near-precision-38 overflow sum -> NULL


@pytest.mark.skip(
    reason="sec6.3 aggregate position: batch_check is structurally dormant "
    "until the 005 v1.x member grammar lands (P-6; K7 refuses every "
    "batch_check at bind) -- no compile_aggregate exists yet to exercise. "
    "Named wait, matching the milestone table's own G-05(b-h) annotation; "
    "B4's scope."
)
def test_g08_decimal_sum_avg_widening_stays_decimal() -> (
    None
): ...  # avg(decimal(38,2)) -> decimal(38,6); admissibility witness, P-9


# ==============================================================================
# 006.1 §13.2 property suite -- this module's slice: evaluation-order
# first-failure, three-valued law (compile_aggregate fidelity is B4's own
# named skip below -- no aggregate-position compiler exists to test, P-6)
# ==============================================================================

_WIDE_SCHEMA = FactSchemaModel(
    columns=[
        {"name": "domain_id", "type": "string"},
        {"name": "c0", "type": "int"},
        {"name": "c1", "type": "int"},
        {"name": "c2", "type": "int"},
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
    ordering=[],
)
_WIDE_STRUCT = StructType(
    [
        StructField("domain_id", StringType(), True),
        StructField("c0", IntegerType(), True),
        StructField("c1", IntegerType(), True),
        StructField("c2", IntegerType(), True),
    ]
)


def _wide_checks() -> ChecksModel:
    return ChecksModel(
        checks=[
            RowCheckModel(
                kind="row",
                id=f"c{i}-positive",
                fact_type="wide",
                expr=f"c{i} > 0",
                reason="business/non-positive",
            )
            for i in range(3)
        ]
    )


@given(
    domain_id_null=st.booleans(),
    fails=st.lists(st.booleans(), min_size=3, max_size=3),
)
@settings(max_examples=25, deadline=None)
def test_property_evaluation_order_first_failure_over_generated_multi_violation_rows(
    spark: SparkSession, domain_id_null: bool, fails: list[bool]
) -> None:
    """§13.2: "evaluation-order first-failure over generated multi-violation
    rows" -- D-6's law (entries evaluated in `compiled.entries` order,
    entry 0 the implicit check) holds for every generated combination of
    (NULL `domain_id`, which of the 3 authored checks also fail): the
    surviving `reason_code`/`reason_detail` always names the FIRST failing
    entry in that fixed order, never any other failing entry."""
    compiled = bc.compile_business_checks(_wide_checks(), "wide", _WIDE_SCHEMA)
    row = (
        None if domain_id_null else "d1",
        -1 if fails[0] else 1,
        -1 if fails[1] else 1,
        -1 if fails[2] else 1,
    )
    df = spark.createDataFrame([row], _WIDE_STRUCT)
    evaluated = bc.evaluate(df, compiled, {})
    viol = bc.business_violations(evaluated)
    if domain_id_null or any(fails):
        first_row = viol.collect()[0]
        expected_ids = (["missing-domain-id"] if domain_id_null else []) + [
            f"c{i}-positive" for i in range(3) if fails[i]
        ]
        assert first_row["reason_code"] == (
            "business/missing-domain-id" if domain_id_null else "business/non-positive"
        )
        detail_ids = [entry["id"] for entry in json.loads(first_row["reason_detail"])]
        assert detail_ids == expected_ids
    else:
        assert viol.count() == 0


@given(which_null=st.integers(min_value=0, max_value=2))
@settings(max_examples=10, deadline=None)
def test_property_three_valued_law_generated_null_rows_never_quarantine(
    spark: SparkSession, which_null: int
) -> None:
    """§13.2: "three-valued law: generated rows where the expr is NULL
    never quarantine" -- exactly one of the three generated columns is
    NULL (the check referencing it therefore evaluates NULL, not FALSE);
    the other two are positive (their checks evaluate TRUE). Every
    generated row is admitted, none violate."""
    compiled = bc.compile_business_checks(_wide_checks(), "wide", _WIDE_SCHEMA)
    values: list[int | None] = [1, 1, 1]
    values[which_null] = None
    row = ("d1", *values)
    df = spark.createDataFrame([row], _WIDE_STRUCT)
    evaluated = bc.evaluate(df, compiled, {})
    assert bc.business_violations(evaluated).count() == 0
    assert bc.admitted_candidates(evaluated).count() == 1


@pytest.mark.skip(
    reason="sec6.3 aggregate position / [DC-2]: compile_aggregate fidelity is a "
    "property over generated aggregate-position expressions structurally "
    "compiled ≡ F.expr(authored_text) -- no compile_aggregate exists yet "
    "(P-6; K7 refuses every batch_check at bind), so there is nothing to "
    "generate expressions AGAINST. Named wait, matching the milestone "
    "table's own G-05(b-h) annotation; B4's scope -- do not invent "
    "compile_aggregate to unblock this."
)
def test_property_compile_aggregate_fidelity_dc2() -> (
    None
): ...  # compile_aggregate ≡ F.expr(authored_text) on non-empty frames; ≡ §7.5's


#         per-node-zero law on the empty frame
