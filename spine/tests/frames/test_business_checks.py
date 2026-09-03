"""`frames.business_checks` — the post_check interpreter's compile/evaluate/
projection half. LLD 006.1 §7 (compilation, evaluation, reason shaping),
§8.1 (fresh-path count identity), §13.1 **G-08** (the executable allowlist
semantics table, A-15's idiom).

**G-08's scope (B1 + A006-4).** §13.1 G-08 covers "every §6.2/§6.3 member"
— §6.2 is the SCALAR position (row `expr`, `control.expr`, and every
function-argument position); §6.3 is the AGGREGATE position
(`batch_check.aggregate`, plus `sum`/`count`/`avg`/`min`/`max` generally).
Both positions' ENGINE rows are asserted on the REAL Spark engine under the
exact session pins (`frames.checks.SESSION_PINS`) this suite already shares
— `g08_parity._evaluate` hands `authored_text` to `F.expr` regardless of
position, so the aggregate rows need neither `compile_aggregate` nor the
005 v1.x member grammar. Every row here is asserted, not skipped.

**§7.5 `batch_check` mechanics (conveyer-swb.15, D006-1's "build now,
dormant behind K7" ruling).** `compile_aggregate` (`core/check_grammar.py`)
and its frames-layer Column-builder (`aggregate_column` below) now exist;
`test_property_compile_aggregate_fidelity_dc2` — the STRUCTURAL-compile-
vs-`F.expr` fidelity claim, G-08's own remaining structural row AND §13.2's
[DC-2] property — is LIVE below, no longer a named-wait skip. `batch_check`
ITSELF remains structurally unreachable through any bound spec (K7,
`core/model.py::ChecksModel`) — the verdict/message/demotion-door mechanics
this file also tests are exercised directly, matching that same dormancy.

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
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from spine.core import check_grammar as cg
from spine.core.checks import check_content_hash
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

    # A006-6: single-homed -- `compile_business_checks`'s per-check version
    # MUST equal `core/checks.py::check_content_hash`'s own answer for the
    # SAME parsed check, never a second, parallel hash expression.
    assert v_a == check_content_hash(checks_a.checks[0])

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
# G-08 -- the executable allowlist semantics table (A-15 idiom), scalar +
# aggregate position (A006-4)
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
# the DATA (`G08_VECTORS`) and the EVALUATION semantics (`_evaluate`/
# `_probe_df_for`, kind/raw/group-aware: value comparison, dtype comparison,
# `raw=True` grammar-bypass, `group=True` aggregate-position reduction over
# a per-vector `rows` override) come from one place.


@pytest.fixture
def _assert_g08_session_pins(spark: SparkSession) -> None:
    # The shared session's `_BASE_CONF` already carries `**SESSION_PINS`
    # (`tests/conftest.py`) -- asserted here, not re-set, so a future
    # conftest change that ever un-wires the pins fails THIS suite loudly
    # rather than silently evaluating G-08 under the wrong semantics.
    assert (
        spark.conf.get("spark.sql.session.timeZone") == SESSION_PINS["spark.sql.session.timeZone"]
    )
    assert spark.conf.get("spark.sql.ansi.enabled") == SESSION_PINS["spark.sql.ansi.enabled"]


@pytest.mark.parametrize(
    "vector", g08_parity.G08_VECTORS, ids=[v.case_id for v in g08_parity.G08_VECTORS]
)
def test_g08_executable_semantics_table(
    spark: SparkSession, _assert_g08_session_pins: None, vector: g08_parity.ParityVector
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
    # one hand-written compound assertion. A006-4: each vector builds its
    # OWN probe frame (`_probe_df_for`) rather than sharing one fixture-built
    # frame -- the aggregate-position rows each name their own `rows`
    # override (including a genuinely empty frame, `rows=()`), which a
    # single shared frame could not represent.
    df = g08_parity._probe_df_for(spark, vector)
    result = g08_parity._evaluate(df, vector)
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


# A006-4: the four former skip stubs below (`test_g08_count_1_vs_count_
# nullable_col_null_skip`/`test_g08_empty_set_per_aggregate_node_coalesce`/
# `test_g08_aggregate_unavailable_all_null_and_overflow_sum`/`test_g08_
# decimal_sum_avg_widening_stays_decimal`) cited a `compile_aggregate`/005
# v1.x wait that does NOT apply to these plain `F.expr`-evaluated engine
# rows -- `g08_parity._evaluate` already hands `authored_text` to `F.expr`
# regardless of position. Deleted; their own discriminator rows now live as
# `group=True` `ParityVector` entries in `g08_parity._AGGREGATE_VECTORS`,
# exercised by the SAME `test_g08_executable_semantics_table` parametrize
# above (`count-1-counts-rows`, `count-col-skips-null`, `sum-empty-is-null`,
# `min-empty-is-null`, `avg-decimal-stays-decimal`, `avg-int-is-double`,
# `sum-int-div-count-is-double`, `sum-decimal-div-count-is-decimal`,
# `sum-all-null-column-is-null`, `sum-decimal38-overflow-is-null-ansi-off`).


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


_DC2_FAMILY: dict[str, cg.Family | None] = {"c0": "numeric", "c1": "numeric"}
_DC2_STRUCT = StructType(
    [
        StructField("c0", DecimalType(10, 2), True),
        StructField("c1", IntegerType(), True),
    ]
)
_DC2_NONEMPTY_ROWS = [(Decimal("1.00"), 5), (Decimal("2.00"), None)]


def _dc2_leaf_expr() -> st.SearchStrategy[str]:
    return st.one_of(
        st.builds(
            lambda fn, col: f"{fn}({col})",
            st.sampled_from(["sum", "min", "max", "avg"]),
            st.sampled_from(["c0", "c1"]),
        ),
        st.just("count(1)"),
        st.builds(lambda col: f"count({col})", st.sampled_from(["c0", "c1"])),
        st.sampled_from(["0", "1", "5"]),
    )


# §13.2's own generator scope, verbatim: "compositions of the five §6.3
# aggregates, §6.2 arithmetic, and literals" -- `st.recursive` composes
# `_dc2_leaf_expr`'s aggregate-calls/literals via unary negation and binary
# `+ - *` up to `max_leaves` deep, generating only TEXT this bead's own
# `compile_aggregate` scope covers (never a CASE/coalesce/round nested
# inside an aggregate argument -- that is compile_aggregate's own named,
# out-of-scope gap, `test_check_grammar.py`'s own dedicated reject test).
_DC2_EXPR_STRATEGY: st.SearchStrategy[str] = st.recursive(
    _dc2_leaf_expr(),
    lambda children: st.one_of(
        st.builds(lambda e: f"-({e})", children),
        st.builds(
            lambda a, b, op: f"({a}) {op} ({b})",
            children,
            children,
            st.sampled_from(["+", "-", "*"]),
        ),
    ),
    max_leaves=6,
)


def _dc2_empty_frame_reference(node: cg.AggNode) -> Decimal | int | None:
    """An INDEPENDENT pure-Python reference for §7.5's per-node-zero law
    over a genuinely empty frame (never shipped, test-only) -- cross-checks
    `aggregate_column`'s own Spark-side construction without sharing any
    code with it (the `repl-driven-python`/ordering-struct precedent this
    codebase already established: build the two sides independently so the
    cross-check has teeth)."""
    if isinstance(node, cg.AggLiteral):
        return Decimal(node.text) if "." in node.text else int(node.text)
    if isinstance(node, cg.AggNeg):
        inner = _dc2_empty_frame_reference(node.operand)
        return None if inner is None else -inner
    if isinstance(node, cg.AggBinOp):
        left = _dc2_empty_frame_reference(node.left)
        right = _dc2_empty_frame_reference(node.right)
        if left is None or right is None:
            return None
        ops = {"+": lambda a, b: a + b, "-": lambda a, b: a - b, "*": lambda a, b: a * b}
        return ops[node.op](left, right)
    assert isinstance(node, cg.AggCall), node  # AggColumnRef never appears bare (§6.3)
    return 0 if node.kind in ("sum", "count") else None  # min/max/avg take no zero


@given(text=_DC2_EXPR_STRATEGY)
@settings(max_examples=60, deadline=None)
def test_property_compile_aggregate_fidelity_dc2(spark: SparkSession, text: str) -> None:
    """§13.2's [DC-2] property, G-08's own remaining structural row: over
    generated aggregate-position expressions, `compile_aggregate`'s
    structural compile (`aggregate_column`, the framework-composed `Column`
    tree) agrees with the engine's own reading of the authored text
    (`F.expr(text)`) on a non-empty frame -- VALUE and dtype both -- and
    agrees with §7.5's own per-node-zero law (an INDEPENDENT pure-Python
    reference, `_dc2_empty_frame_reference`, never `F.expr` -- which does
    NOT implement [EM-4] and would trivially "agree" with nothing) on a
    genuinely empty frame."""
    validated = cg.validate_expression(text, "aggregate", _DC2_FAMILY)
    assert isinstance(validated, cg.ValidatedExpr), (
        f"{text!r}: generator produced a reject: {validated!r}"
    )
    compiled = cg.compile_aggregate(validated, _DC2_FAMILY)
    assert isinstance(compiled, cg.CompiledAggregate), (
        f"{text!r}: generator produced a construct outside compile_aggregate's scope: {compiled!r}"
    )

    nonempty_df = spark.createDataFrame(_DC2_NONEMPTY_ROWS, _DC2_STRUCT)
    structural = bc.aggregate_column(compiled.tree, nonempty_df)
    oracle = F.expr(text)
    structural_row = nonempty_df.agg(structural.alias("v")).collect()[0]
    oracle_row = nonempty_df.agg(oracle.alias("v")).collect()[0]
    assert structural_row["v"] == oracle_row["v"], f"{text!r}: non-empty value diverges from F.expr"
    structural_dtype = nonempty_df.agg(structural.alias("v")).schema.fields[0].dataType
    oracle_dtype = nonempty_df.agg(oracle.alias("v")).schema.fields[0].dataType
    assert structural_dtype == oracle_dtype, f"{text!r}: dtype diverges from F.expr"

    empty_df = spark.createDataFrame([], _DC2_STRUCT)
    structural_empty = bc.aggregate_column(compiled.tree, empty_df)
    empty_value = empty_df.agg(structural_empty.alias("v")).collect()[0]["v"]
    expected_empty = _dc2_empty_frame_reference(compiled.tree)
    if expected_empty is None:
        assert empty_value is None, (
            f"{text!r}: expected NULL over the empty frame, got {empty_value!r}"
        )
    else:
        assert empty_value is not None, f"{text!r}: expected {expected_empty!r}, got NULL"
        assert Decimal(empty_value) == Decimal(expected_empty), (
            f"{text!r}: empty-frame value {empty_value!r} != reference {expected_empty!r}"
        )


# =============================================================================
# §7.5's verdict vocabulary, comparison, and message channel; §8.4's
# demotion door -- dormant behind P-6/K7 (conveyer-swb.15, D006-1's "build
# now, dormant" ruling). Exercised directly, matching this module's own
# updated top docstring.
# =============================================================================


def test_render_batch_check_verdict_match_exact_equality() -> None:
    aggregate = bc.AggregateOutcome(candidate_count=5, aggregate_value=Decimal("10.00"))
    control = bc.ControlOutcome(admitted_row_count=1, value=Decimal("10.00"))
    assert bc.render_batch_check_verdict(aggregate, control, None) == "match"


def test_render_batch_check_verdict_mismatch_exact_equality() -> None:
    aggregate = bc.AggregateOutcome(candidate_count=5, aggregate_value=Decimal("10.00"))
    control = bc.ControlOutcome(admitted_row_count=1, value=Decimal("10.01"))
    assert bc.render_batch_check_verdict(aggregate, control, None) == "mismatch"


def test_render_batch_check_verdict_match_within_tolerance() -> None:
    aggregate = bc.AggregateOutcome(candidate_count=5, aggregate_value=Decimal("10.00"))
    control = bc.ControlOutcome(admitted_row_count=1, value=Decimal("10.01"))
    assert bc.render_batch_check_verdict(aggregate, control, Decimal("0.02")) == "match"


def test_render_batch_check_verdict_mismatch_outside_tolerance() -> None:
    aggregate = bc.AggregateOutcome(candidate_count=5, aggregate_value=Decimal("10.00"))
    control = bc.ControlOutcome(admitted_row_count=1, value=Decimal("10.10"))
    assert bc.render_batch_check_verdict(aggregate, control, Decimal("0.02")) == "mismatch"


def test_render_batch_check_verdict_empty_detail_sum_coalesced_to_zero_matches() -> None:
    # §7.5(c)'s own worked example: an empty candidate set's `sum` already
    # coalesced to typed 0 by `aggregate_column` -- a zero control matches.
    aggregate = bc.AggregateOutcome(candidate_count=0, aggregate_value=Decimal("0"))
    control = bc.ControlOutcome(admitted_row_count=1, value=Decimal("0"))
    assert bc.render_batch_check_verdict(aggregate, control, None) == "match"


def test_render_batch_check_verdict_control_unavailable_zero_admitted_rows() -> None:
    # (g): header-only summary member -- zero rows admitted [AE-5].
    aggregate = bc.AggregateOutcome(candidate_count=5, aggregate_value=Decimal("1"))
    control = bc.ControlOutcome(admitted_row_count=0)
    assert bc.render_batch_check_verdict(aggregate, control, None) == "control-unavailable"


def test_render_batch_check_verdict_control_ambiguous_two_admitted_rows() -> None:
    # (e): two control rows, incl. the value-identical-duplicate pair
    # [AE-11] -- this function only ever sees the COUNT, so a genuinely
    # duplicate pair is indistinguishable from any other ambiguous pair.
    aggregate = bc.AggregateOutcome(candidate_count=5, aggregate_value=Decimal("1"))
    control = bc.ControlOutcome(admitted_row_count=2)
    assert bc.render_batch_check_verdict(aggregate, control, None) == "control-ambiguous"


def test_render_batch_check_verdict_control_unavailable_null_extracted_value() -> None:
    # (f): a NULL extracted control value [AE-4].
    aggregate = bc.AggregateOutcome(candidate_count=5, aggregate_value=Decimal("1"))
    control = bc.ControlOutcome(admitted_row_count=1, value=None)
    assert bc.render_batch_check_verdict(aggregate, control, None) == "control-unavailable"


def test_render_batch_check_verdict_aggregate_unavailable_nonempty_null_aggregate() -> None:
    # (h): a non-empty candidate set with a NULL aggregate (all-NULL
    # column, or decimal overflow) -- distinctly named, never coalesced.
    aggregate = bc.AggregateOutcome(candidate_count=5, aggregate_value=None)
    control = bc.ControlOutcome(admitted_row_count=1, value=Decimal("1"))
    assert bc.render_batch_check_verdict(aggregate, control, None) == "aggregate-unavailable"


def test_render_batch_check_verdict_control_unavailable_empty_set_min_max_avg_null() -> None:
    # §7.5's own literal text: "min/max/avg take no zero: over an empty
    # candidate set they yield NULL and the check fails loud (`control-
    # unavailable` class...)" -- an empty-candidate-set NULL aggregate is
    # this class, distinct from (h)'s non-empty `aggregate-unavailable`.
    aggregate = bc.AggregateOutcome(candidate_count=0, aggregate_value=None)
    control = bc.ControlOutcome(admitted_row_count=1, value=Decimal("1"))
    assert bc.render_batch_check_verdict(aggregate, control, None) == "control-unavailable"


def test_batch_check_failed_message_is_value_free() -> None:
    message = bc.batch_check_failed_message("chk-1", "mismatch", "abcdef0123456789ff")
    assert message == "batch-check-failed: id=chk-1 verdict=mismatch check_version=abcdef0123456789"
    # [S-7]/[S-9]: no aggregate/control VALUE ever rides this string.
    assert "10.00" not in message
    assert "Decimal" not in message


def test_batch_check_drift_segment_is_value_free_and_includes_match() -> None:
    segment = bc.batch_check_drift_segment("chk-1", "match", "abcdef0123456789ff")
    assert segment == "batch_check drift: id=chk-1 verdict=match check_version=abcdef0123456789"
    assert "10.00" not in segment


def test_apply_batch_check_verdict_demotes_to_drift_when_any_fact_table_present() -> None:
    # §8.4's demotion door, decide-then-do: ANY declared fact table already
    # having the batch demotes EVERY outcome (incl. a failure) to the drift
    # segment -- never raised.
    from spine.stages import post_check

    fact_presence = {"detail": False, "summary": True}
    result = post_check._apply_batch_check_verdict(fact_presence, "chk-1", "mismatch", "abc123")
    assert result == "batch_check drift: id=chk-1 verdict=mismatch check_version=abc123"


def test_apply_batch_check_verdict_demotes_a_match_too() -> None:
    # §12's own text: a demoted recompute that agrees is STILL recorded.
    from spine.stages import post_check

    fact_presence = {"detail": True}
    result = post_check._apply_batch_check_verdict(fact_presence, "chk-1", "match", "abc123")
    assert result == "batch_check drift: id=chk-1 verdict=match check_version=abc123"


def test_apply_batch_check_verdict_fresh_path_match_returns_none() -> None:
    from spine.stages import post_check

    fact_presence = {"detail": False}
    assert post_check._apply_batch_check_verdict(fact_presence, "chk-1", "match", "abc123") is None


def test_apply_batch_check_verdict_fresh_path_failure_raises_loud() -> None:
    # Fresh path (no declared fact table has the batch): a failing verdict
    # fails the batch loudly, §7.5's own value-free message.
    from spine.stages import post_check

    fact_presence = {"detail": False}
    with pytest.raises(ValueError, match=r"batch-check-failed: id=chk-1 verdict=mismatch"):
        post_check._apply_batch_check_verdict(fact_presence, "chk-1", "mismatch", "abc123")


# --- security gate wf_c9aadeb2-8eb, finding F-5: `_extract_control_value` --
# must re-derive its control expression through gate 1, never trust an
# already-`ValidatedExpr` (or raw text) byte-for-byte -- `F.expr` admits
# `reflect()`/`java_method()` (arbitrary static JVM calls). Dormant behind
# K7, exercised directly (same precedent as the `_apply_batch_check_verdict`
# tests immediately above).


def test_extract_control_value_single_row_extracts_scalar(spark: SparkSession) -> None:
    from spine.stages import post_check

    rows = spark.createDataFrame([(1, 10.5)], schema=["id", "amount"])
    result = post_check._extract_control_value(rows, "amount", {"amount": "numeric"})
    assert result == bc.ControlOutcome(admitted_row_count=1, value=10.5)


def test_extract_control_value_zero_rows_no_grammar_call_needed(spark: SparkSession) -> None:
    from spine.stages import post_check

    empty = spark.createDataFrame([], schema="id int, amount double")
    result = post_check._extract_control_value(empty, "amount", {"amount": "numeric"})
    assert result == bc.ControlOutcome(admitted_row_count=0)


def test_extract_control_value_two_rows_ambiguous_no_grammar_call_needed(
    spark: SparkSession,
) -> None:
    from spine.stages import post_check

    two = spark.createDataFrame([(1, 10.5), (2, 20.5)], schema=["id", "amount"])
    result = post_check._extract_control_value(two, "amount", {"amount": "numeric"})
    assert result == bc.ControlOutcome(admitted_row_count=2)


def test_extract_control_value_refuses_reflect_even_with_exactly_one_row(
    spark: SparkSession,
) -> None:
    # F-5's own worked exploit: a caller-supplied control-expression text
    # carrying `reflect(...)` (an arbitrary static JVM call `F.expr` would
    # otherwise execute unchecked) must be refused at gate 1 re-derivation,
    # never reach `F.expr`, even when exactly one row is admitted.
    from spine.stages import post_check

    rows = spark.createDataFrame([(1, 10.5)], schema=["id", "amount"])
    with pytest.raises(ValueError, match=r"check-expression-rejected"):
        post_check._extract_control_value(
            rows, "reflect('java.lang.Runtime', 'getRuntime')", {"amount": "numeric"}
        )


def test_extract_control_value_refuses_java_method_too(spark: SparkSession) -> None:
    from spine.stages import post_check

    rows = spark.createDataFrame([(1, 10.5)], schema=["id", "amount"])
    with pytest.raises(ValueError, match=r"check-expression-rejected"):
        post_check._extract_control_value(
            rows,
            'java_method("java.lang.Runtime", "getRuntime")',
            {"amount": "numeric"},
        )
