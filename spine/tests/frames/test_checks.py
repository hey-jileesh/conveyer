"""`frames.checks` — I-P2 pre_violations, I-12 violation subtraction, [C-8]. §7.5."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from spine.frames import checks

_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("req1", StringType(), True),
        StructField("req2", StringType(), True),
    ]
)


def _sample(spark: SparkSession):
    return spark.createDataFrame(
        [(1, "a", "x"), (2, None, "y"), (3, "c", None), (4, None, None)],
        schema=_SCHEMA,
    )


# --- pre_violations: same-predicate consistency (§7.5) ----------------------


def test_pre_violations_and_negated_predicate_partition_raw_df_exactly(
    spark: SparkSession,
) -> None:
    raw_df = _sample(spark)
    required = ["req1", "req2"]

    viol = checks.pre_violations(raw_df, required)
    valid = raw_df.filter(~checks.required_null_predicate(required))

    viol_ids = sorted(r["id"] for r in viol.collect())
    valid_ids = sorted(r["id"] for r in valid.collect())
    assert viol_ids == [2, 3, 4]
    assert valid_ids == [1]
    # no overlap, full cover -- the SAME predicate, negated, partitions raw_df
    assert set(viol_ids) & set(valid_ids) == set()
    assert set(viol_ids) | set(valid_ids) == {r["id"] for r in raw_df.collect()}
    assert viol.count() + valid.count() == raw_df.count()


def test_pre_violations_zero_required_columns_is_deterministically_empty(
    spark: SparkSession,
) -> None:
    raw_df = _sample(spark)

    viol = checks.pre_violations(raw_df, [])
    valid = raw_df.filter(~checks.required_null_predicate([]))

    assert viol.count() == 0
    assert valid.count() == raw_df.count()


def test_pre_violations_single_required_column(spark: SparkSession) -> None:
    raw_df = _sample(spark)

    viol = checks.pre_violations(raw_df, ["req1"])

    assert sorted(r["id"] for r in viol.collect()) == [2, 4]


# --- violation_subtraction: multiplicity-preserving bag subtraction [C-8] ---


def test_violation_subtraction_count_identity_no_duplicates(spark: SparkSession) -> None:
    candidate = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ["k", "v"])
    violations = spark.createDataFrame([("b", 2)], ["k", "v"])

    admitted = checks.violation_subtraction(candidate, violations)

    assert sorted((r["k"], r["v"]) for r in admitted.collect()) == [("a", 1), ("c", 3)]
    assert candidate.count() == admitted.count() + violations.count()


def test_violation_subtraction_preserves_multiplicity_of_duplicate_rows(
    spark: SparkSession,
) -> None:
    # 3 identical (a, 1) rows in candidate; only 2 flagged as violations ->
    # exactly 1 copy of (a, 1) must remain admitted (bag subtraction, not a
    # naive value-based anti-join which would remove all 3).
    candidate = spark.createDataFrame([("a", 1), ("a", 1), ("a", 1), ("b", 2)], ["k", "v"])
    violations = spark.createDataFrame([("a", 1), ("a", 1)], ["k", "v"])

    admitted = checks.violation_subtraction(candidate, violations)
    rows = sorted((r["k"], r["v"]) for r in admitted.collect())

    assert rows == [("a", 1), ("b", 2)]
    assert candidate.count() == admitted.count() + violations.count()


def test_violation_subtraction_over_supplied_violations_saturates_at_zero(
    spark: SparkSession,
) -> None:
    # violations claims MORE copies of (a, 1) than candidate actually has --
    # a contract-violating input (violations must be a genuine subset), but
    # violation_subtraction must still be a total function, never raise.
    candidate = spark.createDataFrame([("a", 1), ("a", 1), ("b", 2)], ["k", "v"])
    violations = spark.createDataFrame([("a", 1), ("a", 1), ("a", 1), ("a", 1)], ["k", "v"])

    admitted = checks.violation_subtraction(candidate, violations)

    assert sorted((r["k"], r["v"]) for r in admitted.collect()) == [("b", 2)]


def test_violation_subtraction_no_shared_columns_returns_candidate_unchanged(
    spark: SparkSession,
) -> None:
    candidate = spark.createDataFrame([("a", 1)], ["k", "v"])
    violations = spark.createDataFrame([("z",)], ["unrelated"])

    admitted = checks.violation_subtraction(candidate, violations)

    assert sorted((r["k"], r["v"]) for r in admitted.collect()) == [("a", 1)]


# --- check_count_identity: pure values, never raises ------------------------


def test_check_count_identity_ok_when_counts_balance() -> None:
    result = checks.check_count_identity(candidate_count=4, admitted_count=2, violations_count=2)

    assert result.ok
    assert result.candidate_count == 4
    assert result.admitted_count == 2
    assert result.violations_count == 2


def test_check_count_identity_reports_mismatch_as_data_not_an_exception() -> None:
    result = checks.check_count_identity(candidate_count=4, admitted_count=2, violations_count=3)

    assert not result.ok
