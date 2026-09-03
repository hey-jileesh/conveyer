"""`frames.checks` — I-12 violation/hash subtraction, [C-8], §7.5, §8.2;
005.1 §6.1-§6.6's compiled-checks half (`compile_contract`/`evaluate`/
`zero_failures`/`typed_projection`/`violations`/`locator_subtraction`),
bead conveyer-azr.14/azr.19."""

from __future__ import annotations

import json

from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from spine.core.model import ColumnSpec, FactColumnSpec, FactSchemaModel, RawContractModel
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


# `violation_subtraction` is RETIRED (006.1 P-7(c)/§8.1, bead conveyer-6pg.13,
# B3) -- its own coverage (multiplicity-preserving bag subtraction, [C-8])
# is removed here alongside it; see `frames/checks.py`'s own module
# docstring for the full retirement rationale.

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


# =============================================================================
# 005.1 §6.1-§6.5 — compile_contract / evaluate / typed_projection /
# violations / locator_subtraction (bead conveyer-azr.14, n1-checks)
# =============================================================================


def _raw_schema(declared: list[str], *, with_locators: bool = False) -> StructType:
    fields: list[StructField] = []
    if with_locators:
        fields += [
            StructField("source_uri", StringType(), False),
            StructField("object_seq", IntegerType(), False),
            StructField("row_index", IntegerType(), False),
        ]
    fields.append(StructField("malformed_text", StringType(), True))
    fields += [StructField(name, StringType(), True) for name in declared]
    fields.append(StructField("extras", MapType(StringType(), StringType()), False))
    return StructType(fields)


def _reason_detail_entries(reason_detail: str) -> list[dict]:
    return json.loads(reason_detail)


# --- compile_contract: normative evaluation order (§6.1) --------------------


def test_compile_contract_evaluation_order_is_row_major_column_minor(
    spark: SparkSession,
) -> None:
    """`spark` is requested but unused beyond forcing the session-scoped
    fixture into existence BEFORE `compile_contract` runs (conveyer-swb.22
    F-1): `compile_contract` builds `F.col(...)` expressions internally
    (`checks.py::_typed_expr`), which needs an active `SparkContext` despite
    never executing a DataFrame action — see `compile_contract`'s own
    docstring. Without this, the test only passes when some OTHER test in
    the same run already brought Spark up first (fixture-ordering coupling,
    not a real fixture dependency); every other Spark-touching test in this
    suite already declares `spark` itself for the identical reason."""
    contract = RawContractModel(
        columns=[
            ColumnSpec(name="a", type="int", nullable=False, min="1", max="10"),
            ColumnSpec(name="b", type="decimal(5,2)", allowed_values=["1.00", "2.00"]),
            ColumnSpec(name="c", type="string", pattern="[A-Z]+"),
        ]
    )

    compiled = checks.compile_contract(contract)

    ids = [entry.check_id for entry in compiled.entries]
    assert ids == [
        "malformed-row",
        "encoding-suspect",
        "cast:int",  # check 3: non-string columns only, contract order (b before... a is int)
        "cast:decimal(5,2)",
        "not-nullable",  # check 4: only "a" declares nullable:false
        "allowed-values",  # check 5: only "b"
        "pattern",  # check 6: only "c"
        "bounds",  # check 7: only "a" declares min/max
    ]
    # column-minor within check 3 follows contract order (a, b) -- c is
    # string, skipped entirely (identity typed-expr never fails).
    cast_entries = [e for e in compiled.entries if e.check_id.startswith("cast:")]
    assert [e.column for e in cast_entries] == ["a", "b"]


def test_compile_contract_omits_encoding_suspect_when_opted_out(spark: SparkSession) -> None:
    """`spark` forces the session-scoped fixture up first -- see
    `test_compile_contract_evaluation_order_is_row_major_column_minor`'s
    own docstring (conveyer-swb.22 F-1)."""
    contract = RawContractModel(columns=[ColumnSpec(name="a")], forbid_replacement_chars=False)

    compiled = checks.compile_contract(contract)

    assert "encoding-suspect" not in [e.check_id for e in compiled.entries]
    assert compiled.entries[0].check_id == "malformed-row"


def test_compile_contract_declared_columns_is_contract_order(spark: SparkSession) -> None:
    """`spark` forces the session-scoped fixture up first -- see
    `test_compile_contract_evaluation_order_is_row_major_column_minor`'s
    own docstring (conveyer-swb.22 F-1)."""
    contract = RawContractModel(columns=[ColumnSpec(name="z"), ColumnSpec(name="a")])

    compiled = checks.compile_contract(contract)

    assert compiled.declared_columns == ("z", "a")


# --- evaluate / typed_projection / violations: end-to-end (§6.2-§6.4) -------


def _identity_contract() -> RawContractModel:
    return RawContractModel(
        columns=[
            ColumnSpec(name="domain_id", required=True, nullable=False),
            ColumnSpec(name="amount", type="decimal(5,2)", min="0.00", max="100.00"),
            ColumnSpec(name="qty", type="int", min="1", max="10"),
            ColumnSpec(name="status", allowed_values=["open", "closed"]),
            ColumnSpec(name="code", pattern="[A-Z]{3}"),
            ColumnSpec(
                name="event_date", type="date(yyyy-MM-dd)", min="2020-01-01", max="2029-12-31"
            ),
        ]
    )


def _identity_rows(spark: SparkSession) -> DataFrame:
    declared = ["domain_id", "amount", "qty", "status", "code", "event_date"]
    schema = _raw_schema(declared)
    rows = [
        # 1: clean row
        (None, "d1", "50.00", "5", "open", "ABC", "2024-06-01", {}),
        # 2: malformed row (also co-occurs with not-nullable on domain_id, A-13)
        ("bad,line", None, None, None, None, None, None, {}),
        # 3: cast failure (amount)
        (None, "d2", "abc", "5", "open", "ABC", "2024-06-01", {}),
        # 4: not-nullable violation
        (None, None, "50.00", "5", "open", "ABC", "2024-06-01", {}),
        # 5: allowed-values violation
        (None, "d3", "50.00", "5", "weird", "ABC", "2024-06-01", {}),
        # 6: pattern violation
        (None, "d4", "50.00", "5", "open", "abc", "2024-06-01", {}),
        # 7: bounds violation
        (None, "d5", "999.00", "5", "open", "ABC", "2024-06-01", {}),
        # 8: multi-failure row: cast(amount) + not-nullable(domain_id) + pattern(code)
        (None, None, "abc", "5", "open", "abc", "2024-06-01", {}),
        # 9: U+FFFD in a declared column value
        (None, "d�", "50.00", "5", "open", "ABC", "2024-06-01", {}),
        # 10: U+FFFD in an extras KEY (mojibake header token, [DC-9])
        (None, "d6", "50.00", "5", "open", "ABC", "2024-06-01", {"�key": "v"}),
    ]
    return spark.createDataFrame(rows, schema=schema)


def test_evaluate_typed_projection_violations_end_to_end(spark: SparkSession) -> None:
    contract = _identity_contract()
    compiled = checks.compile_contract(contract)
    raw_df = _identity_rows(spark)

    # the STAGE's job on the genuinely-fresh path (§6.6, not this module's):
    # filter `evaluate`'s output to zero failures BEFORE `typed_projection`.
    evaluated = checks.evaluate(raw_df, compiled)
    valid_typed = checks.typed_projection(checks.zero_failures(evaluated), compiled)
    viol_df = checks.violations(evaluated)

    # exactly one clean row admitted, fully typed
    admitted_rows = valid_typed.collect()
    assert len(admitted_rows) == 1
    admitted = admitted_rows[0].asDict()
    assert admitted["domain_id"] == "d1"
    assert str(admitted["amount"]) == "50.00"
    assert admitted["qty"] == 5
    assert admitted["status"] == "open"
    assert admitted["code"] == "ABC"
    assert str(admitted["event_date"]) == "2024-06-01"

    # count identity: raw_count == |valid| + |violations| (§6.3/§6.6)
    assert raw_df.count() == valid_typed.count() + viol_df.count()
    assert viol_df.count() == 9

    by_reason = {
        r["domain_id"]: r
        for r in viol_df.select("domain_id", "reason_code", "reason_detail").collect()
    }
    # row 2 (malformed): malformed-row primary, not-nullable co-occurs (A-13)
    malformed_row = next(r for r in viol_df.collect() if r["malformed_text"] == "bad,line")
    assert malformed_row["reason_code"] == "unreadable/malformed-row"
    detail = _reason_detail_entries(malformed_row["reason_detail"])
    assert [d["code"] for d in detail] == ["unreadable/malformed-row", "contract/null-violation"]

    assert by_reason["d2"]["reason_code"] == "contract/cast-failure"
    assert by_reason["d3"]["reason_code"] == "contract/value-not-allowed"
    assert by_reason["d4"]["reason_code"] == "contract/pattern-mismatch"
    assert by_reason["d5"]["reason_code"] == "contract/out-of-bounds"

    multi = next(
        r
        for r in viol_df.collect()
        if r["qty"] == "5" and r["domain_id"] is None and r["code"] == "abc"
    )
    multi_detail = _reason_detail_entries(multi["reason_detail"])
    assert [d["code"] for d in multi_detail] == [
        "contract/cast-failure",
        "contract/null-violation",
        "contract/pattern-mismatch",
    ]

    ufffd_rows = [r for r in viol_df.collect() if r["reason_code"] == "unreadable/encoding-suspect"]
    assert len(ufffd_rows) == 2  # row 9 (declared value) and row 10 (extras key)


def test_typed_projection_declared_columns_only_no_locators_no_extras(spark: SparkSession) -> None:
    contract = RawContractModel(columns=[ColumnSpec(name="a"), ColumnSpec(name="b", type="int")])
    compiled = checks.compile_contract(contract)
    schema = _raw_schema(["a", "b"], with_locators=True)
    raw_df = spark.createDataFrame([("s3://x/f.csv", 1, 1, None, "hello", "42", {})], schema=schema)

    out = checks.typed_projection(raw_df, compiled)

    assert out.columns == ["a", "b"]
    row = out.collect()[0]
    assert row["a"] == "hello"
    assert row["b"] == 42


def test_typed_projection_never_filters_by_failure_state_rerun_semantics(
    spark: SparkSession,
) -> None:
    """005.1 §6.5: on a rerun door, a row whose cell fails the CURRENT
    contract's cast stays in `valid_df` with a NULL cell -- recorded, never
    dropped. `typed_projection` must therefore admit ALL rows handed to it,
    including ones with unparseable cells (the caller decides what subset
    of rows to pass in; this function never filters)."""
    contract = RawContractModel(columns=[ColumnSpec(name="a", type="int")])
    compiled = checks.compile_contract(contract)
    schema = _raw_schema(["a"])
    raw_df = spark.createDataFrame([(None, "not-an-int", {})], schema=schema)

    out = checks.typed_projection(raw_df, compiled)

    rows = out.collect()
    assert len(rows) == 1  # NOT dropped
    assert rows[0]["a"] is None  # cast failure -> NULL cell, recorded


# --- bounds: universal quarantine on an invalid boundary (pinned obligation #1) --


def test_bounds_check_unparseable_temporal_min_flags_every_row(spark: SparkSession) -> None:
    contract = RawContractModel(
        columns=[ColumnSpec(name="d", type="date(yyyy-MM-dd)", min="2024-02-30", max="2029-12-31")]
    )
    compiled = checks.compile_contract(contract)
    schema = _raw_schema(["d"])
    raw_df = spark.createDataFrame(
        [(None, "2024-06-01", {}), (None, "2025-01-01", {})], schema=schema
    )

    evaluated = checks.evaluate(raw_df, compiled)
    viol_df = checks.violations(evaluated)

    assert viol_df.count() == raw_df.count()
    assert all(r["reason_code"] == "contract/out-of-bounds" for r in viol_df.collect())


def test_bounds_check_temporal_min_greater_than_max_flags_every_row(spark: SparkSession) -> None:
    """`core/model.py` does NOT check temporal min<=max ordering (only
    int/long/decimal) -- this is exactly the gap 005.1's pinned obligation
    #1 assigns to `compile_contract`."""
    contract = RawContractModel(
        columns=[ColumnSpec(name="d", type="date(yyyy-MM-dd)", min="2029-12-31", max="2020-01-01")]
    )
    compiled = checks.compile_contract(contract)
    schema = _raw_schema(["d"])
    raw_df = spark.createDataFrame(
        [(None, "2024-06-01", {}), (None, "2025-01-01", {})], schema=schema
    )

    evaluated = checks.evaluate(raw_df, compiled)
    viol_df = checks.violations(evaluated)

    assert viol_df.count() == raw_df.count()
    assert all(r["reason_code"] == "contract/out-of-bounds" for r in viol_df.collect())


def test_bounds_check_valid_boundaries_admit_in_range_values(spark: SparkSession) -> None:
    contract = RawContractModel(columns=[ColumnSpec(name="n", type="int", min="1", max="10")])
    compiled = checks.compile_contract(contract)
    schema = _raw_schema(["n"])
    raw_df = spark.createDataFrame(
        [(None, "0", {}), (None, "5", {}), (None, "10", {}), (None, "11", {})], schema=schema
    )

    evaluated = checks.evaluate(raw_df, compiled)
    valid = checks.typed_projection(checks.zero_failures(evaluated), compiled)

    assert sorted(r["n"] for r in valid.collect()) == [5, 10]


# --- violations: reason shaping, truncation at 32 (§6.4) ---------------------


def test_violations_reason_detail_truncates_at_32_with_marker(spark: SparkSession) -> None:
    columns = [ColumnSpec(name=f"c{i}", type="int", nullable=False) for i in range(33)]
    contract = RawContractModel(columns=columns)
    compiled = checks.compile_contract(contract)
    schema = _raw_schema([c.name for c in columns])
    row = tuple([None, *([None] * 33), {}])
    raw_df = spark.createDataFrame([row], schema=schema)

    evaluated = checks.evaluate(raw_df, compiled)
    viol_df = checks.violations(evaluated)

    detail = _reason_detail_entries(viol_df.collect()[0]["reason_detail"])
    assert len(detail) == 33  # 32 real entries + 1 truncation marker
    assert detail[-1] == {"truncated": 33}
    assert all("code" in entry for entry in detail[:-1])


def test_violations_reason_detail_has_no_truncation_marker_under_the_limit(
    spark: SparkSession,
) -> None:
    contract = RawContractModel(columns=[ColumnSpec(name="a", nullable=False)])
    compiled = checks.compile_contract(contract)
    schema = _raw_schema(["a"])
    raw_df = spark.createDataFrame([(None, None, {})], schema=schema)

    evaluated = checks.evaluate(raw_df, compiled)
    viol_df = checks.violations(evaluated)

    detail = _reason_detail_entries(viol_df.collect()[0]["reason_detail"])
    assert all("truncated" not in entry for entry in detail)


def test_violations_drops_the_internal_failures_column(spark: SparkSession) -> None:
    contract = RawContractModel(columns=[ColumnSpec(name="a", nullable=False)])
    compiled = checks.compile_contract(contract)
    schema = _raw_schema(["a"])
    raw_df = spark.createDataFrame([(None, None, {})], schema=schema)

    viol_df = checks.violations(checks.evaluate(raw_df, compiled))

    assert checks._FAILURES_COL not in viol_df.columns


# --- Property test: evaluation-order first-failure (§12.4) ------------------

# A contract with one check per §6.1 kind, so a generated multi-violation
# row can independently trip any subset of {not-nullable, allowed-values,
# pattern, bounds} (checks 4-7) -- checks 1-3 stay off (no malformed rows,
# no cast failures: every generated cell is a valid int string) so the
# property isolates evaluation-ORDER, not check applicability.
_ORDERED_CHECK_COLUMNS = ["not_nullable_col", "allowed_values_col", "pattern_col", "bounds_col"]


def _property_contract() -> RawContractModel:
    return RawContractModel(
        columns=[
            ColumnSpec(name="not_nullable_col", type="int", nullable=False),
            ColumnSpec(name="allowed_values_col", type="int", allowed_values=["1", "2"]),
            ColumnSpec(name="pattern_col", type="int", pattern="[13579]"),
            ColumnSpec(name="bounds_col", type="int", min="100", max="200"),
        ]
    )


@given(
    violate=st.lists(st.booleans(), min_size=4, max_size=4),
)
@settings(max_examples=40, deadline=None)
def test_evaluation_order_first_failure_over_generated_multi_violation_rows(
    spark: SparkSession, violate: list[bool]
) -> None:
    """`reason_code` is always the FIRST check (in §6.1's normative order)
    that the generated row actually violates -- for any subset of the 4
    independently-triggerable checks in `_property_contract`."""
    contract = _property_contract()
    compiled = checks.compile_contract(contract)
    # values chosen so `violate[i]` toggles EXACTLY check i and nothing else:
    # not_nullable: None violates, "5" doesn't; allowed_values: "5" violates
    # (not in {1,2}), "1" doesn't; pattern: "2" violates ([13579] rejects
    # even digits), "1" doesn't; bounds: "5" violates (outside [100,200]),
    # "150" doesn't.
    not_nullable_val = None if violate[0] else "5"
    allowed_values_val = "5" if violate[1] else "1"
    pattern_val = "2" if violate[2] else "1"
    bounds_val = "5" if violate[3] else "150"
    schema = _raw_schema(_ORDERED_CHECK_COLUMNS)
    raw_df = spark.createDataFrame(
        [(None, not_nullable_val, allowed_values_val, pattern_val, bounds_val, {})], schema=schema
    )

    evaluated = checks.evaluate(raw_df, compiled)
    viol_df = checks.violations(evaluated)
    rows = viol_df.collect()

    if not any(violate):
        assert len(rows) == 0
        return

    expected_first = [
        "contract/null-violation",
        "contract/value-not-allowed",
        "contract/pattern-mismatch",
        "contract/out-of-bounds",
    ]
    first_violated_index = next(i for i, v in enumerate(violate) if v)
    assert rows[0]["reason_code"] == expected_first[first_violated_index]
    detail = _reason_detail_entries(rows[0]["reason_detail"])
    detail_codes = [d["code"] for d in detail]
    expected_codes = [expected_first[i] for i, v in enumerate(violate) if v]
    assert detail_codes == expected_codes


# --- locator_subtraction: §6.5's pure anti-join half -------------------------


def test_locator_subtraction_anti_joins_durable_and_filters_malformed(
    spark: SparkSession,
) -> None:
    schema = _raw_schema([], with_locators=True)
    raw_df = spark.createDataFrame(
        [
            ("s3://x/a.csv", 1, 1, None, {}),
            ("s3://x/a.csv", 1, 2, None, {}),
            ("s3://x/a.csv", 1, 3, "bad", {}),  # malformed -- excluded regardless
            ("s3://x/a.csv", 1, 4, None, {}),
        ],
        schema=schema,
    )
    locator_schema = StructType(
        [
            StructField("source_uri", StringType(), False),
            StructField("object_seq", IntegerType(), False),
            StructField("row_index", IntegerType(), False),
        ]
    )
    durable_q = spark.createDataFrame([("s3://x/a.csv", 1, 2)], schema=locator_schema)

    out = checks.locator_subtraction(raw_df, durable_q)

    assert sorted(r["row_index"] for r in out.collect()) == [1, 4]


def test_locator_subtraction_tolerates_extra_columns_on_durable_locators(
    spark: SparkSession,
) -> None:
    """§6.5: `durable_q` is `read_batch(quarantine, batch_id)` projected to
    the locator triple by the CALLER -- but `locator_subtraction` defensively
    re-projects too, so a caller passing the full quarantine row shape still
    works."""
    schema = _raw_schema([], with_locators=True)
    raw_df = spark.createDataFrame([("s3://x/a.csv", 1, 1, None, {})], schema=schema)
    full_quarantine_schema = StructType(
        [
            StructField("source_uri", StringType(), True),
            StructField("object_seq", IntegerType(), True),
            StructField("row_index", IntegerType(), True),
            StructField("reason_code", StringType(), False),
        ]
    )
    durable_q = spark.createDataFrame(
        [("s3://x/a.csv", 1, 1, "contract/null-violation")], schema=full_quarantine_schema
    )

    out = checks.locator_subtraction(raw_df, durable_q)

    assert out.count() == 0


# --- zero_failures: §6.6's fresh-path stage-level filter --------------------


def test_zero_failures_keeps_only_rows_with_an_empty_failures_array(spark: SparkSession) -> None:
    contract = RawContractModel(columns=[ColumnSpec(name="a", required=True, nullable=False)])
    compiled = checks.compile_contract(contract)
    schema = _raw_schema(["a"])
    raw_df = spark.createDataFrame([(None, "x", {}), (None, None, {})], schema=schema)

    evaluated = checks.evaluate(raw_df, compiled)
    clean = checks.zero_failures(evaluated)

    assert clean.count() == 1
    assert clean.collect()[0]["a"] == "x"


def test_zero_failures_never_drops_the_internal_column_before_typed_projection(
    spark: SparkSession,
) -> None:
    """`typed_projection` is a plain `select` that never references the
    internal failures column either way -- `zero_failures`'s own output can
    feed straight into it without an extra `.drop()`."""
    contract = RawContractModel(columns=[ColumnSpec(name="a")])
    compiled = checks.compile_contract(contract)
    schema = _raw_schema(["a"])
    raw_df = spark.createDataFrame([(None, "x", {})], schema=schema)

    evaluated = checks.evaluate(raw_df, compiled)
    clean = checks.zero_failures(evaluated)
    valid = checks.typed_projection(clean, compiled)

    assert valid.columns == ["a"]
    assert valid.collect()[0]["a"] == "x"


# --- hash_subtraction: post_check's §8.2.4 guard-skip rerun mechanism -------


# 006.1 §8.3 (bead conveyer-6pg.13, B3): `candidate_row_hash` now needs a
# `fact_type` + `FactSchemaModel` (the tag, P-7(b)) -- one shared 3-column
# schema (`domain_id`/`n`/`payload`, matching every candidate frame below;
# `hash_subtraction` itself only ever reads `row_hash`, so an extra
# undeclared `payload` column on the 2-column tests is harmless).
_HASH_FACT_TYPE = "t"
_HASH_SCHEMA = FactSchemaModel(
    columns=[
        FactColumnSpec(name="domain_id", type="string"),
        FactColumnSpec(name="n", type="int"),
        FactColumnSpec(name="payload", type="string"),
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
)


def test_hash_subtraction_removes_only_the_durably_hashed_rows(spark: SparkSession) -> None:
    from spine.frames import quarantine

    candidate = spark.createDataFrame(
        [("a", 1, "p1"), ("b", 2, "p2"), ("c", 3, "p3")], ["domain_id", "n", "payload"]
    )
    hashed_candidate = quarantine.candidate_row_hash(candidate, _HASH_FACT_TYPE, _HASH_SCHEMA)
    b_hash = hashed_candidate.filter(F.col("domain_id") == "b").select("row_hash").collect()[0][0]
    durable = spark.createDataFrame([(b_hash,)], ["row_hash"])

    admitted = checks.hash_subtraction(hashed_candidate, durable)

    assert sorted(r["domain_id"] for r in admitted.collect()) == ["a", "c"]
    assert "row_hash" not in admitted.columns  # dropped -- admitted keeps candidate's own shape


def test_hash_subtraction_no_durable_hashes_admits_everything(spark: SparkSession) -> None:
    from spine.frames import quarantine

    candidate = spark.createDataFrame([("a", 1, "p1")], ["domain_id", "n", "payload"])
    hashed_candidate = quarantine.candidate_row_hash(candidate, _HASH_FACT_TYPE, _HASH_SCHEMA)
    durable = spark.createDataFrame([], schema=StructType([StructField("row_hash", StringType())]))

    admitted = checks.hash_subtraction(hashed_candidate, durable)

    assert sorted(r["domain_id"] for r in admitted.collect()) == ["a"]


def test_hash_subtraction_tolerates_extra_columns_on_durable_row_hashes_frame(
    spark: SparkSession,
) -> None:
    """Mirrors `locator_subtraction`'s own "tolerates extra columns" test --
    `durable_row_hashes_df` is typically a full read-back quarantine frame,
    only its `row_hash` column is read here."""
    from spine.frames import quarantine

    candidate = spark.createDataFrame(
        [("a", 1, "p1"), ("b", 2, "p2")], ["domain_id", "n", "payload"]
    )
    hashed_candidate = quarantine.candidate_row_hash(candidate, _HASH_FACT_TYPE, _HASH_SCHEMA)
    a_hash = hashed_candidate.filter(F.col("domain_id") == "a").select("row_hash").collect()[0][0]
    durable_full = spark.createDataFrame([(a_hash, "business/x")], ["row_hash", "reason_code"])

    admitted = checks.hash_subtraction(hashed_candidate, durable_full)

    assert sorted(r["domain_id"] for r in admitted.collect()) == ["b"]
