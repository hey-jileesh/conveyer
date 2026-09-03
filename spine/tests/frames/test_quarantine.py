"""`frames.quarantine` — the 005.1 §8.1 rewrite: `shape_pre_quarantine`/
`shape_post_quarantine`, `candidate_row_hash` (§8.2.4, bead conveyer-azr.19),
the §7.3 UDF seam, and [DC-3]'s in-plan timestamp rendering (bead
conveyer-azr.15, n1-quarantine). A-09: every `pre-check-snapshot.json`/
`post-check-snapshot.json` canonical-json vector reproduces byte-exact
THROUGH the shaper path (not just `canonical_json` directly, already covered
by `tests/unit/test_canonical.py`); both §7.1 snapshot structures; `row_hash`
stability across repeated runs; [DC-3]'s in-plan rendering exercised with a
real `TimestampType` column.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spine.core import canonical, identity
from spine.core.model import FactColumnSpec, FactSchemaModel, LineageStamp
from spine.frames import quarantine

_STAMP = LineageStamp(
    batch_id="b1", delivery_id="d1", feed_id="f1", received_at=datetime(2026, 1, 1, tzinfo=UTC)
)
_QAT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

_FIXTURES_DIR = Path(__file__).resolve().parents[3] / "contracts" / "fixtures" / "canonical-json"

_QUARANTINE_COLUMNS = {
    "batch_id",
    "delivery_id",
    "feed_id",
    "check_stage",
    "source_uri",
    "object_seq",
    "row_index",
    "domain_id",
    "record_key",
    "row_hash",
    "reason_code",
    "reason_detail",
    "check_version",
    "quarantined_at",
    "row_snapshot",
}

# §6.4-shaped pre_check violation frame: durable raw columns (locators,
# lineage, declared string columns `domain_id`/`amount`/`status`, `extras`,
# `malformed_text`) plus `reason_code`/`reason_detail` -- matches
# `pre-check-snapshot.json`'s two vectors exactly (both are `{domain_id,
# amount, status, extras, malformed_text}` snapshots), built by hand per
# this bead's brief ("no dependency on n1-checks code").
_PRE_VIOL_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("delivery_id", StringType(), False),
        StructField("feed_id", StringType(), False),
        StructField("received_at", TimestampType(), False),
        StructField("source_uri", StringType(), False),
        StructField("object_seq", IntegerType(), False),
        StructField("row_index", LongType(), False),
        StructField("read_spec_version", StringType(), False),
        StructField("malformed_text", StringType(), True),
        StructField("domain_id", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("extras", MapType(StringType(), StringType()), False),
        StructField("reason_code", StringType(), False),
        StructField("reason_detail", StringType(), True),
    ]
)


def _pre_viol_row(vec_value: dict[str, Any], object_seq: int, row_index: int) -> tuple[Any, ...]:
    return (
        "b1",
        "d1",
        "f1",
        datetime(2026, 1, 1, tzinfo=UTC),
        "s3://bucket/obj1.csv",
        object_seq,
        row_index,
        "rsv-hash",
        vec_value["malformed_text"],
        vec_value["domain_id"],
        vec_value["amount"],
        vec_value["status"],
        vec_value["extras"],
        "unreadable/malformed-row" if vec_value["malformed_text"] else "contract/null-violation",
        '[{"code":"contract/null-violation","column":"domain_id","expected":"non-null"}]',
    )


def _pre_vectors() -> list[dict[str, Any]]:
    return json.loads((_FIXTURES_DIR / "pre-check-snapshot.json").read_text())


def _post_vectors() -> list[dict[str, Any]]:
    return json.loads((_FIXTURES_DIR / "post-check-snapshot.json").read_text())


def _post_vectors_tagged() -> list[dict[str, Any]]:
    # 006.1 §16.3 item 2's addendum (bead conveyer-6pg.14, B4): the ONE
    # committed vector in `post-check-snapshot.json` predates the
    # `_conveyer_fact_type` tag (P-7(b)) -- `post-check-snapshot-tagged.json`
    # is the NEW post-structure vector family, whose `value` already carries
    # the tag, so it byte-reproduces through the REAL shaper directly (see
    # `test_shape_post_quarantine_reproduces_a_tagged_post_structure_vector_
    # byte_exact` below), unlike the legacy vector's own workaround test.
    return json.loads((_FIXTURES_DIR / "post-check-snapshot-tagged.json").read_text())


# ============================================================================
# §8.1 rewrite: shape_pre_quarantine -- §7.1 pre snapshot, §7.3 UDF seam
# ============================================================================


def test_shape_pre_quarantine_produces_exactly_the_442_columns(spark: SparkSession) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, 1)], _PRE_VIOL_SCHEMA)

    shaped = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT)

    assert set(shaped.columns) == _QUARANTINE_COLUMNS


@pytest.mark.parametrize("vector_index", [0, 1], ids=["typed-row", "malformed-row"])
def test_shape_pre_quarantine_reproduces_the_committed_snapshot_vector_byte_exact(
    spark: SparkSession, vector_index: int
) -> None:
    # A-09: `pre-check-snapshot.json`'s two vectors reproduce byte-exact
    # THROUGH the shaper, not just via a direct `canonical_json` call.
    vec = _pre_vectors()[vector_index]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, vector_index + 1)], _PRE_VIOL_SCHEMA)

    shaped = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT)
    row = shaped.collect()[0]

    assert row["row_snapshot"] == vec["canonical"]
    assert row["row_hash"] == vec["sha256"]


def test_shape_pre_quarantine_domain_id_and_record_key_are_null(spark: SparkSession) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, 1)], _PRE_VIOL_SCHEMA)

    row = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]

    assert row["domain_id"] is None
    assert row["record_key"] is None


def test_shape_pre_quarantine_locators_pass_through_and_check_stage_is_literal(
    spark: SparkSession,
) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 3, 42)], _PRE_VIOL_SCHEMA)

    row = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]

    assert row["source_uri"] == "s3://bucket/obj1.csv"
    assert row["object_seq"] == 3
    assert row["row_index"] == 42
    assert row["check_stage"] == "pre_check"
    assert row["batch_id"] == "b1"
    assert row["delivery_id"] == "d1"
    assert row["feed_id"] == "f1"
    assert row["check_version"] == "cv-hash"


def test_shape_pre_quarantine_reason_code_and_detail_pass_through(spark: SparkSession) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, 1)], _PRE_VIOL_SCHEMA)

    row = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]

    assert row["reason_code"] == "contract/null-violation"
    assert row["reason_detail"] == (
        '[{"code":"contract/null-violation","column":"domain_id","expected":"non-null"}]'
    )


@pytest.mark.parametrize(
    "missing_column", ["extras", "malformed_text", "reason_code", "reason_detail"]
)
def test_shape_pre_quarantine_rejects_viol_df_missing_a_required_column(
    spark: SparkSession, missing_column: str
) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, 1)], _PRE_VIOL_SCHEMA).drop(
        missing_column
    )

    with pytest.raises(ValueError, match="missing §6.4-shaped column"):
        quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT)


def test_shape_pre_quarantine_row_hash_stable_across_repeated_runs(spark: SparkSession) -> None:
    vec = _pre_vectors()[0]
    df = spark.createDataFrame([_pre_viol_row(vec["value"], 1, 1)], _PRE_VIOL_SCHEMA)

    hash_1 = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]["row_hash"]
    hash_2 = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]["row_hash"]

    assert hash_1 == hash_2 == vec["sha256"]


def test_shape_pre_quarantine_declared_columns_derived_structurally(
    spark: SparkSession,
) -> None:
    # A declared column set different from the fixture vector's -- proves
    # the shaper derives "declared" by exclusion (FRAMEWORK_RAW_COLUMNS +
    # reason columns), not from any hardcoded name list.
    schema = StructType(
        [
            StructField("batch_id", StringType(), False),
            StructField("delivery_id", StringType(), False),
            StructField("feed_id", StringType(), False),
            StructField("received_at", TimestampType(), False),
            StructField("source_uri", StringType(), False),
            StructField("object_seq", IntegerType(), False),
            StructField("row_index", LongType(), False),
            StructField("read_spec_version", StringType(), False),
            StructField("malformed_text", StringType(), True),
            StructField("widget_code", StringType(), True),
            StructField("extras", MapType(StringType(), StringType()), False),
            StructField("reason_code", StringType(), False),
            StructField("reason_detail", StringType(), True),
        ]
    )
    row = (
        "b1",
        "d1",
        "f1",
        datetime(2026, 1, 1, tzinfo=UTC),
        "s3://bucket/o.csv",
        1,
        1,
        "rsv",
        None,
        "WZ-1",
        {},
        "contract/pattern-mismatch",
        "[]",
    )
    df = spark.createDataFrame([row], schema)

    shaped_row = quarantine.shape_pre_quarantine(df, _STAMP, "cv-hash", _QAT).collect()[0]

    expected_value = {"widget_code": "WZ-1", "extras": {}, "malformed_text": None}
    assert shaped_row["row_snapshot"] == canonical.canonical_json(expected_value)
    assert shaped_row["row_hash"] == canonical.row_hash(expected_value)


# ============================================================================
# 006.1 §10 REWRITE: shape_post_quarantine -- tag + record_key + reason
# passthrough, [DC-3] over the full snapshot subject, **G-11**
# ============================================================================


@pytest.fixture
def _utc_session_tz(spark: SparkSession):
    """[DC-3]'s in-plan `F.date_format` rendering DOES honor
    `spark.sql.session.timeZone` (that is exactly why it closes the UDF-
    boundary hazard, module docstring) — a test-local UTC pin, restored
    afterwards so it never leaks into other tests sharing the session-scoped
    `spark` fixture. (`tests/conftest.py`'s own `_BASE_CONF` already carries
    `**SESSION_PINS`, so this is redundant-but-harmless belt-and-braces —
    kept so this file stays self-sufficient if that wiring ever moves.)
    """
    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        yield
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)


# One fact type, one non-key timestamp column (`event_time`) -- G-11's own
# fixture obligation [EM-7]: "fixtures include a fact type with a non-key
# timestamp column" (`record_key` below is `domain_id` alone).
_ORDERS_SCHEMA = FactSchemaModel(
    columns=[
        {"name": "domain_id", "type": "string"},
        {"name": "amount", "type": "decimal(10,2)"},
        {"name": "event_time", "type": "timestamp"},
        {"name": "quantity", "type": "int"},
        {"name": "is_active", "type": "bool"},
        {"name": "note", "type": "string"},
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
    ordering=["event_time"],
)
_ORDERS_FACT_TYPE = "orders"

_POST_VIOL_SCHEMA = StructType(
    [
        StructField("domain_id", StringType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("event_time", TimestampType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("note", StringType(), True),
        StructField("reason_code", StringType(), False),
        StructField("reason_detail", StringType(), True),
    ]
)


def _post_viol_typed_value(vec_value: dict[str, Any]) -> dict[str, Any]:
    """This file's OWN untagging parser (007.1 §5.3/§5.4's convention: every
    consumer writes its own, never shares code) over `post-check-snapshot.
    json`'s tagged-JSON `value` -- the SAME conversion `_post_viol_row`
    performs to build a Spark row tuple, reused here to build a plain
    Python dict for a DIRECT `canonical.canonical_json`/`row_hash` call
    (the independent cross-check A-09 needs, never the shaper's own output
    compared against itself)."""
    return {
        "domain_id": vec_value["domain_id"],
        "amount": Decimal(vec_value["amount"]["$decimal"]),
        "event_time": datetime.fromisoformat(vec_value["event_time"]["$timestamp"]),
        "quantity": vec_value["quantity"],
        "is_active": vec_value["is_active"],
        "note": vec_value["note"],
    }


def _post_viol_row(
    vec_value: dict[str, Any], reason_code: str, reason_detail: str | None = None
) -> tuple[Any, ...]:
    return (
        vec_value["domain_id"],
        Decimal(vec_value["amount"]["$decimal"]),
        datetime.fromisoformat(vec_value["event_time"]["$timestamp"]),
        vec_value["quantity"],
        vec_value["is_active"],
        vec_value["note"],
        reason_code,
        reason_detail,
    )


def _post_viol_df(spark: SparkSession, rows: list[tuple[Any, ...]]) -> Any:
    return spark.createDataFrame(rows, _POST_VIOL_SCHEMA)


def test_shape_post_quarantine_produces_exactly_the_442_columns(spark: SparkSession) -> None:
    vec = _post_vectors()[0]
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], "business/negative-amount")])

    shaped = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
    )

    assert set(shaped.columns) == _QUARANTINE_COLUMNS


def test_shape_post_quarantine_snapshot_carries_the_fact_type_tag_and_reproduces_canonical_json(
    spark: SparkSession, _utc_session_tz: None
) -> None:
    """A-09 + [DC-3] + P-7(b): the committed `post-check-snapshot.json`
    vector's `value` carries no `_conveyer_fact_type` key (a 005.1-era
    vector, predating 006.1's tag rule) -- so the EXPECTED snapshot/hash
    here is the vector's `value` plus the tag, computed independently via
    `canonical.canonical_json`/`row_hash` (never a second implementation of
    the shaper's own logic), asserted against the REAL shaper's output. The
    vector's real `event_time` timestamp field still exercises the in-plan
    [DC-3] rendering path through the actual shaper.

    **006.1 §16.3 item 2 addendum, disposition ruling (bead conveyer-6pg.14,
    B4):** this legacy vector's own `"canonical"`/`"sha256"` fields no
    longer byte-reproduce through `shape_post_quarantine` DIRECTLY (that is
    exactly the gap this test's own tag-adding workaround above closes) --
    ruled **keep-as-is, not deprecated**: `test_canonical.py`'s blanket
    `test_canonical_json_reproduces_every_committed_vector` still reproduces
    it byte-exact via a bare `canonical.canonical_json`/`row_hash` call (no
    shaper involved, no tag expected -- that claim was never about the
    shaper), and this file's own `_post_viol_row`/`_post_viol_typed_value`
    helpers continue to consume its `value` as ordinary orders-shaped
    candidate-row DATA across a dozen other tests in this module, unrelated
    to whether its own `"canonical"`/`"sha256"` fields still name the
    shaper's output shape. The vector remains a valid, real canonical-JSON
    fixture; it is simply no longer a literal example of "what the post_check
    shaper produces" -- `post-check-snapshot-tagged.json` (below) is that
    family's new, going-forward member."""
    vec = _post_vectors()[0]
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], "business/negative-amount")])

    shaped = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
    )
    row = shaped.collect()[0]

    expected_value = {
        **_post_viol_typed_value(vec["value"]),
        "_conveyer_fact_type": _ORDERS_FACT_TYPE,
    }
    assert row["row_snapshot"] == canonical.canonical_json(expected_value)
    assert row["row_hash"] == canonical.row_hash(expected_value)


@pytest.mark.parametrize("vector_index,fact_type", [(0, "orders"), (1, "shipments")])
def test_shape_post_quarantine_reproduces_a_tagged_post_structure_vector_byte_exact(
    spark: SparkSession, _utc_session_tz: None, vector_index: int, fact_type: str
) -> None:
    """006.1 §16.3 item 2's addendum, the post-structure vector family
    itself (`contracts/fixtures/canonical-json/post-check-snapshot-tagged.
    json`, [DS-5] synthetic-only): unlike the legacy vector above, THIS
    vector's own `value` already carries `_conveyer_fact_type` -- so its
    committed `"canonical"`/`"sha256"` fields byte-reproduce through the
    REAL `shape_post_quarantine` shaper DIRECTLY, no manual tag-adding
    workaround needed. Parametrized over both committed entries (same
    payload, `orders` vs. `shipments`) so this test doubles as the fixture-
    grain confirmation of P-7(b)/G-03's tag-discrimination claim: two
    value-identical rows of different fact types reproduce two DIFFERENT
    committed hashes, exactly as `test_g11_cross_type_tag_discriminates_
    value_identical_rows` already pins at the live-shaper grain."""
    vec = _post_vectors_tagged()[vector_index]
    assert vec["value"]["_conveyer_fact_type"] == fact_type
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], "business/negative-amount")])

    shaped = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, fact_type, _ORDERS_SCHEMA
    )
    row = shaped.collect()[0]

    assert row["row_snapshot"] == vec["canonical"]
    assert row["row_hash"] == vec["sha256"]


def test_shape_post_quarantine_locators_null_reason_detail_passes_through(
    spark: SparkSession,
) -> None:
    vec = _post_vectors()[0]
    df = _post_viol_df(
        spark,
        [_post_viol_row(vec["value"], "business/negative-amount", '[{"id":"x","version":"v1"}]')],
    )

    row = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
    ).collect()[0]

    assert row["source_uri"] is None
    assert row["object_seq"] is None
    assert row["row_index"] is None
    assert row["check_stage"] == "post_check"
    assert row["reason_code"] == "business/negative-amount"
    assert row["reason_detail"] == '[{"id":"x","version":"v1"}]'


def test_shape_post_quarantine_domain_id_reads_the_declared_domain_id_col(
    spark: SparkSession,
) -> None:
    vec = _post_vectors()[0]
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], "business/negative-amount")])

    row = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
    ).collect()[0]

    assert row["domain_id"] == "abc-123"


def test_shape_post_quarantine_domain_id_null_when_the_value_is_null(spark: SparkSession) -> None:
    # The 005.1-era "column absent" case is no longer representable (006.1
    # §4.1 guarantees `domain_id_col` is always a declared column) -- what
    # remains is the per-ROW VALUE being NULL (the implicit-check row, D-6).
    row_tuple = (
        None,
        Decimal("1.00"),
        datetime(2026, 1, 1, tzinfo=UTC),
        1,
        False,
        "n",
        "business/missing-domain-id",
        None,
    )
    df = _post_viol_df(spark, [row_tuple])

    row = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
    ).collect()[0]

    assert row["domain_id"] is None


def test_shape_post_quarantine_rejects_viol_df_missing_a_declared_column(
    spark: SparkSession,
) -> None:
    vec = _post_vectors()[0]
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], "business/negative-amount")]).drop(
        "amount"
    )

    with pytest.raises(ValueError, match="missing declared column"):
        quarantine.shape_post_quarantine(
            df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
        )


def test_shape_post_quarantine_rejects_viol_df_missing_reason_columns(
    spark: SparkSession,
) -> None:
    vec = _post_vectors()[0]
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], "business/negative-amount")]).drop(
        "reason_code"
    )

    with pytest.raises(ValueError, match="missing declared column"):
        quarantine.shape_post_quarantine(
            df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
        )


@pytest.mark.parametrize(
    "reason_value",
    ["not-a-valid-reason", "business/a", "business/negative-amount"],
    ids=["no-namespace", "conforming-a", "conforming-negative-amount"],
)
def test_shape_post_quarantine_reason_code_passes_through_unchanged_no_grammar_check(
    spark: SparkSession, reason_value: str
) -> None:
    """006.1 §10: business-check reasons are bind-time-validated data (K6) --
    this shaper performs NO runtime reason-grammar check of its own (unlike
    005.1's interim A-14 mechanism); whatever `reason_code` value arrives
    lands unchanged (`reason_code` is `nullable=False` on a real evaluated
    violations frame, `frames/business_checks.py::business_violations` --
    every violating row carries at least one failed entry). `nonconforming_
    reasons` below is untouched by this rewrite -- kept for its own sake,
    no longer this shaper's own gate."""
    vec = _post_vectors()[0]
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], reason_value)])

    row = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
    ).collect()[0]

    assert row["reason_code"] == reason_value


def test_shape_post_quarantine_row_hash_stable_across_repeated_runs(
    spark: SparkSession, _utc_session_tz: None
) -> None:
    vec = _post_vectors()[0]
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], "business/negative-amount")])

    def _run() -> str:
        return quarantine.shape_post_quarantine(
            df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
        ).collect()[0]["row_hash"]

    assert _run() == _run()


def test_shape_post_quarantine_every_declared_timestamp_column_is_rendered_em7(
    spark: SparkSession, _utc_session_tz: None
) -> None:
    """G-11's own fixture obligation [EM-7]: the DC-3 tying test at THIS
    call site runs over the FULL snapshot subject, not just `record_key`
    columns -- `event_time` here is declared but NOT a `record_key` column
    (`_ORDERS_SCHEMA.record_key == ["domain_id"]`), so this proves
    `_render_timestamps_for_snapshot` walks every declared TimestampType
    column, matching `_make_post_snapshot_udf`'s own docstring claim."""
    vec = _post_vectors()[0]
    event_time = datetime.fromisoformat(vec["value"]["event_time"]["$timestamp"])
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], "business/negative-amount")])

    row = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
    ).collect()[0]

    expected_value = {
        **_post_viol_typed_value(vec["value"]),
        "_conveyer_fact_type": _ORDERS_FACT_TYPE,
    }
    assert row["row_snapshot"] == canonical.canonical_json(expected_value)
    # the rendered field is the canonical STRING form, present verbatim:
    assert canonical.canonical_json(event_time)[1:-1] in row["row_snapshot"]


# A006-7/§13.1 G-11: the SAME bounded-aware-datetime strategy `tests/
# integration/test_engine_semantics.py::_BOUNDED_AWARE_DATETIMES` uses for
# the primitive `date_format` vs `canonical_json` property -- reproduced
# here rather than cross-imported (this package's own convention, `frames/
# quarantine.py`'s "every consumer writes its own, never shares code" note
# just above applies equally to test-side utilities; `tests/` has no
# `__init__.py` anywhere, so a `from tests.integration.test_engine_
# semantics import ...` would need a package shape this repo deliberately
# doesn't have). A day off each end (no `OverflowError`-triggering boundary
# values, conveyer-azr.24); offsets bounded to +-14h (the real IANA range).
_BOUNDED_AWARE_DATETIMES = st.datetimes(
    min_value=datetime.min + timedelta(days=1),
    max_value=datetime.max - timedelta(days=1),
    timezones=st.sampled_from(
        [UTC] + [timezone(timedelta(hours=h)) for h in (-12, -8, -5, 0, 1, 5, 8, 12, 14)]
    ),
)


@given(instant=_BOUNDED_AWARE_DATETIMES)
@settings(
    max_examples=25, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
def test_property_shape_post_quarantine_renders_generated_instants_like_canonical_json(
    spark: SparkSession, _utc_session_tz: None, instant: datetime
) -> None:
    """A006-7/G-11's own PROPERTY half: the EXAMPLE test just above
    (`..._em7`) pins ONE committed vector's `event_time` instant; this
    generalizes the SAME tying claim -- plan-side rendering, THROUGH THE
    REAL SHAPER (`shape_post_quarantine`, not just the primitive `date_
    format` vs `canonical_json` comparison `test_engine_semantics.py`
    already covers) -- over generated instants. `event_time` stays a
    non-key column (`_ORDERS_SCHEMA.record_key == ["domain_id"]`), the
    same fixture obligation [EM-7] pins.

    `suppress_health_check=[HealthCheck.function_scoped_fixture]`: `_utc_
    session_tz` pins `spark.sql.session.timeZone` ONCE for this whole test
    function's duration (before hypothesis begins generating examples,
    restored once after) -- it is not per-example state, so hypothesis's
    default warning about a function-scoped fixture not resetting between
    generated inputs does not apply here; the same posture `test_engine_
    semantics.py`'s own property test takes with its (session-scoped)
    `spark` fixture."""
    vec_value = _post_vectors()[0]["value"]
    typed_value = {**_post_viol_typed_value(vec_value), "event_time": instant}
    row_tuple = (
        typed_value["domain_id"],
        typed_value["amount"],
        typed_value["event_time"],
        typed_value["quantity"],
        typed_value["is_active"],
        typed_value["note"],
        "business/negative-amount",
        None,
    )
    df = _post_viol_df(spark, [row_tuple])

    row = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
    ).collect()[0]

    expected_value = {**typed_value, "_conveyer_fact_type": _ORDERS_FACT_TYPE}
    assert row["row_snapshot"] == canonical.canonical_json(expected_value)


def test_shape_post_quarantine_snapshot_excludes_reason_columns(spark: SparkSession) -> None:
    vec = _post_vectors()[0]
    df = _post_viol_df(
        spark,
        [_post_viol_row(vec["value"], "business/negative-amount", '[{"id":"x","version":"v1"}]')],
    )

    row = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
    ).collect()[0]

    assert "reason_code" not in row["row_snapshot"]
    assert "reason_detail" not in row["row_snapshot"]


def test_shape_post_quarantine_quarantined_at_lands_the_literal_instant(
    spark: SparkSession,
) -> None:
    # Spark-side equality (not a `.collect()`-then-compare-python-datetime
    # assertion): pyspark's own driver-side `TimestampType` marshaling on
    # `.collect()` goes through the SAME OS-local-zone conversion [DC-3]
    # documents for the UDF boundary, so comparing collected python
    # `datetime` values against a hand-built UTC literal is itself exactly
    # the hazard this bead is about -- staying inside Spark's own instant
    # comparison sidesteps it entirely.
    vec = _post_vectors()[0]
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], "business/negative-amount")])

    shaped = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, _ORDERS_FACT_TYPE, _ORDERS_SCHEMA
    )

    assert shaped.filter(F.col("quarantined_at") == F.lit(_QAT)).count() == 1


# ============================================================================
# G-11: `record_key` at the writer -- 007.1 F-2's gate, D-6 (006.1 §10)
# ============================================================================


def test_g11_record_key_reproduces_a_committed_record_key_vector_through_the_gate(
    spark: SparkSession,
) -> None:
    """G-11: "the shaper reproduces the applicable `contracts/fixtures/
    record-key/` vectors through the gate (complete key ⇒ vector hash)."
    `basic.json`'s `{"policy_no": "POL-0042"}` single-string-column entry,
    reproduced through the REAL shaper end-to-end (never `derive_record_key`
    called directly -- that is `tests/unit/test_identity.py`'s own job)."""
    schema = FactSchemaModel(
        columns=[
            {"name": "policy_no", "type": "string"},
            {"name": "amount", "type": "decimal(10,2)"},
        ],
        domain_id_col="policy_no",
        record_key=["policy_no"],
        ordering=[],
    )
    entry = next(
        e
        for e in json.loads(
            (
                Path(__file__).resolve().parents[3]
                / "contracts"
                / "fixtures"
                / "record-key"
                / "basic.json"
            ).read_text()
        )
        if e["value"] == {"policy_no": "POL-0042"}
    )
    viol_schema = StructType(
        [
            StructField("policy_no", StringType(), True),
            StructField("amount", DecimalType(10, 2), True),
            StructField("reason_code", StringType(), False),
            StructField("reason_detail", StringType(), True),
        ]
    )
    df = spark.createDataFrame([("POL-0042", Decimal("1.00"), "business/x", None)], viol_schema)

    row = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, "policies", schema
    ).collect()[0]

    assert row["record_key"] == entry["sha256"]


def test_g11_record_key_null_when_any_declared_key_column_is_null(spark: SparkSession) -> None:
    # D-6's gate: PARTIAL key material ⇒ NULL, never a derivation over a
    # `None`-bearing map (that totality rule is `derive_record_key`'s own,
    # at the OTHER call site -- commit's, 007.1 F-1 -- never this one, §10
    # point 1).
    schema = FactSchemaModel(
        columns=[
            {"name": "domain_id", "type": "string"},
            {"name": "line_no", "type": "int"},
            {"name": "amount", "type": "decimal(10,2)"},
        ],
        domain_id_col="domain_id",
        record_key=["domain_id", "line_no"],  # multi-column key
        ordering=[],
    )
    viol_schema = StructType(
        [
            StructField("domain_id", StringType(), True),
            StructField("line_no", IntegerType(), True),
            StructField("amount", DecimalType(10, 2), True),
            StructField("reason_code", StringType(), False),
            StructField("reason_detail", StringType(), True),
        ]
    )
    df = spark.createDataFrame([("d1", None, Decimal("1.00"), "business/x", None)], viol_schema)

    row = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "lines", schema).collect()[
        0
    ]

    assert row["record_key"] is None


def test_g11_record_key_gate_interplay_domain_id_not_in_record_key_still_derives(
    spark: SparkSession,
) -> None:
    # 006.1 §10 point 1's interplay restatement: on an implicit-check row
    # (`domain_id` null), `record_key` derives iff `domain_id_col ∉
    # record_key` columns -- here it is NOT a key column, so a null
    # `domain_id` does not block derivation over the OTHER key column(s).
    schema = FactSchemaModel(
        columns=[
            {"name": "domain_id", "type": "string"},
            {"name": "line_no", "type": "int"},
        ],
        domain_id_col="domain_id",
        record_key=["line_no"],
        ordering=[],
    )
    viol_schema = StructType(
        [
            StructField("domain_id", StringType(), True),
            StructField("line_no", IntegerType(), True),
            StructField("reason_code", StringType(), False),
            StructField("reason_detail", StringType(), True),
        ]
    )
    df = spark.createDataFrame([(None, 7, "business/missing-domain-id", None)], viol_schema)

    row = quarantine.shape_post_quarantine(df, _STAMP, "cv-hash", _QAT, "lines", schema).collect()[
        0
    ]

    assert row["domain_id"] is None
    assert row["record_key"] == identity.derive_record_key({"line_no": 7})


def test_g11_cross_type_tag_discriminates_value_identical_rows(spark: SparkSession) -> None:
    """P-7(b)/G-03's golden, at the writer: two fact types sharing the SAME
    declared columns and the SAME row values must hash differently (the
    `_conveyer_fact_type` tag lives inside the hashed snapshot object)."""
    vec = _post_vectors()[0]
    df = _post_viol_df(spark, [_post_viol_row(vec["value"], "business/negative-amount")])

    row_a = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, "orders", _ORDERS_SCHEMA
    ).collect()[0]
    row_b = quarantine.shape_post_quarantine(
        df, _STAMP, "cv-hash", _QAT, "shipments", _ORDERS_SCHEMA
    ).collect()[0]

    assert row_a["row_hash"] != row_b["row_hash"]
    assert row_a["row_snapshot"] != row_b["row_snapshot"]


_TAG_TYPE = st.sampled_from(["orders", "shipments", "refunds", "returns"])
_TAG_VALUE = st.fixed_dictionaries(
    {
        "domain_id": st.text(min_size=1, max_size=6),
        "amount": st.integers(min_value=-1000, max_value=1000),
    }
)


@given(type_a=_TAG_TYPE, type_b=_TAG_TYPE, value_a=_TAG_VALUE, value_b=_TAG_VALUE)
@settings(max_examples=200)
def test_property_tag_discrimination_hashes_collide_iff_type_and_value_collide(
    type_a: str, type_b: str, value_a: dict[str, object], value_b: dict[str, object]
) -> None:
    """§13.2: "tag-discrimination: for generated same-schema type pairs,
    snapshot hashes collide iff type AND value collide" -- P-7(b)'s tag is
    hashed INSIDE the snapshot object (`canonical.row_hash`, the same
    function `_make_post_snapshot_udf`'s closure calls, module docstring),
    so this is a pure property over `core.canonical.row_hash` directly
    (never needs a live Spark session) — the real UDF-mediated golden
    (`test_g11_cross_type_tag_discriminates_value_identical_rows` above)
    pins ONE concrete pair through the actual shaper; this property
    generalizes the SAME claim over many generated (type, value) pairs."""
    hash_a = canonical.row_hash({**value_a, "_conveyer_fact_type": type_a})
    hash_b = canonical.row_hash({**value_b, "_conveyer_fact_type": type_b})
    collides = hash_a == hash_b
    same_identity = type_a == type_b and value_a == value_b
    assert collides == same_identity


# ============================================================================
# candidate_row_hash: post_check's §8.3 guard-skip rerun UDF cost
# ============================================================================

# 006.1 §8.3 (bead conveyer-6pg.13, B3): `candidate_row_hash` now needs a
# `fact_type` + `FactSchemaModel` (the tag, P-7(b)) -- two small dedicated
# schemas below, matching each test's own candidate shape (never `_ORDERS_
# SCHEMA`, whose 6 declared columns don't match these simple fixtures).
_HASH_FACT_TYPE = "orders"
_HASH_SCHEMA_NP = FactSchemaModel(
    columns=[FactColumnSpec(name="domain_id", type="string"), FactColumnSpec(name="n", type="int")],
    domain_id_col="domain_id",
    record_key=["domain_id"],
)
_HASH_SCHEMA_NPAYLOAD = FactSchemaModel(
    columns=[
        FactColumnSpec(name="domain_id", type="string"),
        FactColumnSpec(name="n", type="int"),
        FactColumnSpec(name="payload", type="string"),
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
)
_HASH_SCHEMA_TS = FactSchemaModel(
    columns=[
        FactColumnSpec(name="domain_id", type="string"),
        FactColumnSpec(name="event_time", type="timestamp"),
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
)


def test_candidate_row_hash_adds_row_hash_leaves_other_columns_unchanged(
    spark: SparkSession,
) -> None:
    candidate = spark.createDataFrame([("a", 1, "p1")], ["domain_id", "n", "payload"])

    hashed = quarantine.candidate_row_hash(candidate, _HASH_FACT_TYPE, _HASH_SCHEMA_NPAYLOAD)

    assert set(hashed.columns) == {"domain_id", "n", "payload", "row_hash"}
    row = hashed.collect()[0]
    assert row["domain_id"] == "a"
    assert row["n"] == 1
    assert row["payload"] == "p1"


def test_candidate_row_hash_matches_canonical_row_hash_over_all_columns(
    spark: SparkSession,
) -> None:
    candidate = spark.createDataFrame([("a", 1, "p1")], ["domain_id", "n", "payload"])

    hashed = quarantine.candidate_row_hash(candidate, _HASH_FACT_TYPE, _HASH_SCHEMA_NPAYLOAD)

    # the hashed subject is the DECLARED columns plus the reserved tag
    # (P-7(b)) -- never the bare candidate frame's own column set alone.
    expected = canonical.row_hash(
        {"domain_id": "a", "n": 1, "payload": "p1", "_conveyer_fact_type": _HASH_FACT_TYPE}
    )
    assert hashed.collect()[0]["row_hash"] == expected


def test_candidate_row_hash_is_stable_across_repeated_calls(spark: SparkSession) -> None:
    candidate = spark.createDataFrame([("a", 1)], ["domain_id", "n"])

    hash_1 = quarantine.candidate_row_hash(candidate, _HASH_FACT_TYPE, _HASH_SCHEMA_NP).collect()[
        0
    ]["row_hash"]
    hash_2 = quarantine.candidate_row_hash(candidate, _HASH_FACT_TYPE, _HASH_SCHEMA_NP).collect()[
        0
    ]["row_hash"]

    assert hash_1 == hash_2


def test_candidate_row_hash_renders_timestamp_columns_in_plan_before_the_udf(
    spark: SparkSession,
) -> None:
    """[DC-3]: a `TimestampType` candidate column (a real fact column shape,
    unlike pre_check's always-`string` declared columns) must be rendered
    via `F.date_format` before the UDF boundary, honoring the session's UTC
    pin -- the same mechanism `shape_post_quarantine` already relies on."""
    schema = StructType(
        [StructField("domain_id", StringType(), True), StructField("event_time", TimestampType())]
    )
    candidate = spark.createDataFrame([("a", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))], schema)

    hashed = quarantine.candidate_row_hash(candidate, _HASH_FACT_TYPE, _HASH_SCHEMA_TS)

    expected = canonical.row_hash(
        {
            "domain_id": "a",
            "event_time": "2026-01-02T03:04:05.000000Z",
            "_conveyer_fact_type": _HASH_FACT_TYPE,
        }
    )
    assert hashed.collect()[0]["row_hash"] == expected


def test_candidate_row_hash_distinguishes_different_rows(spark: SparkSession) -> None:
    candidate = spark.createDataFrame([("a", 1), ("b", 2)], ["domain_id", "n"])

    hashed = quarantine.candidate_row_hash(candidate, _HASH_FACT_TYPE, _HASH_SCHEMA_NP)

    hashes = {r["row_hash"] for r in hashed.collect()}
    assert len(hashes) == 2


def test_candidate_row_hash_tags_fact_type_discriminates_value_identical_rows(
    spark: SparkSession,
) -> None:
    """G-03's own golden, at this function's grain: two value-identical
    candidate rows of DIFFERENT fact types must hash differently -- the
    `_conveyer_fact_type` tag (P-7(b)) is what the §8.3 rerun subtraction's
    type-discrimination soundness rests on."""
    candidate = spark.createDataFrame([("a", 1)], ["domain_id", "n"])

    hash_a = quarantine.candidate_row_hash(candidate, "orders", _HASH_SCHEMA_NP).collect()[0][
        "row_hash"
    ]
    hash_b = quarantine.candidate_row_hash(candidate, "shipments", _HASH_SCHEMA_NP).collect()[0][
        "row_hash"
    ]

    assert hash_a != hash_b
