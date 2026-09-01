"""`frames.facts.stamp_fact_identity` — LLD 007.1 F-1 §5.1 (mechanics +
completion block). K-02 (UDF grade): the registered `derive_fact_stamps`
UDF, driven through the real `stamp_fact_identity` plan builder, reproduces
every committed `contracts/fixtures/fact-hash/` vector (28) -- the pure-
derivation grade (`spine.core.canonical.row_hash` directly) is
`tests/unit/test_fact_hash.py`'s (B8); this file is the UDF-grade
re-assertion milestone B9a owes (that file's own header names it).

K-04: the [DC-3] tying test at THIS call site -- plan-side `F.date_format`
rendering (honoring `spark.sql.session.timeZone`) reproduces `canonical.
canonical_json`'s own rendering of the same generated aware instants
(005.1 §12.4's idiom), over `stamp_fact_identity`'s own struct-column
builder -- not a second, independently-authored rendering path.

Also covers the two traps named at this milestone's call site (007.1 §5.1's
completion block): a declared column missing from the candidate frame fails
plan construction loudly, before any table-side check; the derived stamps
are excluded from the hashed object because they are never selected into it
in the first place (no exclusion list to drift) -- proven here by carrying
EXTRA frame columns (framework stamps) alongside the declared ones and
showing they never move `content_hash`, at UDF grade (K-03's own property
is pinned at plain-value grade by `test_fact_hash.py`; this is its
UDF-grade twin).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spine.core import canonical, identity
from spine.frames import facts

_FIXTURES_DIR = Path(__file__).resolve().parents[3] / "contracts" / "fixtures" / "fact-hash"


# --- this file's OWN untagging parser (§5.4's convention: every consumer
# writes its own, never shares code with `test_fact_hash.py`, 004 D-13) ----


def _parse_fixture_value(raw: Any) -> Any:
    if isinstance(raw, dict):
        if set(raw.keys()) == {"$decimal"}:
            return Decimal(raw["$decimal"])
        if set(raw.keys()) == {"$date"}:
            return date.fromisoformat(raw["$date"])
        if set(raw.keys()) == {"$timestamp"}:
            return datetime.fromisoformat(raw["$timestamp"])
        return {k: _parse_fixture_value(v) for k, v in raw.items()}
    if isinstance(raw, list):
        return [_parse_fixture_value(item) for item in raw]
    return raw


def _load_vectors() -> list[tuple[str, dict[str, Any]]]:
    vectors: list[tuple[str, dict[str, Any]]] = []
    for path in sorted(_FIXTURES_DIR.glob("*.json")):
        for entry in json.loads(path.read_text(encoding="utf-8")):
            vectors.append((path.name, entry))
    return vectors


_VECTORS = _load_vectors()


def _infer_spark_type(value: Any) -> DataType:
    """A per-vector schema, dynamically inferred from the tagged fixture
    value's own Python type -- every vector authors a DIFFERENT declared-
    column shape (§5.4's files), so a single fixed test schema (this
    module's usual convention elsewhere) cannot cover the family; this is
    the UDF-grade analogue of `test_fact_hash.py`'s own plain-value
    `_parse_fixture_value`, extended to produce a real Spark type per leaf.
    `Decimal`'s scale is read off the value's own exponent (never a fixed
    `DecimalType(p,s)`) -- 007.1 K-14's own note: a `DecimalType` column
    normalizes scale at write, so `edge-values.json`'s `1.2`-vs-`1.20` pair
    would silently collapse under a mismatched fixed scale."""
    if value is None:
        # null-and-empty.json's only null-valued declared column (`note`)
        # sits alongside string siblings ("", "null") in the same vector
        # family -- StringType is the only type this family's nulls exercise.
        return StringType()
    if isinstance(value, bool):
        return BooleanType()
    if isinstance(value, int):
        return LongType()  # edge-values.json's int64 min/max needs LongType
    if isinstance(value, Decimal):
        exponent = value.as_tuple().exponent
        scale = max(-exponent, 0) if isinstance(exponent, int) else 0
        digits = len(value.as_tuple().digits)
        precision = max(digits, scale + 1, 1)
        return DecimalType(min(precision + 4, 38), scale)
    if isinstance(value, datetime):
        return TimestampType()  # must precede `date` -- datetime is a date subtype
    if isinstance(value, date):
        return DateType()
    if isinstance(value, str):
        return StringType()
    raise TypeError(f"no Spark type mapping for {type(value)!r} (value={value!r})")


def _vector_frame(spark: SparkSession, typed_map: dict[str, Any]):
    fields = [StructField(name, _infer_spark_type(v), True) for name, v in typed_map.items()]
    return spark.createDataFrame([Row(**typed_map)], StructType(fields))


def test_fact_hash_fixtures_total_28_vectors() -> None:
    # Zero-cases guard (`test_fact_hash.py`'s own convention): fails loudly
    # if the fixtures directory moves or empties, rather than the
    # parametrized test below silently collecting zero cases.
    assert _FIXTURES_DIR.is_dir()
    assert len(_VECTORS) == 28


@pytest.mark.parametrize(
    "filename,entry",
    _VECTORS,
    ids=[f"{filename}#{i}" for i, (filename, _) in enumerate(_VECTORS)],
)
def test_stamp_fact_identity_udf_reproduces_every_committed_vector(
    spark: SparkSession, filename: str, entry: dict[str, Any]
) -> None:
    """K-02, UDF grade: the real `derive_fact_stamps` UDF, driven through
    `stamp_fact_identity`'s own struct-selection/[DC-3]-rendering plan
    (never a bypass straight to `core.canonical`), reproduces every
    committed vector's `content_hash`. `declared_cols` is selected from the
    vector's own key order (§5.4: "authored in declared -- non-sorted --
    order"), proving declaration order never changes the hash (canonical
    rendering sorts keys bytewise, independent of struct-field order)."""
    typed_map = _parse_fixture_value(entry["value"])
    declared_cols = list(typed_map.keys())
    timestamp_cols = [name for name, value in typed_map.items() if isinstance(value, datetime)]
    df = _vector_frame(spark, typed_map)

    # key_cols is irrelevant to content_hash (identity/value disjointness,
    # §5.1 fragment 1) -- reuse declared_cols so `stamp_fact_identity`'s
    # precondition (a non-empty, present key-column set) is trivially met
    # without asserting anything about record_key here.
    stamped = facts.stamp_fact_identity(df, declared_cols, declared_cols, timestamp_cols)
    row = stamped.collect()[0]

    assert row["content_hash"] == entry["sha256"]


def test_coverage_json_coincidence_entry_matches_record_key_derivation(
    spark: SparkSession,
) -> None:
    # §5.4's `coverage.json` deliberate coincidence entry, re-asserted at
    # UDF grade: content_hash and record_key are the SAME function
    # (`core.canonical.row_hash`) over a map, so an identical map -> an
    # identical hash, through BOTH derivation paths inside the one UDF.
    coverage = json.loads((_FIXTURES_DIR / "coverage.json").read_text(encoding="utf-8"))
    coincidence = next(e for e in coverage if e["value"] == {"policy_no": "POL-0042"})

    df = _vector_frame(spark, {"policy_no": "POL-0042"})
    stamped = facts.stamp_fact_identity(df, ["policy_no"], ["policy_no"], [])
    row = stamped.collect()[0]

    assert row["content_hash"] == coincidence["sha256"]
    assert row["record_key"] == coincidence["sha256"]


# --- the missing-declared-column trap, §5.1's own text ----------------------


def test_stamp_fact_identity_raises_loudly_on_missing_declared_column(
    spark: SparkSession,
) -> None:
    schema = StructType([StructField("domain_id", StringType(), True)])
    df = spark.createDataFrame([Row(domain_id="POL-1")], schema)

    with pytest.raises(ValueError, match=r"stamp_fact_identity.*amount.*missing"):
        facts.stamp_fact_identity(df, ["domain_id", "amount"], ["domain_id"], [])


# --- stamp exclusion at UDF grade (K-03's own twin) --------------------------


def test_extra_frame_columns_never_move_content_hash(spark: SparkSession) -> None:
    """K-03's UDF-grade twin: framework stamp columns present on the frame
    but NOT in `declared_cols` never enter the hashed struct -- because they
    are selected from the declaration, never introspected from the frame,
    there is no exclusion list for a future stamp-set change to fall out of
    sync with."""
    schema = StructType(
        [
            StructField("batch_id", StringType(), True),
            StructField("delivery_id", StringType(), True),
            StructField("domain_id", StringType(), True),
            StructField("amount", DecimalType(10, 2), True),
        ]
    )
    row_a = Row(batch_id="batch-1", delivery_id="del-1", domain_id="POL-1", amount=Decimal("50.00"))
    row_b = Row(batch_id="batch-2", delivery_id="del-9", domain_id="POL-1", amount=Decimal("50.00"))
    df_a = spark.createDataFrame([row_a], schema)
    df_b = spark.createDataFrame([row_b], schema)
    declared_cols = ["domain_id", "amount"]

    hash_a = facts.stamp_fact_identity(df_a, declared_cols, ["domain_id"], []).collect()[0][
        "content_hash"
    ]
    hash_b = facts.stamp_fact_identity(df_b, declared_cols, ["domain_id"], []).collect()[0][
        "content_hash"
    ]

    expected = canonical.row_hash({"domain_id": "POL-1", "amount": Decimal("50.00")})
    assert hash_a == hash_b == expected


# --- record_key: totality, gate: none (§5.2's commit-site row) -------------


def test_record_key_derives_unconditionally_over_null_key_material(spark: SparkSession) -> None:
    """§5.2's table: commit's `record_key` gate is NONE -- unlike
    `frames/quarantine.py::shape_post_quarantine`'s gated derivation, a null
    key column still derives a (non-null) `record_key` here, rendering
    canonically."""
    schema = StructType(
        [
            StructField("domain_id", StringType(), True),
            StructField("amount", DecimalType(10, 2), True),
        ]
    )
    df = spark.createDataFrame([Row(domain_id=None, amount=Decimal("5.00"))], schema)

    stamped = facts.stamp_fact_identity(df, ["domain_id", "amount"], ["domain_id"], [])
    row = stamped.collect()[0]

    assert row["record_key"] is not None
    assert row["record_key"] == identity.derive_record_key({"domain_id": None})


def test_record_key_is_derived_from_key_cols_subset_only(spark: SparkSession) -> None:
    # A sanity twin: record_key derives from key_cols alone, even when
    # declared_cols carries more columns (content_hash's own wider subject).
    schema = StructType(
        [
            StructField("domain_id", StringType(), True),
            StructField("amount", DecimalType(10, 2), True),
        ]
    )
    df = spark.createDataFrame([Row(domain_id="POL-1", amount=Decimal("5.00"))], schema)

    stamped = facts.stamp_fact_identity(df, ["domain_id", "amount"], ["domain_id"], [])
    row = stamped.collect()[0]

    assert row["record_key"] == identity.derive_record_key({"domain_id": "POL-1"})
    assert row["content_hash"] == canonical.row_hash(
        {"domain_id": "POL-1", "amount": Decimal("5.00")}
    )


# --- typed columns preserved on the appended frame (§5.1's own text) -------


def test_stamped_frame_keeps_typed_declared_columns_including_timestamp(
    spark: SparkSession,
) -> None:
    """ "The pre-rendered copy feeds the derivation struct only -- the
    appended frame keeps its typed columns" (§5.1's completion block): a
    `TimestampType` declared column stays `TimestampType` (never coerced to
    the rendered string) on the RETURNED frame, even though it was rendered
    to a string in-plan to feed the UDF struct.

    Never assert a collected `TimestampType` value via Python `datetime`
    equality (`.collect()` marshals it through the driver's OS-local zone,
    the SAME hazard [DC-3] guards against at the UDF boundary -- unrelated
    to whether this function itself is correct) -- instead verify the
    literal landed correctly with an in-Spark filter+count, staying inside
    the JVM's own instant comparison (`frames/quarantine.py`'s test suite
    convention, `tests/frames/test_lineage.py`)."""
    schema = StructType(
        [
            StructField("domain_id", StringType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )
    instant = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    df = spark.createDataFrame([Row(domain_id="POL-1", event_time=instant)], schema)

    stamped = facts.stamp_fact_identity(
        df, ["domain_id", "event_time"], ["domain_id"], ["event_time"]
    )

    fields = {f.name: f.dataType for f in stamped.schema.fields}
    assert isinstance(fields["event_time"], TimestampType)
    assert stamped.filter(F.col("event_time") == F.lit(instant)).count() == 1
    row = stamped.collect()[0]
    assert row["content_hash"] == canonical.row_hash({"domain_id": "POL-1", "event_time": instant})


# --- K-04: the [DC-3] tying property, at THIS call site ----------------------


@st.composite
def _aware_datetime(draw: st.DrawFn) -> datetime:
    # Bounded a day off each end -- `test_fact_hash.py`'s own idiom -- keeps
    # generated instants inside `canonical_json`'s representable domain
    # regardless of the sampled offset.
    naive = draw(
        st.datetimes(
            min_value=datetime.min + timedelta(days=1),
            max_value=datetime.max - timedelta(days=1),
        )
    )
    tz = draw(st.sampled_from([UTC, timezone(timedelta(hours=5)), timezone(timedelta(hours=-8))]))
    return naive.replace(tzinfo=tz)


@given(instant=_aware_datetime())
@settings(max_examples=50, deadline=None)
def test_dc3_tying_property_plan_side_rendering_matches_canonical_json(
    spark: SparkSession, instant: datetime
) -> None:
    """K-04: plan-side rendering (`facts._declared_struct_columns`'s
    `F.date_format` expression, under the session's pinned UTC zone) ==
    `canonical.canonical_json`'s own rendering of the SAME instant
    (005.1 §12.4's idiom) -- generated aware instants, not one fixed
    example. `canonical_json`'s own output is a JSON string (quoted); the
    plan-side rendering is the bare column value, so the comparison strips
    the JSON string's surrounding quotes."""
    schema = StructType([StructField("event_time", TimestampType(), True)])
    df = spark.createDataFrame([Row(event_time=instant)], schema)

    (struct_col,) = facts._declared_struct_columns(["event_time"], ["event_time"])
    rendered = df.select(struct_col.alias("event_time")).collect()[0]["event_time"]

    expected = canonical.canonical_json(instant)
    assert expected[0] == '"' and expected[-1] == '"'
    assert rendered == expected[1:-1]


def test_dc3_tying_property_concrete_example(spark: SparkSession) -> None:
    # A readable, non-generated instance of K-04: a fixed instant with a
    # non-UTC offset, asserted against both the plan-side rendering and
    # `canonical_json`'s own output.
    instant = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=-5)))
    schema = StructType([StructField("event_time", TimestampType(), True)])
    df = spark.createDataFrame([Row(event_time=instant)], schema)

    (struct_col,) = facts._declared_struct_columns(["event_time"], ["event_time"])
    rendered = df.select(struct_col.alias("event_time")).collect()[0]["event_time"]

    assert rendered == "2026-01-02T08:04:05.000000Z"
    assert canonical.canonical_json(instant) == '"2026-01-02T08:04:05.000000Z"'
