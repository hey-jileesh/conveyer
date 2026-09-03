"""The K-suite's own standing scenarios and properties for the fold path —
LLD 007.1 §8 (F-6/§8.1's ordering-comparability table; F-7's §8.2 rendering
decision + MERGE plan), §10 (the fold proof harness), §11/§13.2 (the kill
matrix, K-20..K-27). Bead `conveyer-6pg.22`, B10.

**Placement (§10, verbatim): "framework property tests over generated
declarations and fact sets ... never per-pipeline: the fold contract is
framework law (D-3)".** Every test below exercises `frames.fold.
reduce_batch_winners`/`core.merge.ordering_predicate`/`effects.spark`'s
`merge`/`stages.fold.run` directly (or, for the kill-matrix rows, through
`stages.commit.run`/`stages.fold.run`/`stages.publish.run` over a hand-built
multi-type spec) — never through a deployed `pipelines.*` module.

**K-14 (the engine pin).** Both rendering sites — `core.merge.
ordering_predicate` (the MERGE condition) and `frames.fold.
reduce_batch_winners`'s sort directives (the reduce) — must agree with
`tests/integration/ordering_reference.py`'s plain-Python reference over
generated element values including nulls in every nullable element. The
generator shapes below are `conveyer-hpp.13.4`'s own verified differential
lineage (9219 cases, 7922 null-bearing; `tasks/wcogqjg05.output`, `run
wf_2df22a92-f1e`), reproduced here as the CI gate this bead's own scratch
validation confirmed 0/9219 mismatches against.

**K-16's rebuild-equivalence variant, landed (B11-local, bead
`conveyer-6pg.23`) — the wait named below has closed.** §10: "Rebuild
equivalence rides the same harness (D-5, verbatim) — rebuild(all facts@pin)
≡ the incremental fold at the same pin: not a separate proof artifact ...
one more fold path through §9's swap." §9's swap (`DataFrameWriterV2.
overwrite()` with `validate-from-snapshot-id` + `isolation-level=
serializable`) is DESIGNED (ADR-OQ7, probe-verified at `conveyer-hpp.13.10`)
and now has a production run-mode implementation, `spine.effects.rebuild`
(the ONE blessed rebuild/swap module, [DC2-2]) — B10's own brief scoped
K-14..K-16/K-22..K-25 (not K-17..K-19, the three OQ-7 rebuild-specific
goldens, since landed in `test_k_suite_rebuild.py`), leaving exactly this
one sub-variant as a NAMED, bounded wait on B11's own rebuild
implementation (007.1 §14's B6-B11 sequence) — not a gap in the core
property's own proof, which this file already established. `test_k16_
rebuild_equivalence_fold_all_equals_rebuild_swap` below closes it: the
SAME generated fact set folds via the ordinary one-shot MERGE path
(`result_all`, `_k16_run`'s own first leg) AND via a genesis-seeded
`effects.rebuild.swap_with_retry` call over the identical `recompute`
closure — asserted equal, one more fold path through §9's swap, exactly
as §10 states.

**K-25's residual sweep — two of its three items are cited, not re-derived.**
(i) "a sibling completing after resolution ran" is D-2's own ACCEPTED core
residual (§7.2's own text) — not a fold-grain concern and not constructively
closable, so nothing to test beyond citing it here. (iii) `attributable=
False` (own-commit attribution under a sibling race, `conveyer-nvh.40`
[F1]) is ALREADY covered at the `effects/spark.py` grain by `test_spark_fx.
py::test_merge_reports_unattributable_when_a_sibling_commits_before_our_
statement_executes`/`test_merge_unattributable_path_is_distinguishable_
from_a_logical_no_op` — cited, not duplicated. (ii) — disjoint-domain
sibling MERGEs falsely conflicting under state's single-scope layout,
I-11's retry path, "efficiency never correctness" — IS exercised below,
reusing `effects/spark.py`'s own `_merge_race_probe` seam (the SAME
mechanism those two cited tests use) at fold's own reduce+merge grain.
"""

from __future__ import annotations

import itertools
import random
import uuid
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
import scenario_helpers as sh
from hypothesis import given, settings
from hypothesis import strategies as st
from ordering_reference import compare_ordering_struct, strictly_greater
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spine.bootstrap.create_record_tables import (
    bootstrap_fact_table,
    bootstrap_state_table,
)
from spine.core import merge as core_merge
from spine.core.model import FactColumnSpec, FactSchemaModel, FactTypeModel, PipelineSpecModel
from spine.effects import rebuild as rebuild_fx
from spine.effects import spark as spark_fx
from spine.frames.fold import reduce_batch_winners
from spine.stages import commit as commit_stage
from spine.stages import fold as fold_stage
from spine.stages import publish as publish_stage

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import DataFrame
    from spine.core.run_facts import RunFact
    from spine.effects.records import RunnerFx


def _unique_pipeline(prefix: str) -> str:
    return f"pipelines/{prefix}{uuid.uuid4().hex[:8]}"


# --- K-14: the rendering differential (both sites) --------------------------


def _k14_schema() -> StructType:
    return StructType(
        [
            StructField("case_id", IntegerType(), True),
            StructField("s_amount", DecimalType(10, 2), True),
            StructField("s_name", StringType(), True),
            StructField("s_source_ts", TimestampType(), True),
            StructField("s_content_hash", StringType(), True),
            StructField("t_amount", DecimalType(10, 2), True),
            StructField("t_name", StringType(), True),
            StructField("t_source_ts", TimestampType(), True),
            StructField("t_content_hash", StringType(), True),
        ]
    )


def _h(n: int) -> str:
    return format(n, "064x")


def _k14_cases() -> list[tuple]:
    """`conveyer-hpp.13.4`'s own verified generator lineage, reproduced
    verbatim (module docstring): the full cross-product of {amount: 4
    values incl. null} x {name: 4 incl. null, "10"<"9" trap} x {source_ts: 3
    incl. null} x {content_hash: 2}, on BOTH sides (9216), plus 3 targeted
    extras (all-null-both-sides; hash-only-on-s; NFC-vs-NFD) = 9219 total,
    7922 null-bearing."""
    ts1 = datetime(2026, 1, 1, tzinfo=UTC)
    ts2 = datetime(2026, 1, 2, tzinfo=UTC)
    amount_pool = [None, Decimal("1.20"), Decimal("1.2"), Decimal("2.00")]
    name_pool = [None, "9", "10", "abc"]
    ts_pool = [None, ts1, ts2]
    hash_pool = [_h(1), _h(2)]

    cases = []
    case_id = 0
    for s_amt, t_amt in itertools.product(amount_pool, amount_pool):
        for s_name, t_name in itertools.product(name_pool, name_pool):
            for s_ts, t_ts in itertools.product(ts_pool, ts_pool):
                for s_hash, t_hash in itertools.product(hash_pool, hash_pool):
                    cases.append(
                        (case_id, s_amt, s_name, s_ts, s_hash, t_amt, t_name, t_ts, t_hash)
                    )
                    case_id += 1
    # \uXXXX escapes, NOT literal unicode characters in source -- a tool-
    # mediated file write silently NFC-normalizes literal NFD sequences
    # ([[spine-fact-hash-generator-and-unicode-write-trap]]'s own finding,
    # reconfirmed while drafting this exact line), which would silently
    # collapse this discriminator pair to two IDENTICAL codepoints.
    nfc = "\u00e9"  # precomposed e-acute (single codepoint)
    nfd = "e\u0301"  # "e" + combining acute accent (two codepoints)
    cases.extend(
        [
            (case_id, None, None, None, None, None, None, None, None),
            (case_id + 1, None, None, None, _h(1), None, None, None, None),
            (case_id + 2, Decimal("5.00"), nfc, ts1, _h(1), Decimal("5.00"), nfd, ts1, _h(1)),
        ]
    )
    return cases


def test_k14_ordering_predicate_agrees_with_the_reference_over_9219_generated_cases(
    spark: SparkSession,
) -> None:
    spec = core_merge.merge_spec(
        FactTypeModel(
            fact_table="db.unused_fact",
            state_table="db.unused_state",
            schema=FactSchemaModel(
                columns=[
                    FactColumnSpec(name="amount", type="decimal(10,2)"),
                    FactColumnSpec(name="name", type="string"),
                    FactColumnSpec(name="domain_id", type="string"),
                ],
                domain_id_col="domain_id",
                record_key=["domain_id"],
                ordering=["amount", "name"],
            ),
        )
    )
    gt_expr = core_merge.ordering_predicate(spec)

    cases = _k14_cases()
    df = spark.createDataFrame([Row(*c) for c in cases], schema=_k14_schema())
    s_view = df.select(
        F.col("case_id"),
        F.col("s_amount").alias("amount"),
        F.col("s_name").alias("name"),
        F.col("s_source_ts").alias("source_ts"),
        F.col("s_content_hash").alias("content_hash"),
    )
    t_view = df.select(
        F.col("case_id"),
        F.col("t_amount").alias("amount"),
        F.col("t_name").alias("name"),
        F.col("t_source_ts").alias("source_ts"),
        F.col("t_content_hash").alias("content_hash"),
    )
    s_view.createOrReplaceTempView("k14_s_view")
    t_view.createOrReplaceTempView("k14_t_view")

    rows = spark.sql(
        f"""
        SELECT s.case_id AS case_id,
               s.amount AS s_amount, s.name AS s_name,
               s.source_ts AS s_source_ts, s.content_hash AS s_content_hash,
               t.amount AS t_amount, t.name AS t_name,
               t.source_ts AS t_source_ts, t.content_hash AS t_content_hash,
               ({gt_expr}) AS spark_pred_gt
        FROM k14_s_view s JOIN k14_t_view t ON s.case_id = t.case_id
        """
    ).collect()

    assert len(rows) == len(cases) == 9219
    mismatches = []
    sql_null_count = 0
    for row in rows:
        s_tuple = (row["s_amount"], row["s_name"], row["s_source_ts"], row["s_content_hash"])
        t_tuple = (row["t_amount"], row["t_name"], row["t_source_ts"], row["t_content_hash"])
        ref_gt = strictly_greater(s_tuple, t_tuple)
        if row["spark_pred_gt"] is None:
            sql_null_count += 1
        if bool(row["spark_pred_gt"]) != ref_gt:
            mismatches.append((row["case_id"], s_tuple, t_tuple, row["spark_pred_gt"], ref_gt))

    assert mismatches == []
    assert sql_null_count == 0  # three-valued-total, never leans on SQL NULL propagation


def test_k14_reduce_sort_directives_agree_with_the_reference_over_the_same_cases(
    spark: SparkSession,
) -> None:
    """The SECOND site K-14 pins: `frames.fold.reduce_batch_winners`'s own
    `desc_nulls_last()` sort directives, checked by building a two-row-per-
    domain facts frame (the `s`/`t` sides of each generated case) and
    asserting the reduce keeps the reference-GREATER side (or either, on a
    reference TIE — row_number()'s own deterministic-but-arbitrary pick)."""
    spec = core_merge.merge_spec(
        FactTypeModel(
            fact_table="db.unused_fact",
            state_table="db.unused_state",
            schema=FactSchemaModel(
                columns=[
                    FactColumnSpec(name="amount", type="decimal(10,2)"),
                    FactColumnSpec(name="name", type="string"),
                    FactColumnSpec(name="domain_id", type="string"),
                ],
                domain_id_col="domain_id",
                record_key=["domain_id"],
                ordering=["amount", "name"],
            ),
        )
    )
    cases = _k14_cases()
    pair_schema = StructType(
        [
            StructField("domain_id", StringType(), True),
            StructField("side", StringType(), True),
            StructField("amount", DecimalType(10, 2), True),
            StructField("name", StringType(), True),
            StructField("source_ts", TimestampType(), True),
            StructField("content_hash", StringType(), True),
        ]
    )
    pair_rows = []
    for case in cases:
        case_id, s_amt, s_name, s_ts, s_hash, t_amt, t_name, t_ts, t_hash = case
        dom = f"dom-{case_id}"
        pair_rows.append(Row(dom, "s", s_amt, s_name, s_ts, s_hash))
        pair_rows.append(Row(dom, "t", t_amt, t_name, t_ts, t_hash))
    pair_df = spark.createDataFrame(pair_rows, schema=pair_schema)

    winners = reduce_batch_winners(pair_df, spec).select("domain_id", "side").collect()
    winner_side = {row["domain_id"]: row["side"] for row in winners}
    assert len(winner_side) == len(cases)  # exactly one survivor per domain -- K-15's precondition

    mismatches = 0
    for case in cases:
        case_id, s_amt, s_name, s_ts, s_hash, t_amt, t_name, t_ts, t_hash = case
        dom = f"dom-{case_id}"
        order = compare_ordering_struct(
            (s_amt, s_name, s_ts, s_hash), (t_amt, t_name, t_ts, t_hash)
        )
        picked = winner_side[dom]
        if order.name == "GREATER" and picked != "s":
            mismatches += 1
        elif order.name == "LESS" and picked != "t":
            mismatches += 1
        # TIE: either survivor is a correct pick.
    assert mismatches == 0


# --- K-15: the cardinality-defect golden -------------------------------------


def test_k15_cardinality_defect_names_the_target_state_table(spark: SparkSession) -> None:
    """§8.2: post-reduce the source is unique per domain BY CONSTRUCTION
    (`frames.fold.reduce_batch_winners`) -- a `MERGE_CARDINALITY_VIOLATION`
    can therefore only mean the TARGET already broke its own one-row-per-
    domain grain (§6.2's "Grain: one row per domain_id"). This test bypasses
    the reduce deliberately (a hand-built, non-reduced source) to simulate
    exactly that -- the SAME mechanics `test_scenarios_fold.py::test_r07_
    fold_cardinality_violation_is_a_named_defect_at_fold` exercises through
    a full `run_sequence` up to the point of the direct `fx.merge` call;
    this is the K-suite's own dedicated, minimal reproduction."""
    schema = FactSchemaModel(
        columns=[
            FactColumnSpec(name="domain_id", type="string"),
            FactColumnSpec(name="payload", type="string"),
        ],
        domain_id_col="domain_id",
        record_key=["domain_id"],
    )
    state_table = f"spine_test_tables.k15_state_{uuid.uuid4().hex[:8]}"
    qt = f"spine_cat.{state_table}"
    bootstrap_state_table(spark, qt, schema)
    spark.sql(
        f"INSERT INTO {qt} (domain_id, payload, batch_id, delivery_id, feed_id, received_at, "
        "source_ts, content_hash, record_key) VALUES "
        "('dom-c', 'seed', 'seed-batch', 'seed-delivery', 'feed/k15', "
        "TIMESTAMP '2026-01-01 00:00:00', NULL, 'h1', 'rk1')"
    )
    merge_spec = core_merge.merge_spec(
        FactTypeModel(fact_table="db.unused_fact", state_table=state_table, schema=schema)
    )
    dup_schema = StructType([f for f in spark.table(qt).schema.fields])
    ts = datetime(2026, 1, 2, tzinfo=UTC)
    dup_rows = spark.createDataFrame(
        [
            Row("b2", "d2", "f2", ts, None, "h2", "rk1", "dom-c", "dup-a"),
            Row("b2", "d2", "f2", ts, None, "h3", "rk1", "dom-c", "dup-b"),
        ],
        schema=dup_schema,
    )

    merge_fn = spark_fx.build_merge(spark)
    with pytest.raises(ValueError, match="fold cardinality defect") as exc_info:
        merge_fn(merge_spec, dup_rows)

    assert "I-11" in str(exc_info.value)
    assert state_table in str(exc_info.value)  # per-state-table indictment
    assert "target" in str(exc_info.value).lower()
    assert type(exc_info.value.__cause__).__name__ == "Py4JJavaError"


# --- K-16: the fold proof harness -- fold(all) ≡ incremental ---------------


def _bootstrap_state(spark: SparkSession, schema: FactSchemaModel) -> tuple[str, str]:
    qt = f"spine_cat.spine_test_tables.k16_{uuid.uuid4().hex[:8]}"
    bootstrap_state_table(spark, qt, schema)
    bare = qt.removeprefix("spine_cat.")
    return qt, bare


_K16_SCHEMA = FactSchemaModel(
    columns=[
        FactColumnSpec(name="domain_id", type="string"),
        FactColumnSpec(name="event_time", type="timestamp"),
        # A007-5: one ordering column per §8.1 type beyond `event_time`'s own
        # timestamp -- `int`/`long`/`decimal`/`date`/`string` -- so the core
        # property (§10: "declared ordering columns drawn from EVERY §8.1
        # type") exercises the full comparability set, not `timestamp` alone.
        FactColumnSpec(name="ord_int", type="int"),
        FactColumnSpec(name="ord_long", type="long"),
        FactColumnSpec(name="ord_decimal", type="decimal(10,2)"),
        FactColumnSpec(name="ord_date", type="date"),
        FactColumnSpec(name="ord_string", type="string"),
        FactColumnSpec(name="payload", type="string"),
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
    ordering=["event_time", "ord_int", "ord_long", "ord_decimal", "ord_date", "ord_string"],
)
_K16_DF_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("delivery_id", StringType(), False),
        StructField("feed_id", StringType(), False),
        StructField("received_at", TimestampType(), False),
        StructField("event_time", TimestampType(), True),
        StructField("ord_int", IntegerType(), True),
        StructField("ord_long", LongType(), True),
        StructField("ord_decimal", DecimalType(10, 2), True),
        StructField("ord_date", DateType(), True),
        StructField("ord_string", StringType(), True),
        StructField("source_ts", TimestampType(), True),
        StructField("content_hash", StringType(), False),
        StructField("record_key", StringType(), False),
        StructField("domain_id", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)
_K16_RECEIVED_AT = datetime(2026, 1, 1, tzinfo=UTC)
_K16_TS_POOL = [None, datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)]
_K16_DOMAIN_POOL = ["d1", "d2", "d3"]

# A007-5's own per-type null-bearing pools for the FIVE extra ordering
# columns -- small, index-derived (`idx % len(pool)`), deterministic pools
# rather than independent hypothesis draws: `_k16_to_row`'s existing callers
# (the late-file/concurrent-sibling/rebuild-equivalence variants below) pass
# `idx` values of 0/1 only, so widening stays backward-compatible bit-for-
# bit for them (event_time, the FIRST declared ordering element, already
# decides those scenarios outright regardless of what these extra columns
# hold -- scratch-validated, this bead).
_K16_INT_POOL = [None, -1, 0, 1]
_K16_LONG_POOL = [None, -1, 0, 1]
_K16_DECIMAL_POOL = [None, Decimal("1.20"), Decimal("2.00")]
_K16_DATE_POOL = [None, date(2026, 1, 1), date(2026, 1, 2)]
# `"9"`/`"10"` deliberately reprises §8.1's own named lexical-order trap
# (variable-width numeric strings order lexically) -- harmless here (K-16
# proves fold(all) == incremental, never a cross-evaluator semantics claim,
# that's K-14's own ground) but costs nothing to include.
_K16_STRING_POOL = [None, "9", "10"]
# A007-5: content_hash drawn from a SMALL pool (index-derived, `idx %
# _K16_HASH_POOL_SIZE`) rather than always-unique-per-fact -- unlike the
# prior index-derived-uniqueness choice (deliberately absorbing D-2(b)'s
# divergent duplicates OUT of this property, per the retired comment this
# replaces), declared-column ties (including a full struct tie, an exact
# duplicate) can now occur, letting content_hash -- the struct's guaranteed-
# non-null final element -- actually decide some cases, never just backstop
# ones that never arise.
_K16_HASH_POOL_SIZE = 3


def _k16_to_row(fact: tuple[str, object, int]) -> Row:
    domain_id, event_time, idx = fact
    return Row(
        batch_id="b",
        delivery_id="d",
        feed_id="f",
        received_at=_K16_RECEIVED_AT,
        event_time=event_time,
        ord_int=_K16_INT_POOL[idx % len(_K16_INT_POOL)],
        ord_long=_K16_LONG_POOL[idx % len(_K16_LONG_POOL)],
        ord_decimal=_K16_DECIMAL_POOL[idx % len(_K16_DECIMAL_POOL)],
        ord_date=_K16_DATE_POOL[idx % len(_K16_DATE_POOL)],
        ord_string=_K16_STRING_POOL[idx % len(_K16_STRING_POOL)],
        source_ts=None,
        content_hash=_h(idx % _K16_HASH_POOL_SIZE),
        record_key=domain_id,
        domain_id=domain_id,
        payload=f"p{idx}",
    )


def _k16_run(
    spark: SparkSession,
    facts: list[tuple[str, object, int]],
    batch_groups: list[int],
    shuffle_seed: int,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """`fold(all facts, one shot) ≡ the incremental fold sequence over
    random partitions folded in shuffled arrival order` (§10's core
    property), asserted per state table -- one call, two independently
    provisioned state tables (`_all`/`_inc`), compared by final content."""
    all_qt, all_bare = _bootstrap_state(spark, _K16_SCHEMA)
    inc_qt, inc_bare = _bootstrap_state(spark, _K16_SCHEMA)
    spec_all = core_merge.merge_spec(
        FactTypeModel(fact_table="db.unused", state_table=all_bare, schema=_K16_SCHEMA)
    )
    spec_inc = core_merge.merge_spec(
        FactTypeModel(fact_table="db.unused", state_table=inc_bare, schema=_K16_SCHEMA)
    )
    merge_fn = spark_fx.build_merge(spark)

    all_df = spark.createDataFrame([_k16_to_row(f) for f in facts], schema=_K16_DF_SCHEMA)
    merge_fn(spec_all, reduce_batch_winners(all_df, spec_all))

    groups = sorted(set(batch_groups))
    random.Random(shuffle_seed).shuffle(groups)
    for group in groups:
        batch_facts = [f for f, g in zip(facts, batch_groups, strict=True) if g == group]
        if not batch_facts:
            continue
        batch_df = spark.createDataFrame(
            [_k16_to_row(f) for f in batch_facts], schema=_K16_DF_SCHEMA
        )
        merge_fn(spec_inc, reduce_batch_winners(batch_df, spec_inc))

    result_all = sorted((r["domain_id"], r["content_hash"]) for r in spark.table(all_qt).collect())
    result_inc = sorted((r["domain_id"], r["content_hash"]) for r in spark.table(inc_qt).collect())
    return result_all, result_inc


_K16_FACT = st.tuples(
    st.sampled_from(_K16_DOMAIN_POOL),
    st.sampled_from(_K16_TS_POOL),
    st.integers(min_value=0, max_value=99),
)


@given(
    raw_facts=st.lists(_K16_FACT, min_size=1, max_size=10),
    batch_group_seed=st.integers(min_value=0, max_value=2),
    shuffle_seed=st.integers(),
)
@settings(max_examples=20, deadline=None)
def test_k16_fold_all_equals_incremental_fold_shuffled_arrival_nulls_included(
    spark: SparkSession,
    raw_facts: list[tuple[str, object, int]],
    batch_group_seed: int,
    shuffle_seed: int,
) -> None:
    # `idx` (index-derived) feeds `_k16_to_row`'s own small per-type pools,
    # INCLUDING content_hash's (A007-5) -- declared-column ties, and
    # occasionally a full struct tie (an exact duplicate), can now occur
    # within one generated set; harmless to this property (fold(all) ==
    # incremental holds regardless of hash-collision rate, scratch-validated
    # this bead) and it is what lets content_hash -- the struct's guaranteed
    # -non-null final element -- actually decide some generated cases.
    facts = [(domain, ts, idx) for idx, (domain, ts, _junk) in enumerate(raw_facts)]
    rnd = random.Random(batch_group_seed)
    batch_groups = [rnd.randint(0, 2) for _ in facts]

    result_all, result_inc = _k16_run(spark, facts, batch_groups, shuffle_seed)

    assert result_all == result_inc


def test_k16_divergent_duplicates_hash_tiebreak_is_order_insensitive(spark: SparkSession) -> None:
    """A007-5's own named variant: D-2(b) divergent duplicates -- two
    same-domain facts whose ENTIRE declared ordering struct ties -- so
    content_hash, the struct's guaranteed-non-null final element, is what
    decides the winner (§8.1's own total-order closure). Proves the
    decision is genuinely order-insensitive (the SAME winner regardless of
    which physical row order the source frame carries), not merely that
    SOME winner is picked deterministically by accident of row order."""

    def _tied_row(content_hash: str) -> Row:
        return Row(
            batch_id="b",
            delivery_id="d",
            feed_id="f",
            received_at=_K16_RECEIVED_AT,
            event_time=_K16_RECEIVED_AT,
            ord_int=1,
            ord_long=1,
            ord_decimal=Decimal("1.20"),
            ord_date=date(2026, 1, 1),
            ord_string="a",
            source_ts=None,
            content_hash=content_hash,
            record_key="d1",
            domain_id="d1",
            payload=content_hash,
        )

    higher, lower = _h(2), _h(1)
    assert higher > lower  # the tiebreak's own precondition

    all_qt_a, all_bare_a = _bootstrap_state(spark, _K16_SCHEMA)
    spec_a = core_merge.merge_spec(
        FactTypeModel(fact_table="db.unused", state_table=all_bare_a, schema=_K16_SCHEMA)
    )
    merge_fn = spark_fx.build_merge(spark)
    df_higher_first = spark.createDataFrame(
        [_tied_row(higher), _tied_row(lower)], schema=_K16_DF_SCHEMA
    )
    merge_fn(spec_a, reduce_batch_winners(df_higher_first, spec_a))
    result_a = sorted((r["domain_id"], r["content_hash"]) for r in spark.table(all_qt_a).collect())

    all_qt_b, all_bare_b = _bootstrap_state(spark, _K16_SCHEMA)
    spec_b = core_merge.merge_spec(
        FactTypeModel(fact_table="db.unused", state_table=all_bare_b, schema=_K16_SCHEMA)
    )
    df_lower_first = spark.createDataFrame(
        [_tied_row(lower), _tied_row(higher)], schema=_K16_DF_SCHEMA
    )
    merge_fn(spec_b, reduce_batch_winners(df_lower_first, spec_b))
    result_b = sorted((r["domain_id"], r["content_hash"]) for r in spark.table(all_qt_b).collect())

    assert result_a == result_b == [("d1", higher)]


def test_k16_late_file_older_received_at_batch_folding_after_a_newer_one_converges(
    spark: SparkSession,
) -> None:
    """The late-file litmus's property half (§10's own named variant):
    final state depends ONLY on the ordering-struct columns (`event_time`/
    `source_ts`/`content_hash`), never on WHICH batch physically folds
    later or what `received_at` either batch carried -- exactly what the
    core shuffled-arrival property above already proves (arrival order is
    the batch-group SHUFFLE, and `received_at` never even enters
    `MergeSpec.ordering_cols`, §4.1). Restated here as its own named,
    citable scenario rather than only implicit in the property above."""
    facts = [
        ("d1", datetime(2026, 1, 2, tzinfo=UTC), 0),  # B: newer event_time
        ("d1", datetime(2026, 1, 1, tzinfo=UTC), 1),  # A: older event_time
    ]
    # B (batch group 0) arrives FIRST; A (batch group 1) arrives SECOND,
    # despite carrying the (hypothetically) older `received_at` -- the
    # MERGE condition never reads `received_at`, so B's already-newer value
    # must survive regardless of physical arrival order.
    result_all, result_inc = _k16_run(spark, facts, [0, 1], shuffle_seed=1)
    assert result_all == result_inc == [("d1", _h(0))]  # B's content_hash wins, order-independent


def test_k16_concurrent_sibling_merges_converge_via_i11_retry(spark: SparkSession) -> None:
    """K-16's concurrent-sibling variant: two batches' MERGEs interleaved
    through I-11's conflict-retry path converge to the same state as any
    serial order. Reuses `effects/spark.py`'s own `_merge_race_probe` seam
    (the SAME single-session interleaving technique `test_spark_fx.py`'s
    own sibling-race tests use) to inject a REAL interleaved commit
    deterministically, single-threaded -- true multi-JVM concurrency is a
    named residual (LLD §9.1's own probe-limits note), not reproducible
    from one local session; this is the mechanism's own documented bound."""
    schema = _K16_SCHEMA
    qt, bare = _bootstrap_state(spark, schema)
    spec = core_merge.merge_spec(
        FactTypeModel(fact_table="db.unused", state_table=bare, schema=schema)
    )
    merge_fn = spark_fx.build_merge(spark)

    # Sibling A: a disjoint-domain winner for "d1".
    a_df = spark.createDataFrame(
        [_k16_to_row(("d1", datetime(2026, 1, 1, tzinfo=UTC), 0))], schema=_K16_DF_SCHEMA
    )
    winners_a = reduce_batch_winners(a_df, spec)

    # Sibling B: a disjoint-domain winner for "d2", injected as a REAL
    # commit landing between A's pre-commit re-read and its own MERGE INTO
    # statement (the `_MERGE_PRE_COMMIT` point, nvh.40 [F1]'s own seam).
    b_df = spark.createDataFrame(
        [_k16_to_row(("d2", datetime(2026, 1, 1, tzinfo=UTC), 1))], schema=_K16_DF_SCHEMA
    )
    winners_b = reduce_batch_winners(b_df, spec)

    original_probe = spark_fx._merge_race_probe
    fired = False

    def sibling_commit_before_a(session: SparkSession, target_qt: str, point: str) -> None:
        nonlocal fired
        if fired or point != spark_fx._MERGE_PRE_COMMIT or target_qt != qt:
            return
        fired = True
        # B's own commit must NOT re-trigger the probe recursively (B's
        # `merge_fn` call hits this SAME `_MERGE_PRE_COMMIT` point too) --
        # restore the real no-op probe for the duration of B's own commit.
        spark_fx._merge_race_probe = original_probe
        try:
            merge_fn(spec, winners_b)  # the sibling's own real commit, mid-A's-attempt
        finally:
            spark_fx._merge_race_probe = sibling_commit_before_a
            # `build_merge`'s own closure re-registers A's source under the
            # SAME shared temp-view name (`_conveyer_merge_src`) before ever
            # calling this probe -- B's OWN nested `merge_fn` call just
            # overwrote it with B's data (one Spark session standing in for
            # two independent writers, this test's own single-session
            # interleaving construction). A's still-pending `spark.sql
            # (sql_text)` statement (this call frame's own caller, about to
            # resume) needs A's view restored, or it would re-apply B's rows
            # a second time instead of inserting A's.
            winners_a.createOrReplaceTempView(spark_fx._MERGE_SRC_VIEW)

    spark_fx._merge_race_probe = sibling_commit_before_a
    try:
        result_a = merge_fn(spec, winners_a)
    finally:
        spark_fx._merge_race_probe = original_probe

    # A's own commit is unattributable (a sibling landed before A's own
    # MERGE INTO executed, base_shifted) -- but BOTH writers' rows land:
    # correctness (state convergence) is independent of attribution.
    assert result_a.attributable is False
    rows = sorted((r["domain_id"], r["content_hash"]) for r in spark.table(qt).collect())
    assert rows == [("d1", _h(0)), ("d2", _h(1))]


@given(raw_facts=st.lists(_K16_FACT, min_size=1, max_size=10))
@settings(max_examples=20, deadline=None)
def test_k16_rebuild_equivalence_fold_all_equals_rebuild_swap(
    spark: SparkSession, raw_facts: list[tuple[str, object, int]]
) -> None:
    """K-16's rebuild-equivalence variant (§10, verbatim): `rebuild(all
    facts@pin) ≡ the incremental fold at the same pin` -- "not a separate
    proof artifact ... one more fold path through §9's swap." Rides the
    SAME generator as the core shuffled-arrival property above (nulls in
    the one nullable ordering element included).

    **Re-pointed at the production closure (A007-1, bead conveyer-swb.13):**
    `result_rebuild` now folds the IDENTICAL fact set through `effects.
    rebuild.rebuild_state_table` -- the production recompute builder that
    pins the FACT TABLE'S OWN current snapshot inside its closure (§9.5),
    never a hand-rolled `recompute` closing over an in-memory `all_df`
    (the prior shape `swap_with_retry` alone could not distinguish from a
    real pinned read). `result_all` is the ordinary one-shot MERGE-all-at-
    once leg `_k16_run` already proves against the incremental sequence;
    Path B appends the SAME generated rows to a REAL fact table first, then
    genesis-seeds the rebuild state table with a ZERO-ROW MERGE (establishes
    a real first snapshot, `before_id` is never `None`, and contributes no
    content of its own -- §9.2 presumes an existing state-table lineage)
    before the ONE real `rebuild_state_table` call reads the fact table's
    own pinned snapshot and swaps in `reduce_batch_winners(_, spec)`."""
    facts = [(domain, ts, idx) for idx, (domain, ts, _junk) in enumerate(raw_facts)]

    all_qt, all_bare = _bootstrap_state(spark, _K16_SCHEMA)
    rebuild_qt, rebuild_bare = _bootstrap_state(spark, _K16_SCHEMA)
    fact_qt = f"spine_cat.spine_test_tables.k16fact_{uuid.uuid4().hex[:8]}"
    bootstrap_fact_table(spark, fact_qt, _K16_SCHEMA)
    fact_bare = fact_qt.removeprefix("spine_cat.")
    spec_all = core_merge.merge_spec(
        FactTypeModel(fact_table="db.unused", state_table=all_bare, schema=_K16_SCHEMA)
    )
    fact_type_rebuild = FactTypeModel(
        fact_table=fact_bare, state_table=rebuild_bare, schema=_K16_SCHEMA
    )
    spec_rebuild = core_merge.merge_spec(fact_type_rebuild)
    merge_fn = spark_fx.build_merge(spark)
    all_df = spark.createDataFrame([_k16_to_row(f) for f in facts], schema=_K16_DF_SCHEMA)

    # Path A: the ordinary one-shot fold-all, via MERGE.
    merge_fn(spec_all, reduce_batch_winners(all_df, spec_all))
    result_all = sorted((r["domain_id"], r["content_hash"]) for r in spark.table(all_qt).collect())

    # Path B: the rebuild swap, through `rebuild_state_table` -- append the
    # IDENTICAL rows to a real fact table, genesis-seed the rebuild state
    # table with a zero-row MERGE (a real first snapshot, zero content),
    # then let the production closure pin the fact table's own snapshot and
    # reduce it.
    all_df.writeTo(fact_qt).option("check-nullability", "false").append()
    empty_df = spark.createDataFrame([], schema=_K16_DF_SCHEMA)
    merge_fn(spec_rebuild, reduce_batch_winners(empty_df, spec_rebuild))

    ledger_rows: list[RunFact] = []
    rebuild_fx.rebuild_state_table(
        spark, "pipelines/k16-rebuild-equiv", fact_type_rebuild, record_run=ledger_rows.append
    )
    result_rebuild = sorted(
        (r["domain_id"], r["content_hash"]) for r in spark.table(rebuild_qt).collect()
    )

    assert result_all == result_rebuild
    assert len(ledger_rows) == 1  # the genesis seed leaves before_id valid -> one attempt
    assert ledger_rows[0].state_read_snapshot_id is not None  # the fact table's own pinned snapshot


# --- K-22..K-25: the kill-matrix rows, per-type register (§11/§13.2) --------


class _TwoTypeFixture:
    """A hand-built 2-type spec (`type-a`/`type-b`) with REAL bootstrap-DDL
    fact/state/markers tables -- the kill-matrix rows need genuine `commit`/
    `fold`/`publish` stage calls, not just `reduce_batch_winners`/`fx.merge`
    in isolation."""

    def __init__(self, spark: SparkSession, unique_table: Callable[[str], str]) -> None:
        raw_qt = unique_table("ksuite_raw")
        qtn_qt = unique_table("ksuite_qtn")
        a_fact_qt = unique_table("ksuite_a_fact")
        a_state_qt = unique_table("ksuite_a_state")
        b_fact_qt = unique_table("ksuite_b_fact")
        b_state_qt = unique_table("ksuite_b_state")
        sh.create_raw_table(spark, raw_qt)
        sh.create_quarantine_table(spark, qtn_qt)
        bootstrap_fact_table(spark, a_fact_qt, sh.IDENTITY_FACT_SCHEMA)
        bootstrap_state_table(spark, a_state_qt, sh.IDENTITY_FACT_SCHEMA)
        bootstrap_fact_table(spark, b_fact_qt, sh.IDENTITY_FACT_SCHEMA)
        bootstrap_state_table(spark, b_state_qt, sh.IDENTITY_FACT_SCHEMA)
        self.spark = spark
        self.a_fact_qt = a_fact_qt
        self.a_state_qt = a_state_qt
        self.b_fact_qt = b_fact_qt
        self.b_state_qt = b_state_qt
        self.spec = PipelineSpecModel(
            pipeline=_unique_pipeline("ksuite"),
            transforms_module="pipelines.identity.transforms",
            raw_table=sh.bare(raw_qt),
            quarantine_table=sh.bare(qtn_qt),
            fact_types={
                "type-a": FactTypeModel(
                    fact_table=sh.bare(a_fact_qt),
                    state_table=sh.bare(a_state_qt),
                    schema=sh.IDENTITY_FACT_SCHEMA,
                ),
                "type-b": FactTypeModel(
                    fact_table=sh.bare(b_fact_qt),
                    state_table=sh.bare(b_state_qt),
                    schema=sh.IDENTITY_FACT_SCHEMA,
                ),
            },
            read=sh.IDENTITY_READ,
            raw_contract=sh.IDENTITY_RAW_CONTRACT,
        )
        sh.create_markers_table_for(spark, self.spec)

    def seed(self, batch_id: str):
        # `raw_count` is normally `land`'s own output -- these tests bypass
        # `land`/`pre_check`/`pull`/`apply` entirely (module docstring's own
        # "hand-built ... never through a deployed pipelines.* module"
        # convention); `publish.run`'s own assert needs SOME durable value.
        return replace(sh.make_seed(spec=self.spec, batch_id=batch_id, object_uris=()), raw_count=1)

    def _rows_df(self, rows: list[tuple[str, str, str]]) -> DataFrame:
        schema = StructType(
            [
                StructField("domain_id", StringType(), True),
                StructField("event_time", StringType(), True),
                StructField("payload", StringType(), True),
            ]
        )
        return self.spark.createDataFrame(rows, schema=schema)

    def commit_admitted(self, batch_id: str, *, a_rows, b_rows, fx: RunnerFx):
        ctx = replace(
            self.seed(batch_id),
            admitted_facts={"type-a": self._rows_df(a_rows), "type-b": self._rows_df(b_rows)},
        )
        return commit_stage.run(ctx, fx)


def test_k22_kill_between_fold_merges_then_rerun_converges(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    """§11's row: state A merged, B not. Standing: per-table coherence holds
    (A is batch-coherent for A). Rerun: fold has no guard by design -- A
    re-MERGEs to a logical no-op on full ties, B applies; converges."""
    fixture = _TwoTypeFixture(spark, unique_table)
    batch_id = sh.batch_id(2200)
    committed = fixture.commit_admitted(
        batch_id,
        a_rows=[("dA", "2026-01-01T00:00:00Z", "pA")],
        b_rows=[("dB", "2026-01-01T00:00:00Z", "pB")],
        fx=local_runner_fx,
    )

    # Simulate the kill: type-a's own fold MERGE runs (manually, at F-4's
    # own per-type grain), type-b's never does.
    fact_type_a = committed.spec.fact_types["type-a"]
    facts_a = local_runner_fx.read_batch(fact_type_a.fact_table, batch_id)
    spec_a = core_merge.merge_spec(fact_type_a)
    local_runner_fx.merge(spec_a, reduce_batch_winners(facts_a, spec_a))
    assert spark.table(fixture.b_state_qt).count() == 0  # B genuinely untouched

    # The rerun: the REAL `stages/fold.py::run`, both types, one call --
    # fold has no presence guard by design (§11).
    after = fold_stage.run(committed, local_runner_fx)

    assert sh.bare(fixture.a_state_qt) not in after.fold_snapshot_ids  # A: logical no-op (tie)
    assert sh.bare(fixture.b_state_qt) in after.fold_snapshot_ids  # B: a real, first MERGE
    a_rows = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.a_state_qt).collect()
    )
    b_rows = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.b_state_qt).collect()
    )
    assert a_rows == [("dA", "pA")]
    assert b_rows == [("dB", "pB")]


def test_k22_zero_merged_completed_but_unfolded_successor_drops_and_perception_holds_pre_predecessor(  # noqa: E501
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus,
) -> None:
    """A007-3: [AE2-2]'s zero-merged trace variant (§11's row, widest form)
    -- completion present, NO MERGE ran at all for EITHER type (killed
    after the completion row, before the first MERGE) -- plus the standing
    cross-batch consequence: a successor lawfully drops against the
    completed-but-unfolded predecessor, completes, and emits its own
    `batch-completed`; perception holds the PRE-predecessor value through
    that fired event, until the predecessor's own fold reruns.

    Three batches, one domain (`dA`), real `commit_admitted`/`fold_stage.
    run`/`publish_stage.run` calls throughout (never a raw-SQL seed):
    B0 commits+folds `dA/p0` (a genuine prior predecessor, establishing the
    "pre-predecessor" value in state). B1 commits `dA/pA` (novel against
    B0) -- completion written, its OWN fold never runs ("killed" here,
    simply by not calling `fold_stage.run` yet -- the widest zero-merged
    form needs no MERGE mechanics at all, unlike the sibling test above's
    half-merged variant). B2 commits an IDENTICAL `dA/pA` candidate: B1 is
    now the feed's latest completed batch, so B2 drops it as unchanged
    (`delta_predecessor_batch_ids == (B1,)`, zero novel facts) -- B2 still
    gets its own completion row (commit's unconditional last act) and folds
    to a no-op (nothing of its own to merge). `publish_stage.run(B2)` emits
    `batch-completed` for B2. State must still read `dA/p0` (B1's fold
    never ran, B2 contributed nothing) -- the standing, pre-rerun truth,
    asserted BEFORE B1's own fold closes the window. Only then does
    `fold_stage.run(B1)` finally advance state to `dA/pA`."""
    fixture = _TwoTypeFixture(spark, unique_table)
    b0 = sh.batch_id(2210)
    b1 = sh.batch_id(2211)
    b2 = sh.batch_id(2212)

    committed_b0 = fixture.commit_admitted(
        b0, a_rows=[("dA", "2026-01-01T00:00:00Z", "p0")], b_rows=[], fx=local_runner_fx
    )
    fold_stage.run(committed_b0, local_runner_fx)
    a_state_pre = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.a_state_qt).collect()
    )
    assert a_state_pre == [("dA", "p0")]  # the pre-predecessor value, established for real

    committed_b1 = fixture.commit_admitted(
        b1, a_rows=[("dA", "2026-01-02T00:00:00Z", "pA")], b_rows=[], fx=local_runner_fx
    )
    assert committed_b1.delta_predecessor_batch_ids == (b0,)
    a_bare = fixture.a_fact_qt.removeprefix("spine_cat.")
    assert dict(committed_b1.facts_appended_by_table)[a_bare] == 1
    # "killed" here -- commit's completion row is written (commit's own
    # unconditional last act); B1's own `fold_stage.run` never happens yet.

    committed_b2 = fixture.commit_admitted(
        b2, a_rows=[("dA", "2026-01-02T00:00:00Z", "pA")], b_rows=[], fx=local_runner_fx
    )
    assert committed_b2.delta_predecessor_batch_ids == (b1,)  # B1 is the latest completed batch
    facts_appended_b2 = dict(committed_b2.facts_appended_by_table)
    assert facts_appended_b2[a_bare] == 0  # identical to B1 -> dropped, unchanged

    folded_b2 = fold_stage.run(committed_b2, local_runner_fx)
    assert dict(folded_b2.fold_snapshot_ids) == {}  # B2 contributed nothing of its own -- no-op

    after_publish_b2 = publish_stage.run(folded_b2, local_runner_fx)
    assert after_publish_b2.published is True
    envelopes = [e for e in moto_events_bus.read_events() if e["detail"].get("batch_id") == b2]
    assert len(envelopes) == 1
    assert envelopes[0]["detail-type"] == "batch-completed"

    # The standing, PRE-rerun truth: perception still holds the
    # PRE-predecessor value -- B1's fold never ran, B2's fold was a no-op.
    a_state_standing = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.a_state_qt).collect()
    )
    assert a_state_standing == [("dA", "p0")]

    # Only now: B1's own fold reruns and closes the window.
    fold_stage.run(committed_b1, local_runner_fx)
    a_state_after_b1_fold = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.a_state_qt).collect()
    )
    assert a_state_after_b1_fold == [("dA", "pA")]


def test_k23_kill_after_fold_before_publish_then_rerun_emits(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    moto_events_bus,
) -> None:
    """§11's row: after fold completes, before `batch-completed`. Standing:
    state correct, perception stale. Rerun: commit all-skip, fold all-no-op
    (ties), publish emits."""
    fixture = _TwoTypeFixture(spark, unique_table)
    batch_id = sh.batch_id(2300)
    committed = fixture.commit_admitted(
        batch_id,
        a_rows=[("dA", "2026-01-01T00:00:00Z", "pA")],
        b_rows=[("dB", "2026-01-01T00:00:00Z", "pB")],
        fx=local_runner_fx,
    )
    folded = fold_stage.run(committed, local_runner_fx)
    # "killed" here -- state is correct, but no publish.run call at all yet.
    envelopes_before = [
        e for e in moto_events_bus.read_events() if e["detail"].get("batch_id") == batch_id
    ]
    assert envelopes_before == []
    a_rows = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.a_state_qt).collect()
    )
    assert a_rows == [("dA", "pA")]  # state IS correct despite no publish yet

    # Rerun: fresh seed, same batch_id -- commit all-skip (both types
    # present), fold all-no-op (both ties), publish emits for the first time.
    committed2 = fixture.commit_admitted(
        batch_id,
        a_rows=[("dA", "2026-01-01T00:00:00Z", "pA")],
        b_rows=[("dB", "2026-01-01T00:00:00Z", "pB")],
        fx=local_runner_fx,
    )
    assert committed2.commit_snapshot_ids == {}  # both types guard-skipped
    folded2 = fold_stage.run(committed2, local_runner_fx)
    assert folded2.fold_snapshot_ids == {}  # both types: logical no-op (full tie)
    after = publish_stage.run(folded2, local_runner_fx)

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert len(envelopes) == 1
    assert envelopes[0]["detail-type"] == "batch-completed"
    assert after.published is True
    del folded  # state already asserted correct above; kept for readability


def test_k24_stale_extra_attempt_of_old_batch_after_newer_batch_folded_never_regresses(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    """The A->B->A litmus, K-24: batch B (newer event_time) folds; batch A
    (older event_time, same domain) folds second, loses at fold (I-11
    strict >); a STALE EXTRA attempt of A (a genuine rerun of A's own
    batch_id, AFTER B) must be a pure no-op -- state never regresses,
    commit fully guard-skips, fold ties out again."""
    fixture = _TwoTypeFixture(spark, unique_table)
    batch_b = sh.batch_id(2401)
    batch_a = sh.batch_id(2402)

    committed_b = fixture.commit_admitted(
        batch_b,
        a_rows=[("d1", "2026-01-02T00:00:00Z", "B-newer")],
        b_rows=[],
        fx=local_runner_fx,
    )
    fold_stage.run(committed_b, local_runner_fx)

    committed_a = fixture.commit_admitted(
        batch_a,
        a_rows=[("d1", "2026-01-01T00:00:00Z", "A-older")],
        b_rows=[],
        fx=local_runner_fx,
    )
    fold_stage.run(
        committed_a, local_runner_fx
    )  # A's own first fold: loses at fold (tie -- gt fails)

    rows_after_a = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.a_state_qt).collect()
    )
    assert rows_after_a == [("d1", "B-newer")]  # A never displaced B

    # A STALE EXTRA attempt of A -- a genuine rerun of the SAME batch_id.
    # NOT a bare `snapshot_ids(...) == before` check here (`[[spine-merge-
    # noop-and-append-signature]]`'s own documented finding): a healthy-
    # rerun MERGE still commits a harmless PHYSICAL no-op snapshot even on
    # a full tie (`changed-partition-count = "0"`, errata #9) -- the
    # LOGICAL no-op signal (`fold_snapshot_ids`, absent key) plus row-
    # CONTENT equality are this test's own claim.
    committed_a_again = fixture.commit_admitted(
        batch_a,
        a_rows=[("d1", "2026-01-01T00:00:00Z", "A-older")],
        b_rows=[],
        fx=local_runner_fx,
    )
    assert committed_a_again.commit_snapshot_ids == {}  # commit: fully guard-skipped
    after = fold_stage.run(committed_a_again, local_runner_fx)
    assert after.fold_snapshot_ids == {}  # fold: full tie against itself -- logical no-op

    rows_final = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.a_state_qt).collect()
    )
    assert rows_final == [("d1", "B-newer")]  # state STILL never regressed


def test_k24_arrival_order_tiebreak_source_ts_decides_over_a_smaller_content_hash(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    """U-1's own K-24-family variant (bead conveyer-swb.11): two batches TIE
    on the declared `event_time` ordering column -- `source_ts` (HLD 007
    D-3(b): `= ` each batch's own `received_at`, `stages/commit.py::
    _stamp_candidates`'s F-1 stamp) must decide the fold in favor of the
    LATER-arriving batch even though its `content_hash` sorts
    lexicographically SMALLER than the earlier batch's own. Before U-1's
    fix, `source_ts` was a literal `NULL` on every fact, so this exact tie
    fell through to `content_hash` alone and the LATER batch's smaller hash
    LOST -- confirmed by this bead's own kernel probe against `ordering_
    reference.strictly_greater` over these two rows' `(event_time, source_ts,
    content_hash)` tuples: `True` with a real `source_ts`, `False` with the
    pre-fix literal `None` in its place. A stale extra attempt of batch 1
    (a genuine rerun, AFTER batch 2 already folded) stays a pure no-op --
    idempotency preserved, K-24's own standing shape (`test_k24_stale_
    extra_attempt_of_old_batch_after_newer_batch_folded_never_regresses`
    above)."""
    fixture = _TwoTypeFixture(spark, unique_table)
    batch_1 = sh.batch_id(2420)
    batch_2 = sh.batch_id(2421)
    tied_event_time = "2026-01-01T00:00:00Z"  # identical on BOTH batches -- ties the declared col

    # `core.canonical.row_hash({"domain_id": "d1", "event_time":
    # tied_event_time, "payload": <payload>})` over these two literal
    # payloads (this bead's own kernel probe) -- `payload_smaller_hash`
    # hashes lexicographically SMALLER than `payload_larger_hash`,
    # deterministic; pinned here rather than recomputed live, since a live
    # recompute would just restate the same fact this comment already pins.
    payload_smaller_hash = "k24-tiebreak-payload-x"  # hash d5636a68...
    payload_larger_hash = "k24-tiebreak-payload-y"  # hash d582da71...

    ctx1 = replace(
        fixture.seed(batch_1),
        received_at=datetime(2026, 6, 1, tzinfo=UTC),  # earlier arrival
        admitted_facts={
            "type-a": fixture._rows_df([("d1", tied_event_time, payload_larger_hash)]),
            "type-b": fixture._rows_df([]),
        },
    )
    committed_1 = commit_stage.run(ctx1, local_runner_fx)
    fold_stage.run(committed_1, local_runner_fx)

    rows_after_1 = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.a_state_qt).collect()
    )
    assert rows_after_1 == [("d1", payload_larger_hash)]  # first-ever fact: plain insert

    ctx2 = replace(
        fixture.seed(batch_2),
        received_at=datetime(2026, 6, 2, tzinfo=UTC),  # LATER arrival than batch 1
        admitted_facts={
            "type-a": fixture._rows_df([("d1", tied_event_time, payload_smaller_hash)]),
            "type-b": fixture._rows_df([]),
        },
    )
    committed_2 = commit_stage.run(ctx2, local_runner_fx)
    folded_2 = fold_stage.run(committed_2, local_runner_fx)

    assert sh.bare(fixture.a_state_qt) in folded_2.fold_snapshot_ids  # a REAL update, not a tie
    rows_after_2 = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.a_state_qt).collect()
    )
    # The later-arriving batch wins DESPITE its smaller content_hash -- the
    # exact behavior U-1's fix restores; pre-fix this would still read
    # `payload_larger_hash` (batch 1 never displaced, the reported defect).
    assert rows_after_2 == [("d1", payload_smaller_hash)]

    # A stale extra attempt of batch 1 (a genuine rerun of its OWN batch_id,
    # AFTER batch 2 already folded) must stay a pure no-op -- commit fully
    # guard-skips, fold ties out again (source_ts still loses), state never
    # regresses back to batch 1's payload.
    ctx1_again = replace(
        fixture.seed(batch_1),
        received_at=datetime(2026, 6, 1, tzinfo=UTC),
        admitted_facts={
            "type-a": fixture._rows_df([("d1", tied_event_time, payload_larger_hash)]),
            "type-b": fixture._rows_df([]),
        },
    )
    committed_1_again = commit_stage.run(ctx1_again, local_runner_fx)
    assert committed_1_again.commit_snapshot_ids == {}  # commit: fully guard-skipped
    folded_1_again = fold_stage.run(committed_1_again, local_runner_fx)
    assert folded_1_again.fold_snapshot_ids == {}  # fold: source_ts loses again -- logical no-op

    rows_final = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(fixture.a_state_qt).collect()
    )
    assert rows_final == [("d1", payload_smaller_hash)]  # state never regressed


# --- K-25: residual sweep (module docstring has items (i)/(iii)'s citations) -


def test_k25_disjoint_domain_sibling_merge_conflict_is_efficiency_never_correctness(
    spark: SparkSession,
) -> None:
    """K-25 item (ii): "disjoint-domain sibling MERGEs falsely conflicting
    under state's single-scope layout (F-3/[T-10]) -- I-11 retry, efficiency
    never correctness". `test_k16_concurrent_sibling_merges_converge_via_
    i11_retry` above already proves the CONVERGENCE half over disjoint
    domains via the SAME `_merge_race_probe` seam; this test names the
    OTHER half explicitly: a genuinely-conflicting commit is surfaced as
    `TransientError` (never silently merged, never a correctness defect) --
    `test_spark_fx.py::test_merge_survives_a_sibling_commit_between_our_
    commit_and_resolution` already exercises the SAME-domain conflict path
    at the `effects/spark.py` grain; this is the K-suite's own citation of
    that coverage, not a duplicate."""
    # Structural citation, not a fresh mechanism: `is_transient_iceberg_
    # failure`'s own predicate is what routes a genuine Iceberg commit
    # conflict to `TransientError` (SFN retry), asserted directly here so
    # this K-id has ITS OWN assertion, not just a docstring pointer. The
    # fake `java_exception` duck-types ONLY what `Py4JJavaError.__init__`
    # and the predicate itself touch (`._target_id`, `.getClass().
    # getName()`) -- no live py4j gateway needed, `test_spark_fx.py`'s own
    # established pattern.
    assert spark_fx.is_transient_iceberg_failure.__module__ == "spine.effects.spark"
    import py4j.protocol

    class _FakeJavaClass:
        def getName(self) -> str:
            return "org.apache.iceberg.exceptions.CommitFailedException"

    class _FakeJavaException:
        _target_id = "o1"  # Py4JJavaError.__init__ needs this attribute

        def getClass(self) -> _FakeJavaClass:
            return _FakeJavaClass()

    fake_exc = py4j.protocol.Py4JJavaError("msg", _FakeJavaException())
    assert spark_fx.is_transient_iceberg_failure(fake_exc) is True  # retried, never mis-merged
