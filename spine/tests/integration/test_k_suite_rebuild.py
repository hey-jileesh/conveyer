"""The K-suite's own standing scenarios for the rebuild swap — LLD 007.1 §9
(F-7, the conditional lineage-preserving swap), §10 (the three OQ-7 harness
cases), §11/§13.2 (the rebuild kill-matrix rows, K-26/K-27). Bead
`conveyer-6pg.23`, B11-local.

**Placement (§10, verbatim, restated from `test_k_suite_fold.py`): "framework
property tests ... never per-pipeline: the fold contract is framework law
(D-3)."** Every test below drives `spine.effects.rebuild` directly — the ONE
blessed rebuild/swap module ([DC2-2]) — over hand-built fact/state table
pairs, exactly like the sibling K-suite fold file's own K-22..K-25 rows.

**K-17/K-18/K-19 (the three OQ-7 cases, §10) — each reproduces its own
probe-verified claim (`spine/tests/probes/probe_oq7_swap.py`, bead
`conveyer-hpp.13.10`, Sheet A) through the REAL production path** —
`core.merge.merge_spec`/`ordering_predicate`, `frames.fold.
reduce_batch_winners`, `effects.spark.build_merge`/`render_merge`, and
`effects.rebuild.attempt_state_swap`/`swap_with_retry` — never the probe's
own simplified hand-rolled SQL. The probe *establishes* the engine
behavior once; this file *defends* it from then on (§10/§13.3's own rule).

**K-17 (mid-rebuild fold).** Rebuild pins `before_id` and recomputes its
rebuilt frame; a live fold lands BEFORE the swap executes, moving state
past `before_id`; the swap must refuse (`SwapOutcome.committed is False`)
with the live fold's own rows intact; a fresh `swap_with_retry` call
(re-pin -> recompute -> retry) converges.

**K-18 (the straddle).** A fold `MERGE` whose scan fixes its starting
snapshot BEFORE the swap lands, but whose own commit executes AFTER —
reproduced via the SAME `time.sleep`-delayed-UDF technique the probe's own
A2 case uses (a genuine single-JVM interleaving, not a mock of one; see
the probe's own module docstring for the caveat this construction
carries). `effects.spark.build_merge` already maps the resulting Iceberg
conflict to `TransientError` (I-11's existing channel, `is_transient_
iceberg_failure`) — no new mapping needed on the MERGE side (§9.2's own
words: "no per-statement option needed on the MERGE side"). A subsequent
ordinary fold of the SAME (now-committed) facts converges.

**K-19 (tie-idempotency).** The swap already reflects `fold(all facts
incl. B)`; B's OWN live fold lands AFTER the swap over the IDENTICAL
ordering-key values — a full tie. Asserted via `changed-partition-count ==
"0"` on B's own re-MERGE snapshot (errata #9's signal — never a bare
snapshot-id diff: Iceberg's MERGE always physically snapshots, even a
no-op) AND via `MergeResult.snapshot_id is None` (I-19's own no-op
contract at the `effects/spark.py` grain).

**K-26 (rebuild killed before its swap).** Pins are reads; the computed
frame is ephemeral — nothing on the state table changes if the process
dies before `attempt_state_swap`/`swap_with_retry` is ever called.

**K-27 (rebuild killed between swap and `rebuild-completed` emission).**
§9.3's ruling, restated at test grain: state is already correct after a
successful swap; this module emits NO event (the interim, §16's 008 row —
`rebuild.py`'s own module docstring has the full account). "Completed by
rerunning the run mode" is asserted directly: a second `swap_with_retry`
call over the SAME facts converges to the SAME content (idempotent by
content, §9.5), even though it commits its own new physical snapshot
(a `DataFrameWriterV2.overwrite()` REPLACE, unlike MERGE, always writes a
new snapshot — never a "no-op" claim at this grain, no claim of one is
made).
"""

from __future__ import annotations

import threading
import time
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from spine.bootstrap.create_record_tables import bootstrap_fact_table, bootstrap_state_table
from spine.core import merge as core_merge
from spine.core.model import FactColumnSpec, FactSchemaModel, FactTypeModel
from spine.effects import rebuild as rebuild_fx
from spine.effects import spark as spark_fx
from spine.effects.records import TransientError
from spine.frames.fold import reduce_batch_winners

if TYPE_CHECKING:
    from spine.core.merge import MergeSpec
    from spine.core.run_facts import RunFact

_SCHEMA = FactSchemaModel(
    columns=[
        FactColumnSpec(name="domain_id", type="string"),
        FactColumnSpec(name="event_time", type="timestamp"),
        FactColumnSpec(name="payload", type="string"),
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
    ordering=["event_time"],
)
_DF_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("delivery_id", StringType(), False),
        StructField("feed_id", StringType(), False),
        StructField("received_at", TimestampType(), False),
        StructField("event_time", TimestampType(), True),
        StructField("source_ts", TimestampType(), True),
        StructField("content_hash", StringType(), False),
        StructField("record_key", StringType(), False),
        StructField("domain_id", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)


def _h(idx: int) -> str:
    # A stand-in 64-hex `content_hash` -- unique per `idx`, never a real
    # digest (this file's own facts are entirely synthetic).
    return f"{idx:064x}"


def _row(
    batch_id: str, domain_id: str, event_time: datetime, content_hash: str, payload: str
) -> Row:
    return Row(
        batch_id=batch_id,
        delivery_id="d",
        feed_id="f",
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        event_time=event_time,
        source_ts=None,
        content_hash=content_hash,
        record_key=domain_id,
        domain_id=domain_id,
        payload=payload,
    )


def _make_tables(spark: SparkSession, prefix: str) -> tuple[str, str, MergeSpec]:
    """`(fact_qt, state_qt, spec)` -- fresh, collision-free per call, mirroring
    `test_k_suite_fold.py::_bootstrap_state`'s own local convention."""
    fact_qt = f"spine_cat.spine_test_tables.{prefix}fact_{uuid.uuid4().hex[:8]}"
    state_qt = f"spine_cat.spine_test_tables.{prefix}state_{uuid.uuid4().hex[:8]}"
    bootstrap_fact_table(spark, fact_qt, _SCHEMA)
    bootstrap_state_table(spark, state_qt, _SCHEMA)
    fact_bare = fact_qt.removeprefix("spine_cat.")
    state_bare = state_qt.removeprefix("spine_cat.")
    spec = core_merge.merge_spec(
        FactTypeModel(fact_table=fact_bare, state_table=state_bare, schema=_SCHEMA)
    )
    return fact_qt, state_qt, spec


def _append_facts(spark: SparkSession, fact_qt: str, rows: list[Row]) -> None:
    spark.createDataFrame(rows, schema=_DF_SCHEMA).writeTo(fact_qt).option(
        "check-nullability", "false"
    ).append()


# --- pure sanity: the "both options, always, together" rendering -----------


def test_swap_write_options_always_carries_both_options_together() -> None:
    """§9.2's two forbidden look-alikes, defended structurally: every value
    `swap_write_options` returns carries BOTH `validate-from-snapshot-id`
    AND `isolation-level=serializable` -- never one without the other."""
    options = rebuild_fx.swap_write_options(before_id=12345)
    assert options == {
        "validate-from-snapshot-id": "12345",
        "isolation-level": "serializable",
    }


# --- K-17: mid-rebuild fold --------------------------------------------------


def test_k17_mid_rebuild_fold_swap_refuses_then_repin_recompute_converges(
    spark: SparkSession,
) -> None:
    fact_qt, state_qt, spec = _make_tables(spark, "k17")
    merge_fn = spark_fx.build_merge(spark)
    ledger_rows: list[RunFact] = []

    _append_facts(spark, fact_qt, [_row("b1", "d1", datetime(2026, 1, 1, tzinfo=UTC), _h(0), "p0")])
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "b1"), spec)
    )

    _append_facts(spark, fact_qt, [_row("b2", "d2", datetime(2026, 1, 1, tzinfo=UTC), _h(1), "p1")])

    def recompute():
        return reduce_batch_winners(spark.table(fact_qt), spec)

    # Rebuild's own pin + recompute (attempt 1's inputs), captured manually
    # so a live fold can be injected strictly BETWEEN pin and swap.
    before_id_1 = spark_fx.current_snapshot_id(spark, state_qt)
    assert before_id_1 is not None
    rebuilt_df_1 = recompute()

    # A live fold lands mid-rebuild.
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "b2"), spec)
    )

    outcome = rebuild_fx.attempt_state_swap(spark, spec.target_table, rebuilt_df_1, before_id_1)
    assert outcome.committed is False, "K17: stale swap must be refused"
    rows_after_refusal = sorted(r["domain_id"] for r in spark.table(state_qt).collect())
    assert rows_after_refusal == ["d1", "d2"], "K17: the live fold's own rows must be intact"

    result = rebuild_fx.swap_with_retry(
        spark, "pipelines/k17-probe", spec.target_table, recompute, ledger_rows.append
    )
    rows_final = sorted(r["domain_id"] for r in spark.table(state_qt).collect())
    assert rows_final == ["d1", "d2"]
    assert result.attempts == 1  # the fresh pin already reflects the live fold
    assert len(ledger_rows) == 1
    assert ledger_rows[0].outcome == "ok"
    assert ledger_rows[0].stage == "rebuild"


# --- K-18: the straddle ------------------------------------------------------


def test_k18_straddling_merge_conflicts_and_retry_converges(spark: SparkSession) -> None:
    fact_qt, state_qt, spec = _make_tables(spark, "k18")
    merge_fn = spark_fx.build_merge(spark)
    ledger_rows: list[RunFact] = []

    _append_facts(spark, fact_qt, [_row("b1", "d1", datetime(2026, 1, 1, tzinfo=UTC), _h(0), "p0")])
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "b1"), spec)
    )

    def _delay_and_return(value: str) -> str:
        time.sleep(2)
        return value

    delay_udf_name = f"_k18_delay_{uuid.uuid4().hex[:8]}"
    spark.udf.register(delay_udf_name, _delay_and_return, StringType())
    merge_result: dict[str, str] = {}

    def run_straddling_merge() -> None:
        try:
            _append_facts(
                spark, fact_qt, [_row("b2", "d1", datetime(2026, 1, 2, tzinfo=UTC), _h(1), "p1")]
            )
            delayed = (
                spark.table(fact_qt)
                .where(F.col("batch_id") == "b2")
                .withColumn("content_hash", F.expr(f"{delay_udf_name}(content_hash)"))
            )
            merge_fn(spec, delayed)
            merge_result["outcome"] = "COMMITTED"
        except Exception as exc:  # noqa: BLE001 -- captured for assertion, not swallowed
            merge_result["outcome"] = "RAISED"
            merge_result["exc_type"] = type(exc).__name__

    thread = threading.Thread(target=run_straddling_merge)
    thread.start()
    time.sleep(0.7)  # let the MERGE's scan fix its starting snapshot before the swap lands

    def recompute():
        return reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "b1"), spec)

    result = rebuild_fx.swap_with_retry(
        spark, "pipelines/k18-probe", spec.target_table, recompute, ledger_rows.append
    )
    thread.join(timeout=15)

    assert merge_result["outcome"] == "RAISED", (
        f"K18: expected the straddling MERGE to raise, got {merge_result}"
    )
    assert merge_result["exc_type"] == "TransientError", (
        "K18: effects.spark.build_merge already maps the Iceberg conflict -- I-11's "
        "existing channel, no new mapping needed on the MERGE side"
    )

    # The SFN-style retry: fold the now-committed straddling batch's facts
    # against the rebuilt state -- must converge.
    before_retry = spark_fx.current_snapshot_id(spark, state_qt)
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "b2"), spec)
    )
    after_retry = spark_fx.current_snapshot_id(spark, state_qt)
    assert after_retry != before_retry
    row = spark.table(state_qt).where("domain_id = 'd1'").collect()[0]
    assert row["payload"] == "p1", "K18: retry must converge, applying the straddling batch"
    assert result.attempts == 1


# --- K-19: tie-idempotency ----------------------------------------------------


def test_k19_batch_committed_before_pin_folded_after_swap_is_a_logical_noop(
    spark: SparkSession,
) -> None:
    fact_qt, state_qt, spec = _make_tables(spark, "k19")
    merge_fn = spark_fx.build_merge(spark)
    ledger_rows: list[RunFact] = []

    # Seed: an earlier batch, folded already, so the state table carries
    # real snapshot lineage before rebuild ever runs (§9.2 presumes an
    # existing state table with history -- rebuild is a swap IN the
    # lineage, never a first write).
    _append_facts(
        spark, fact_qt, [_row("bSeed", "d1", datetime(2026, 1, 1, tzinfo=UTC), _h(0), "vSeed")]
    )
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "bSeed"), spec)
    )

    # B's facts committed BEFORE the rebuild's pin -- B's own fold has NOT
    # yet run.
    _append_facts(spark, fact_qt, [_row("bB", "d4", datetime(2026, 2, 1, tzinfo=UTC), _h(9), "vB")])

    def recompute():
        # rebuild = fold(all facts) -- already includes B's own contribution.
        return reduce_batch_winners(spark.table(fact_qt), spec)

    result = rebuild_fx.swap_with_retry(
        spark, "pipelines/k19-probe", spec.target_table, recompute, ledger_rows.append
    )
    before_id = spark_fx.current_snapshot_id(spark, state_qt)

    # B's OWN live fold runs AFTER the swap, over the IDENTICAL facts -- a
    # full ordering tie against what the swap already wrote.
    b_winners = reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "bB"), spec)
    merge_result = merge_fn(spec, b_winners)
    after_id = spark_fx.current_snapshot_id(spark, state_qt)

    assert after_id != before_id, "errata #9: MERGE always physically snapshots, even a no-op"
    summary = None
    for row in spark.sql(f"SELECT snapshot_id, summary FROM {state_qt}.snapshots").collect():
        if int(row["snapshot_id"]) == after_id:
            summary = dict(row["summary"])
    assert summary is not None
    assert summary.get("changed-partition-count") == "0", (
        "K19: the reliable no-op signal -- never a bare snapshot-id diff"
    )
    assert merge_result.snapshot_id is None, (
        "K19: I-19's own no-op contract at the MergeResult grain"
    )

    row = spark.table(state_qt).where("domain_id = 'd4'").collect()[0]
    assert row["payload"] == "vB", "K19: tie must never update -- D-2/D-4 discipline"
    assert result.state_snapshot_id is not None


# --- A007-1: rebuild_state_table's own pin-order guarantee ------------------


def test_pin_order_before_id_precedes_fact_snapshot_pin_by_construction(
    spark: SparkSession,
) -> None:
    """A007-1's own by-construction claim, proven directly against the
    PRODUCTION closure (`effects.rebuild.rebuild_state_table`), not the
    manual `attempt_state_swap`/hand-rolled `recompute` calls K-17/K-18/
    K-19 exercise: `before_id` (state) is captured strictly BEFORE the fact
    table's own snapshot pin (`rebuild_state_table`'s closure, §9.2's load-
    bearing order) -- verified here by recording every call's own `qt`
    argument, in order, off the ONE shared public helper (`effects/spark.
    py::current_snapshot_id`, bead conveyer-swb.25's M1 fix -- re-pointed
    here from the pre-fix private `rebuild_fx._current_state_snapshot_id`)
    `rebuild_state_table`'s own M4 genesis pre-check, `swap_with_retry`'s
    `before_id` capture, and the fact pin all go through, so the claim is
    checked at the actual call-order grain, not inferred from outcomes
    alone.

    Several calls reach `current_snapshot_id` for THIS table's own qt/
    fact_qt before attempt 1's swap even fires: `rebuild_state_table`'s own
    M4 genesis pre-check (state qt -- a no-op read here, this table is
    already seeded via `bSeed` above, never `None`); `swap_with_retry`'s
    own `before_id` capture (state qt again -- THIS is the SECOND state-qt
    call, the one the live-batch injection below targets, not the first);
    the injected live batch's own nested `merge_fn` housekeeping reads
    (state qt, twice more -- an injection-mechanism artifact, not itself
    part of the claim under test); and finally the fact snapshot pin (fact
    qt, inside `recompute`). The assertion below is robust to exactly how
    many state-qt reads precede the fact pin (a private implementation
    detail of the injection technique, not load-bearing) while still
    proving the actual claim: EVERY state-qt read precedes the fact qt's
    first appearance, never the reverse.

    A live batch (fact append + fold into state) is injected to land in the
    WINDOW strictly between those two captures. Because the fact pin
    happens strictly AFTER the append (this is what "before_id first"
    buys), that live batch's own fact is naturally INCLUDED in THIS
    attempt's own fresh recompute -- never silently dropped -- but the
    swap still REFUSES this attempt, because the live fold moved STATE
    past the already-captured `before_id` (over-refusal, the permitted
    direction, §9.2's own account). `rebuild_state_table`'s internal re-
    pin/recompute/retry then converges on attempt 2 to the SAME content a
    plain fold-all of every committed fact would produce -- proving the
    refuse -> re-pin -> recompute -> retry loop holds through the
    production closure, not just through the hand-built OQ-7 goldens."""
    fact_qt, state_qt, spec = _make_tables(spark, "pinorder")
    fact_type = FactTypeModel(
        fact_table=fact_qt.removeprefix("spine_cat."),
        state_table=state_qt.removeprefix("spine_cat."),
        schema=_SCHEMA,
    )
    merge_fn = spark_fx.build_merge(spark)

    _append_facts(
        spark, fact_qt, [_row("bSeed", "d1", datetime(2026, 1, 1, tzinfo=UTC), _h(0), "vSeed")]
    )
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "bSeed"), spec)
    )

    call_order: list[str] = []
    state_qt_calls = {"n": 0}
    original = spark_fx.current_snapshot_id

    def instrumented(spark_: SparkSession, qt: str) -> int | None:
        call_order.append(qt)
        if qt == state_qt:
            state_qt_calls["n"] += 1
            if state_qt_calls["n"] == 2:
                # The SECOND state-qt call is `swap_with_retry`'s own
                # `before_id` capture -- the FIRST is `rebuild_state_table`'s
                # own M4 genesis pre-check (a no-op read here; this table is
                # already seeded, never `None`). Capture the STALE `before_
                # id` first, THEN land the live batch -- state's real
                # current snapshot moves past this returned value only
                # AFTER this call returns, exactly matching a live fold
                # racing in the window between `before_id`'s capture and
                # `swap_with_retry`'s own next step (`recompute()`).
                value = original(spark_, qt)
                _append_facts(
                    spark_,
                    fact_qt,
                    [_row("bLive", "d2", datetime(2026, 1, 1, tzinfo=UTC), _h(1), "vLive")],
                )
                merge_fn(
                    spec,
                    reduce_batch_winners(
                        spark_.table(fact_qt).where(F.col("batch_id") == "bLive"), spec
                    ),
                )
                return value
        return original(spark_, qt)

    spark_fx.current_snapshot_id = instrumented
    try:
        ledger_rows: list[RunFact] = []
        rebuild_fx.rebuild_state_table(
            spark, "pipelines/pin-order-probe", fact_type, record_run=ledger_rows.append
        )
    finally:
        spark_fx.current_snapshot_id = original

    # The pin-order claim, at the actual call-order grain: EVERY state-table
    # read (`rebuild_state_table`'s own M4 genesis pre-check, `swap_with_
    # retry`'s own `before_id` capture, and the injected live-batch's own
    # nested `merge_fn` housekeeping reads -- all of them state-qt) precedes
    # the fact table's own FIRST snapshot pin -- proven by the fact qt's
    # first appearance in `call_order`, never earlier. This is robust to
    # exactly how many state-qt reads the injection's own nested merge
    # issues internally (not itself part of the claim under test) while
    # still proving the load-bearing order: state pin(s) strictly before
    # the fact pin, never the reverse.
    fact_qt_index = call_order.index(fact_qt)
    assert fact_qt_index > 0, "at least one state-qt read (before_id) must precede the fact pin"
    assert all(qt == state_qt for qt in call_order[:fact_qt_index]), (
        "every call before the fact snapshot pin's first appearance must be a state-qt read"
    )

    assert len(ledger_rows) == 2
    assert ledger_rows[0].outcome == "failed"
    assert ledger_rows[0].error_type == "org.apache.iceberg.exceptions.ValidationException"
    assert ledger_rows[1].outcome == "ok"

    rows_final = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    expected = sorted(
        (r["domain_id"], r["payload"])
        for r in reduce_batch_winners(spark.table(fact_qt), spec).collect()
    )
    assert rows_final == expected == [("d1", "vSeed"), ("d2", "vLive")], (
        "the live batch's own fact was naturally INCLUDED in attempt 1's own fresh pin "
        "(never excluded) -- retry converges to the same content a plain fold-all would"
    )


def test_rebuild_pipeline_loops_declared_fact_types_in_order(spark: SparkSession) -> None:
    """A007-1's per-pipeline orchestration: one swap per declared fact
    type's own state table, keyed by state table, one `RunFact` per
    attempt threaded through the SAME `record_run` for every type -- IN
    DECLARED (insertion) order (F-4's own per-table iteration convention),
    proven by the test's own NAME claim (M8, bead conveyer-swb.25): the
    original assertion here only checked SET equality of `feed_id`s, which
    is blind to order entirely -- a `rebuild_pipeline` that iterated `b`
    before `a` would have passed it just the same."""
    fact_qt_a, state_qt_a, spec_a = _make_tables(spark, "rpA")
    fact_qt_b, state_qt_b, spec_b = _make_tables(spark, "rpB")
    fact_type_a = FactTypeModel(
        fact_table=fact_qt_a.removeprefix("spine_cat."),
        state_table=state_qt_a.removeprefix("spine_cat."),
        schema=_SCHEMA,
    )
    fact_type_b = FactTypeModel(
        fact_table=fact_qt_b.removeprefix("spine_cat."),
        state_table=state_qt_b.removeprefix("spine_cat."),
        schema=_SCHEMA,
    )
    merge_fn = spark_fx.build_merge(spark)

    a_rows = [_row("bA", "d1", datetime(2026, 1, 1, tzinfo=UTC), _h(0), "pA")]
    _append_facts(spark, fact_qt_a, a_rows)
    a_winners = reduce_batch_winners(
        spark.table(fact_qt_a).where(F.col("batch_id") == "bA"), spec_a
    )
    merge_fn(spec_a, a_winners)

    b_rows = [_row("bB", "d2", datetime(2026, 1, 1, tzinfo=UTC), _h(1), "pB")]
    _append_facts(spark, fact_qt_b, b_rows)
    b_winners = reduce_batch_winners(
        spark.table(fact_qt_b).where(F.col("batch_id") == "bB"), spec_b
    )
    merge_fn(spec_b, b_winners)

    class _FakeSpec:
        def __init__(self, pipeline: str, fact_types: dict[str, FactTypeModel]) -> None:
            self.pipeline = pipeline
            self.fact_types = fact_types

    fake_spec = _FakeSpec("pipelines/rp-probe", {"a": fact_type_a, "b": fact_type_b})
    ledger_rows: list[RunFact] = []
    results = rebuild_fx.rebuild_pipeline(
        spark,
        fake_spec,  # type: ignore[arg-type]
        record_run=ledger_rows.append,
    )

    expected_state_tables = {fact_type_a.state_table, fact_type_b.state_table}
    assert set(results) == expected_state_tables
    assert all(result.attempts == 1 for result in results.values())
    assert len(ledger_rows) == 2
    assert {row.pipeline for row in ledger_rows} == {"pipelines/rp-probe"}
    # M8: ORDER, not just set membership -- `spec.fact_types`' own declared
    # (insertion) order is "a" then "b"; a `rebuild_pipeline` that visited
    # them in the opposite order would fail this, unlike the prior set-only
    # assertion it replaces.
    assert [row.feed_id for row in ledger_rows] == [
        fact_type_a.state_table,
        fact_type_b.state_table,
    ]
    assert sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt_a).collect()) == [
        ("d1", "pA")
    ]
    assert sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt_b).collect()) == [
        ("d2", "pB")
    ]


# --- M4: genesis-seed a never-folded state table -----------------------------


def test_rebuild_state_table_genesis_seeds_a_never_folded_state_table_and_converges(
    spark: SparkSession,
) -> None:
    """M4 (bead conveyer-swb.25, critique gate wf_78ea4599-a5b): a
    bootstrapped-but-never-folded state table (`_make_tables`'s own
    `bootstrap_state_table` -- DDL creation only, zero snapshots, zero rows,
    NO manual genesis-seed step anywhere in this test) converges through
    `rebuild_state_table` directly, in ONE attempt -- replacing the prior
    behavior of an unconditional `TransientError` (a retry-class error
    wrongly applied to what is a PERMANENT condition until the first fold
    or rebuild runs)."""
    fact_qt, state_qt, spec = _make_tables(spark, "m4genesis")
    fact_type = FactTypeModel(
        fact_table=fact_qt.removeprefix("spine_cat."),
        state_table=state_qt.removeprefix("spine_cat."),
        schema=_SCHEMA,
    )
    assert spark_fx.current_snapshot_id(spark, state_qt) is None, (
        "the state table must be genuinely never-folded (zero snapshots) for this test to mean "
        "anything -- bootstrap DDL creation alone must not itself supply one"
    )

    _append_facts(spark, fact_qt, [_row("b1", "d1", datetime(2026, 1, 1, tzinfo=UTC), _h(0), "p1")])

    ledger_rows: list[RunFact] = []
    result = rebuild_fx.rebuild_state_table(
        spark, "pipelines/m4-genesis-probe", fact_type, record_run=ledger_rows.append
    )

    assert result.attempts == 1, (
        "genesis-seeding must not itself count as a refused/retried attempt"
    )
    assert len(ledger_rows) == 1, (
        "genesis-seeding must not emit its own ledger row -- only the swap"
    )
    assert ledger_rows[0].outcome == "ok"
    assert ledger_rows[0].stage == "rebuild"
    rows_final = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert rows_final == [("d1", "p1")]


# --- A007-9: refused-swap ledger row carries the OBSERVED wrapped class -----


class _FakeJavaClass:
    def __init__(self, name: str) -> None:
        self._name = name

    def getName(self) -> str:
        return self._name


class _FakeJavaException:
    def __init__(self, name: str) -> None:
        self._klass = _FakeJavaClass(name)

    def getClass(self) -> _FakeJavaClass:
        return self._klass


class _FakePy4JJavaError(Exception):
    def __init__(self, java_exception: _FakeJavaException) -> None:
        self.java_exception = java_exception


class _RefusingWriter:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def option(self, key: str, value: str) -> _RefusingWriter:
        return self

    def overwrite(self, cond: object) -> None:
        raise self._exc


class _RefusingDataFrame:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def writeTo(self, qt: str) -> _RefusingWriter:
        return _RefusingWriter(self._exc)


def test_a007_9_refusal_records_the_observed_wrapped_java_class_not_a_hardcoded_literal() -> None:
    """A007-9: `SwapOutcome.error_type` (and the ledger row `_rebuild_
    attempt_fact` derives from it) carries the ACTUAL wrapped Java
    exception's class name -- `CommitStateUnknownException` here, never a
    single hardcoded `"ValidationException"` literal borrowed from probe
    A1's own one observed refusal class. Proven directly against
    `attempt_state_swap`'s exception handling with a FAKE `Py4JJavaError`
    (`Py4JJavaError.__init__` only touches `._target_id` -- fakeable
    without a live gateway, `effects/spark.py`'s own module docstring) and
    a duck-typed refusing `DataFrame`/`DataFrameWriterV2` stand-in -- no
    live Spark commit/refusal needed."""
    exc = _FakePy4JJavaError(
        _FakeJavaException("org.apache.iceberg.exceptions.CommitStateUnknownException")
    )
    rebuilt_df = _RefusingDataFrame(exc)

    outcome = rebuild_fx.attempt_state_swap(
        None,  # type: ignore[arg-type]  -- never touched on the refusal path
        "spine_test_tables.fake_state",
        rebuilt_df,  # type: ignore[arg-type]
        before_id=1,
    )

    assert outcome.committed is False
    assert outcome.state_snapshot_id is None
    assert outcome.error_type == "org.apache.iceberg.exceptions.CommitStateUnknownException"


# --- K-26: rebuild killed before its swap ------------------------------------


def test_k26_rebuild_killed_before_its_swap_leaves_state_untouched(spark: SparkSession) -> None:
    """Pins are reads; the computed frame is ephemeral -- disposability
    (D-5's promise), no cleanup duty beyond ordinary temp hygiene."""
    fact_qt, state_qt, spec = _make_tables(spark, "k26")
    merge_fn = spark_fx.build_merge(spark)

    _append_facts(
        spark, fact_qt, [_row("bSeed", "d1", datetime(2026, 1, 1, tzinfo=UTC), _h(0), "vSeed")]
    )
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "bSeed"), spec)
    )

    before_id = spark_fx.current_snapshot_id(spark, state_qt)
    rows_before = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())

    # Rebuild "starts": pin `before_id` (a read), recompute the rebuilt
    # frame (ephemeral, never written anywhere) -- then is KILLED before
    # ever calling `attempt_state_swap`/`swap_with_retry`.
    def recompute():
        return reduce_batch_winners(spark.table(fact_qt), spec)

    _ = spark_fx.current_snapshot_id(spark, state_qt)  # the pin, a read only
    _ = recompute()  # the ephemeral computed frame -- never written
    # (simulated kill here -- no swap call follows)

    after_id = spark_fx.current_snapshot_id(spark, state_qt)
    rows_after = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert after_id == before_id, "K26: state snapshot must be untouched"
    assert rows_after == rows_before, "K26: state rows must be untouched"


# --- K-27: rebuild killed between swap and rebuild-completed emission -------


def test_k27_killed_between_swap_and_announcement_completed_by_rerunning(
    spark: SparkSession,
) -> None:
    """State is already correct after a successful swap; only the (not-yet-
    landed, §9.3/§16) announcement would be stale. This module emits no
    event (`rebuild.py`'s own module docstring) -- "completed by rerunning
    the run mode" is asserted directly: a second `swap_with_retry` call
    over the SAME facts converges to the SAME content."""
    fact_qt, state_qt, spec = _make_tables(spark, "k27")
    merge_fn = spark_fx.build_merge(spark)
    ledger_rows: list[RunFact] = []

    _append_facts(
        spark, fact_qt, [_row("bSeed", "d1", datetime(2026, 1, 1, tzinfo=UTC), _h(0), "vSeed")]
    )
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "bSeed"), spec)
    )
    _append_facts(spark, fact_qt, [_row("bB", "d2", datetime(2026, 2, 1, tzinfo=UTC), _h(9), "vB")])

    def recompute():
        return reduce_batch_winners(spark.table(fact_qt), spec)

    result1 = rebuild_fx.swap_with_retry(
        spark, "pipelines/k27-probe", spec.target_table, recompute, ledger_rows.append
    )
    rows_after_swap1 = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect()
    )
    # simulated kill here -- no event class exists yet, nothing to emit or
    # fail to emit; "completed by rerunning" = rerun the SAME run mode.
    result2 = rebuild_fx.swap_with_retry(
        spark, "pipelines/k27-probe", spec.target_table, recompute, ledger_rows.append
    )
    rows_after_swap2 = sorted(
        (r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect()
    )

    assert rows_after_swap1 == rows_after_swap2 == [("d1", "vSeed"), ("d2", "vB")]
    assert result1.state_snapshot_id != result2.state_snapshot_id, (
        "K27: a DataFrameWriterV2 REPLACE always commits its own new snapshot, unlike a "
        "MERGE no-op -- no 'unchanged snapshot' claim is made at this grain"
    )
    assert len(ledger_rows) == 2
    assert all(row.outcome == "ok" for row in ledger_rows)


# --- RB-2: budget exhaustion never forces a write ---------------------------


def test_budget_exhaustion_raises_transient_error_never_forces_the_write(
    spark: SparkSession, capsys: pytest.CaptureFixture[str]
) -> None:
    """RB-2's "no --force path" holds by construction: a perpetually-refused
    swap raises `TransientError` (D-1's ordinary job-failure/SFN-retry
    channel) after `max_attempts`, never issuing an unconditional write.

    A007-7: also asserts the `RebuildSwapRetries` EMF -- one line per
    refused attempt (3, `max_attempts=3`), each carrying `pipeline` and the
    (correctly-named, U-4) `state_table` dimension, and NO row value
    ([S-7]/[S-18]: the live folds' own domain ids/payloads never leak into
    the metric payload)."""
    pipeline = "pipelines/kbudget-probe"
    fact_qt, state_qt, spec = _make_tables(spark, "kbudget")
    merge_fn = spark_fx.build_merge(spark)
    ledger_rows: list[RunFact] = []

    _append_facts(
        spark, fact_qt, [_row("bSeed", "d1", datetime(2026, 1, 1, tzinfo=UTC), _h(0), "vSeed")]
    )
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "bSeed"), spec)
    )

    calls = {"n": 0}

    def recompute():
        # Every call lands a fresh live fold AFTER `before_id` is captured
        # but BEFORE the swap executes -- forces refusal every attempt.
        calls["n"] += 1
        n = calls["n"]
        _append_facts(
            spark,
            fact_qt,
            [
                _row(
                    f"live-b{n}", f"live{n}", datetime(2026, 1, 1, tzinfo=UTC), _h(100 + n), "vLive"
                )
            ],
        )
        merge_fn(
            spec,
            reduce_batch_winners(
                spark.table(fact_qt).where(F.col("batch_id") == f"live-b{n}"), spec
            ),
        )
        return reduce_batch_winners(spark.table(fact_qt), spec)

    capsys.readouterr()  # drain setup's own EMF lines before asserting the retry loop's own
    with pytest.raises(TransientError):
        rebuild_fx.swap_with_retry(
            spark,
            pipeline,
            spec.target_table,
            recompute,
            ledger_rows.append,
            max_attempts=3,
        )
    assert len(ledger_rows) == 3
    assert all(row.outcome == "failed" for row in ledger_rows)
    rows_final = sorted(r["domain_id"] for r in spark.table(state_qt).collect())
    assert "live1" in rows_final and "live2" in rows_final and "live3" in rows_final, (
        "RB-2: every live fold's own rows must remain in state -- the swap never forced a write"
    )

    emitted = capsys.readouterr().out
    retry_lines = [line for line in emitted.splitlines() if '"Name": "RebuildSwapRetries"' in line]
    assert len(retry_lines) == 3, "A007-7: one RebuildSwapRetries EMF line per refused attempt"
    for line in retry_lines:
        assert f'"pipeline": "{pipeline}"' in line
        assert f'"state_table": "{spec.target_table}"' in line
    for row_value in ("vSeed", "vLive", "live1", "live2", "live3"):
        assert row_value not in emitted, "[S-7]/[S-18]: the metric payload must carry no row value"
