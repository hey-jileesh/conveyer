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
reduce_batch_winners`, `effects.spark._build_merge`/`render_merge`, and
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
carries). `effects.spark._build_merge` already maps the resulting Iceberg
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
    merge_fn = spark_fx._build_merge(spark)
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
    before_id_1 = rebuild_fx._current_state_snapshot_id(spark, state_qt)
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
    merge_fn = spark_fx._build_merge(spark)
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
        "K18: effects.spark._build_merge already maps the Iceberg conflict -- I-11's "
        "existing channel, no new mapping needed on the MERGE side"
    )

    # The SFN-style retry: fold the now-committed straddling batch's facts
    # against the rebuilt state -- must converge.
    before_retry = rebuild_fx._current_state_snapshot_id(spark, state_qt)
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "b2"), spec)
    )
    after_retry = rebuild_fx._current_state_snapshot_id(spark, state_qt)
    assert after_retry != before_retry
    row = spark.table(state_qt).where("domain_id = 'd1'").collect()[0]
    assert row["payload"] == "p1", "K18: retry must converge, applying the straddling batch"
    assert result.attempts == 1


# --- K-19: tie-idempotency ----------------------------------------------------


def test_k19_batch_committed_before_pin_folded_after_swap_is_a_logical_noop(
    spark: SparkSession,
) -> None:
    fact_qt, state_qt, spec = _make_tables(spark, "k19")
    merge_fn = spark_fx._build_merge(spark)
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
    before_id = rebuild_fx._current_state_snapshot_id(spark, state_qt)

    # B's OWN live fold runs AFTER the swap, over the IDENTICAL facts -- a
    # full ordering tie against what the swap already wrote.
    b_winners = reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "bB"), spec)
    merge_result = merge_fn(spec, b_winners)
    after_id = rebuild_fx._current_state_snapshot_id(spark, state_qt)

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


# --- K-26: rebuild killed before its swap ------------------------------------


def test_k26_rebuild_killed_before_its_swap_leaves_state_untouched(spark: SparkSession) -> None:
    """Pins are reads; the computed frame is ephemeral -- disposability
    (D-5's promise), no cleanup duty beyond ordinary temp hygiene."""
    fact_qt, state_qt, spec = _make_tables(spark, "k26")
    merge_fn = spark_fx._build_merge(spark)

    _append_facts(
        spark, fact_qt, [_row("bSeed", "d1", datetime(2026, 1, 1, tzinfo=UTC), _h(0), "vSeed")]
    )
    merge_fn(
        spec, reduce_batch_winners(spark.table(fact_qt).where(F.col("batch_id") == "bSeed"), spec)
    )

    before_id = rebuild_fx._current_state_snapshot_id(spark, state_qt)
    rows_before = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())

    # Rebuild "starts": pin `before_id` (a read), recompute the rebuilt
    # frame (ephemeral, never written anywhere) -- then is KILLED before
    # ever calling `attempt_state_swap`/`swap_with_retry`.
    def recompute():
        return reduce_batch_winners(spark.table(fact_qt), spec)

    _ = rebuild_fx._current_state_snapshot_id(spark, state_qt)  # the pin, a read only
    _ = recompute()  # the ephemeral computed frame -- never written
    # (simulated kill here -- no swap call follows)

    after_id = rebuild_fx._current_state_snapshot_id(spark, state_qt)
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
    merge_fn = spark_fx._build_merge(spark)
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
    spark: SparkSession,
) -> None:
    """RB-2's "no --force path" holds by construction: a perpetually-refused
    swap raises `TransientError` (D-1's ordinary job-failure/SFN-retry
    channel) after `max_attempts`, never issuing an unconditional write."""
    fact_qt, state_qt, spec = _make_tables(spark, "kbudget")
    merge_fn = spark_fx._build_merge(spark)
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

    with pytest.raises(TransientError):
        rebuild_fx.swap_with_retry(
            spark,
            "pipelines/kbudget-probe",
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
