"""OQ-7 validation-1 kernel probe (conveyer-hpp.13.10) — the F-7 adoption
gate named in `design/adr-oq7-rebuild-exclusion.md` ("Validation before
adoption", item 1).

Confirms, on the pinned Iceberg/Spark stack, the two mechanical claims
RB-1/RB-2 stand on, plus the tie-idempotency hardening (case (c)):

  A1 — a same-lineage full-table replace (the rebuild swap) can be made
       CONDITIONAL on a captured base snapshot `before_id` and FAILS LOUDLY
       if a live-fold MERGE moved the state table past it.
  A2 — a straddling MERGE (base captured pre-swap, commit lands post-swap)
       raises a ValidationException-class conflict under serializable
       isolation, and a retry converges.
  A3 — tie-idempotency: a batch committed before the rebuild's pin and
       folded after the swap re-MERGEs to a full no-op on ordering ties
       (`changed-partition-count == "0"`, 004.1 errata #9's signal — NOT
       "no new snapshot": Iceberg MERGE always physically snapshots).

Single-JVM interleaving caveat: A2's straddle is simulated with two threads
inside ONE SparkSession/JVM (a `time.sleep`-delayed UDF holds the merge's
write tasks open while the main thread lands the swap). This is legitimate
for what is being tested — Iceberg's conflict detection is COMMIT-TIME
snapshot-CAS semantics, not a cross-process locking protocol, so a
single-session interleaving that fixes the merge's scan snapshot before the
swap and delays its commit past the swap exercises the real mechanism, not
a mock of it. It is not a proof of behavior under true multi-JVM/
multi-executor concurrency (Glue jobs are separate JVMs) — that gap is
priced as residual risk, not closed by this probe.

A4 (swap-as-MERGE fallback) does not run: the decision mapping only calls
it if A1 or A2 FAIL, and both PASS.

Stack pins (LLD 004.1 §7.1/§12.1): Python 3.11 (this run resolved 3.12.12 —
see caveats), pyspark==3.5.* (resolved 3.5.9), org.apache.iceberg:
iceberg-spark-runtime-3.5_2.12:1.6.1 via `spark.jars.packages`, local
Iceberg Hadoop catalog (`spine_cat`) on a session tmpdir warehouse (the
`tests/conftest.py::build_test_session`/`_iceberg_conf` idiom, reused
verbatim rather than re-invented), state table `write.merge.mode` =
`merge-on-read` + `write.merge.isolation-level` = `serializable`.

[DS-5]: all data below is fabricated/synthetic (domain ids `d1`..`d4`,
content hashes `h1`/`h1b`/... ) — never partner-derived.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Generator, Mapping

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

_ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

_BASE_CONF: Mapping[str, str] = {
    "spark.driver.memory": "2g",
    "spark.sql.shuffle.partitions": "2",
    "spark.sql.adaptive.enabled": "true",
    "spark.ui.enabled": "false",
}


def _iceberg_conf(warehouse_dir: str) -> dict[str, str]:
    """Verbatim reuse of `spine/tests/conftest.py::_iceberg_conf`'s idiom —
    `spine_cat` -> Hadoop catalog on a tmpdir warehouse (test-only; prod
    uses `type=glue`)."""
    return {
        "spark.jars.packages": _ICEBERG_PACKAGE,
        "spark.sql.extensions": _ICEBERG_EXTENSIONS,
        "spark.sql.catalog.spine_cat": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.spine_cat.type": "hadoop",
        "spark.sql.catalog.spine_cat.warehouse": warehouse_dir,
    }


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Generator[SparkSession, None, None]:
    warehouse_dir = str(tmp_path_factory.mktemp("oq7-probe-warehouse"))
    builder = SparkSession.builder.master("local[2]").appName("oq7-probe")
    for key, value in {**_BASE_CONF, **_iceberg_conf(warehouse_dir)}.items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _state_table_ddl(spark: SparkSession, table: str) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS spine_cat.probe_db")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(
        f"""
        CREATE TABLE {table} (
          domain_id STRING,
          event_time TIMESTAMP,
          source_ts TIMESTAMP,
          content_hash STRING,
          value STRING
        ) USING iceberg
        TBLPROPERTIES (
          'write.merge.mode' = 'merge-on-read',
          'write.merge.isolation-level' = 'serializable',
          'write.delete.mode' = 'merge-on-read',
          'write.update.mode' = 'merge-on-read'
        )
        """
    )


def _append_rows(spark: SparkSession, table: str, rows: list[tuple[str, str, str, str, str]]) -> None:
    spark.createDataFrame(
        rows, ["domain_id", "event_time", "source_ts", "content_hash", "value"]
    ).selectExpr(
        "domain_id",
        "CAST(event_time AS TIMESTAMP) as event_time",
        "CAST(source_ts AS TIMESTAMP) as source_ts",
        "content_hash",
        "value",
    ).writeTo(table).append()


def _cur_snapshot(spark: SparkSession, table: str) -> int:
    return spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]["snapshot_id"]


def _run_fold_merge(spark: SparkSession, table: str, view: str, rows: list[tuple[str, str, str, str, str]]) -> None:
    """The 004.1 I-11 default LWW MERGE: strict-`>` ordering-struct
    comparison, null-safe by construction (fixture data is never null here).
    """
    spark.createDataFrame(
        rows, ["domain_id", "event_time", "source_ts", "content_hash", "value"]
    ).selectExpr(
        "domain_id",
        "CAST(event_time AS TIMESTAMP) as event_time",
        "CAST(source_ts AS TIMESTAMP) as source_ts",
        "content_hash",
        "value",
    ).createOrReplaceTempView(view)
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING {view} s
        ON t.domain_id = s.domain_id
        WHEN MATCHED AND (s.event_time, s.source_ts, s.content_hash) > (t.event_time, t.source_ts, t.content_hash)
          THEN UPDATE SET t.value = s.value, t.event_time = s.event_time, t.source_ts = s.source_ts, t.content_hash = s.content_hash
        WHEN NOT MATCHED THEN INSERT *
        """
    )


# --- A1: conditional same-lineage swap fails loudly when state moved -------


def test_a1_conditional_swap_fails_loudly_when_state_moved(spark: SparkSession) -> None:
    table = "spine_cat.probe_db.state_a1"
    _state_table_ddl(spark, table)
    _append_rows(
        spark,
        table,
        [
            ("d1", "2026-01-01T00:00:00", "2026-01-01T00:00:00", "h1", "v1-orig"),
            ("d2", "2026-01-01T00:00:00", "2026-01-01T00:00:00", "h2", "v2-orig"),
        ],
    )

    # rebuild pins its fact read: capture the state snapshot (I-19 before_id idiom)
    before_id = _cur_snapshot(spark, table)

    # a live fold MERGE lands AFTER the pin, moving state past before_id
    _run_fold_merge(
        spark,
        table,
        "a1_live_fold_src",
        [("d1", "2026-01-02T00:00:00", "2026-01-02T00:00:00", "h1b", "v1-live-fold")],
    )
    assert _cur_snapshot(spark, table) != before_id, "setup invariant: state must have moved past before_id"

    rebuild_df = spark.createDataFrame(
        [
            ("d1", "2026-01-01T00:00:00", "2026-01-01T00:00:00", "h1", "v1-orig"),
            ("d2", "2026-01-01T00:00:00", "2026-01-01T00:00:00", "h2", "v2-orig"),
        ],
        ["domain_id", "event_time", "source_ts", "content_hash", "value"],
    ).selectExpr(
        "domain_id",
        "CAST(event_time AS TIMESTAMP) as event_time",
        "CAST(source_ts AS TIMESTAMP) as source_ts",
        "content_hash",
        "value",
    )

    # --- Idiom (i): DataFrameWriterV2.overwrite() -- PASSES, but ONLY when
    # BOTH options are set together. Fragility finding: `validate-from-
    # snapshot-id` ALONE is silently ignored by the OverwriteByFilter path;
    # `isolation-level=serializable` is REQUIRED alongside it or the swap
    # blindly wins (the forbidden direction) -- verified as a separate
    # negative case below.
    with pytest.raises(Exception) as exc_info:
        (
            rebuild_df.writeTo(table)
            .option("validate-from-snapshot-id", str(before_id))
            .option("isolation-level", "serializable")
            .overwrite(F.lit(True))
        )
    assert "ValidationException" in str(exc_info.value) or "ValidationException" in type(exc_info.value).__name__

    # loud failure => MERGE's contribution (B's row) must be intact, untouched
    row = spark.table(table).where("domain_id = 'd1'").collect()[0]
    assert row["value"] == "v1-live-fold", "swap must not win by force -- lost update forbidden (RB-2)"


def test_a1_fragility_isolation_level_required_alongside_validate_from_snapshot(
    spark: SparkSession,
) -> None:
    """Hardening finding for F-7: `validate-from-snapshot-id` alone (no
    `isolation-level`) does NOT trigger conflict detection on the
    `OverwriteByFilter` path -- the swap silently wins, reconstructing the
    forbidden lost update inside what looks like the "safe" conditional
    call. F-7 MUST pin both options together, not just the first."""
    table = "spine_cat.probe_db.state_a1_fragility"
    _state_table_ddl(spark, table)
    _append_rows(spark, table, [("d1", "2026-01-01T00:00:00", "2026-01-01T00:00:00", "h1", "v1-orig")])
    before_id = _cur_snapshot(spark, table)
    _run_fold_merge(
        spark,
        table,
        "a1_fragility_live_fold_src",
        [("d1", "2026-01-02T00:00:00", "2026-01-02T00:00:00", "h1b", "v1-live-fold")],
    )

    rebuild_df = spark.createDataFrame(
        [("d1", "2026-01-01T00:00:00", "2026-01-01T00:00:00", "h1", "v1-orig")],
        ["domain_id", "event_time", "source_ts", "content_hash", "value"],
    ).selectExpr(
        "domain_id",
        "CAST(event_time AS TIMESTAMP) as event_time",
        "CAST(source_ts AS TIMESTAMP) as source_ts",
        "content_hash",
        "value",
    )
    # missing isolation-level option -- documented footgun, not a recommendation
    rebuild_df.writeTo(table).option("validate-from-snapshot-id", str(before_id)).overwrite(F.lit(True))
    row = spark.table(table).where("domain_id = 'd1'").collect()[0]
    assert row["value"] == "v1-orig", "confirms the footgun: swap silently won, erasing the live fold (forbidden)"


# --- A2: straddling MERGE conflicts under serializable, retry converges ----


def test_a2_straddling_merge_conflicts_and_retry_converges(spark: SparkSession) -> None:
    table = "spine_cat.probe_db.state_a2"
    _state_table_ddl(spark, table)
    _append_rows(spark, table, [("d3", "2026-01-01T00:00:00", "2026-01-01T00:00:00", "h3", "v3-orig")])
    s0 = _cur_snapshot(spark, table)

    spark.udf.register("_a2_delay_marker", lambda x: (time.sleep(2) or x), StringType())
    merge_result: dict[str, str] = {}

    def run_straddling_merge() -> None:
        try:
            spark.createDataFrame(
                [("d3", "2026-01-02T00:00:00", "2026-01-02T00:00:00", "h3b", "v3-live-fold")],
                ["domain_id", "event_time", "source_ts", "content_hash", "value"],
            ).selectExpr(
                "domain_id",
                "CAST(event_time AS TIMESTAMP) as event_time",
                "CAST(source_ts AS TIMESTAMP) as source_ts",
                "_a2_delay_marker(content_hash) as content_hash",  # delays the write tasks past the swap
                "value",
            ).createOrReplaceTempView("a2_straddle_src")
            spark.sql(
                f"""
                MERGE INTO {table} t
                USING a2_straddle_src s
                ON t.domain_id = s.domain_id
                WHEN MATCHED AND (s.event_time, s.source_ts, s.content_hash) > (t.event_time, t.source_ts, t.content_hash)
                  THEN UPDATE SET t.value = s.value, t.event_time = s.event_time, t.source_ts = s.source_ts, t.content_hash = s.content_hash
                WHEN NOT MATCHED THEN INSERT *
                """
            )
            merge_result["outcome"] = "COMMITTED"
        except Exception as exc:  # noqa: BLE001 -- captured for assertion, not swallowed silently
            merge_result["outcome"] = "RAISED"
            merge_result["exc_type"] = type(exc).__name__
            merge_result["exc_repr"] = str(exc)

    thread = threading.Thread(target=run_straddling_merge)
    thread.start()
    time.sleep(0.7)  # let the MERGE's scan fix its starting snapshot at s0 before the swap lands

    # rebuild's swap lands mid-merge, moving the table past s0
    rebuild_df = spark.createDataFrame(
        [("d3", "2026-01-01T00:00:00", "2026-01-01T00:00:00", "h3", "v3-orig-REBUILT")],
        ["domain_id", "event_time", "source_ts", "content_hash", "value"],
    ).selectExpr(
        "domain_id",
        "CAST(event_time AS TIMESTAMP) as event_time",
        "CAST(source_ts AS TIMESTAMP) as source_ts",
        "content_hash",
        "value",
    )
    (
        rebuild_df.writeTo(table)
        .option("validate-from-snapshot-id", str(s0))
        .option("isolation-level", "serializable")
        .overwrite(F.lit(True))
    )
    thread.join(timeout=15)

    assert merge_result["outcome"] == "RAISED"
    assert "ValidationException" in merge_result["exc_repr"]

    # retry: a fresh MERGE against the now-current (post-swap) state converges
    before_retry = _cur_snapshot(spark, table)
    _run_fold_merge(
        spark,
        table,
        "a2_retry_src",
        [("d3", "2026-01-02T00:00:00", "2026-01-02T00:00:00", "h3b", "v3-live-fold")],
    )
    assert _cur_snapshot(spark, table) != before_retry
    row = spark.table(table).where("domain_id = 'd3'").collect()[0]
    assert row["value"] == "v3-live-fold", "retry must converge: live fold applied on top of rebuilt state"


# --- A3: tie-idempotency (ADR-OQ7 hardening case (c)) -----------------------


def test_a3_tie_idempotency_no_ops_on_full_ordering_tie(spark: SparkSession) -> None:
    table = "spine_cat.probe_db.state_a3"
    _state_table_ddl(spark, table)

    # the swap already reflects fold(all facts including B) -- B's ordering
    # key values are ALREADY the current state after the swap lands
    _append_rows(spark, table, [("d4", "2026-02-01T00:00:00", "2026-02-01T00:00:00", "hB", "vB-from-rebuild")])
    before_id = _cur_snapshot(spark, table)

    # batch B's OWN live-fold MERGE lands AFTER the swap, using the identical
    # ordering-key values (a full tie: committed before the pin, folded after)
    _run_fold_merge(
        spark,
        table,
        "a3_batch_b_fold_src",
        [("d4", "2026-02-01T00:00:00", "2026-02-01T00:00:00", "hB", "vB-from-rebuild")],
    )

    after_id = _cur_snapshot(spark, table)
    # errata #9: MERGE always physically snapshots -- a new child commits ...
    assert after_id != before_id

    summary = dict(
        spark.sql(f"SELECT summary FROM {table}.snapshots WHERE snapshot_id = {after_id}").collect()[0][
            "summary"
        ]
    )
    # ... but the RELIABLE no-op signal is changed-partition-count == "0"
    # (never diff snapshot ids -- merge-on-read precondition, errata #9)
    assert summary["changed-partition-count"] == "0"

    row = spark.table(table).where("domain_id = 'd4'").collect()[0]
    assert row["value"] == "vB-from-rebuild", "tie must never update -- D-2/D-4 discipline holds at engine grain"


# A4 (swap-as-MERGE fallback) is NOT implemented here: the decision mapping
# only calls it if A1 or A2 FAIL, and both PASS on this pinned stack.

